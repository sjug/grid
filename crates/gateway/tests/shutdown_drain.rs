//! A gateway told to stop finishes the streams in flight that fit its shutdown budget.

#[path = "support/ports.rs"]
mod ports;

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    reason = "tests; the backend paces its chunks with thread::sleep"
)]
mod tests {
    use std::{
        io::{self, BufReader, Write as _},
        net::{SocketAddr, TcpStream},
        path::PathBuf,
        process::{Child, Command, Stdio},
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    use super::ports::{bound_listener, free_port};

    /// The shutdown budget the gateway runs with, as the operator renders it.
    const BUDGET_SECS: u64 = 30;

    /// Time between the chunks the backend streams.
    const CADENCE: Duration = Duration::from_millis(250);

    /// The last event the backend sends, so a reader can tell a finished stream from a cut one.
    const DONE: &[u8] = b"data: [DONE]\n\n";

    /// Bound on waiting for the gateway to listen and for a stream to end.
    const TIMEOUT: Duration = Duration::from_secs(60);

    #[test]
    #[ignore = "reproduces a stopping gateway cutting a stream that fits its shutdown budget: Praxis caps \
                the grace before runtime shutdown at 5 s, and the rest of shutdown_timeout_secs bounds only \
                the exit"]
    fn a_stream_that_fits_the_shutdown_budget_finishes() {
        let outcome = stream_through_shutdown(Duration::from_secs(12));
        assert!(
            outcome.finished(),
            "a stream with 12 s left at SIGTERM, inside the {BUDGET_SECS} s budget, ended {:?} after SIGTERM: \
             {:?}",
            outcome.ended,
            outcome.ending
        );
    }

    #[test]
    fn a_stream_ending_soon_after_shutdown_begins_finishes() {
        let outcome = stream_through_shutdown(Duration::from_secs(2));
        assert!(
            outcome.finished(),
            "a stream with 2 s left at SIGTERM ended {:?} after it: {:?}, so the scenario cannot see a stream \
             finish",
            outcome.ended,
            outcome.ending
        );
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// How a stream through a stopping gateway ended.
    #[derive(Debug)]
    struct Outcome {
        /// How long after SIGTERM the client read the stream's end.
        ended: Duration,
        /// How the response body ended.
        ending: Ending,
    }

    impl Outcome {
        /// Whether the client read the whole body, the backend's last event included.
        fn finished(&self) -> bool {
            matches!(&self.ending, Ending::Complete { done: true })
        }
    }

    /// How a chunked response body ended.
    #[derive(Debug)]
    enum Ending {
        /// The terminating chunk arrived.
        Complete {
            /// Whether the decoded body held [`DONE`].
            done: bool,
        },
        /// The connection ended or failed before the terminating chunk, with the error that ended it.
        Cut(#[expect(dead_code, reason = "read through Debug in failure messages")] io::ErrorKind),
    }

    /// The gateway process, killed if a test leaves it running.
    struct Gateway(Child);

    impl Drop for Gateway {
        fn drop(&mut self) {
            let _killed = self.0.kill();
            let _reaped = self.0.wait();
        }
    }

    /// Stream through a gateway with a [`BUDGET_SECS`] shutdown budget and send it SIGTERM once the first
    /// event arrives. The backend streams the rest only once SIGTERM is sent, ending `left` later.
    fn stream_through_shutdown(left: Duration) -> Outcome {
        let (terminated_tx, terminated_rx) = mpsc::channel();
        let backend = stream_backend(left, terminated_rx);
        let listen = free_port();
        let config = write_config(listen, backend);
        let gateway = Gateway(
            Command::new(env!("CARGO_BIN_EXE_grid-gateway"))
                .arg("--config")
                .arg(&config)
                .env_remove("GRID_SERVING_CONFIG")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("the gateway starts"),
        );
        let mut client = connect(listen);
        client
            .write_all(b"GET /v1/stream HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n\r\n")
            .expect("request sent");
        client.set_read_timeout(Some(TIMEOUT)).expect("timeout set");
        let mut reader = BufReader::new(client);
        let head = read_head(&mut reader).expect("the response head arrives");
        assert!(head.starts_with("HTTP/1.1 200"), "the stream is answered: {head}");
        assert!(
            head.to_ascii_lowercase().contains("transfer-encoding: chunked"),
            "the stream is chunked: {head}"
        );
        let mut body = next_chunk(&mut reader)
            .expect("the first chunk arrives")
            .expect("the first chunk carries data");
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(gateway.0.id().to_string())
            .status()
            .expect("kill runs");
        assert!(status.success(), "SIGTERM sent");
        let terminated = Instant::now();
        terminated_tx.send(()).expect("the backend waits for SIGTERM");
        let ending = loop {
            match next_chunk(&mut reader) {
                Ok(Some(data)) => body.extend_from_slice(&data),
                Ok(None) => {
                    break Ending::Complete {
                        done: body.windows(DONE.len()).any(|window| window == DONE),
                    };
                },
                Err(error) => break Ending::Cut(error.kind()),
            }
        };
        let ended = terminated.elapsed();
        drop(gateway);
        let _removed = std::fs::remove_file(&config);
        Outcome { ended, ending }
    }

    /// One line from `reader`, its line ending included.
    fn read_line(reader: &mut impl io::BufRead) -> io::Result<String> {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok(line)
    }

    /// The response's status line and headers, through the blank line that ends them.
    fn read_head(reader: &mut impl io::BufRead) -> io::Result<String> {
        let mut head = String::new();
        loop {
            let line = read_line(reader)?;
            if line == "\r\n" {
                return Ok(head);
            }
            head.push_str(&line);
        }
    }

    /// The next chunk's data, or `None` at the terminating chunk once its trailer section is read.
    fn next_chunk(reader: &mut impl io::BufRead) -> io::Result<Option<Vec<u8>>> {
        let line = read_line(reader)?;
        let size = line.trim_end().split(';').next().unwrap_or_default();
        let size =
            usize::from_str_radix(size, 16).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if size == 0 {
            while read_line(reader)? != "\r\n" {}
            return Ok(None);
        }
        let mut data = vec![0; size];
        reader.read_exact(&mut data)?;
        let mut crlf = [0; 2];
        reader.read_exact(&mut crlf)?;
        Ok(Some(data))
    }

    /// A backend answering one request with a chunked event stream: one event at once, then, once
    /// `terminated` says SIGTERM was sent, an event every [`CADENCE`] and [`DONE`] `left` later.
    fn stream_backend(left: Duration, terminated: mpsc::Receiver<()>) -> SocketAddr {
        let listener = bound_listener();
        let address = listener.local_addr().expect("bound");
        thread::spawn(move || {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            let mut reader = BufReader::new(&stream);
            while read_line(&mut reader).is_ok_and(|line| line != "\r\n") {}
            let mut stream = &stream;
            let chunk = |data: &[u8]| [format!("{:x}\r\n", data.len()).as_bytes(), data, b"\r\n"].concat();
            let first = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n")
                .and_then(|()| stream.write_all(&chunk(b"data: 0\n\n")));
            if first.is_err() || terminated.recv().is_err() {
                return;
            }
            let deadline = Instant::now().checked_add(left).expect("a near deadline");
            let mut index = 1_u32;
            while Instant::now() < deadline {
                thread::sleep(CADENCE);
                if stream
                    .write_all(&chunk(format!("data: {index}\n\n").as_bytes()))
                    .is_err()
                {
                    return;
                }
                index = index.saturating_add(1);
            }
            drop(stream.write_all(&[chunk(DONE).as_slice(), b"0\r\n\r\n"].concat()));
        });
        address
    }

    /// A gateway config on `listen` sending every request to `backend`, with a [`BUDGET_SECS`] shutdown budget.
    fn write_config(listen: u16, backend: SocketAddr) -> PathBuf {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("shutdown-drain-{listen}.yaml"));
        let yaml = format!(
            "shutdown_timeout_secs: {BUDGET_SECS}\ninsecure_options:\n  allow_private_endpoints: true\nlisteners:\n  \
             - name: default\n    address: \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nfilter_chains:\n  - \
             name: main\n    filters:\n      - filter: router\n        routes:\n          - path_prefix: \"/\"\n            \
             cluster: backend\n      - filter: load_balancer\n        clusters:\n          - name: backend\n            \
             endpoints: [\"{backend}\"]\n"
        );
        std::fs::write(&path, yaml).expect("config written");
        path
    }

    /// A connection to the gateway on `listen`, once it accepts one.
    fn connect(listen: u16) -> TcpStream {
        let deadline = Instant::now().checked_add(TIMEOUT).expect("a near deadline");
        loop {
            match TcpStream::connect(("127.0.0.1", listen)) {
                Ok(stream) => return stream,
                Err(error) if Instant::now() > deadline => panic!("the gateway never listened: {error}"),
                Err(_) => thread::sleep(Duration::from_millis(50)),
            }
        }
    }
}
