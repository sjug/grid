//! How much memory and how many descriptors a gateway keeps after concurrency peaks of rising size,
//! and after light traffic that follows them.

#[path = "support/ports.rs"]
mod ports;

#[cfg(test)]
#[cfg(target_os = "linux")]
#[expect(
    clippy::expect_used,
    clippy::print_stdout,
    clippy::disallowed_methods,
    reason = "a measurement that prints its table; the backend paces its events with thread::sleep"
)]
mod tests {
    use std::{
        io::{self, BufReader, Write as _},
        net::{SocketAddr, TcpStream},
        path::PathBuf,
        process::{Child, Command, Stdio},
        sync::{
            Arc, Barrier, Condvar, Mutex, PoisonError,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use super::ports::{bound_listener, free_port};

    // ---------------------------------------------------------------------------
    // Constants
    // ---------------------------------------------------------------------------

    /// Concurrent streams at each peak, in the order the process sees them: rising, then a light one
    /// to show whether traffic after the largest peak returns memory.
    const PEAKS: [usize; 4] = [64, 256, 1_024, 8];

    /// Events each stream carries: one at once, the rest one every [`CADENCE`] once the peak is read.
    const EVENTS: usize = 12;

    /// Time between a stream's events after the first.
    const CADENCE: Duration = Duration::from_millis(250);

    /// Bytes in each event, so each stream moves some body through the gateway.
    const EVENT_BYTES: usize = 1_024;

    /// Idle time after a peak before memory is read again.
    const IDLE: Duration = Duration::from_secs(15);

    /// Bound on connecting, on any one read, and on waiting for the gateway to listen.
    const TIMEOUT: Duration = Duration::from_secs(60);

    #[test]
    #[ignore = "a measurement, not a check: run with --ignored --nocapture"]
    fn memory_and_descriptors_after_rising_concurrency_peaks() {
        let gate = Arc::new(Gate::default());
        let backend = stream_backend(Arc::clone(&gate));
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
        let deadline = Instant::now().checked_add(TIMEOUT).expect("a near deadline");
        while connect(listen).is_err() {
            assert!(Instant::now() < deadline, "the gateway never listened");
            thread::sleep(Duration::from_millis(50));
        }
        let pid = gateway.0.id();
        thread::sleep(Duration::from_secs(1));
        println!(
            "streams of {EVENTS} events of {EVENT_BYTES} B, held open until each peak is read; idle {IDLE:?} after \
             each peak; memory in MiB"
        );
        println!("peak streams  finished  resident at peak  resident idle  high water  descriptors at peak / idle");
        println!(
            "{:>12}  {:>8}  {:>16}  {:>13.1}  {:>10}  {:>27}",
            0,
            "-",
            "-",
            resident_mib(pid),
            "-",
            descriptors(pid)
        );
        for peak in PEAKS {
            let seen = run_peak(listen, peak, pid, &gate);
            assert_eq!(
                seen.ready, peak,
                "every stream of the {peak} was open when the peak was read"
            );
            thread::sleep(IDLE);
            println!(
                "{peak:>12}  {:>8}  {:>16.1}  {:>13.1}  {:>10.1}  {:>27}",
                seen.finished,
                seen.resident_at_peak,
                resident_mib(pid),
                high_water_mib(pid),
                format!("{} / {}", seen.open_at_peak, descriptors(pid)),
            );
            assert_eq!(seen.finished, peak, "every stream of the {peak} arrived whole");
        }
        drop(gateway);
        let _removed = std::fs::remove_file(&config);
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// The gateway process, killed when the measurement ends.
    struct Gateway(Child);

    impl Drop for Gateway {
        fn drop(&mut self) {
            let _killed = self.0.kill();
            let _reaped = self.0.wait();
        }
    }

    /// Holds every stream after its first event until the peak it belongs to has been read.
    #[derive(Default)]
    struct Gate {
        /// The peak streams opened now belong to.
        current: AtomicU64,
        /// The last peak released, and the condition its streams wait on.
        released: (Mutex<u64>, Condvar),
    }

    impl Gate {
        /// The peak a stream opening now belongs to.
        fn current(&self) -> u64 {
            self.current.load(Ordering::SeqCst)
        }

        /// Start the next peak, returning its number.
        fn open_next(&self) -> u64 {
            self.current.fetch_add(1, Ordering::SeqCst).saturating_add(1)
        }

        /// Let every stream of `peak` finish.
        fn release(&self, peak: u64) {
            let (released, changed) = &self.released;
            *released.lock().unwrap_or_else(PoisonError::into_inner) = peak;
            changed.notify_all();
        }

        /// Wait until `peak` is released.
        fn wait(&self, peak: u64) {
            let (released, changed) = &self.released;
            let mut last = released.lock().unwrap_or_else(PoisonError::into_inner);
            while *last < peak {
                last = changed.wait(last).unwrap_or_else(PoisonError::into_inner);
            }
            drop(last);
        }
    }

    /// What one peak saw.
    struct Seen {
        /// Streams whose whole body arrived intact.
        finished: usize,
        /// Open descriptors with every stream held at its first event.
        open_at_peak: usize,
        /// Streams that reached their first event before the peak was read.
        ready: usize,
        /// Resident memory in MiB with every stream held at its first event.
        resident_at_peak: f64,
    }

    /// Open `peak` streams through the gateway on `listen` and hold them all at their first event
    /// while process `pid`'s resident memory and open descriptors are read, then let them finish.
    fn run_peak(listen: u16, peak: usize, pid: u32, gate: &Gate) -> Seen {
        let number = gate.open_next();
        let opened = Arc::new(Barrier::new(peak.saturating_add(1)));
        let ready = Arc::new(AtomicUsize::new(0));
        let streams: Vec<_> = std::iter::repeat_with(|| {
            let (opened, ready) = (Arc::clone(&opened), Arc::clone(&ready));
            thread::spawn(move || {
                let first = connect(listen).and_then(|stream| {
                    let mut reader = BufReader::new(stream);
                    reader
                        .get_mut()
                        .write_all(b"GET /v1/stream HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n\r\n")?;
                    let head = read_head(&mut reader)?;
                    let first =
                        next_chunk(&mut reader)?.ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
                    Ok((reader, head, first))
                });
                if first.is_ok() {
                    ready.fetch_add(1, Ordering::SeqCst);
                }
                opened.wait();
                let Ok((mut reader, head, mut body)) = first else {
                    return false;
                };
                let complete = loop {
                    match next_chunk(&mut reader) {
                        Ok(Some(chunk)) => body.extend_from_slice(&chunk),
                        Ok(None) => break true,
                        Err(_) => break false,
                    }
                };
                complete && head.starts_with("HTTP/1.1 200") && is_chunked(&head) && body == expected_body()
            })
        })
        .take(peak)
        .collect();
        opened.wait();
        let (resident_at_peak, open_at_peak) = (resident_mib(pid), descriptors(pid));
        gate.release(number);
        let finished = streams
            .into_iter()
            .filter_map(|stream| stream.join().ok())
            .filter(|finished| *finished)
            .count();
        Seen {
            finished,
            open_at_peak,
            ready: ready.load(Ordering::SeqCst),
            resident_at_peak,
        }
    }

    /// One event the backend sends.
    fn event() -> String {
        format!("data: {}\n\n", "x".repeat(EVENT_BYTES))
    }

    /// The decoded body of a whole stream: [`EVENTS`] events.
    fn expected_body() -> Vec<u8> {
        event().repeat(EVENTS).into_bytes()
    }

    /// Whether a response head declares a chunked body.
    fn is_chunked(head: &str) -> bool {
        head.lines().any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.eq_ignore_ascii_case("transfer-encoding") && value.trim().eq_ignore_ascii_case("chunked")
            })
        })
    }

    /// One line from `reader`, its line ending included.
    fn read_line(reader: &mut impl io::BufRead) -> io::Result<String> {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok(line)
    }

    /// The status line and headers of a message, through the blank line that ends them.
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
        if &crlf != b"\r\n" {
            return Err(io::ErrorKind::InvalidData.into());
        }
        Ok(Some(data))
    }

    /// A backend answering every request with a chunked stream of [`EVENTS`] events: the first at once,
    /// the rest only once `gate` releases the peak the request arrived in.
    fn stream_backend(gate: Arc<Gate>) -> SocketAddr {
        let listener = bound_listener();
        let address = listener.local_addr().expect("bound");
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let gate = Arc::clone(&gate);
                let peak = gate.current();
                thread::spawn(move || {
                    let mut reader = BufReader::new(&stream);
                    while read_line(&mut reader).is_ok_and(|line| line != "\r\n") {}
                    let mut stream = &stream;
                    let event = event();
                    let chunk = format!("{:x}\r\n{event}\r\n", event.len());
                    let first = stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
                        )
                        .and_then(|()| stream.write_all(chunk.as_bytes()));
                    if first.is_err() {
                        return;
                    }
                    gate.wait(peak);
                    let mut sent = Ok(());
                    for _ in 1..EVENTS {
                        thread::sleep(CADENCE);
                        sent = sent.and_then(|()| stream.write_all(chunk.as_bytes()));
                    }
                    drop(sent.and_then(|()| stream.write_all(b"0\r\n\r\n")));
                });
            }
        });
        address
    }

    /// A gateway config on `listen` sending every request to `backend`.
    fn write_config(listen: u16, backend: SocketAddr) -> PathBuf {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("retained-memory-{listen}.yaml"));
        let yaml = format!(
            "insecure_options:\n  allow_private_endpoints: true\nlisteners:\n  - name: default\n    address: \
             \"127.0.0.1:{listen}\"\n    filter_chains: [main]\nfilter_chains:\n  - name: main\n    filters:\n      - \
             filter: router\n        routes:\n          - path_prefix: \"/\"\n            cluster: backend\n      - \
             filter: load_balancer\n        clusters:\n          - name: backend\n            endpoints: \
             [\"{backend}\"]\n"
        );
        std::fs::write(&path, yaml).expect("config written");
        path
    }

    /// A connection to the gateway on `listen`, bounded by [`TIMEOUT`] for connecting and for each read.
    fn connect(listen: u16) -> io::Result<TcpStream> {
        let stream = TcpStream::connect_timeout(&SocketAddr::from(([127, 0, 0, 1], listen)), TIMEOUT)?;
        stream.set_read_timeout(Some(TIMEOUT))?;
        Ok(stream)
    }

    /// A `/proc/<pid>/status` field in KiB, as MiB.
    fn status_mib(pid: u32, field: &str) -> f64 {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).expect("the gateway runs");
        let kib = status
            .lines()
            .find_map(|line| line.strip_prefix(field))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|value| value.parse::<u32>().ok())
            .expect("the field is reported");
        f64::from(kib) / 1_024.0
    }

    /// Process `pid`'s resident memory in MiB.
    fn resident_mib(pid: u32) -> f64 {
        status_mib(pid, "VmRSS:")
    }

    /// Process `pid`'s peak resident memory in MiB.
    fn high_water_mib(pid: u32) -> f64 {
        status_mib(pid, "VmHWM:")
    }

    /// Process `pid`'s open descriptors.
    fn descriptors(pid: u32) -> usize {
        std::fs::read_dir(format!("/proc/{pid}/fd")).map_or(0, Iterator::count)
    }
}
