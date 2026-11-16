//! What a gateway adds to request latency: the same open-loop load sent straight to a backend, through
//! the gateway with fixed routing, through the gateway choosing the site, and through a consumer gateway
//! and, over mutual TLS, a provider gateway in front of the backend.

#[path = "support/ports.rs"]
mod ports;

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::print_stdout,
    clippy::disallowed_methods,
    reason = "a measurement that prints its table; the backend and the client pace with thread::sleep"
)]
mod tests {
    use std::{
        io::{self, BufReader, Write as _},
        net::{SocketAddr, TcpStream},
        path::PathBuf,
        process::{Child, Command, Stdio},
        sync::{Arc, Mutex, mpsc},
        thread,
        time::{Duration, Instant},
    };

    use super::ports::{bound_listener, free_port};

    // ---------------------------------------------------------------------------
    // Constants
    // ---------------------------------------------------------------------------

    /// Each load level: requests offered per second and how long the backend takes to answer one.
    const LEVELS: [(u32, Duration); 3] = [
        (500, Duration::from_millis(20)),
        (2_000, Duration::from_millis(200)),
        (2_000, Duration::from_millis(500)),
    ];

    /// Client connections, each sending one request at a time: past the most a level holds in flight.
    const CONNECTIONS: usize = 1_280;

    /// Load sent, untimed, before each measurement.
    const WARMUP: Duration = Duration::from_secs(2);

    /// Load measured at each level.
    const MEASURED: Duration = Duration::from_secs(10);

    /// Bytes in each response body.
    const BODY_BYTES: usize = 1_024;

    /// Bound on waiting for a gateway to listen, on connecting, and on each read and write.
    const TIMEOUT: Duration = Duration::from_secs(60);

    #[test]
    #[ignore = "a measurement, not a check: run with --ignored --nocapture in release"]
    fn latency_direct_through_one_gateway_and_across_a_mutual_tls_hop() {
        let identity = Identity::mint();
        println!(
            "open loop, {CONNECTIONS} client connections, {BODY_BYTES} B bodies, {WARMUP:?} warm-up then {MEASURED:?} \
             measured; latency from each request's scheduled start; ms"
        );
        println!(
            "{:<15} {:>6} {:>8}  {:>7} {:>7} {:>7} {:>7}  {:>9}  {:>6}",
            "path", "rps", "service", "p50", "p90", "p99", "max", "late p99", "errors"
        );
        for (rate, service) in LEVELS {
            let backend = backend(service);
            let fixed = Gateway::fixed(backend);
            let selecting = Gateway::selecting(backend);
            let provider = Gateway::provider(backend, &identity);
            let consumer = Gateway::consumer(provider.address, &identity);
            for (path, target) in [
                ("direct", backend),
                ("fixed routing", fixed.address),
                ("site selection", selecting.address),
                ("mutual TLS hop", consumer.address),
            ] {
                let run = open_loop(target, rate);
                println!(
                    "{path:<15} {rate:>6} {:>8}  {:>7.1} {:>7.1} {:>7.1} {:>7.1}  {:>9.2}  {:>6}",
                    service.as_millis(),
                    run.latency(500),
                    run.latency(900),
                    run.latency(990),
                    run.latency(1_000),
                    run.late(990),
                    run.errors,
                );
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// What one open-loop run measured.
    struct Run {
        /// Requests that failed or got no full response.
        errors: usize,
        /// Milliseconds from each measured request's scheduled start to its full response, sorted.
        latencies: Vec<f64>,
        /// Milliseconds each measured request waited for a free connection past its scheduled start, sorted.
        lateness: Vec<f64>,
    }

    impl Run {
        /// How late requests left at `per_mille`: the client's own delay, near zero while it keeps up.
        fn late(&self, per_mille: usize) -> f64 {
            at(&self.lateness, per_mille)
        }

        /// Latency at `per_mille` thousandths of the way through the sorted runs.
        fn latency(&self, per_mille: usize) -> f64 {
            at(&self.latencies, per_mille)
        }
    }

    /// The value `per_mille` thousandths of the way through `sorted`, zero when it is empty.
    fn at(sorted: &[f64], per_mille: usize) -> f64 {
        let last = sorted.len().saturating_sub(1);
        sorted
            .get(last.saturating_mul(per_mille) / 1_000)
            .copied()
            .unwrap_or_default()
    }

    /// A gateway process, killed when dropped.
    struct Gateway {
        /// Where it listens.
        address: SocketAddr,
        /// The process.
        child: Child,
        /// Files it was started with.
        files: Vec<PathBuf>,
    }

    impl Gateway {
        /// A consumer gateway sending every request to the `provider` gateway over mutual TLS as `site-a`.
        fn consumer(provider: SocketAddr, identity: &Identity) -> Self {
            Self::start(
                "",
                &format!(
                    "      - filter: router\n        routes:\n          - path_prefix: \"/\"\n            cluster: \
                     site-b\n      - filter: load_balancer\n        clusters:\n          - name: site-b\n            \
                     tls:\n              ca:\n                ca_path: {ca}\n              client_cert:\n                \
                     cert_path: {cert}\n                key_path: {key}\n              sni: {sni}\n              verify: \
                     true\n            endpoints: [\"{provider}\"]\n",
                    ca = identity.path("ca.crt"),
                    cert = identity.path("site-a.crt"),
                    key = identity.path("site-a.key"),
                    sni = certs::site_dns_name("site-b"),
                ),
                None,
            )
        }

        /// A gateway sending every request to `backend`.
        fn fixed(backend: SocketAddr) -> Self {
            Self::start(
                "",
                &format!(
                    "      - filter: router\n        routes:\n          - path_prefix: \"/\"\n            cluster: \
                     local-llama\n{}",
                    load_balancer(backend)
                ),
                None,
            )
        }

        /// A provider gateway in front of `backend`, serving mutual TLS as `site-b` to clients the grid CA signed.
        fn provider(backend: SocketAddr, identity: &Identity) -> Self {
            Self::start(
                &format!(
                    "    tls:\n      certificates:\n        - cert_path: {cert}\n          key_path: {key}\n      \
                     client_ca:\n        ca_path: {ca}\n      client_cert_mode: require\n",
                    cert = identity.path("site-b.crt"),
                    key = identity.path("site-b.key"),
                    ca = identity.path("ca.crt"),
                ),
                &format!(
                    "      - filter: router\n        routes:\n          - path_prefix: \"/\"\n            cluster: \
                     local-llama\n{}",
                    load_balancer(backend)
                ),
                None,
            )
        }

        /// A gateway choosing among the grid's sites for `llama`, which only `backend` serves.
        fn selecting(backend: SocketAddr) -> Self {
            let serving = r#"{"local_site":"site-a","window_secs":60,"load_window_ms":24000,"horizon_ms":5000,"candidates":[{"kind":"inference_model","name":"llama","site":"site-a","cluster":"local-llama","fresh":true,"weight":1}],"peers":[]}"#;
            Self::start(
                "",
                &format!(
                    "      - filter: grid_site_route\n        model_header: X-Model\n        clusters: \
                     [\"local-llama\"]\n{}",
                    load_balancer(backend)
                ),
                Some(serving),
            )
        }

        /// A gateway whose listener carries `tls` and whose one chain runs `filters`, with `serving` as its
        /// serving config when given.
        fn start(tls: &str, filters: &str, serving: Option<&str>) -> Self {
            let listen = free_port();
            let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
            let config = dir.join(format!("gateway-overhead-{listen}.yaml"));
            std::fs::write(
                &config,
                format!(
                    "insecure_options:\n  allow_private_endpoints: true\nlisteners:\n  - name: default\n    address: \
                     \"127.0.0.1:{listen}\"\n{tls}    filter_chains: [main]\nfilter_chains:\n  - name: main\n    \
                     filters:\n{filters}"
                ),
            )
            .expect("config written");
            let mut files = vec![config.clone()];
            let mut command = Command::new(env!("CARGO_BIN_EXE_grid-gateway"));
            command
                .arg("--config")
                .arg(&config)
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            match serving {
                Some(serving) => {
                    let path = dir.join(format!("gateway-overhead-{listen}.json"));
                    std::fs::write(&path, serving).expect("serving config written");
                    command.env("GRID_SERVING_CONFIG", &path);
                    files.push(path);
                },
                None => {
                    command.env_remove("GRID_SERVING_CONFIG");
                },
            }
            let gateway = Self {
                address: SocketAddr::from(([127, 0, 0, 1], listen)),
                child: command.spawn().expect("the gateway starts"),
                files,
            };
            let deadline = Instant::now().checked_add(TIMEOUT).expect("a near deadline");
            loop {
                let left = deadline.saturating_duration_since(Instant::now());
                assert!(!left.is_zero(), "the gateway never listened");
                if TcpStream::connect_timeout(&gateway.address, left).is_ok() {
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }
            gateway
        }
    }

    impl Drop for Gateway {
        fn drop(&mut self) {
            let _killed = self.child.kill();
            let _reaped = self.child.wait();
            for file in &self.files {
                let _removed = std::fs::remove_file(file);
            }
        }
    }

    /// A grid CA with `site-a` and `site-b` leaves, written under a directory of their own.
    struct Identity(PathBuf);

    impl Identity {
        /// A fresh CA and two site leaves.
        fn mint() -> Self {
            let dir =
                PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("gateway-overhead-{}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("identity directory");
            let ca = certs::generate_ca("grid-ca").expect("ca");
            std::fs::write(dir.join("ca.crt"), &ca.cert_pem).expect("ca written");
            for site in ["site-a", "site-b"] {
                let leaf = certs::generate_site_cert(&ca, site).expect("leaf");
                std::fs::write(dir.join(format!("{site}.crt")), &leaf.cert_pem).expect("leaf written");
                std::fs::write(dir.join(format!("{site}.key")), &leaf.key_pem).expect("key written");
            }
            Self(dir)
        }

        /// The path of `file` in the identity directory.
        fn path(&self, file: &str) -> String {
            self.0.join(file).to_string_lossy().into_owned()
        }
    }

    impl Drop for Identity {
        fn drop(&mut self) {
            let _removed = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The load balancer block sending `local-llama` to `backend`.
    fn load_balancer(backend: SocketAddr) -> String {
        format!(
            "      - filter: load_balancer\n        clusters:\n          - name: local-llama\n            endpoints: \
             [\"{backend}\"]\n"
        )
    }

    /// A keep-alive backend answering every request after `service` with a [`BODY_BYTES`] body.
    fn backend(service: Duration) -> SocketAddr {
        let listener = bound_listener();
        let address = listener.local_addr().expect("bound");
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                thread::spawn(move || {
                    let mut reader = BufReader::new(&stream);
                    let mut stream = &stream;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {BODY_BYTES}\r\n\r\n{}",
                        "x".repeat(BODY_BYTES)
                    );
                    while read_head(&mut reader).is_ok() {
                        thread::sleep(service);
                        if stream.write_all(response.as_bytes()).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        address
    }

    /// Offer `rate` requests a second to `target` over [`CONNECTIONS`] keep-alive connections for
    /// [`WARMUP`] and then [`MEASURED`], each request timed from when it was scheduled to start.
    fn open_loop(target: SocketAddr, rate: u32) -> Run {
        let (jobs, inbox) = mpsc::channel::<(Instant, bool)>();
        let inbox = Arc::new(Mutex::new(inbox));
        let (results_tx, results) = mpsc::channel::<(bool, Option<(f64, f64)>)>();
        let workers: Vec<_> = std::iter::repeat_with(|| {
            let (inbox, results_tx) = (Arc::clone(&inbox), results_tx.clone());
            thread::spawn(move || {
                let mut connection: Option<BufReader<TcpStream>> = None;
                loop {
                    let job = inbox.lock().expect("the inbox").recv();
                    let Ok((scheduled, measured)) = job else {
                        return;
                    };
                    let started = Instant::now();
                    let answered = request(&mut connection, target);
                    let done = Instant::now();
                    let timing = answered.then(|| {
                        (
                            done.duration_since(scheduled).as_secs_f64() * 1_000.0,
                            started.duration_since(scheduled).as_secs_f64() * 1_000.0,
                        )
                    });
                    results_tx
                        .send((measured, timing))
                        .expect("the run collects every result");
                }
            })
        })
        .take(CONNECTIONS)
        .collect();
        drop(results_tx);
        let gap = Duration::from_secs(1).checked_div(rate).expect("a positive rate");
        let seconds = u32::try_from(WARMUP.saturating_add(MEASURED).as_secs()).expect("a short run");
        let begun = Instant::now();
        let mut scheduled_measured = 0_usize;
        for index in 0..rate.saturating_mul(seconds) {
            let scheduled = begun.checked_add(gap.saturating_mul(index)).expect("a near instant");
            thread::sleep(scheduled.saturating_duration_since(Instant::now()));
            let measured = scheduled.duration_since(begun) >= WARMUP;
            scheduled_measured = scheduled_measured.saturating_add(usize::from(measured));
            jobs.send((scheduled, measured)).expect("a worker is waiting");
        }
        drop(jobs);
        let failed_workers = workers
            .into_iter()
            .map(thread::JoinHandle::join)
            .filter(Result::is_err)
            .count();
        assert_eq!(failed_workers, 0, "every client connection's worker ran to the end");
        let mut run = Run {
            errors: 0,
            latencies: Vec::new(),
            lateness: Vec::new(),
        };
        for (measured, timing) in results {
            match (measured, timing) {
                (false, _) => {},
                (true, Some((latency, late))) => {
                    run.latencies.push(latency);
                    run.lateness.push(late);
                },
                (true, None) => run.errors = run.errors.saturating_add(1),
            }
        }
        assert_eq!(
            run.latencies.len().saturating_add(run.errors),
            scheduled_measured,
            "every measured request is answered or counted as an error"
        );
        run.latencies.sort_by(f64::total_cmp);
        run.lateness.sort_by(f64::total_cmp);
        run
    }

    /// Send one request for `llama` over `connection`, opening it to `target` first if needed, and
    /// read the whole response. A failed exchange drops the connection.
    fn request(connection: &mut Option<BufReader<TcpStream>>, target: SocketAddr) -> bool {
        if connection.is_none() {
            *connection = TcpStream::connect_timeout(&target, TIMEOUT)
                .and_then(|stream| {
                    stream.set_read_timeout(Some(TIMEOUT))?;
                    stream.set_write_timeout(Some(TIMEOUT))?;
                    Ok(stream)
                })
                .ok()
                .map(BufReader::new);
        }
        let Some(reader) = connection.as_mut() else {
            return false;
        };
        let exchanged = reader
            .get_mut()
            .write_all(b"GET /v1/models HTTP/1.1\r\nHost: gateway\r\nX-Model: llama\r\n\r\n")
            .and_then(|()| read_response(reader));
        if exchanged.is_err() {
            *connection = None;
        }
        exchanged.is_ok()
    }

    /// Read one response with a `Content-Length` body from `reader`.
    fn read_response(reader: &mut impl io::BufRead) -> io::Result<()> {
        let head = read_head(reader)?;
        if !head.starts_with("HTTP/1.1 200") {
            return Err(io::Error::other(head));
        }
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .ok_or_else(|| io::Error::other("no content length"))?;
        let mut body = vec![0; length];
        reader.read_exact(&mut body)
    }

    /// The status line and headers of the next message on `reader`, through the blank line.
    fn read_head(reader: &mut impl io::BufRead) -> io::Result<String> {
        let mut head = String::new();
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line)? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            if line == "\r\n" {
                return Ok(head);
            }
            head.push_str(&line);
        }
    }
}
