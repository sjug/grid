//! Closed-loop routing: every arrival routed by the real selection over a snapshot the real store
//! and ordering build from what the gateway last heard, every site serving at its own pace, so the
//! requests one snapshot sends change the load a later one reads.
//!
//! The delivery follows the shipped relay. Each site's operator scrapes its engines every
//! [`SCRAPE_MS`] and keeps the latest observation. The operator at the gateway's own site, `a`,
//! polls the other sites' operators every `peer_poll_ms` and keeps the latest observation each
//! returned. The gateway polls its local operator every [`GATEWAY_POLL_MS`], ingests the cached
//! observation of every site with its own timestamp, and reorders, reading worst load over
//! [`HORIZON_MS`] and freshness over the window the operator would send for that cadence.
//!
//! Each site's capacity is taught first, as a warm-up teaches it, by one sample holding its slots
//! with nothing waiting, then idle samples past the freshness window, so every run starts from a
//! learned, idle grid. Requests arrive evenly, take a fixed service time and wait in one queue per
//! site; nothing else about an engine is modelled, there is one gateway, and selection is
//! `select_spread` alone, without shedding, admission or prefix affinity. The runs print what
//! happened rather than assert it: they show whether the rule oscillates under these assumptions
//! and how the relay cadence changes that, not why a particular grid did.

use std::sync::Arc;

use super::{Harness, KeepAll, MODEL, NOW_MS, REFRESH_MS, SETTLE, SiteSpec, config};
use crate::{
    descriptor::{CapabilityKind, validate_serving_candidates},
    route::select_spread,
    snapshot::{Inputs, RouteSnapshot},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Simulated milliseconds per step.
const STEP_MS: i64 = 100;

/// How often each site's operator scrapes its engines.
const SCRAPE_MS: i64 = 1_000;

/// How often the gateway polls its local operator.
const GATEWAY_POLL_MS: i64 = 1_000;

/// How far behind a site's newest sample worst load is read: the operator's three scrapes plus two
/// seconds, at one-second scrapes.
const HORIZON_MS: i64 = 3 * SCRAPE_MS + 2_000;

/// The most a peer poll may take, as the freshness window budgets it.
const PEER_BUDGET_MS: i64 = 10_000;

/// Width of each reported window.
const REPORT_MS: i64 = 5_000;

/// Engines each site runs, each publishing its own load.
const ENDPOINTS: usize = 10;

/// Requests a site runs at once at full strength: sixteen per engine.
const SLOTS: usize = 16 * ENDPOINTS;

/// How long one request runs once it starts.
const SERVICE_MS: i64 = 2_000;

/// The sites: `a` is the gateway's own, and `c` is the one a scenario changes.
const SITES: [&str; 3] = ["a", "b", "c"];

/// Simulated time each scenario runs.
const DURATION_MS: i64 = 120_000;

/// The scenarios the simulation prints.
const SCENARIOS: [Scenario; 7] = [
    Scenario {
        name: "steady: three equal sites at two thirds of capacity, peers polled every 5 s",
        arrivals_per_s: |_| 160.0,
        c_slots: |_| SLOTS,
        peer_poll_ms: 5_000,
    },
    Scenario {
        name: "c loses half its slots at 30 s, peers polled every 5 s",
        arrivals_per_s: |_| 160.0,
        c_slots: |at_ms| if at_ms < 30_000 { SLOTS } else { SLOTS / 2 },
        peer_poll_ms: 5_000,
    },
    Scenario {
        name: "c loses half its slots at 30 s, peers polled every 1 s",
        arrivals_per_s: |_| 160.0,
        c_slots: |at_ms| if at_ms < 30_000 { SLOTS } else { SLOTS / 2 },
        peer_poll_ms: 1_000,
    },
    Scenario {
        name: "c stalls to one slot per engine from 30 s to 60 s, then recovers, peers polled every 5 s",
        arrivals_per_s: |_| 160.0,
        c_slots: |at_ms| if (30_000..60_000).contains(&at_ms) { ENDPOINTS } else { SLOTS },
        peer_poll_ms: 5_000,
    },
    Scenario {
        name: "c stalls to one slot per engine from 30 s to 60 s, then recovers, peers polled every 1 s",
        arrivals_per_s: |_| 160.0,
        c_slots: |at_ms| if (30_000..60_000).contains(&at_ms) { ENDPOINTS } else { SLOTS },
        peer_poll_ms: 1_000,
    },
    Scenario {
        name: "offered load at 1.25 times every site's capacity from 30 s to 90 s, peers polled every 5 s",
        arrivals_per_s: |at_ms| if (30_000..90_000).contains(&at_ms) { 300.0 } else { 160.0 },
        c_slots: |_| SLOTS,
        peer_poll_ms: 5_000,
    },
    Scenario {
        name: "offered load at 1.25 times every site's capacity from 30 s to 90 s, peers polled every 1 s",
        arrivals_per_s: |at_ms| if (30_000..90_000).contains(&at_ms) { 300.0 } else { 160.0 },
        c_slots: |_| SLOTS,
        peer_poll_ms: 1_000,
    },
];

#[test]
#[ignore = "a simulation, not an invariant: prints each site's share and queue per window"]
#[expect(clippy::print_stdout, reason = "reports the runs")]
fn closed_loop_routing() {
    println!(
        "{} sites of {ENDPOINTS} engines with {} slots each, {SERVICE_MS} ms per request; operators scrape every \
         {SCRAPE_MS} ms, the gateway at a polls its operator every {GATEWAY_POLL_MS} ms; horizon {HORIZON_MS} ms",
        SITES.len(),
        SLOTS / ENDPOINTS
    );
    for scenario in &SCENARIOS {
        println!(
            "\n{} (freshness window {} ms)",
            scenario.name,
            window_ms(scenario.peer_poll_ms)
        );
        println!(
            "{:>7}  {:>17}  {:>17}  {:>6}",
            "window", "share a / b / c", "waiting a / b / c", "c slots"
        );
        for window in run(scenario) {
            println!(
                "{:>5} s  {:>5.2} {:>5.2} {:>5.2}  {:>5.1} {:>5.1} {:>5.1}  {:>6}",
                window.start_ms / 1_000,
                window.share[0],
                window.share[1],
                window.share[2],
                window.waiting[0],
                window.waiting[1],
                window.waiting[2],
                window.c_slots,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Test Utilities
// ---------------------------------------------------------------------------

/// One run's inputs.
struct Scenario {
    /// What the run shows.
    name: &'static str,
    /// Requests offered per second at a simulated instant, spread evenly over the steps.
    arrivals_per_s: fn(i64) -> f64,
    /// Slots site `c` runs at a simulated instant; `a` and `b` keep [`SLOTS`].
    c_slots: fn(i64) -> usize,
    /// How often the operator at `a` polls the other sites' operators.
    peer_poll_ms: i64,
}

/// One site's state.
#[derive(Default)]
struct Site {
    /// The observation the operator at `a` holds for this site: when, running, waiting.
    cached: Option<(i64, usize, usize)>,
    /// This site's operator's latest observation of its engines: when, running, waiting.
    latest: Option<(i64, usize, usize)>,
    /// Remaining milliseconds of each running request.
    running: Vec<i64>,
    /// Requests waiting for a slot.
    waiting: usize,
}

/// What one reported window saw.
struct Window {
    /// Slots site `c` ran at its end.
    c_slots: usize,
    /// Each site's share of the requests routed in it.
    share: [f64; 3],
    /// Simulated start of the window.
    start_ms: i64,
    /// Each site's mean waiting requests over it.
    waiting: [f64; 3],
}

/// The freshness window the operator sends for `peer_poll_ms`: a scrape, a peer poll and its
/// budget, the gateway's poll, two seconds of margin and one more peer poll.
const fn window_ms(peer_poll_ms: i64) -> i64 {
    SCRAPE_MS + peer_poll_ms + PEER_BUDGET_MS + GATEWAY_POLL_MS + 2_000 + peer_poll_ms
}

/// Run `scenario` for [`DURATION_MS`], one [`Window`] per [`REPORT_MS`].
fn run(scenario: &Scenario) -> Vec<Window> {
    let specs: Vec<SiteSpec> = SITES
        .iter()
        .map(|site| SiteSpec {
            site,
            capacity: SLOTS as f64,
            enforced: SLOTS as f64,
            in_flight: 0.0,
        })
        .collect();
    let window = window_ms(scenario.peer_poll_ms);
    let mut grid = Harness::new(window);
    grid.set_units(ENDPOINTS as f64);
    let mut snapshot = grid.refresh(&specs, NOW_MS, |spec| spec.capacity / ENDPOINTS as f64, 0.0);
    let start_ms = NOW_MS + window + SETTLE * REFRESH_MS;
    for at in (NOW_MS + REFRESH_MS..=start_ms).step_by(usize::try_from(REFRESH_MS).expect("a positive refresh")) {
        snapshot = grid.refresh(&specs, at, |_| 0.0, 0.0);
    }
    let mut sites: Vec<Site> = SITES.iter().map(|_| Site::default()).collect();
    let (mut owed, mut turn) = (0.0, 0_usize);
    let mut windows = Vec::new();
    let (mut routed, mut waited) = ([0_u32; 3], [0_usize; 3]);
    let steps_per_window = REPORT_MS / STEP_MS;
    for step in 0..DURATION_MS / STEP_MS {
        let at_ms = step * STEP_MS;
        let now_ms = start_ms + at_ms;
        let c_slots = (scenario.c_slots)(at_ms);
        for (index, site) in sites.iter_mut().enumerate() {
            let slots = if index == 2 { c_slots } else { SLOTS };
            site.running.iter_mut().for_each(|left| *left -= STEP_MS);
            site.running.retain(|left| *left > 0);
            start_waiting(site, slots);
            if at_ms % SCRAPE_MS == 0 {
                site.latest = Some((now_ms, site.running.len(), site.waiting));
            }
            let polled = index == 0 || (at_ms - peer_poll_offset(scenario.peer_poll_ms, index)).rem_euclid(scenario.peer_poll_ms) == 0;
            if polled {
                site.cached = site.latest;
            }
        }
        if at_ms % GATEWAY_POLL_MS == 0 {
            for (name, site) in SITES.iter().zip(&sites) {
                deliver(&grid, name, site, now_ms);
            }
            snapshot = order(&mut grid, &specs, now_ms, window);
        }
        owed += (scenario.arrivals_per_s)(at_ms) * STEP_MS as f64 / 1_000.0;
        while owed >= 1.0 {
            owed -= 1.0;
            let pick = select_spread(&snapshot, CapabilityKind::InferenceModel, MODEL, turn, |_| true, &KeepAll)
                .expect("a site is picked");
            turn += 1;
            let index = SITES
                .iter()
                .position(|site| *site == &*pick.candidate.site)
                .expect("a known site");
            routed[index] += 1;
            sites[index].waiting += 1;
        }
        for (index, site) in sites.iter_mut().enumerate() {
            start_waiting(site, if index == 2 { c_slots } else { SLOTS });
            waited[index] += site.waiting;
        }
        if (step + 1) % steps_per_window == 0 {
            let total = f64::from(routed.iter().sum::<u32>().max(1));
            windows.push(Window {
                c_slots,
                share: routed.map(|count| f64::from(count) / total),
                start_ms: at_ms + STEP_MS - REPORT_MS,
                waiting: waited.map(|sum| sum as f64 / steps_per_window as f64),
            });
            (routed, waited) = ([0; 3], [0; 3]);
        }
    }
    windows
}

/// Start waiting requests at `site` while it has a free slot of `slots`.
fn start_waiting(site: &mut Site, slots: usize) {
    while site.running.len() < slots && site.waiting > 0 {
        site.waiting -= 1;
        site.running.push(SERVICE_MS);
    }
}

/// When, within each peer poll interval, the operator at `a` polls the site at `index`: the
/// peers spread over the interval, on whole steps.
fn peer_poll_offset(peer_poll_ms: i64, index: usize) -> i64 {
    let (index, sites) = (i64::try_from(index), i64::try_from(SITES.len()));
    peer_poll_ms * index.expect("a site index") / sites.expect("three sites") / STEP_MS * STEP_MS
}

/// Ingest the observation the operator at `a` holds for `site`, at its own time, as the gateway's
/// poll at `now_ms` delivers it. A re-served observation changes nothing.
fn deliver(grid: &Harness, name: &str, site: &Site, now_ms: i64) {
    let Some((at, running, waiting)) = site.cached else {
        return;
    };
    let labels = format!(r#"grid_site="{name}",grid_provider="pool-{name}""#);
    let batch = [
        ("llm_d_epp_average_running_requests", running as f64 / ENDPOINTS as f64),
        ("llm_d_epp_average_queue_size", waiting as f64 / ENDPOINTS as f64),
        ("llm_d_epp_ready_endpoints", ENDPOINTS as f64),
        ("llm_d_epp_flow_control_queue_size", 0.0),
    ]
    .map(|(metric, value)| format!("{metric}{{{labels}}} {value} {at}"))
    .join("\n");
    grid.store.ingest_at(&batch, now_ms, now_ms, name);
}

/// The snapshot the gateway at `a` orders at `now_ms`, reading freshness over `window_ms` and
/// worst load over [`HORIZON_MS`].
fn order(grid: &mut Harness, specs: &[SiteSpec], now_ms: i64, window_ms: i64) -> RouteSnapshot {
    let validated = validate_serving_candidates(specs.iter().map(config).collect()).expect("serving candidates");
    RouteSnapshot::from_store(
        validated,
        Arc::from(SITES[0]),
        &mut Inputs {
            signals: &grid.store,
            now_ms,
            window_ms,
            horizon_ms: HORIZON_MS,
            availability: &grid.availability,
            learned: &mut grid.gauged.learned,
        },
    )
    .published(&mut grid.gauged)
}
