//! Hot-path microbenchmarks.
//!
//! The budgets come from `docs/TESTING_AND_BENCHMARKS.md`:
//!
//! * `inspect_request` on a 4 KB payload: **< 80 µs**
//! * `resolve_candidates`: **< 5 µs**
//!
//! Run with `cargo bench`. These measure the in-process work only; the
//! end-to-end overhead budget (< 1.5 ms p99) needs the mock upstream and is
//! covered by the integration tests rather than here.

use std::sync::Arc;

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dashmap::DashMap;
use http::HeaderMap;

use oxide_route::config::{
    ProviderConfig, ProviderKind, RouterConfig, RoutingStrategy, VirtualModelConfig, WeightedTarget,
};
use oxide_route::proxy::inspector::inspect_request;
use oxide_route::proxy::router_engine::resolve_candidates;
use oxide_route::state::UpstreamHealthMetrics;

/// Builds a chat request whose encoded size is roughly 4 KB.
fn payload_4kb() -> Bytes {
    // ~4 KB of filler, padded so the encoded body lands near the target size.
    let text = "lorem ipsum dolor sit amet ".repeat(160);
    let body = serde_json::json!({
        "model": "smart-router",
        "stream": true,
        "messages": [
            {"role": "system", "content": "You are a terse assistant."},
            {"role": "user", "content": text},
        ],
    })
    .to_string();

    Bytes::from(body)
}

fn bench_config() -> RouterConfig {
    let mut config = RouterConfig::default();

    for name in ["openai_us", "anthropic_prod", "gemini_vertex"] {
        config.providers.insert(
            name.to_string(),
            ProviderConfig {
                kind: ProviderKind::OpenaiCompatible,
                api_key: None,
                base_url: Some(format!("http://127.0.0.1:9/{name}")),
                timeout_ms: 1_000,
                ttft_timeout_ms: 500,
            },
        );
    }

    config.models.insert(
        "smart-router".to_string(),
        VirtualModelConfig {
            primary: None,
            strategy: RoutingStrategy::LatencyEwma,
            targets: vec![
                WeightedTarget {
                    upstream: "openai_us/gpt-4o".to_string(),
                    weight: 70,
                },
                WeightedTarget {
                    upstream: "anthropic_prod/claude-3-7-sonnet-latest".to_string(),
                    weight: 30,
                },
            ],
            fallbacks: vec!["gemini_vertex/gemini-2.5-pro".to_string()],
            retry_on_status: vec![429, 500, 503],
        },
    );

    config
}

fn bench_inspect(c: &mut Criterion) {
    let body = payload_4kb();
    assert!(body.len() > 3_500, "payload should be roughly 4 KB");

    c.bench_function("inspect_request/4kb", |bencher| {
        bencher.iter(|| {
            let request =
                inspect_request(HeaderMap::new(), black_box(body.clone())).expect("inspects");
            black_box(request)
        });
    });
}

fn bench_resolve(c: &mut Criterion) {
    let config = bench_config();
    let request = inspect_request(
        HeaderMap::new(),
        Bytes::from(r#"{"model":"smart-router"}"#.to_string()),
    )
    .expect("inspects");

    let health: DashMap<String, Arc<UpstreamHealthMetrics>> = DashMap::new();
    for (reference, ttft_us) in [
        ("openai_us/gpt-4o", 42_000u64),
        ("anthropic_prod/claude-3-7-sonnet-latest", 118_000),
    ] {
        let metrics = Arc::new(UpstreamHealthMetrics::new());
        metrics.record_success(std::time::Duration::from_micros(ttft_us));
        health.insert(reference.to_string(), metrics);
    }

    c.bench_function("resolve_candidates/ewma", |bencher| {
        bencher.iter(|| {
            let resolution = resolve_candidates(black_box(&config), black_box(&request), &health)
                .expect("resolves");
            black_box(resolution)
        });
    });
}

criterion_group!(benches, bench_inspect, bench_resolve);
criterion_main!(benches);
