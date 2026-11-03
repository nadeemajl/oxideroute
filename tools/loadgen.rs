//! Closed-loop load generator for OxideRoute benchmarks.
//!
//! Hits a chat-completions endpoint with a fixed concurrency and records the
//! streaming metrics that the router's budgets are written in:
//!
//! * `headers_ms` — time until the HTTP response line and headers arrive.
//!   Against OxideRoute this should stay pinned to `ttft_ms`, because the
//!   gateway withholds `200 OK` until the first content frame (invariant 3).
//! * `ttft_ms` — time until the first non-empty `delta.content` frame.
//! * `itl_ms` — gaps between consecutive content-bearing frames. llama.cpp
//!   batches several tokens per frame, so this is frame cadence, not token
//!   cadence; the report says so.
//! * `completion_tokens` — from the `usage` block requested via
//!   `stream_options.include_usage`, falling back to a frame count when the
//!   upstream ignores the option.
//!
//! Output is one JSON document per run so the reporting script never has to
//! re-run the benchmark.
//!
//! ```text
//! cargo run --release --bin loadgen -- \
//!   --url http://127.0.0.1:8088/v1/chat/completions \
//!   --model qwen-local --requests 40 --concurrency 4 \
//!   --max-tokens 64 --json-out bench/results/router-c4.json
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

/// Fixed prompt. Deliberately short so prompt processing stays out of the
/// measurement and the numbers describe decode-side behaviour.
const PROMPT: &str = "Write one short paragraph about why backpressure matters in a proxy.";

#[derive(Parser, Debug)]
#[command(name = "loadgen", about = "Closed-loop load generator for OxideRoute")]
struct Args {
    /// Full chat-completions URL, e.g. `http://127.0.0.1:8088/v1/chat/completions`.
    #[arg(long)]
    url: String,

    /// Value for the request `model` field.
    #[arg(long)]
    model: String,

    /// Total number of measured requests.
    #[arg(long, default_value_t = 32)]
    requests: usize,

    /// Number of requests in flight at once.
    #[arg(long, default_value_t = 1)]
    concurrency: usize,

    /// `max_tokens` for every request.
    #[arg(long, default_value_t = 64)]
    max_tokens: u32,

    /// Unmeasured requests issued before the run to warm caches and the model.
    #[arg(long, default_value_t = 2)]
    warmup: usize,

    /// Disable SSE and send `stream: false`.
    #[arg(long)]
    no_stream: bool,

    /// Per-request timeout.
    #[arg(long, default_value_t = 180)]
    timeout_secs: u64,

    /// Optional bearer token.
    #[arg(long)]
    api_key: Option<String>,

    /// Extra header in `Name: Value` form; repeatable.
    #[arg(long = "header")]
    headers: Vec<String>,

    /// JSON object merged into the request body, for vendor-specific knobs
    /// such as `{"chat_template_kwargs": {"enable_thinking": false}}`.
    #[arg(long)]
    extra_json: Option<String>,

    /// Label written into the JSON document.
    #[arg(long, default_value = "run")]
    label: String,

    /// Where to write the JSON result. Prints a summary to stdout either way.
    #[arg(long)]
    json_out: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Sample {
    index: usize,
    status: u16,
    headers_ms: f64,
    ttft_ms: Option<f64>,
    total_ms: f64,
    itl_ms: Vec<f64>,
    completion_tokens: u64,
    chunks: u64,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Summary {
    count: usize,
    min: f64,
    p50: f64,
    p90: f64,
    p99: f64,
    max: f64,
    mean: f64,
}

impl Summary {
    fn of(mut values: Vec<f64>) -> Option<Self> {
        if values.is_empty() {
            return None;
        }
        values.sort_by(|a, b| a.total_cmp(b));
        let count = values.len();
        let mean = values.iter().sum::<f64>() / count as f64;
        Some(Self {
            count,
            min: values[0],
            p50: pick(&values, 50.0),
            p90: pick(&values, 90.0),
            p99: pick(&values, 99.0),
            max: values[count - 1],
            mean,
        })
    }
}

/// Nearest-rank percentile over a sorted slice.
fn pick(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (pct / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[derive(Debug, Serialize, Deserialize)]
struct Report {
    label: String,
    url: String,
    model: String,
    stream: bool,
    max_tokens: u32,
    concurrency: usize,
    requested: usize,
    succeeded: usize,
    failed: usize,
    statuses: BTreeMap<String, usize>,
    wall_seconds: f64,
    requests_per_second: f64,
    completion_tokens: u64,
    output_tokens_per_second: f64,
    ttft_ms: Option<Summary>,
    headers_ms: Option<Summary>,
    total_ms: Option<Summary>,
    itl_ms: Option<Summary>,
    per_request: Vec<Sample>,
    started_unix_ms: u128,
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Extracts `choices[0].delta.content` when it is a non-empty string.
fn delta_content(value: &serde_json::Value) -> Option<&str> {
    value
        .get("choices")?
        .get(0)?
        .get("delta")?
        .get("content")?
        .as_str()
        .filter(|s| !s.is_empty())
}

fn usage_tokens(value: &serde_json::Value) -> Option<u64> {
    value.get("usage")?.get("completion_tokens")?.as_u64()
}

struct Runner {
    args: Arc<Args>,
    client: reqwest::Client,
    extra_headers: Vec<(String, String)>,
    extra_body: Option<serde_json::Value>,
}

impl Runner {
    fn body(&self) -> serde_json::Value {
        let mut body = serde_json::json!({
            "model": self.args.model,
            "max_tokens": self.args.max_tokens,
            "temperature": 0.0,
            "messages": [{"role": "user", "content": PROMPT}],
        });
        if self.args.no_stream {
            body["stream"] = serde_json::Value::Bool(false);
        } else {
            body["stream"] = serde_json::Value::Bool(true);
            body["stream_options"] = serde_json::json!({"include_usage": true});
        }
        if let (Some(extra), Some(object)) = (&self.extra_body, body.as_object_mut()) {
            if let Some(fields) = extra.as_object() {
                for (key, value) in fields {
                    object.insert(key.clone(), value.clone());
                }
            }
        }
        body
    }

    fn request(&self, index: usize) -> reqwest::RequestBuilder {
        let mut builder = self
            .client
            .post(&self.args.url)
            .json(&self.body())
            .header("Content-Type", "application/json")
            .header("x-loadgen-index", index.to_string());
        if let Some(key) = &self.args.api_key {
            builder = builder.header("Authorization", format!("Bearer {key}"));
        }
        for (name, value) in &self.extra_headers {
            builder = builder.header(name, value);
        }
        builder
    }

    async fn one(&self, index: usize) -> Sample {
        let start = Instant::now();
        let mut sample = Sample {
            index,
            status: 0,
            headers_ms: 0.0,
            ttft_ms: None,
            total_ms: 0.0,
            itl_ms: Vec::new(),
            completion_tokens: 0,
            chunks: 0,
            error: None,
        };

        let response = self.request(index).send().await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                sample.error = Some(error.to_string());
                sample.total_ms = start.elapsed().as_secs_f64() * 1000.0;
                return sample;
            }
        };

        sample.headers_ms = start.elapsed().as_secs_f64() * 1000.0;
        sample.status = response.status().as_u16();

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            sample.error = Some(format!(
                "HTTP {}: {}",
                sample.status,
                body.chars().take(200).collect::<String>()
            ));
            sample.total_ms = start.elapsed().as_secs_f64() * 1000.0;
            return sample;
        }

        if self.args.no_stream {
            match response.json::<serde_json::Value>().await {
                Ok(value) => {
                    sample.completion_tokens = usage_tokens(&value).unwrap_or(0);
                    sample.chunks = 1;
                    sample.total_ms = start.elapsed().as_secs_f64() * 1000.0;
                    sample.ttft_ms = Some(sample.total_ms);
                }
                Err(error) => {
                    sample.error = Some(error.to_string());
                    sample.total_ms = start.elapsed().as_secs_f64() * 1000.0;
                }
            }
            return sample;
        }

        let mut stream = response.bytes_stream();
        let mut buffer: Vec<u8> = Vec::new();
        let mut last_content: Option<Instant> = None;
        let mut usage: Option<u64> = None;

        while let Some(next) = stream.next().await {
            let bytes = match next {
                Ok(bytes) => bytes,
                Err(error) => {
                    sample.error = Some(error.to_string());
                    break;
                }
            };
            buffer.extend_from_slice(&bytes);

            while let Some(newline) = buffer.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buffer.drain(..=newline).collect();
                let text = String::from_utf8_lossy(&line);
                let text = text.trim();
                let Some(payload) = text.strip_prefix("data:") else {
                    continue;
                };
                let payload = payload.trim();
                if payload == "[DONE]" {
                    continue;
                }
                let Ok(value) = serde_json::from_str::<serde_json::Value>(payload) else {
                    continue;
                };
                if let Some(tokens) = usage_tokens(&value) {
                    usage = Some(tokens);
                }
                if delta_content(&value).is_some() {
                    let now = Instant::now();
                    if sample.ttft_ms.is_none() {
                        sample.ttft_ms = Some((now - start).as_secs_f64() * 1000.0);
                    } else if let Some(previous) = last_content {
                        sample.itl_ms.push((now - previous).as_secs_f64() * 1000.0);
                    }
                    last_content = Some(now);
                    sample.chunks += 1;
                }
            }
        }

        sample.total_ms = start.elapsed().as_secs_f64() * 1000.0;
        sample.completion_tokens = usage.unwrap_or(sample.chunks);
        sample
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.concurrency == 0 {
        return Err("--concurrency must be at least 1".into());
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.timeout_secs))
        .pool_max_idle_per_host(args.concurrency.max(8))
        .build()?;

    let mut extra_headers = Vec::new();
    for raw in &args.headers {
        match raw.split_once(':') {
            Some((name, value)) => {
                extra_headers.push((name.trim().to_string(), value.trim().to_string()));
            }
            None => return Err(format!("--header '{raw}' is not `Name: Value`").into()),
        }
    }

    let extra_body = match &args.extra_json {
        Some(raw) => Some(serde_json::from_str::<serde_json::Value>(raw)?),
        None => None,
    };

    let args = Arc::new(args);
    let runner = Arc::new(Runner {
        args: Arc::clone(&args),
        client,
        extra_headers,
        extra_body,
    });

    // Warmup: model load, TLS session, connection pool, and branch prediction
    // in the router all settle before the first measured sample.
    for index in 0..args.warmup {
        let _ = runner.one(usize::MAX - index).await;
    }

    let started_unix_ms = now_unix_ms();
    let first_start = Instant::now();
    let permits = Arc::new(Semaphore::new(args.concurrency));
    let mut handles = Vec::with_capacity(args.requests);

    for index in 0..args.requests {
        let runner = Arc::clone(&runner);
        let permits = Arc::clone(&permits);
        handles.push(tokio::spawn(async move {
            let permit = match permits.acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => return None,
            };
            let measured = runner.one(index).await;
            drop(permit);
            Some(measured)
        }));
    }

    let mut samples = Vec::with_capacity(args.requests);
    for handle in handles {
        match handle.await {
            Ok(Some(sample)) => samples.push(sample),
            Ok(None) => {}
            Err(error) => eprintln!("worker panicked: {error}"),
        }
    }
    let wall = first_start.elapsed().as_secs_f64();
    samples.sort_by_key(|s| s.index);

    let mut statuses: BTreeMap<String, usize> = BTreeMap::new();
    for sample in &samples {
        let bucket = if sample.error.is_some() && sample.status == 0 {
            "transport".to_string()
        } else {
            sample.status.to_string()
        };
        *statuses.entry(bucket).or_default() += 1;
    }

    let ok: Vec<&Sample> = samples.iter().filter(|s| s.error.is_none()).collect();
    let succeeded = ok.len();
    let failed = samples.len() - succeeded;
    let completion_tokens: u64 = ok.iter().map(|s| s.completion_tokens).sum();

    let ttft = Summary::of(ok.iter().filter_map(|s| s.ttft_ms).collect());
    let headers = Summary::of(ok.iter().map(|s| s.headers_ms).collect());
    let total = Summary::of(ok.iter().map(|s| s.total_ms).collect());
    let itl = Summary::of(ok.iter().flat_map(|s| s.itl_ms.clone()).collect());

    let report = Report {
        label: args.label.clone(),
        url: args.url.clone(),
        model: args.model.clone(),
        stream: !args.no_stream,
        max_tokens: args.max_tokens,
        concurrency: args.concurrency,
        requested: args.requests,
        succeeded,
        failed,
        statuses,
        wall_seconds: wall,
        requests_per_second: if wall > 0.0 {
            succeeded as f64 / wall
        } else {
            0.0
        },
        completion_tokens,
        output_tokens_per_second: if wall > 0.0 {
            completion_tokens as f64 / wall
        } else {
            0.0
        },
        ttft_ms: ttft,
        headers_ms: headers,
        total_ms: total,
        itl_ms: itl,
        per_request: samples,
        started_unix_ms,
    };

    if let Some(path) = &args.json_out {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(path, serde_json::to_string_pretty(&report)?)?;
    }

    println!(
        "{}: ok={}/{} rps={:.2} tok/s={:.1} ttft(p50/p99)={:.0}/{:.0}ms total(p50/p99)={:.0}/{:.0}ms",
        report.label,
        report.succeeded,
        report.requested,
        report.requests_per_second,
        report.output_tokens_per_second,
        report.ttft_ms.map_or(0.0, |s| s.p50),
        report.ttft_ms.map_or(0.0, |s| s.p99),
        report.total_ms.map_or(0.0, |s| s.p50),
        report.total_ms.map_or(0.0, |s| s.p99),
    );
    if report.failed > 0 {
        println!("  failures: {:?}", report.statuses);
        if let Some(first) = report.per_request.iter().find(|s| s.error.is_some()) {
            println!("  first failure: {:?}", first.error);
        }
    }

    Ok(())
}
