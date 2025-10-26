use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_util::MetricKindMask;
use std::net::SocketAddr;
use tokio::time::{self, Duration};
use tracing::warn;

fn resolve_listener(node_id: usize) -> Result<SocketAddr, String> {
    if let Ok(addr) = std::env::var("METRICS_LISTEN_ADDR") {
        return addr
            .parse()
            .map_err(|e| format!("invalid METRICS_LISTEN_ADDR '{}': {}", addr, e));
    }

    let default_port: u16 = 9890u16.saturating_add(node_id as u16);
    let port = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(default_port);

    format!("0.0.0.0:{}", port)
        .parse()
        .map_err(|e| format!("unable to parse metrics listen addr: {}", e))
}

pub fn init_metrics(node_id: usize) -> Result<(), String> {
    let listen_addr = resolve_listener(node_id)?;

    let exporter_started = match PrometheusBuilder::new()
        .idle_timeout(MetricKindMask::ALL, None)
        .with_http_listener(listen_addr)
        .install()
    {
        Ok(()) => {
            warn!(%listen_addr, "Prometheus metrics exporter started ✅");
            true
        }
        Err(e) => {
            warn!(%listen_addr, error = %e, "failed to start Prometheus exporter");
            false
        }
    };

    metrics::describe_histogram!(
        "smrol.threshold_task_wait_ms",
        "Time threshold worker tasks wait before execution (ms)"
    );
    metrics::describe_histogram!(
        "smrol.threshold_task_exec_ms",
        "Execution time of threshold worker tasks (ms)"
    );
    metrics::describe_gauge!(
        "smrol.threshold_pending_jobs",
        "Current number of pending threshold worker jobs"
    );
    metrics::describe_gauge!(
        "smrol.channel_backlog",
        "Pending messages in asynchronous channels"
    );
    metrics::describe_gauge!("smrol.node_up", "node is up");
    metrics::describe_counter!("smrol.heartbeat", "Node heartbeat tick");

    let node_label = node_id.to_string();
    metrics::gauge!("smrol.node_up", "node" => node_label.clone()).set(1.0);
    warn!(node_id, "✅ wrote smrol.node_up=1 (boot)");

    tokio::spawn({
        let node = node_label.clone();
        async move {
            let mut ticker = time::interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                metrics::counter!("smrol.heartbeat", "node" => node.clone()).increment(1);
                warn!(node = %node, "💓 smrol.heartbeat++");
            }
        }
    });

    if exporter_started {
        Ok(())
    } else {
        Err("unable to start Prometheus exporter".into())
    }
}
