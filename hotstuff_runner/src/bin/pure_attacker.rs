use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hotstuff_runner::event::TestTransaction;
use hotstuff_runner::pompe::{load_pompe_config, PompeMessage, PompeTransaction};
use hotstuff_runner::pompe_network::PompeNetwork;
use hotstuff_runner::smrol::{
    message::{SmrolMessage, SmrolTransaction},
    network::SmrolTcpNetwork,
};
use rand::Rng;
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_logging();

    let protocol = env::var("ADVERSARY_MODE")
        .or_else(|_| env::var("ATTACK_PROTOCOL"))
        .unwrap_or_else(|_| "pompe".to_string())
        .to_lowercase();

    let node_id = env::var("NODE_ID")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<usize>()?;
    let node_least_id = env::var("NODE_LEAST_ID")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<usize>()?;
    let node_num = env::var("NODE_NUM")
        .unwrap_or_else(|_| "4".to_string())
        .parse::<usize>()?;

    let hosts = parse_node_hosts()?;
    let ids: Vec<usize> = (node_least_id..node_least_id + node_num).collect();
    let sleep_micros = env::var("ATTACK_SLEEP_MICROS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());

    sleep(Duration::from_millis(2000)).await; // wait for network to stabilize
    match protocol.as_str() {
        "pompe" => {
            warn!("🚀 Starting Pompe pure attacker (node {})", node_id);
            run_pompe_attacker(node_id, &ids, &hosts, sleep_micros).await?;
        }
        "smrol" => {
            warn!("🚀 Starting SMROL pure attacker (node {})", node_id);
            run_smrol_attacker(node_id, &ids, &hosts, sleep_micros).await?;
        }
        other => {
            return Err(format!(
                "Unknown ATTACK_PROTOCOL '{}'; expected pompe or smrol",
                other
            )
            .into());
        }
    }

    Ok(())
}

async fn run_pompe_attacker(
    node_id: usize,
    node_ids: &[usize],
    _hosts: &HashMap<String, String>,
    sleep_micros: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let network = Arc::new(PompeNetwork::new(node_id, node_ids.to_vec()));
    let config = load_pompe_config();
    let mut generator = TransactionGenerator::new();
    let mut sent: u64 = 0;

    loop {
        let tx = generator.generate_transaction();
        let pompe_tx = PompeTransaction {
            id: tx.id,
            from: tx.from.clone(),
            to: tx.to.clone(),
            amount: tx.amount,
            client_id: format!("attacker_{}", node_id),
            timestamp: tx.timestamp,
            nonce: tx.nonce,
        };
        let tx_hash = pompe_tx.hash();

        let msg = PompeMessage::Ordering1Request {
            tx_hash,
            transaction: pompe_tx,
            batch_size: config.batch_size,
            initiator_node_id: node_id,
        };

        if let Err(e) = network.broadcast_skip_self(msg).await {
            warn!("[pompe-attacker] broadcast failed: {}", e);
        }
        sent += 1;
        if sent % 10_000 == 0 {
            info!("[pompe-attacker] sent {} transactions", sent);
        }

        if let Some(us) = sleep_micros {
            sleep(Duration::from_micros(us)).await;
        }
    }
}

async fn run_smrol_attacker(
    node_id: usize,
    node_ids: &[usize],
    hosts: &HashMap<String, String>,
    sleep_micros: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut peer_addrs = HashMap::new();
    for id in node_ids {
        let key = format!("node{}", id);
        let ip = hosts
            .get(&key)
            .ok_or_else(|| format!("Missing host entry for {}", key))?;
        let port = 21000 + *id as u16;
        let addr: SocketAddr = format!("{}:{}", ip, port).parse()?;
        peer_addrs.insert(*id, addr);
    }

    let network = Arc::new(SmrolTcpNetwork::new(node_id, peer_addrs));
    let mut generator = TransactionGenerator::new();
    let mut sent: u64 = 0;
    let mut rng = rand::thread_rng();

    loop {
        let tx = generator.generate_transaction();
        let smrol_tx =
            SmrolTransaction::from_test_transaction(tx.clone(), format!("attacker_{}", node_id));
        let sequence = rng.gen_range(0u64, 1_000_000_000u64);
        let mut hasher = Sha256::new();
        hasher.update(tx.id.to_le_bytes());
        hasher.update(tx.from.as_bytes());
        hasher.update(tx.to.as_bytes());
        hasher.update(tx.amount.to_le_bytes());
        hasher.update(sequence.to_le_bytes());
        let tx_hash = format!("{:x}", hasher.finalize());

        let message = SmrolMessage::SeqRequest {
            tx_hash,
            transaction: smrol_tx,
            sender_id: node_id,
            sequence_number: sequence,
        };

        if let Err(e) = network.broadcast_skip_self(message).await {
            warn!("[smrol-attacker] broadcast failed: {}", e);
        }

        sent += 1;
        if sent % 10_000 == 0 {
            info!("[smrol-attacker] sent {} requests", sent);
        }

        if let Some(us) = sleep_micros {
            sleep(Duration::from_micros(us)).await;
        }
    }
}

fn parse_node_hosts() -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let hosts = env::var("NODE_HOSTS")
        .map_err(|_| "NODE_HOSTS env var is required (format: node0:1.2.3.4,node1:5.6.7.8)")?;
    let map = hosts
        .split(',')
        .filter_map(|entry| {
            let mut parts = entry.split(':');
            let name = parts.next()?.trim().to_string();
            let ip = parts.next()?.trim().to_string();
            Some((name, ip))
        })
        .collect::<HashMap<_, _>>();
    Ok(map)
}

fn setup_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .try_init();
}

struct TransactionGenerator {
    current_tx_id: u64,
    current_nonce: u64,
    accounts: Vec<String>,
    large_payload: Option<String>,
}

impl TransactionGenerator {
    fn new() -> Self {
        let accounts = vec![
            "alice".to_string(),
            "bob".to_string(),
            "charlie".to_string(),
            "david".to_string(),
            "eve".to_string(),
        ];
        let payload_bytes = env::var("ADVERSARY_PAYLOAD_BYTES")
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .filter(|size| *size > 0);
        let large_payload = payload_bytes.map(|size| {
            let mut payload = String::with_capacity(size);
            while payload.len() < size {
                let remaining = size - payload.len();
                let chunk = remaining.min(1024);
                payload.push_str(&"X".repeat(chunk));
            }
            payload
        });
        Self {
            current_tx_id: 0,
            current_nonce: 0,
            accounts,
            large_payload,
        }
    }

    fn generate_transaction(&mut self) -> TestTransaction {
        let mut rng = rand::thread_rng();
        let (from, to) = if let Some(payload) = &self.large_payload {
            let mut from_payload = payload.clone();
            from_payload.push_str(&format!("-{}", self.current_tx_id));
            (from_payload, payload.clone())
        } else {
            let from_idx = rng.gen_range(0, self.accounts.len());
            let mut to_idx = rng.gen_range(0, self.accounts.len());
            while to_idx == from_idx {
                to_idx = rng.gen_range(0, self.accounts.len());
            }
            (
                self.accounts[from_idx].clone(),
                self.accounts[to_idx].clone(),
            )
        };
        let amount = rng.gen_range(1, 100000);
        self.current_tx_id += 1;
        self.current_nonce += 1;
        TestTransaction {
            id: rng.gen_range(100_0001u64, 3_000_001u64),
            from,
            to,
            amount,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            nonce: self.current_nonce,
        }
    }
}
