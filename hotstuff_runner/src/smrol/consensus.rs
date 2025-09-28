use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::smrol::adapter::SmrolHotStuffAdapter;
use crate::smrol::finalization::OutputFinalization;
use crate::smrol::message::SmrolMessage;
use crate::smrol::network::{SmrolNetworkMessage, SmrolTcpNetwork};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info};
use uuid::Uuid;

/// Input to the consensus instance for epoch `e`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusInput {
    pub epoch: u64,
    pub m_e: Vec<TransactionEntry>,
    pub s_e: Vec<SequenceEntry>,
}

/// Entry in `M_e` (Algorithm 3, predicate \(Q(x)\) condition 2).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionEntry {
    pub vc_tx: Vec<u8>,
    pub s_tx: u64,
    pub sigma: Vec<u8>,
}

/// Entry in `S_e` (Algorithm 3, predicate \(Q(x)\) condition 3).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct SequenceEntry {
    pub j: usize,
    pub h_e_prime: u64,
    pub vc_j_h_e: Vec<u8>,
    pub sigma_j_h_e: Vec<u8>,
    pub s_tx: u64,
}

/// Vote message used to collect 2f+1 acknowledgements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusVote {
    pub epoch: u64,
    pub vote_signature: Vec<u8>,
    pub sender_id: usize,
}

#[derive(Debug, Clone)]
pub struct EpochState {
    pub m_e: Vec<TransactionEntry>,
    pub s_e: Vec<SequenceEntry>,
    pub h_e: u64,
    pub votes: HashMap<usize, Vec<u8>>,
    pub final_ledger: Vec<String>,
    pub t_e: HashSet<usize>,
    pub s_help: HashSet<(usize, u64)>,
    pub log_received: HashMap<usize, Vec<LogEntry>>,
}

impl EpochState {
    fn new() -> Self {
        Self {
            m_e: Vec::new(),
            s_e: Vec::new(),
            h_e: 0,
            votes: HashMap::new(),
            final_ledger: Vec::new(),
            t_e: HashSet::new(),
            s_help: HashSet::new(),
            log_received: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub vc_j: Vec<u8>,
    pub sigma_j: Vec<u8>,
}

pub struct Consensus {
    pub process_id: usize,
    pub n: usize,
    pub f: usize,
    pub k: usize,
    pub network: Arc<SmrolTcpNetwork>,
    pub signing_key: SigningKey,
    pub verifying_keys: HashMap<usize, VerifyingKey>,
    pub current_epoch: u64,
    pub mi: HashMap<u64, Vec<TransactionEntry>>,
    pub vc_ledger: HashSet<Vec<u8>>,
    pub pending_e: HashMap<u64, HashSet<Vec<u8>>>,
    pub epoch_states: HashMap<u64, EpochState>,
    hotstuff_adapter: Option<Arc<SmrolHotStuffAdapter>>,
}

impl Consensus {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        process_id: usize,
        n: usize,
        f: usize,
        network: Arc<SmrolTcpNetwork>,
        signing_key: SigningKey,
        verifying_keys: HashMap<usize, VerifyingKey>,
    ) -> Self {
        let k = std::cmp::max(1, 2 * f + 1);
        Self {
            process_id,
            n,
            f,
            k,
            network,
            signing_key,
            verifying_keys,
            current_epoch: 1,
            mi: HashMap::new(),
            vc_ledger: HashSet::new(),
            pending_e: HashMap::new(),
            epoch_states: HashMap::new(),
            hotstuff_adapter: None,
        }
    }

    pub fn set_hotstuff_adapter(&mut self, adapter: Arc<SmrolHotStuffAdapter>) {
        self.hotstuff_adapter = Some(adapter);
    }

    /// Execute Algorithm 3 for epoch `e` until `Consensus[e]` is invoked.
    pub async fn run_consensus(&mut self, epoch: u64) -> Result<(), String> {
        info!(
            "🏛️ [Consensus] node={} start epoch={}",
            self.process_id, epoch
        );

        if !self.should_start_consensus(epoch) {
            debug!(
                "[Consensus] node={} epoch={} waiting for more transactions",
                self.process_id, epoch
            );
            return Ok(());
        }

        let (m_i_e, s_i_e, h_e) = self.prepare_consensus_input(epoch)?;

        let mut epoch_state = self
            .epoch_states
            .remove(&epoch)
            .unwrap_or_else(EpochState::new);
        epoch_state.m_e = m_i_e.clone();
        epoch_state.s_e = s_i_e.clone();
        epoch_state.h_e = h_e;
        epoch_state.t_e.clear();
        epoch_state.s_help.clear();
        for entry in &s_i_e {
            epoch_state.t_e.insert(entry.j);
        }
        self.epoch_states.insert(epoch, epoch_state);

        self.invoke_consensus(epoch, m_i_e, s_i_e).await
    }

    fn should_start_consensus(&self, epoch: u64) -> bool {
        self.mi
            .get(&epoch)
            .map(|entries| entries.len() >= self.k)
            .unwrap_or(false)
    }

    fn prepare_consensus_input(
        &mut self,
        epoch: u64,
    ) -> Result<(Vec<TransactionEntry>, Vec<SequenceEntry>, u64), String> {
        let mut entries = self.mi.get(&epoch).cloned().unwrap_or_default();
        if entries.len() < self.k {
            return Err(format!("M_i for epoch {} has < {} entries", epoch, self.k));
        }

        entries.sort_by_key(|entry| entry.s_tx);
        let m_e: Vec<TransactionEntry> = entries.into_iter().take(self.k).collect();
        let h_e = m_e.iter().map(|entry| entry.s_tx).max().unwrap_or(0);

        // Placeholder: in a full implementation, collect 2f+1 outputs from PNFIFO-BC.
        let mut s_e = Vec::new();
        for node_id in 0..self.n {
            let seq_entry = SequenceEntry {
                j: node_id,
                h_e_prime: h_e,
                vc_j_h_e: format!("vc:{}:{}", node_id, h_e).into_bytes(),
                sigma_j_h_e: format!("sig:{}:{}", node_id, h_e).into_bytes(),
                s_tx: h_e,
            };
            s_e.push(seq_entry);
            if s_e.len() == 2 * self.f + 1 {
                break;
            }
        }

        debug!(
            "[Consensus] node={} epoch={} prepared M={} S={} h_e={}",
            self.process_id,
            epoch,
            m_e.len(),
            s_e.len(),
            h_e
        );

        Ok((m_e, s_e, h_e))
    }

    async fn invoke_consensus(
        &mut self,
        epoch: u64,
        m_e: Vec<TransactionEntry>,
        s_e: Vec<SequenceEntry>,
    ) -> Result<(), String> {
        info!(
            "[Consensus] node={} invoke epoch={}",
            self.process_id, epoch
        );

        let merkle_root = Self::compute_merkle_root(&m_e);
        let transactions: Vec<String> = m_e.iter().map(|entry| hex::encode(&entry.vc_tx)).collect();

        let message = SmrolMessage::ConsensusProposal {
            epoch,
            transactions,
            merkle_root,
            sender_id: self.process_id,
        };
        self.broadcast_consensus_message(message).await?;

        // In this prototype we treat local proposal as the decided value once enough votes arrive.
        // The vote handler will call `handle_consensus_output` with the stored state.
        // Broadcast own vote immediately.
        let vote = self.create_vote(epoch)?;
        self.broadcast_consensus_message(SmrolMessage::ConsensusVote {
            epoch,
            vote_signature: vote.vote_signature.clone(),
            sender_id: vote.sender_id,
        })
        .await?;

        let epoch_state = self
            .epoch_states
            .get_mut(&epoch)
            .ok_or_else(|| format!("epoch {} state missing after proposal broadcast", epoch))?;
        epoch_state
            .votes
            .insert(self.process_id, vote.vote_signature.clone());

        // Optimistically process the proposal locally.
        self.handle_consensus_output(epoch, m_e, s_e).await
    }

    pub async fn handle_consensus_output(
        &mut self,
        epoch: u64,
        m_e: Vec<TransactionEntry>,
        s_e: Vec<SequenceEntry>,
    ) -> Result<(), String> {
        info!(
            "[Consensus] node={} epoch={} output received",
            self.process_id, epoch
        );

        let state = self
            .epoch_states
            .entry(epoch)
            .or_insert_with(EpochState::new);
        state.m_e = m_e;
        state.s_e = s_e;
        state.h_e = state
            .m_e
            .iter()
            .map(|entry| entry.s_tx)
            .max()
            .unwrap_or(state.h_e);
        state.t_e = state.s_e.iter().map(|entry| entry.j).collect();
        state.s_help.clear();

        self.process_s_help_logic(epoch)?;

        let (need_help, help_snapshot, h_e_prime) = {
            let snapshot = self.epoch_states.get(&epoch).unwrap();
            (
                !snapshot.s_help.is_empty(),
                snapshot.s_help.clone(),
                snapshot.h_e,
            )
        };

        if need_help && h_e_prime > 0 {
            self.call_help_log(epoch, &help_snapshot, h_e_prime - 1)
                .await?;
        }

        self.process_final_ledger(epoch).await
    }

    fn process_s_help_logic(&mut self, epoch: u64) -> Result<(), String> {
        let (entries, h_e_prime) = {
            let state = self
                .epoch_states
                .get(&epoch)
                .ok_or_else(|| format!("missing epoch state {}", epoch))?;
            (state.s_e.clone(), state.h_e)
        };

        if h_e_prime <= 1 {
            return Ok(());
        }

        let target = h_e_prime.saturating_sub(1);
        let state = self.epoch_states.get_mut(&epoch).unwrap();
        for entry in entries {
            if entry.h_e_prime < target {
                state.s_help.insert((entry.j, entry.h_e_prime + 1));
            }
        }
        Ok(())
    }

    async fn call_help_log(
        &self,
        epoch: u64,
        s_help: &HashSet<(usize, u64)>,
        target_height: u64,
    ) -> Result<bool, String> {
        info!(
            "[Consensus] node={} epoch={} CallHelpLog targets={} height={}",
            self.process_id,
            epoch,
            s_help.len(),
            target_height
        );
        for (node_id, seq_no) in s_help {
            debug!(
                "[Consensus] help request for node={} seq={}",
                node_id, seq_no
            );
        }
        Ok(true)
    }

    async fn process_final_ledger(&mut self, epoch: u64) -> Result<(), String> {
        let (m_e, s_e, t_e) = {
            let state = self
                .epoch_states
                .get(&epoch)
                .ok_or_else(|| format!("missing epoch state {}", epoch))?;
            (state.m_e.clone(), state.s_e.clone(), state.t_e.clone())
        };

        let mut finalizer = OutputFinalization::new(
            self.process_id,
            self.f,
            Arc::clone(&self.network),
            self.signing_key.clone(),
        );
        let finalized = finalizer.finalize_epoch(epoch, m_e, s_e, t_e).await?;

        if let Some(state) = self.epoch_states.get_mut(&epoch) {
            state.final_ledger = finalized.clone();
        }

        if let Some(adapter) = &self.hotstuff_adapter {
            adapter.output_to_hotstuff(finalized.clone(), epoch);
        }

        self.current_epoch = self.current_epoch.max(epoch + 1);
        Ok(())
    }

    async fn wait_for_log(&mut self, epoch: u64, node: usize, up_to: u64) -> Result<(), String> {
        debug!(
            "[Consensus] waiting for LOG node={} up_to={} epoch={}",
            node, up_to, epoch
        );

        let log_entries: Vec<LogEntry> = (1..=up_to)
            .map(|idx| LogEntry {
                vc_j: format!("vc:{}:{}", node, idx).into_bytes(),
                sigma_j: format!("sig:{}:{}", node, idx).into_bytes(),
            })
            .collect();

        if let Some(state) = self.epoch_states.get_mut(&epoch) {
            state.log_received.insert(node, log_entries);
            Ok(())
        } else {
            Err(format!(
                "missing epoch state {} while waiting for logs",
                epoch
            ))
        }
    }

    fn extract_transaction(vc_tx: &[u8]) -> String {
        let prefix_len = std::cmp::min(8, vc_tx.len());
        format!("tx_{}", hex::encode(&vc_tx[..prefix_len]))
    }

    fn remove_from_mi(&mut self, vc_tx: &[u8]) {
        for entries in self.mi.values_mut() {
            entries.retain(|entry| entry.vc_tx != vc_tx);
        }
    }

    pub async fn handle_consensus_message(
        &mut self,
        sender_id: usize,
        message: SmrolMessage,
    ) -> Result<(), String> {
        match message {
            SmrolMessage::ConsensusProposal {
                epoch,
                transactions: _,
                merkle_root: _,
                sender_id: _,
            } => {
                debug!(
                    "[Consensus] received proposal epoch={} from {}",
                    epoch, sender_id
                );
                self.handle_consensus_proposal(sender_id, epoch).await
            }
            SmrolMessage::ConsensusVote {
                epoch,
                vote_signature,
                sender_id: _,
            } => {
                debug!(
                    "[Consensus] received vote epoch={} from {}",
                    epoch, sender_id
                );
                self.handle_consensus_vote(sender_id, epoch, vote_signature)
                    .await
            }
            _ => Ok(()),
        }
    }

    async fn handle_consensus_proposal(
        &mut self,
        _sender_id: usize,
        epoch: u64,
    ) -> Result<(), String> {
        let vote = self.create_vote(epoch)?;
        self.broadcast_consensus_message(SmrolMessage::ConsensusVote {
            epoch,
            vote_signature: vote.vote_signature,
            sender_id: vote.sender_id,
        })
        .await
    }

    async fn handle_consensus_vote(
        &mut self,
        sender_id: usize,
        epoch: u64,
        vote_signature: Vec<u8>,
    ) -> Result<(), String> {
        let state = self
            .epoch_states
            .entry(epoch)
            .or_insert_with(EpochState::new);
        state.votes.insert(sender_id, vote_signature);

        if state.votes.len() >= 2 * self.f + 1 {
            info!(
                "[Consensus] epoch={} reached quorum {}/{}",
                epoch,
                state.votes.len(),
                2 * self.f + 1
            );
            let m_e = state.m_e.clone();
            let s_e = state.s_e.clone();
            drop(state);
            self.handle_consensus_output(epoch, m_e, s_e).await?;
        }
        Ok(())
    }

    fn create_vote(&self, epoch: u64) -> Result<ConsensusVote, String> {
        let vote_payload = format!("vote_epoch_{}_node_{}", epoch, self.process_id);
        let signature = self.signing_key.sign(vote_payload.as_bytes()).to_bytes();
        Ok(ConsensusVote {
            epoch,
            vote_signature: signature.to_vec(),
            sender_id: self.process_id,
        })
    }

    async fn broadcast_consensus_message(&self, message: SmrolMessage) -> Result<(), String> {
        let network_msg = SmrolNetworkMessage {
            from_node_id: self.process_id,
            to_node_id: None,
            message,
            timestamp: Self::now_millis(),
            message_id: format!("consensus_{}_{}", self.process_id, Uuid::new_v4()),
        };
        self.network.send_message(network_msg).await
    }

    fn compute_merkle_root(entries: &[TransactionEntry]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        for entry in entries {
            hasher.update(&entry.vc_tx);
            hasher.update(entry.s_tx.to_le_bytes());
            hasher.update(&entry.sigma);
        }
        let digest = hasher.finalize();
        digest.into()
    }

    fn now_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    pub fn add_to_mi(&mut self, epoch: u64, entry: TransactionEntry) {
        self.mi.entry(epoch).or_default().push(entry);
    }

    pub fn get_current_epoch(&self) -> u64 {
        self.current_epoch
    }

    pub fn get_mi_size(&self, epoch: u64) -> usize {
        self.mi.get(&epoch).map(|m| m.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    async fn create_test_consensus(process_id: usize) -> Consensus {
        let signing_key = SigningKey::from_bytes(&[process_id as u8 + 1; 32]);
        let mut verifying_keys = HashMap::new();
        let mut peer_addrs = HashMap::new();

        for i in 0..4 {
            let key_bytes = [i as u8 + 1; 32];
            let key = SigningKey::from_bytes(&key_bytes);
            verifying_keys.insert(i, key.verifying_key());
            let addr: SocketAddr = format!("127.0.0.1:{}", 21000 + i).parse().unwrap();
            peer_addrs.insert(i, addr);
        }

        let network = Arc::new(SmrolTcpNetwork::new(process_id, peer_addrs));

        Consensus::new(process_id, 4, 1, network, signing_key, verifying_keys)
    }

    #[tokio::test]
    async fn test_consensus_initialization() {
        let consensus = create_test_consensus(0).await;
        assert_eq!(consensus.current_epoch, 1);
        assert_eq!(consensus.process_id, 0);
        assert_eq!(consensus.n, 4);
        assert_eq!(consensus.f, 1);
    }

    #[tokio::test]
    async fn test_add_to_mi() {
        let mut consensus = create_test_consensus(0).await;

        let entry = TransactionEntry {
            vc_tx: b"test_vc".to_vec(),
            s_tx: 123,
            sigma: b"test_sig".to_vec(),
        };

        consensus.add_to_mi(1, entry);
        assert_eq!(consensus.get_mi_size(1), 1);
    }

    #[tokio::test]
    async fn test_call_help_log() {
        let consensus = create_test_consensus(0).await;
        let mut s_help = HashSet::new();
        s_help.insert((1, 5));
        s_help.insert((2, 7));

        let result = consensus.call_help_log(1, &s_help, 10).await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_sequence_entry_structure() {
        let entry = SequenceEntry {
            j: 1,
            h_e_prime: 100,
            vc_j_h_e: b"vc_1_100".to_vec(),
            sigma_j_h_e: b"sig_1_100".to_vec(),
            s_tx: 100,
        };

        assert_eq!(entry.j, 1);
        assert_eq!(entry.h_e_prime, 100);
        assert_eq!(entry.vc_j_h_e, b"vc_1_100");
        assert_eq!(entry.sigma_j_h_e, b"sig_1_100");
    }
}
