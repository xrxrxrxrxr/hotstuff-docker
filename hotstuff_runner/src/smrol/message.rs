use crate::event::TestTransaction;
use crate::smrol::consensus::{SequenceEntry, TransactionEntry};
use serde::{Deserialize, Serialize};

// 统一的SMROL消息枚举
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum SmrolMessage {
    // === 算法1: PNFIFO-BC 消息 ===
    PnfifoProposal {
        sender_id: usize,
        slot: u64,
        value: Vec<u8>,
    },
    PnfifoVote {
        leader_id: usize,
        sender_id: usize,
        slot: u64,
        signature_share: Vec<u8>,
    },
    PnfifoFinal {
        leader_id: usize,
        sender_id: usize,
        slot: u64,
        value: Vec<u8>,
        combined_signature: Vec<u8>,
    },

    // === 算法2: Transaction Sequencing 消息 ===
    SeqRequest {
        tx_hash: String,
        transaction: SmrolTransaction,
        sender_id: usize,
        sequence_number: u64,
    },
    SeqResponse {
        vc: Vec<u8>,
        signature_share: Vec<u8>,
        sender_id: usize,
        sequence_number: u64,
    },
    SeqOrder {
        vc: Vec<u8>,
        responses: Vec<(usize, u64, Vec<u8>)>,
        sender_id: usize,
    },
    SeqMedian {
        vc: Vec<u8>,
        median_sequence: u64,
        proof: Vec<u8>,
        sender_id: usize,
    },
    SeqFinal {
        vc: Vec<u8>,
        final_sequence: u64,
        combined_signature: Vec<u8>,
        sender_id: usize,
        tx_id: u64,
    },

    // === 算法3: Consensus 消息 ===
    ConsensusProposal {
        epoch: u64,
        m_e: Vec<TransactionEntry>,
        s_e: Vec<SequenceEntry>,
        transactions: Vec<String>,
        merkle_root: [u8; 32],
        sender_id: usize,
    },
    ConsensusVote {
        epoch: u64,
        vote_signature: Vec<u8>,
        sender_id: usize,
    },

    // === 通用消息 ===
    Warmup, // 连接预热消息
}

impl Default for SmrolMessage {
    fn default() -> Self {
        Self::Warmup
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SmrolTransaction {
    pub id: u64,
    pub from: String,
    pub to: String,
    pub amount: u64,
    pub client_id: String,
    pub timestamp: u64,
    pub nonce: u64,
}

impl SmrolTransaction {
    pub fn from_test_transaction(tx: TestTransaction, client_id: String) -> Self {
        Self {
            id: tx.id,
            from: tx.from,
            to: tx.to,
            amount: tx.amount,
            client_id,
            timestamp: tx.timestamp,
            nonce: tx.nonce,
        }
    }

    pub fn to_hotstuff_format(&self, final_sequence: u64) -> String {
        format!(
            "smrol:{}:{}:{}->{}:{}",
            final_sequence, self.id, self.from, self.to, self.amount
        )
    }
    // 不带final sequence
    // pub fn to_hotstuff_format(&self, final_sequence: u64) -> String {
    //     format!(
    //         "smrol:{}:{}->{}:{}",
    //         self.id, self.from, self.to, self.amount
    //     )
    // }
}
