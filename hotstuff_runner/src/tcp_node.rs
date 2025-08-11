// crate/src/node.rs
use hotstuff_rs::{
    block_tree::{pluggables::KVGet, variables::HIGHEST_COMMITTED_BLOCK}, events::*, replica::{self, Configuration, Replica, ReplicaSpec}, types::{
        crypto_primitives::VerifyingKey,
        data_types::{BufferSize, ChainID, EpochLength, ViewNumber, Data},
        block::Block,
        update_sets::{AppStateUpdates, ValidatorSetUpdates},
        validator_set::{ValidatorSet, ValidatorSetState},
    }
};
use crate::{
    app::TestApp,
    network::NodeNetwork,
    tcp_network::TcpNetwork,
    kv_store::MemoryKVStore,
};
use std::time::Duration;
use std::sync::{Arc, Mutex};
use ed25519_dalek::SigningKey;
// use log::info;
use tracing::info;

pub struct Node {
    verifying_key: VerifyingKey,
    replica: Replica<MemoryKVStore>,
    node_id: usize,
    // 添加对应用的引用以支持交易提交
    // app_handle: Arc<Mutex<TestApp>>,
    tx_queue: Arc<Mutex<Vec<String>>>,  // 新增交易队列
}

impl Node {
    /// 按照hotstuff_rs官方模式创建Node
    pub fn new(
        node_id: usize,  // 添加NodeID参数
        keypair: SigningKey,
        network: TcpNetwork,    // 使用TcpNetwork替代NodeNetwork
        init_app_state_updates: AppStateUpdates,
        init_validator_set_updates: ValidatorSetUpdates,
        tx_queue: Arc<Mutex<Vec<String>>>,  // 新增参数：外部交易队列
    ) -> Self {
        let verifying_key: VerifyingKey = keypair.verifying_key().into();
        
        info!("创建Node，验证密钥: {:?}", verifying_key.to_bytes()[0..8].to_vec());
        
        // 1. 从更新构造验证者集合
        let mut initial_validator_set = ValidatorSet::new();
        initial_validator_set.apply_updates(&init_validator_set_updates);
        
        info!("Node验证者集合: {} 个验证者，总权力: {}", 
             initial_validator_set.len(), 
             initial_validator_set.total_power().int());
        
        // 2. 创建验证者集合状态
        let validator_set_state = ValidatorSetState::new(
            initial_validator_set.clone(),
            initial_validator_set.clone(),
            None,
            true, // is_genesis
        );
        
        // 3. 创建KV存储
        let kv_store = MemoryKVStore::new();
        
        // 4. 初始化副本存储
        Replica::initialize(
            kv_store.clone(),
            init_app_state_updates,
            validator_set_state,
        );
        
        // 5. 创建应用程序并保存引用
        // let app = TestApp::new(format!("node-{:?}", verifying_key.to_bytes()[0..4].to_vec()));
        let app = TestApp::new(
            node_id,
            tx_queue.clone()
        );
        // let app_handle = Arc::new(Mutex::new(app.clone()));
        
        // 6. 创建配置 - 使用与官方完全相同的参数
        let config = Configuration::builder()
            .me(keypair)
            .chain_id(ChainID::new(0))
            .block_sync_request_limit(10)
            .block_sync_server_advertise_time(Duration::new(10, 0))      // 官方: 10秒
            .block_sync_response_timeout(Duration::new(3, 0))            // 官方: 3秒
            .block_sync_blacklist_expiry_time(Duration::new(10, 0))      // 官方: 10秒
            .block_sync_trigger_min_view_difference(2)                   // 官方: 2
            .block_sync_trigger_timeout(Duration::new(60, 0))            // 官方: 60秒
            .progress_msg_buffer_capacity(BufferSize::new(1024))
            .epoch_length(EpochLength::new(50))                          // 官方: 50
            .max_view_time(Duration::from_millis(2000))                  // 官方: 2000ms
            .log_events(false)                                           // 官方: false
            .build();

        let kv_clone_commit = kv_store.clone();
        let kv_clone_insert = kv_store.clone();
        let kv_clone_receive = kv_store.clone();

        // 7. 启动副本 - 添加详细的事件处理器（类似官方）
        let replica = ReplicaSpec::builder()
            .app(app)
            .network(network)
            .kv_store(kv_store)
            .configuration(config)
            // === 最关键的事件 ===
            .on_start_view({
                move |event| {
                    let msg = format!("🚀 Node {} 开始View {}", node_id, event.view);
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_propose({
                move |event| {
                    let msg = format!(
                        "📤 Node {} 提议区块，View: {}, 高度: {:?}, hash: {:?}",
                        node_id,
                        event.proposal.view,
                        event.proposal.block.height,
                        event.proposal.block.hash
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_proposal({
                move |event| {
                    let block = &event.proposal.block;
                    let tx_count = if block.data.vec().len() >= 2 {
                        let tx_count_bytes = block.data.vec()[1].bytes();
                        if tx_count_bytes.len() >= 4 {
                            let mut bytes = [0u8; 4];
                            bytes.copy_from_slice(&tx_count_bytes[0..4]);
                                    u32::from_le_bytes(bytes)
                                } else {
                                    0
                                }
                            } else {
                                0
                            };
                    let msg = format!(
                        "📥 Node {} 接收提议, View: {}, TxCount: {}, Hash: {:?}",
                        node_id,
                        event.proposal.view,
                        tx_count,
                        event.proposal.block.hash
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            // .on_receive_proposal({
            //     move |event| {
            //         let proposal = &event.proposal;
            //         let block = &proposal.block;
            //         let block_hash = block.hash;
            //         let view = proposal.view;
            //         let height = block.height;
                    
            //         info!("📥 Node {} [on_receive_proposal] 区块分析开始:", node_id);
            //         info!("Node {}   🔢 View: {}, Height: {}", node_id, view, height.int());
            //         info!("Node {}   🔐 哈希: {:?}", node_id, &block_hash);
                    
            //         // 详细检查 data 字段
            //         let data_len = block.data.vec().len();
            //         info!("Node {}   📦 接收时 data.len(): {}", node_id, data_len);
                    
            //         let tx_count = if data_len >= 2 {
            //             let data_vec = block.data.vec();
            //             info!("Node {}   📦 data_vec.len(): {}", node_id, data_vec.len());
                        
            //             // 显示前几项的详细信息
            //             for (i, datum) in data_vec.iter().enumerate().take(std::cmp::min(5, data_vec.len())) {
            //                 let bytes = datum.bytes();
            //                 info!("Node {}[{}]: {} 字节", node_id, i, bytes.len());
                            
            //                 if i == 0 && bytes.len() >= 8 {
            //                     let mut view_bytes = [0u8; 8];
            //                     view_bytes.copy_from_slice(&bytes[0..8]);
            //                     let view_num = u64::from_le_bytes(view_bytes);
            //                     info!("Node {}       -> 视图号: {}", node_id, view_num);
            //                 } else if i == 1 && bytes.len() >= 4 {
            //                     let mut count_bytes = [0u8; 4];
            //                     count_bytes.copy_from_slice(&bytes[0..4]);
            //                     let count = u32::from_le_bytes(count_bytes);
            //                     info!("Node {}       -> 交易计数: {}", node_id, count);
            //                 } else if i >= 2 {
            //                     let content = String::from_utf8_lossy(bytes);
            //                     let preview = if content.len() > 20 {
            //                         format!("{}...", &content[0..20])
            //                     } else {
            //                         content.to_string()
            //                     };
            //                     info!("Node {}       -> 交易: {}", node_id, preview);
            //                 }
            //             }
                        
            //             // 解析交易计数
            //             if let Some(tx_count_datum) = data_vec.get(1) {
            //                 let datum_bytes = tx_count_datum.bytes();
            //                 if datum_bytes.len() >= 4 {
            //                     let mut tx_count_bytes = [0u8; 4];
            //                     tx_count_bytes.copy_from_slice(&datum_bytes[0..4]);
            //                     u32::from_le_bytes(tx_count_bytes)
            //                 } else { 0 }
            //             } else { 0 }
            //         } else {
            //             info!("Node {}   ⚠️ data_len < 2, 无法解析交易计数", node_id);
            //             0
            //         };
                    
            //         info!("Node {}   🎯 解析的交易计数: {}", node_id, tx_count);
                    
            //         // 测试序列化/反序列化（这是关键测试）
            //         info!("Node {} [序列化测试] 开始...", node_id);
                    
            //         // 方法1：测试 Data 的序列化
            //         use borsh::{BorshSerialize, BorshDeserialize};
                    
            //         match block.data.try_to_vec() {
            //             Ok(serialized_data) => {
            //                 info!("Node {}   ✅ Data 序列化成功: {} 字节", node_id, serialized_data.len());
                            
            //                 // 尝试反序列化
            //                 match Data::try_from_slice(&serialized_data) {
            //                     Ok(deserialized_data) => {
            //                         let deserialized_len = deserialized_data.len().int();
            //                         info!("Node {}   ✅ Data 反序列化成功: data.len() = {}", node_id, deserialized_len);
                                    
            //                         if deserialized_len as usize != data_len {
            //                             info!("Node {}   ❌ 序列化前后长度不一致！{} -> {}", node_id, data_len, deserialized_len);
            //                         } else {
            //                             info!("Node {}   ✅ 序列化前后长度一致", node_id);
            //                         }
            //                     },
            //                     Err(e) => {
            //                         info!("Node {}   ❌ Data 反序列化失败: {:?}", node_id, e);
            //                     }
            //                 }
            //             },
            //             Err(e) => {
            //                 info!("Node {}   ❌ Data 序列化失败: {:?}", node_id, e);
            //             }
            //         }
                    
            //         // 方法2：测试整个 Block 的序列化
            //         match block.try_to_vec() {
            //             Ok(serialized_block) => {
            //                 info!("Node {}   ✅ Block 序列化成功: {} 字节", node_id, serialized_block.len());
                            
            //                 match Block::try_from_slice(&serialized_block) {
            //                     Ok(deserialized_block) => {
            //                         let deserialized_data_len = deserialized_block.data.len().int();
            //                         info!("Node {}   ✅ Block 反序列化成功: data.len() = {}", node_id, deserialized_data_len);
                                    
            //                         if deserialized_data_len as usize != data_len {
            //                             info!("Node {}   ❌ Block序列化前后data长度不一致！{} -> {}", node_id, data_len, deserialized_data_len);
            //                         } else {
            //                             info!("Node {}   ✅ Block序列化前后data长度一致", node_id);
            //                         }
                                    
            //                         // 验证哈希是否一致
            //                         if deserialized_block.hash == block.hash {
            //                             info!("Node {}   ✅ Block序列化前后哈希一致", node_id);
            //                         } else {
            //                             info!("Node {}   ❌ Block序列化前后哈希不一致", node_id);
            //                         }
            //                     },
            //                     Err(e) => {
            //                         info!("Node {}   ❌ Block 反序列化失败: {:?}", node_id, e);
            //                     }
            //                 }
            //             },
            //             Err(e) => {
            //                 info!("Node {}   ❌ Block 序列化失败: {:?}", node_id, e);
            //             }
            //         }
                    
            //         // // 延迟检查KV存储（保持之前的逻辑）
            //         // let kv_clone_check = kv_clone_receive.clone();
            //         // let check_node_id = node_id;
            //         // let check_hash = block_hash;
            //         // let expected_tx_count = tx_count;
                    
            //         // tokio::spawn(async move {
            //         //     tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        
            //         //     info!("🔍 Node {} [KV检查] 检查存储结果:", check_node_id);
            //         //     match kv_clone_check.block(&check_hash) {
            //         //         Ok(Some(stored_block)) => {
            //         //             let stored_data_len = stored_block.data.len().int();
            //         //             info!("Node {}   📦 KV中 data.len(): {}", check_node_id, stored_data_len);
                                
            //         //             if stored_data_len == 0 && expected_tx_count > 0 {
            //         //                 info!("Node {}   ❌ 数据在KV存储中丢失！期望:{}, 实际:0", check_node_id, expected_tx_count);
            //         //             } else if stored_data_len > 0 {
            //         //                 // 验证KV中的数据
            //         //                 let stored_data_vec = stored_block.data.vec();
            //         //                 let stored_tx_count = if stored_data_vec.len() >= 2 {
            //         //                     if let Some(tx_count_datum) = stored_data_vec.get(1) {
            //         //                         let datum_bytes = tx_count_datum.bytes();
            //         //                         if datum_bytes.len() >= 4 {
            //         //                             let mut tx_count_bytes = [0u8; 4];
            //         //                             tx_count_bytes.copy_from_slice(&datum_bytes[0..4]);
            //         //                             u32::from_le_bytes(tx_count_bytes)
            //         //                         } else { 0 }
            //         //                     } else { 0 }
            //         //                 } else { 0 };
                                    
            //         //                 info!("Node {}   🎯 KV中解析的交易数: {}", check_node_id, stored_tx_count);
                                    
            //         //                 if stored_tx_count == expected_tx_count {
            //         //                     info!("Node {}   ✅ KV存储数据完整", check_node_id);
            //         //                 } else {
            //         //                     info!("Node {}   ❌ KV存储数据不完整: 期望{}, 实际{}", check_node_id, expected_tx_count, stored_tx_count);
            //         //                 }
            //         //             }
            //         //         },
            //         //         Ok(None) => {
            //         //             info!("Node {}   ❌ KV中未找到区块", check_node_id);
            //         //         },
            //         //         Err(e) => {
            //         //             info!("Node {}   ❌ KV访问错误: {:?}", check_node_id, e);
            //         //         }
            //         //     }
            //         // });
                    
            //         let msg = format!(
            //             "📥 Node {} 接收提议, View: {}, TxCount: {}, Hash: {:?}",
            //             node_id, view, tx_count, &block_hash.bytes()[0..8]
            //         );
            //         crate::log_node(node_id, log::Level::Info, &msg);
            //     }
            // })
            .on_phase_vote({
                move |event| {
                    let msg = format!(
                        "🗳️ Node {} 阶段投票，View: {}, 阶段: {:?}",
                        node_id,
                        event.vote.view,
                        event.vote.phase
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_phase_vote({
                move |event| {
                    let msg = format!(
                        "📨 Node {} 接收投票，来源: {:?}, View: {}, 阶段: {:?}",
                        node_id,
                        event.origin.to_bytes()[0..4].to_vec(),
                        event.phase_vote.view,
                        event.phase_vote.phase
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_collect_pc({
                move |event| {
                    let msg = format!(
                        "🎯 Node {} 收集PC，View: {}, 签名数: {}",
                        node_id,
                        event.phase_certificate.view,
                        event.phase_certificate.signatures.iter().filter(|sig| sig.is_some()).count()
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            // 在 on_commit_block 中添加与 on_insert_block 相同的详细检查
            // .on_commit_block({
            //     move |event| {
            //         let block_hash = event.block;
                    
            //         info!("💎 Node {} [commit test] 开始测试", node_id);
                    
            //         // 1. 使用原版 block() 函数（有bug的版本）
            //         match kv_clone_commit.block(&block_hash) {
            //             Ok(Some(block)) => {
            //                 let original_data_len = block.data.len().int();
            //                 info!("Node {} [原版] block() 返回 data.len(): {}", node_id, original_data_len);
            //             },
            //             Ok(None) => {
            //                 info!("Node {} [原版] block() 返回 None", node_id);
            //             },
            //             Err(e) => {
            //                 info!("Node {} [原版] block() 错误: {:?}", node_id, e);
            //             }
            //         }
                    
            //         // 2. 手动修复版本的 block_data
            //         info!("Node {} [手动修复] 开始手动重建区块数据...", node_id);
                    
            //         let fixed_block_result = {
            //             // 获取其他组件
            //             let height = kv_clone_commit.block_height(&block_hash);
            //             let justify = kv_clone_commit.block_justify(&block_hash);
            //             let data_hash = kv_clone_commit.block_data_hash(&block_hash);
                        
            //             // 手动修复的 block_data
            //             let fixed_data = match kv_clone_commit.block_data_len(&block_hash) {
            //                 Ok(Some(data_len)) => {
            //                     let mut data_vec = Vec::new();
            //                     let mut all_found = true;
                                
            //                     for i in 0..data_len.int() {
            //                         match kv_clone_commit.block_datum(&block_hash, i as u32) {
            //                             Some(datum) => data_vec.push(datum),
            //                             None => {
            //                                 all_found = false;
            //                                 break;
            //                             }
            //                         }
            //                     }
                                
            //                     if all_found {
            //                         Some(Data::new(data_vec))
            //                     } else {
            //                         None
            //                     }
            //                 },
            //                 _ => None
            //             };
                        
            //             // 组装完整区块
            //             match (height, justify, data_hash, fixed_data) {
            //                 (Ok(Some(h)), Ok(j), Ok(Some(dh)), Some(d)) => {
            //                     Some(Block {
            //                         height: h,
            //                         hash: block_hash,
            //                         justify: j,
            //                         data_hash: dh,
            //                         data: d,
            //                     })
            //                 },
            //                 _ => None
            //             }
            //         };
                    
            //         // 3. 比较结果
            //         match fixed_block_result {
            //             Some(fixed_block) => {
            //                 let fixed_data_len = fixed_block.data.len().int();
            //                 let height = fixed_block.height.int();
                            
            //                 info!("Node {} [手动修复] 成功重建区块:", node_id);
            //                 info!("Node {}   Height: {},  data.len(): {}", 
            //                     node_id, height, fixed_data_len);
                            
            //                 if fixed_data_len > 0 {
            //                     // 解析交易数量
            //                     let tx_count = if fixed_data_len >= 2 {
            //                         let data_vec = fixed_block.data.vec();
            //                         if let Some(tx_count_datum) = data_vec.get(1) {
            //                             let datum_bytes = tx_count_datum.bytes();
            //                             if datum_bytes.len() >= 4 {
            //                                 let mut tx_count_bytes = [0u8; 4];
            //                                 tx_count_bytes.copy_from_slice(&datum_bytes[0..4]);
            //                                 u32::from_le_bytes(tx_count_bytes)
            //                             } else { 0 }
            //                         } else { 0 }
            //                     } else { 0 };
                                
            //                     info!("Node {} [手动修复] 🎯 交易数量: {}", node_id, tx_count);
                                
            //                     // 主要的成功日志
            //                     let msg = format!(
            //                         "💎 Node {} 提交区块(修复版) - Height: {},  交易数: {}, Hash: {}",
            //                         node_id, height, tx_count, fixed_block.hash()
            //                     );
            //                     crate::log_node(node_id, log::Level::Info, &msg);
            //                 } else {
            //                     info!("Node {} [手动修复] ⚠️ 修复后仍然没有数据", node_id);
            //                 }
            //             },
            //             None => {
            //                 info!("Node {} [手动修复] ❌ 手动修复失败", node_id);
            //             }
            //         }
            //     }
            // })
            // .on_commit_block({
                
            //     move |event| {
            //         let block_hash = event.block;
                    
            //         info!("💎 Node {} [block_data调试] 开始", node_id);
            //         info!("Node {}   🔐 区块哈希: {:?}", node_id, &block_hash.bytes()[0..8]);
                    
            //         // 1. 先确认组件都存在
            //         let data_len_result = kv_clone_commit.block_data_len(&block_hash);
            //         match data_len_result {
            //             Ok(Some(data_len)) => {
            //                 info!("Node {}   📏 确认 data_len: {}", node_id, data_len.int());
                            
            //                 // 2. 确认前几个 Datum 存在
            //                 for i in 0..std::cmp::min(3, data_len.int()) {
            //                     match kv_clone_commit.block_datum(&block_hash, i as u32) {
            //                         Some(datum) => {
            //                             info!("Node {}   ✅ 确认 datum[{}]: {} 字节", node_id, i, datum.bytes().len());
            //                         },
            //                         None => {
            //                             info!("Node {}   ❌ datum[{}] 丢失", node_id, i);
            //                         }
            //                     }
            //                 }
                            
            //                 // 3. 现在调用 block_data() 函数并详细跟踪
            //                 info!("Node {} [block_data调试] 开始调用 block_data()...", node_id);
                            
            //                 match kv_clone_commit.block_data(&block_hash) {
            //                     Ok(Some(data)) => {
            //                         let result_len = data.len().int();
            //                         info!("Node {}   📦 block_data() 成功: data.len() = {}", node_id, result_len);
                                    
            //                         if result_len != data_len.int() as u32 {
            //                             info!("Node {}   ❌ 长度不匹配！期望: {}, 实际: {}", 
            //                                 node_id, data_len.int(), result_len);
            //                         }
                                    
            //                         // 验证重建的数据
            //                         if result_len > 0 {
            //                             let data_vec = data.vec();
            //                             info!("Node {}   📊 重建数据验证: vec.len() = {}", node_id, data_vec.len());
                                        
            //                             for (i, datum) in data_vec.iter().enumerate().take(3) {
            //                                 info!("Node {}     [{}]: {} 字节", node_id, i, datum.bytes().len());
            //                             }
            //                         }
            //                     },
            //                     Ok(None) => {
            //                         info!("Node {}   ❌ block_data() 返回 None", node_id);
            //                     },
            //                     Err(e) => {
            //                         info!("Node {}   ❌ block_data() 错误: {:?}", node_id, e);
            //                     }
            //                 }
                            
            //                 // 4. 手动重建 Data 来对比
            //                 info!("Node {} [手动重建] 尝试手动重建 Data...", node_id);
                            
            //                 let mut manual_data_vec = Vec::new();
            //                 let mut all_found = true;
                            
            //                 for i in 0..data_len.int() {
            //                     match kv_clone_commit.block_datum(&block_hash, i as u32) {
            //                         Some(datum) => {
            //                             manual_data_vec.push(datum);
            //                         },
            //                         None => {
            //                             info!("Node {}   ❌ 手动重建失败：datum[{}] 丢失", node_id, i);
            //                             all_found = false;
            //                             break;
            //                         }
            //                     }
            //                 }
                            
            //                 if all_found {
            //                     let manual_data = Data::new(manual_data_vec);
            //                     info!("Node {}   ✅ 手动重建成功: data.len() = {}", node_id, manual_data.len().int());
                                
            //                     // 对比手动重建和 block_data() 的结果
            //                     match kv_clone_commit.block_data(&block_hash) {
            //                         Ok(Some(auto_data)) => {
            //                             if manual_data.len() == auto_data.len() {
            //                                 info!("Node {}   ✅ 手动重建与 block_data() 长度一致", node_id);
            //                             } else {
            //                                 info!("Node {}   ❌ 长度不一致！手动: {}, 自动: {}", 
            //                                     node_id, manual_data.len().int(), auto_data.len().int());
            //                             }
            //                         },
            //                         _ => {
            //                             info!("Node {}   ❌ block_data() 失败但手动重建成功", node_id);
            //                         }
            //                     }
            //                 } else {
            //                     info!("Node {}   ❌ 手动重建失败", node_id);
            //                 }
                            
            //             },
            //             Ok(None) => {
            //                 info!("Node {}   ❌ block_data_len 为 None", node_id);
            //             },
            //             Err(e) => {
            //                 info!("Node {}   ❌ block_data_len 错误: {:?}", node_id, e);
            //             }
            //         }
            //     }
            // })
            .on_commit_block({
                move |event| {
                    let block_hash = event.block;
                    let commit_time = event.timestamp;
                    
                    match kv_clone_commit.block(&block_hash) {
                        Ok(Some(block)) => {
                            let height = block.height.int();
                            // let view = block.view;
                            
                            // 关键修正：正确解析交易数量
                            let tx_count = if block.data.vec().len() >= 2 {
                            let tx_count_bytes = block.data.vec()[1].bytes();
                            if tx_count_bytes.len() >= 4 {
                                let mut bytes = [0u8; 4];
                                bytes.copy_from_slice(&tx_count_bytes[0..4]);
                                        u32::from_le_bytes(bytes)
                                    } else {
                                        0
                                    }
                                } else {
                                    0
                                };
                            
                            // 详细调试信息
                            let total_data_items = block.data.len().int();
                            let raw_data_len = block.data.len(); // 这个可能是字节数或项目数
                            
                            // info!("Node {} [on_commit_block] 详细分析:", node_id);
                            // info!("Node {}   📏 区块高度: {}", node_id, height);
                            // info!("Node {}   📦 data.len(): {}", node_id, total_data_items);
                            // info!("Node {}   🎯 解析交易数: {}", node_id, tx_count);
                            
                            // 验证数据结构
                            if block.data.len().int() >= 2 {
                                let data_vec = block.data.vec();
                                // info!("Node {}   📊 数据项详情:", node_id);
                                // info!("Node {}    总项目: {}", node_id, data_vec.len());

                                // 显示前几个数据项的信息
                                for (i, datum) in data_vec.iter().enumerate().take(5) {
                                    let bytes_len = datum.bytes().len();
                                    let preview = if i == 0 {
                                        // 视图号
                                        if bytes_len >= 8 {
                                            let mut view_bytes = [0u8; 8];
                                            view_bytes.copy_from_slice(&datum.bytes()[0..8]);
                                            let view_num = u64::from_le_bytes(view_bytes);
                                            format!("View#{}", view_num)
                                        } else {
                                            format!("视图号({}字节)", bytes_len)
                                        }
                                    } else if i == 1 {
                                        // 交易计数
                                        if bytes_len >= 4 {
                                            let mut count_bytes = [0u8; 4];
                                            count_bytes.copy_from_slice(&datum.bytes()[0..4]);
                                            let count = u32::from_le_bytes(count_bytes);
                                            format!("TxCount#{}", count)
                                        } else {
                                            format!("交易计数({}字节)", bytes_len)
                                        }
                                    } else {
                                        // 交易数据
                                        let tx_content = String::from_utf8_lossy(datum.bytes());
                                        if tx_content.len() > 30 {
                                            format!("Tx: {}...", &tx_content[0..30])
                                        } else {
                                            format!("Tx: {}", tx_content)
                                        }
                                    };
                                    
                                    info!("Node {}     [{}]: {} ({}字节)", node_id, i, preview, bytes_len);
                                }
                                
                                if data_vec.len() > 5 {
                                    info!("Node {}     ... 还有 {} 项", node_id, data_vec.len() - 5);
                                }
                            }
                            
                            // 主要的统计日志
                            let msg = format!(
                                "💎 Node {} 提交区块 - Height: {}, 交易数: {}, 数据项: {}, hash = {}",
                                node_id, height, tx_count, total_data_items, event.block
                            );
                            crate::log_node(node_id, log::Level::Info, &msg);
                            
                        },
                        Ok(None) => {
                            let msg = format!(
                                "💎 Node {} 提交区块 - 哈希: {:?} (区块详情未找到)",
                                node_id, &block_hash.bytes()[0..8]
                            );
                            crate::log_node(node_id, log::Level::Warn, &msg);
                        },
                        Err(e) => {
                            let msg = format!(
                                "💎 Node {} 提交区块 - 哈希: {:?} (读取错误: {:?})",
                                node_id, &block_hash.bytes()[0..8], e
                            );
                            crate::log_node(node_id, log::Level::Error, &msg);
                        }
                    }
                }
            })
            .on_update_highest_pc({
                move |event| {
                    let msg = format!(
                        "📈 Node {} 更新最高PC，View: {}, 阶段: {:?}",
                        node_id,
                        event.highest_pc.view,
                        event.highest_pc.phase
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            // === 超时和View变更事件 ===
            .on_view_timeout({
                move |event| {
                    let msg = format!(
                        "⏱️ Node {} View {} 超时！",
                        node_id,
                        event.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_timeout_vote({
                move |event| {
                    let msg = format!(
                        "⏰ Node {} 发送超时投票，View: {}",
                        node_id,
                        event.timeout_vote.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_timeout_vote({
                move |event| {
                    let msg = format!(
                        "📩 Node {} 接收超时投票，来源: {:?}, View: {}",
                        node_id,
                        event.origin.to_bytes()[0..4].to_vec(),
                        event.timeout_vote.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_collect_tc({
                move |event| {
                    let msg = format!(
                        "🔄 Node {} 收集TC，View: {}",
                        node_id,
                        event.timeout_certificate.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_advance_view({
                move |event| {
                    let msg = format!(
                        "⏭️ Node {} 推进View到: {}",
                        node_id,
                        event.advance_view.progress_certificate.view()
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_new_view({
                move |event| {
                    let msg = format!(
                        "🆕 Node {} 发送新View消息，View: {}",
                        node_id,
                        event.new_view.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            .on_receive_new_view({
                move |event| {
                    let msg = format!(
                        "📬 Node {} 接收新View消息，来源: {:?}, View: {}",
                        node_id,
                        event.origin.to_bytes()[0..4].to_vec(),
                        event.new_view.view
                    );
                    crate::log_node(node_id, log::Level::Info, &msg);
                }
            })
            // Hotstuff trouble shooting events
            // .on_insert_block({
            //     move |event| {
            //         let block_hash = event.block.hash;
                    
            //         info!("🔗 Node {} [on_insert_block] 开始详细分析", node_id);
            //         info!("Node {} [insert check]  🔐 区块哈希: {:?}", node_id, &block_hash);
                    
            //         // 1. 检查事件中的原始区块数据
            //         let event_data_len = event.block.data.len().int();
            //         info!("Node {} [insert check]  📦 事件中 data.len(): {}", node_id, event_data_len);
                    
            //         if event_data_len > 0 {
            //             let event_data_vec = event.block.data.vec();
            //             info!("Node {} [insert check]  📦 事件中 data.vec().len(): {}", node_id, event_data_vec.len());
                        
            //             // 解析事件中的交易数量
            //             let event_tx_count = if event_data_vec.len() >= 2 {
            //                 let tx_count_bytes = event_data_vec[1].bytes();
            //                 if tx_count_bytes.len() >= 4 {
            //                     let mut bytes = [0u8; 4];
            //                     bytes.copy_from_slice(&tx_count_bytes);
            //                     u32::from_le_bytes(bytes)
            //                 } else { 0 }
            //             } else { 0 };
                        
            //             info!("Node {} [insert check]  🎯 事件中交易数量: {}", node_id, event_tx_count);
            //         }
                    
            //         // 2. 检查 KV 存储中的区块组件
            //         info!("Node {} [insert check][KV组件检查] 开始逐步检查...", node_id);
                    
            //         // 2.1 检查区块高度
            //         match kv_clone_insert.block_height(&block_hash) {
            //             Ok(Some(height)) => {
            //                 info!("Node {} [insert check]  ✅ block_height: {}", node_id, height.int());
            //             },
            //             Ok(None) => {
            //                 info!("Node {} [insert check]  ❌ block_height: None", node_id);
            //             },
            //             Err(e) => {
            //                 info!("Node {} [insert check]  ❌ block_height 错误: {:?}", node_id, e);
            //             }
            //         }
                    
            //         // 2.2 检查数据哈希
            //         match kv_clone_insert.block_data_hash(&block_hash) {
            //             Ok(Some(data_hash)) => {
            //                 info!("Node {} [insert check]  ✅ block_data_hash: {:?}", node_id, &data_hash.bytes()[0..8]);
            //             },
            //             Ok(None) => {
            //                 info!("Node {} [insert check]  ❌ block_data_hash: None", node_id);
            //             },
            //             Err(e) => {
            //                 info!("Node {} [insert check]  ❌ block_data_hash 错误: {:?}", node_id, e);
            //             }
            //         }
                    
            //         // 2.3 关键检查：数据长度
            //         match kv_clone_insert.block_data_len(&block_hash) {
            //             Ok(Some(data_len)) => {
            //                 info!("Node {} [insert check]  📏 block_data_len: {}", node_id, data_len.int());
                            
            //                 // 2.4 如果有数据长度，检查每个 Datum
            //                 for i in 0..std::cmp::min(5, data_len.int()) {
            //                     match kv_clone_insert.block_datum(&block_hash, i as u32) {
            //                         Some(datum) => {
            //                             let bytes_len = datum.bytes().len();
            //                             info!("Node {} [insert check]    ✅ block_datum[{}]: {} 字节", node_id, i, bytes_len);
                                        
            //                             // 显示内容预览
            //                             if i == 0 && bytes_len >= 8 {
            //                                 // 视图号
            //                                 let mut view_bytes = [0u8; 8];
            //                                 view_bytes.copy_from_slice(&datum.bytes()[0..8]);
            //                                 let view_num = u64::from_le_bytes(view_bytes);
            //                                 info!("Node {} [insert check]      -> View: {}", node_id, view_num);
            //                             } else if i == 1 && bytes_len >= 4 {
            //                                 // 交易计数
            //                                 let mut count_bytes = [0u8; 4];
            //                                 count_bytes.copy_from_slice(&datum.bytes()[0..4]);
            //                                 let count = u32::from_le_bytes(count_bytes);
            //                                 info!("Node {} [insert check]      -> TxCount: {}", node_id, count);
            //                             } else if i >= 2 {
            //                                 // 交易内容
            //                                 let content = String::from_utf8_lossy(datum.bytes());
            //                                 let preview = if content.len() > 20 {
            //                                     format!("{}...", &content[0..20])
            //                                 } else {
            //                                     content.to_string()
            //                                 };
            //                                 info!("Node {} [insert check]      -> Tx: {}", node_id, preview);
            //                             }
            //                         },
            //                         None => {
            //                             info!("Node {} [insert check]    ❌ block_datum[{}]: None", node_id, i);
            //                         }
            //                     }
            //                 }
            //             },
            //             Ok(None) => {
            //                 info!("Node {} [insert check]  ❌ 关键问题：block_data_len 为 None！", node_id);
            //             },
            //             Err(e) => {
            //                 info!("Node {} [insert check]  ❌ block_data_len 错误: {:?}", node_id, e);
            //             }
            //         }
                    
            //         // 2.5 最终通过 block() 函数读取完整区块
            //         match kv_clone_insert.block(&block_hash) {
            //             Ok(Some(stored_block)) => {
            //                 let stored_data_len = stored_block.data.len().int();
            //                 info!("Node {} [insert check]  📦 最终 block() 结果: data.len() = {}", node_id, stored_data_len);
                            
            //                 if stored_data_len == 0 {
            //                     info!("Node {} [insert check]  🔥 确认：区块数据在 KV 存储中丢失！", node_id);
            //                 }
            //             },
            //             Ok(None) => {
            //                 info!("Node {} [insert check]  ❌ block() 返回 None", node_id);
            //             },
            //             Err(e) => {
            //                 info!("Node {} [insert check]  ❌ block() 错误: {:?}", node_id, e);
            //             }
            //         }
                    
            //         // 3. 总结日志
            //         let msg = format!(
            //             "🔗 Node {} [insert check]插入区块 - 哈希: {:?} (详细分析完成)",
            //             node_id, &block_hash.bytes()[0..8]
            //         );
            //         crate::log_node(node_id, log::Level::Info, &msg);
            //     }
            // })
            // === 网络和同步事件 ===
            .on_insert_block({
                move |event| {
                    let block_hash = event.block.hash;
                    let tx_count = if event.block.data.vec().len() >= 2 {
                        let tx_count_bytes = event.block.data.vec()[1].bytes();
                        if tx_count_bytes.len() >= 4 {
                            let mut bytes = [0u8; 4];
                            bytes.copy_from_slice(&tx_count_bytes[0..4]);
                                    u32::from_le_bytes(bytes)
                                } else {
                                    0
                                }
                            } else {
                                0
                            };
                    let msg = format!(
                                "🔗 Node {} 插入区块事件 - 交易数: {}, 哈希: {:?}",
                                node_id, tx_count, &block_hash
                            );
                            crate::log_node(node_id, log::Level::Info, &msg);
                    
                    // 从 KV store 读取区块内容
                    match kv_clone_insert.block(&block_hash) {
                        Ok(Some(block)) => {
                            let height = block.height.int();
                            let data_items = block.data.len();
                            
                            // 解析交易数量
                            let tx_count = if block.data.vec().len() >= 2 {
                            let tx_count_bytes = block.data.vec()[1].bytes();
                            if tx_count_bytes.len() >= 4 {
                                let mut bytes = [0u8; 4];
                                bytes.copy_from_slice(&tx_count_bytes[0..4]);
                                        u32::from_le_bytes(bytes)
                                    } else {
                                        0
                                    }
                                } else {
                                    0
                                };
                             
                            let msg = format!(
                                "🔗 Node {} 插入区块kv_store - Height: {}, 交易数: {}, 数据项: {}, 哈希: {:?}",
                                node_id, height, tx_count, data_items.int(), &block_hash
                            );
                            crate::log_node(node_id, log::Level::Info, &msg);
                        },
                        _ => {
                            let msg = format!(
                                "🔗 Node {} 插入区块kv_store - 哈希: {:?} (无法读取详情)",
                                node_id, &block_hash.bytes()[0..8]
                            );
                            crate::log_node(node_id, log::Level::Warn, &msg);
                        }
                    }
                }
            })
            // .on_insert_block({
            //     move |event| {
            //         let msg = format!(
            //             "🔗 Node {} 插入区块, 高度: {}, 哈希: {:?}",
            //             node_id,
            //             event.block.height.int(),
            //             event.block.hash,
            //         );
            //         crate::log_node(node_id, log::Level::Info, &msg);
            //     }
            // })
            .build()
            .start();
        
        info!("✅ Node {} 已启动", node_id);
        
        Self {
            verifying_key,
            replica,
            node_id,
            // app_handle,  // 保存应用引用
            tx_queue,  // 保存交易队列引用
        }
    }

    /// 查询Node的验证密钥
    pub fn verifying_key(&self) -> VerifyingKey {
        self.verifying_key
    }

    /// 查询当前提交的验证者集合
    pub fn committed_validator_set(&self) -> ValidatorSet {
        self.replica
            .block_tree_camera()
            .snapshot()
            .committed_validator_set()
            .expect("应该能够从区块树获取已提交的验证者集合")
    }

    /// 查询进入的最高View号
    pub fn highest_view_entered(&self) -> ViewNumber {
        self.replica
            .block_tree_camera()
            .snapshot()
            .highest_view_entered()
            .expect("应该能够从区块树获取进入的最高View")
    }

    // /// 提交交易到Node
    // pub fn submit_transaction(&self, transaction: String) {
    //     let mut app = self.app_handle.lock().unwrap();
    //     app.add_transaction(transaction.clone());
    //     crate::log_node(self.node_id, log::Level::Info, 
    //                               &format!("📝 接收交易: {}", transaction));
    // }

    /// 批量提交交易
    pub fn submit_transactions(&self, transactions: Vec<String>) {
        // 直接添加到共享队列
        let mut queue = self.tx_queue.lock().unwrap();
        for tx in transactions {
            queue.push(tx.clone());
            info!("📝 提交交易到共享队列: {}", tx);
        }

        // let mut app = self.app_handle.lock().unwrap();
        // for tx in &transactions {
        //     app.add_transaction(tx.clone());
        //     info!("📝 add_tx 提交交易: {} 到 pending tx", tx);
        // }
        // crate::log_node(self.node_id, log::Level::Info, 
        //                           &format!("📝 接收 {} 个交易", transactions.len()));
    }
}