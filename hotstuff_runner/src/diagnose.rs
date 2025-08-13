// hotstuff_runner/src/pompe_diagnostic.rs
//! Pompe BFT 网络诊断工具

use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tracing::{info, warn, error,debug};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DiagnosticMessage {
    pub message_type: String,
    pub from_node_id: usize,
    pub timestamp: u64,
    pub data: String,
}

pub struct PompeDiagnostic {
    node_id: usize,
    peer_addresses: HashMap<usize, SocketAddr>,
}

impl PompeDiagnostic {
    pub fn new(node_id: usize, node_least_id: usize, node_num: usize) -> Self {
        let mut peer_addresses = HashMap::new();
        
        // 构建所有节点的地址映射 - 在Docker环境中使用容器名
        for i in node_least_id..=(node_least_id + node_num - 1) {
            // 🚨 修复：在Docker环境中直接使用容器名，不解析为SocketAddr
            let addr_str = format!("node{}:{}", i, 20000 + i);
            
            // 创建一个虚拟的SocketAddr用于HashMap存储，实际连接时使用字符串
            let addr = format!("0.0.0.0:{}", 20000 + i).parse::<SocketAddr>().unwrap();
            peer_addresses.insert(i, addr);
            
            info!("🔍 [诊断] 节点 {} 地址: {} (实际使用: {})", i, addr, addr_str);
        }
        
        info!("🔍 [诊断] 创建诊断工具，节点 {}, 对等地址: {:?}", node_id, peer_addresses);
        
        Self {
            node_id,
            peer_addresses,
        }
    }

    /// 测试与所有节点的连接
    pub async fn test_all_connections(&self) -> HashMap<usize, bool> {
        let mut results = HashMap::new();
        
        info!("🔍 [诊断] Node {} 开始测试所有连接", self.node_id);
        
        for (&target_node_id, &target_addr) in &self.peer_addresses {
            let success = self.test_connection(target_node_id, target_addr).await;
            results.insert(target_node_id, success);
            
            if success {
                info!("✅ [诊断] Node {} -> Node {} 连接成功", self.node_id, target_node_id);
            } else {
                warn!("❌ [诊断] Node {} -> Node {} 连接失败", self.node_id, target_node_id);
            }
        }
        
        let success_count = results.values().filter(|&&v| v).count();
        info!("📊 [诊断] Node {} 连接测试完成: {}/{} 成功", 
              self.node_id, success_count, results.len());
        
        results
    }

    /// 测试单个连接
    async fn test_connection(&self, target_node_id: usize, _target_addr: SocketAddr) -> bool {
        // 特殊处理自己
        if target_node_id == self.node_id {
            return true;
        }

        // 🚨 修复：在Docker环境中使用容器名
        let docker_addr = format!("node{}:{}", target_node_id, 20000 + target_node_id);

        let diagnostic_msg = DiagnosticMessage {
            message_type: "connection_test".to_string(),
            from_node_id: self.node_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            data: format!("ping from node {}", self.node_id),
        };

        match TcpStream::connect(&docker_addr).await {
            Ok(mut stream) => {
                match serde_json::to_vec(&diagnostic_msg) {
                    Ok(serialized) => {
                        let message_length = serialized.len() as u32;
                        
                        // 发送消息
                        if stream.write_all(&message_length.to_be_bytes()).await.is_ok() 
                            && stream.write_all(&serialized).await.is_ok() 
                            && stream.flush().await.is_ok() {
                            
                            info!("✅ [诊断] Node {} -> Node {} 连接成功 ({})", 
                                  self.node_id, target_node_id, docker_addr);
                            true
                        } else {
                            warn!("⚠️ [诊断] Node {} -> Node {} 发送失败", self.node_id, target_node_id);
                            false
                        }
                    }
                    Err(_) => false,
                }
            }
            Err(e) => {
                warn!("🔍 [诊断] Node {} -> Node {} 连接失败: {}", 
                      self.node_id, target_node_id, e);
                false
            }
        }
    }

    /// 发送测试消息到所有节点
    pub async fn broadcast_test_message(&self, message: &str) -> HashMap<usize, bool> {
        let mut results = HashMap::new();
        
        info!("🔍 [诊断] Node {} 广播测试消息: {}", self.node_id, message);
        
        for (&target_node_id, &target_addr) in &self.peer_addresses {
            let success = self.send_test_message(target_node_id, target_addr, message).await;
            results.insert(target_node_id, success);
        }
        
        let success_count = results.values().filter(|&&v| v).count();
        info!("📊 [诊断] Node {} 测试消息广播完成: {}/{} 成功", 
              self.node_id, success_count, results.len());
        
        results
    }

    async fn send_test_message(&self, target_node_id: usize, _target_addr: SocketAddr, message: &str) -> bool {
        if target_node_id == self.node_id {
            return true;
        }

        // 🚨 修复：使用Docker容器名
        let docker_addr = format!("node{}:{}", target_node_id, 20000 + target_node_id);

        let diagnostic_msg = DiagnosticMessage {
            message_type: "test_message".to_string(),
            from_node_id: self.node_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            data: message.to_string(),
        };

        match TcpStream::connect(&docker_addr).await {
            Ok(mut stream) => {
                match serde_json::to_vec(&diagnostic_msg) {
                    Ok(serialized) => {
                        let message_length = serialized.len() as u32;
                        
                        stream.write_all(&message_length.to_be_bytes()).await.is_ok() 
                            && stream.write_all(&serialized).await.is_ok() 
                            && stream.flush().await.is_ok()
                    }
                    Err(_) => false,
                }
            }
            Err(_) => false,
        }
    }

    /// 检查 Pompe 端口的可达性
    pub async fn check_pompe_ports(&self) -> HashMap<usize, bool> {
        let mut results = HashMap::new();
        
        info!("🔍 [诊断] Node {} 检查所有Pompe端口", self.node_id);
        
        for (&target_node_id, _) in &self.peer_addresses {
            // 🚨 修复：使用Docker容器名检查端口
            let docker_addr = format!("node{}:{}", target_node_id, 20000 + target_node_id);
            let reachable = self.check_port_reachable_by_name(&docker_addr).await;
            results.insert(target_node_id, reachable);
            
            if reachable {
                info!("✅ [诊断] Node {} Pompe端口 {} 可达 ({})", 
                      target_node_id, 20000 + target_node_id, docker_addr);
            } else {
                warn!("❌ [诊断] Node {} Pompe端口 {} 不可达 ({})", 
                      target_node_id, 20000 + target_node_id, docker_addr);
            }
        }
        
        let reachable_count = results.values().filter(|&&v| v).count();
        info!("📊 [诊断] Node {} Pompe端口检查完成: {}/{} 可达", 
              self.node_id, reachable_count, results.len());
        
        results
    }

    async fn check_port_reachable_by_name(&self, addr_str: &str) -> bool {
        match tokio::time::timeout(
            std::time::Duration::from_secs(3),
            TcpStream::connect(addr_str)
        ).await {
            Ok(Ok(_)) => true,
            Ok(Err(e)) => {
                debug!("🔍 [诊断] 端口 {} 连接失败: {}", addr_str, e);
                false
            }
            Err(_) => {
                debug!("🔍 [诊断] 端口 {} 连接超时", addr_str);
                false
            }
        }
    }

    /// 生成网络诊断报告
    pub async fn generate_diagnostic_report(&self) -> DiagnosticReport {
        info!("🔍 [诊断] Node {} 生成完整诊断报告", self.node_id);
        
        let connection_results = self.test_all_connections().await;
        let pompe_port_results = self.check_pompe_ports().await;
        let test_message_results = self.broadcast_test_message("diagnostic_test").await;
        
        let total_nodes = self.peer_addresses.len();
        let successful_connections = connection_results.values().filter(|&&v| v).count();
        let reachable_pompe_ports = pompe_port_results.values().filter(|&&v| v).count();
        let successful_broadcasts = test_message_results.values().filter(|&&v| v).count();
        
        let report = DiagnosticReport {
            node_id: self.node_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            total_peer_nodes: total_nodes,
            successful_connections,
            reachable_pompe_ports,
            successful_broadcasts,
            connection_details: connection_results,
            pompe_port_details: pompe_port_results,
            broadcast_details: test_message_results,
            network_health_score: calculate_health_score(
                successful_connections,
                reachable_pompe_ports, 
                successful_broadcasts,
                total_nodes
            ),
        };
        
        info!("📊 [诊断报告] Node {} 网络健康评分: {:.1}%", 
              self.node_id, report.network_health_score);
        info!("📊 [诊断报告] 连接成功率: {}/{} ({:.1}%)", 
              successful_connections, total_nodes, 
              successful_connections as f32 / total_nodes as f32 * 100.0);
        info!("📊 [诊断报告] Pompe端口可达率: {}/{} ({:.1}%)", 
              reachable_pompe_ports, total_nodes,
              reachable_pompe_ports as f32 / total_nodes as f32 * 100.0);
        info!("📊 [诊断报告] 广播成功率: {}/{} ({:.1}%)", 
              successful_broadcasts, total_nodes,
              successful_broadcasts as f32 / total_nodes as f32 * 100.0);
        
        report
    }

    /// 持续监控网络健康状态
    pub async fn start_continuous_monitoring(&self, interval_seconds: u64) {
        info!("🔍 [诊断] Node {} 开始持续网络监控，间隔: {}秒", 
              self.node_id, interval_seconds);
        
        let mut iteration = 0;
        loop {
            iteration += 1;
            info!("🔍 [监控] Node {} 第 {} 次网络检查", self.node_id, iteration);
            
            let report = self.generate_diagnostic_report().await;
            
            if report.network_health_score < 50.0 {
                error!("🚨 [监控] Node {} 网络健康状况严重: {:.1}%", 
                       self.node_id, report.network_health_score);
            } else if report.network_health_score < 80.0 {
                warn!("⚠️ [监控] Node {} 网络健康状况一般: {:.1}%", 
                      self.node_id, report.network_health_score);
            } else {
                info!("✅ [监控] Node {} 网络健康状况良好: {:.1}%", 
                      self.node_id, report.network_health_score);
            }
            
            tokio::time::sleep(std::time::Duration::from_secs(interval_seconds)).await;
        }
    }
}

#[derive(Debug, Clone)]
pub struct DiagnosticReport {
    pub node_id: usize,
    pub timestamp: u64,
    pub total_peer_nodes: usize,
    pub successful_connections: usize,
    pub reachable_pompe_ports: usize,
    pub successful_broadcasts: usize,
    pub connection_details: HashMap<usize, bool>,
    pub pompe_port_details: HashMap<usize, bool>,
    pub broadcast_details: HashMap<usize, bool>,
    pub network_health_score: f32,
}

impl DiagnosticReport {
    pub fn print_detailed_report(&self) {
        info!("{}", "=".repeat(60));
        info!("🔍 [详细诊断报告] Node {} - {:?}", self.node_id, 
              std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(self.timestamp));
        info!("{}", "=".repeat(60));
        
        info!("📊 总体统计:");
        info!("  • 对等节点总数: {}", self.total_peer_nodes);
        info!("  • 网络健康评分: {:.1}%", self.network_health_score);
        info!("  • 连接成功率: {:.1}% ({}/{})", 
              self.successful_connections as f32 / self.total_peer_nodes as f32 * 100.0,
              self.successful_connections, self.total_peer_nodes);
        info!("  • Pompe端口可达率: {:.1}% ({}/{})",
              self.reachable_pompe_ports as f32 / self.total_peer_nodes as f32 * 100.0,
              self.reachable_pompe_ports, self.total_peer_nodes);
        info!("  • 广播成功率: {:.1}% ({}/{})",
              self.successful_broadcasts as f32 / self.total_peer_nodes as f32 * 100.0,
              self.successful_broadcasts, self.total_peer_nodes);
        
        info!("🔗 连接详情:");
        for (node_id, success) in &self.connection_details {
            let status = if *success { "✅" } else { "❌" };
            info!("  • Node {}: {} {}", node_id, status, 
                  if *success { "连接成功" } else { "连接失败" });
        }
        
        info!("🔌 Pompe端口详情:");
        for (node_id, reachable) in &self.pompe_port_details {
            let status = if *reachable { "✅" } else { "❌" };
            let port = 20000 + node_id;
            info!("  • Node {} (端口 {}): {} {}", node_id, port, status,
                  if *reachable { "可达" } else { "不可达" });
        }
        
        info!("📡 广播详情:");
        for (node_id, success) in &self.broadcast_details {
            let status = if *success { "✅" } else { "❌" };
            info!("  • Node {}: {} {}", node_id, status,
                  if *success { "广播成功" } else { "广播失败" });
        }
        
        info!("{}", "=".repeat(60));
        
        // 提供改进建议
        if self.network_health_score < 80.0 {
            info!("💡 改进建议:");
            
            if self.successful_connections < self.total_peer_nodes {
                info!("  • 检查网络连接和防火墙设置");
                info!("  • 确认所有节点都已启动");
            }
            
            if self.reachable_pompe_ports < self.total_peer_nodes {
                info!("  • 检查Pompe端口 (20000+) 是否正确映射");
                info!("  • 确认Docker网络配置是否正确");
            }
            
            if self.successful_broadcasts < self.total_peer_nodes {
                info!("  • 检查消息序列化和网络稳定性");
                info!("  • 考虑增加重试机制");
            }
        }
    }
}

fn calculate_health_score(
    successful_connections: usize,
    reachable_pompe_ports: usize,
    successful_broadcasts: usize,
    total_nodes: usize,
) -> f32 {
    if total_nodes == 0 {
        return 0.0;
    }
    
    let connection_score = successful_connections as f32 / total_nodes as f32 * 40.0;
    let port_score = reachable_pompe_ports as f32 / total_nodes as f32 * 30.0;
    let broadcast_score = successful_broadcasts as f32 / total_nodes as f32 * 30.0;
    
    connection_score + port_score + broadcast_score
}

// 简化的测试函数，可以在主程序中调用
pub async fn run_pompe_network_diagnostic(node_id: usize, node_least_id: usize, node_num: usize) {
    let diagnostic = PompeDiagnostic::new(node_id, node_least_id, node_num);
    let report = diagnostic.generate_diagnostic_report().await;
    report.print_detailed_report();
}