// hotstuff_runner/src/bin/client.rs
use std::time::Duration;
use std::thread;
use log::{info, error};

fn setup_logger() {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[CLIENT][{}] {}",
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .apply()
        .unwrap();
}

fn main() {
    setup_logger();
    info!("Starting HotStuff client");

    // 在实际实现中，这里应该连接到节点的RPC接口
    // 现在我们只是模拟发送交易
    
    let mut tx_count = 0;
    loop {
        let tx_data = format!("Transaction_{}_timestamp_{}", 
            tx_count, 
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        
        info!("Sending transaction: {}", tx_data);
        
        // 在实际实现中，这里会通过网络发送到节点
        // 例如通过HTTP/gRPC调用节点的API
        
        tx_count += 1;
        thread::sleep(Duration::from_secs(2));
        
        if tx_count >= 20 {
            info!("Sent {} transactions, stopping client", tx_count);
            break;
        }
    }
}