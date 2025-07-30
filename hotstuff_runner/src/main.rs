use tracing::info;

// 只导入最基本的类型，不实现复杂的trait
use hotstuff_rs::{
    types::{
        data_types::{ViewNumber, Data, Datum, BlockHeight},
        block::Block,
        validator_set::ValidatorSet,
        crypto_primitives::VerifyingKey,
    },
    hotstuff::types::PhaseCertificate,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("🚀 Starting HotStuff Runner Basic Test");

    info!("📚 Testing basic hotstuff_rs type imports...");

    // 测试基本类型创建
    info!("🔢 Creating ViewNumber...");
    let view = ViewNumber::new(1);
    info!("✅ Created ViewNumber: {:?}", view);

    info!("📦 Creating test Data...");
    let test_datum = Datum::new(b"Hello HotStuff!".to_vec());
    let data = Data::new(vec![test_datum]);
    info!("✅ Created Data successfully");

    info!("🏗️ Creating BlockHeight...");
    let height = BlockHeight::new(1);
    info!("✅ Created BlockHeight: {:?}", height);

    info!("👥 Creating empty ValidatorSet...");
    let validator_set = ValidatorSet::new();
    info!("✅ Created ValidatorSet with {} validators", validator_set.len());

    // 测试一些基本的方法调用
    info!("🧮 Testing basic operations...");
    info!("   - ViewNumber: {:?}", view);
    info!("   - BlockHeight: {:?}", height);
    info!("   - Data created successfully");
    info!("   - ValidatorSet is empty: {}", validator_set.is_empty());

    info!("✅ All basic type operations completed successfully!");

    info!("🐳 Docker environment test results:");
    info!("   - ✅ Rust compilation: OK");
    info!("   - ✅ hotstuff_rs library import: OK");
    info!("   - ✅ Basic type creation: OK");
    info!("   - ✅ Tokio async runtime: OK");
    info!("   - ✅ Tracing logging: OK");

    info!("🎉 HotStuff Runner Basic Test completed successfully!");
    info!("📋 This confirms that:");
    info!("   1. Docker build environment is working");
    info!("   2. hotstuff_rs library is properly linked");
    info!("   3. Basic Rust async environment is functional");
    info!("   4. Ready for next step: implementing actual consensus nodes");

    // 保持程序运行一小段时间以便观察日志
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    Ok(())
}