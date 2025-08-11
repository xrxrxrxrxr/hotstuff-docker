// hotstuff_runner/src/utils.rs
use std::time::SystemTime;

pub fn format_system_time(time: SystemTime) -> String {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => {
            let secs = duration.as_secs();
            let nanos = duration.subsec_nanos();
            format!("{}.{:09}", secs, nanos)
        }
        Err(_) => "invalid_time".to_string(),
    }
}

// 或者使用 chrono 格式化为可读时间
pub fn format_system_time_readable(time: SystemTime) -> String {
    use chrono::{DateTime, Local};
    let datetime: DateTime<Local> = time.into();
    datetime.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}