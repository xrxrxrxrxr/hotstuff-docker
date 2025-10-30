use core_affinity::{self, CoreId};
use std::{
    collections::{BTreeSet, HashMap},
    env,
};
use tracing::{debug, info, warn};

/// Expand an environment variable like "0-3,6,8" into a sorted list of core IDs.
fn parse_cpu_set(spec: &str, label: &str, var: &str) -> Vec<usize> {
    let mut cores = BTreeSet::new();
    for token in spec.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        if let Some((start, end)) = token.split_once('-') {
            match (start.trim().parse::<usize>(), end.trim().parse::<usize>()) {
                (Ok(start), Ok(end)) if start <= end => {
                    for id in start..=end {
                        cores.insert(id);
                    }
                }
                _ => warn!(
                    "[affinity] {}: invalid range '{}' in {}={}",
                    label, token, var, spec
                ),
            }
        } else {
            match token.parse::<usize>() {
                Ok(id) => {
                    cores.insert(id);
                }
                Err(_) => warn!(
                    "[affinity] {}: invalid token '{}' in {}={}",
                    label, token, var, spec
                ),
            }
        }
    }
    cores.into_iter().collect()
}

/// Load a CPU core set for a component from an environment variable.
pub fn affinity_from_env(var: &str, label: &str) -> Option<Vec<CoreId>> {
    let raw = env::var(var).ok()?.trim().to_string();
    if raw.is_empty() {
        return None;
    }

    let requested = parse_cpu_set(&raw, label, var);
    if requested.is_empty() {
        warn!(
            "[affinity] {}: no valid cores resolved from {}={}",
            label, var, raw
        );
        return None;
    }

    let available = match core_affinity::get_core_ids() {
        Some(list) => list,
        None => {
            warn!(
                "[affinity] {}: core_affinity::get_core_ids() returned None; affinity disabled",
                label
            );
            return None;
        }
    };
    let mut available_map = available
        .into_iter()
        .map(|core| (core.id, core))
        .collect::<HashMap<usize, CoreId>>();

    let mut resolved = Vec::new();
    for id in requested {
        match available_map.remove(&id) {
            Some(core) => resolved.push(core),
            None => warn!(
                "[affinity] {}: requested core {} not available; skipping",
                label, id
            ),
        }
    }

    if resolved.is_empty() {
        warn!(
            "[affinity] {}: no available cores matched {}={}",
            label, var, raw
        );
        return None;
    }

    info!(
        "[affinity] {}: binding to cores [{}] via {}",
        label,
        format_core_list(&resolved),
        var
    );
    Some(resolved)
}

/// Apply affinity for the current thread. We best-effort pin and always log the attempt
/// because `core_affinity::set_for_current` has platform-specific return values.
pub fn bind_current_thread(label: &str, core: CoreId) {
    #[allow(unused_must_use)]
    {
        core_affinity::set_for_current(core);
    }

    debug!(
        "[affinity] {}: requested pin of thread {:?} to core {} (best-effort)",
        label,
        std::thread::current().name(),
        core.id
    );
}

/// Helper to expose core IDs as comma separated string for logging.
pub fn format_core_list(cores: &[CoreId]) -> String {
    let mut list = Vec::with_capacity(cores.len());
    for core in cores {
        list.push(core.id.to_string());
    }
    list.join(",")
}
