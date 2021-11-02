use anyhow::Result;
use std::fs::read_dir;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

/// Trait for asynchronously notifying health information
pub trait HealthReporting: Send + Sync {
    /// Notify that the service is ready. Can be called multiple times.
    /// We use this to signal readiness only at the start of a batch in order to not interrupt the
    /// still running kubernetes pod while it is handling a batch.
    fn notify_ready(&self);
}

/// Implementation sharing health information over an HTTP endpoint.
#[derive(Debug, Default)]
pub struct HttpHealthEndpoint {
    ready: AtomicBool,
}

impl HttpHealthEndpoint {
    /// Creates a new HTTP health enpoint.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if the service is ready, false otherwise.
    pub fn is_ready(&self) -> bool {
        tracing::debug!("ready");
        self.ready.load(Ordering::SeqCst)
    }
}

impl HealthReporting for HttpHealthEndpoint {
    fn notify_ready(&self) {
        tracing::debug!("ready");
        self.ready.store(true, Ordering::SeqCst);
    }
}

// function only signals readiness, if the entire history is downloaded and today's user_data are already downloaded
pub fn readiness_check<P: AsRef<Path>>(path: P) -> Result<bool> {
    let files = read_dir(path)?;
    let files: Vec<String> = files
        .filter_map(|entry| entry.ok())
        .map(|entry| format!("{:?}", entry))
        .collect();
    if files
        .iter()
        .any(|name| name.contains("user_data_entire_history.json"))
        && files.iter().any(|name| {
            name.contains(&format!(
                "user_data_from{:?}.json",
                chrono::offset::Utc::now().timestamp() / (24 * 60 * 60i64) * (24 * 60 * 60)
            ))
        })
    {
        return Ok(true);
    }
    Ok(false)
}
