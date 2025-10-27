use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::Instant;
use tokio::time::{interval, Duration};
use tokio::task::JoinHandle;
use tracing::info;

/// Starts a progress reporting task that logs progress every 10 seconds
///
/// # Arguments
/// * `completed_count` - Arc<AtomicUsize> that tracks the number of completed items
/// * `total_items` - Total number of items to process
/// * `task_name` - Name of the task for logging context
///
/// # Returns
/// A tuple of (JoinHandle, Instant) where JoinHandle can be used to abort the progress reporting task
/// and Instant is the start time for use with log_completion_stats
pub fn start_progress_reporter(
    completed_count: Arc<AtomicUsize>,
    total_items: usize,
    task_name: String,
) -> (JoinHandle<()>, Instant) {
    let start_time = Instant::now();
    let handle = tokio::spawn(async move {
        let mut progress_interval = interval(Duration::from_secs(10));
        loop {
            progress_interval.tick().await;
            let completed = completed_count.load(Ordering::Relaxed);
            let elapsed = start_time.elapsed();
            let elapsed_secs = elapsed.as_secs_f64();

            if completed > 0 {
                let avg_time_per_item = elapsed_secs / completed as f64;
                let remaining = total_items.saturating_sub(completed);
                let estimated_remaining_time = avg_time_per_item * remaining as f64;

                info!(
                    task = %task_name,
                    completed = %completed,
                    total = %total_items,
                    elapsed_seconds = %elapsed_secs,
                    avg_seconds_per_item = %avg_time_per_item,
                    remaining = %remaining,
                    estimated_remaining_seconds = %estimated_remaining_time,
                    "Progress update"
                );
            } else {
                info!(
                    task = %task_name,
                    completed = %completed,
                    total = %total_items,
                    elapsed_seconds = %elapsed_secs,
                    "Progress update (no completions yet)"
                );
            }

            if completed >= total_items {
                break;
            }
        }
    });

    (handle, start_time)
}

/// Logs final completion statistics for a task
///
/// # Arguments
/// * `task_name` - Name of the task for logging context
/// * `total_processed` - Total number of items processed
/// * `start_time` - When the task started
/// * `final_result_size` - Optional size of final result (e.g., map size)
pub fn log_completion_stats(
    task_name: &str,
    total_processed: usize,
    start_time: Instant,
    final_result_size: Option<usize>,
) {
    let final_elapsed = start_time.elapsed();

    match final_result_size {
        Some(result_size) => {
            info!(
                task = %task_name,
                total_processed = %total_processed,
                total_time_seconds = %final_elapsed.as_secs_f64(),
                final_result_size = %result_size,
                "Task completed"
            );
        }
        None => {
            info!(
                task = %task_name,
                total_processed = %total_processed,
                total_time_seconds = %final_elapsed.as_secs_f64(),
                "Task completed"
            );
        }
    }
}