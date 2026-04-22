//! Progress visualization for CLI tasks.

use indicatif::{HumanBytes, HumanCount, ProgressBar};
use s5_node_api::{ProgressState, ProgressType, TaskProgressMap};

/// Create a spinner progress bar.
pub fn new_progress_bar() -> ProgressBar {
    ProgressBar::new_spinner()
}

/// Update progress bar from a TaskProgressMap.
pub fn update_progress_bar(pb: &ProgressBar, progress: &TaskProgressMap) {
    // Find primary state for the progress bar (prefer bytes with total)
    let primary = progress
        .0
        .iter()
        .find(|s| matches!(s.progress_type, ProgressType::Bytes) && s.total.is_some())
        .cloned();

    if let Some(ref state) = primary
        && let Some(total) = state.total
    {
        pb.set_length(total);
        pb.set_position(state.progress);
    }

    // Build message from non-primary states
    let msg: String = progress
        .0
        .iter()
        .filter(|s| primary.as_ref().is_none_or(|p| s.label != p.label))
        .filter(|s| s.progress > 0 || s.total.is_some())
        .map(format_state)
        .collect::<Vec<_>>()
        .join(" • ");
    pb.set_message(msg);
    pb.tick();
}

/// Format all states as a one-liner, hiding zero-value open-ended counters.
pub fn format_one_line(progress: &TaskProgressMap) -> String {
    progress
        .0
        .iter()
        .filter(|s| s.progress > 0 || s.total.is_some())
        .map(format_state)
        .collect::<Vec<_>>()
        .join(" • ")
}

fn format_state(s: &ProgressState) -> String {
    let label = s.display_label();
    let val = match s.progress_type {
        ProgressType::Bytes => HumanBytes(s.progress).to_string(),
        ProgressType::Count => HumanCount(s.progress).to_string(),
    };
    match s.total {
        Some(total) => {
            let total_str = match s.progress_type {
                ProgressType::Bytes => HumanBytes(total).to_string(),
                ProgressType::Count => HumanCount(total).to_string(),
            };
            let pct = if total > 0 {
                s.progress * 100 / total
            } else {
                0
            };
            format!("{} / {} ({}%) {}", val, total_str, pct, label)
        }
        None => format!("{} {}", val, label),
    }
}
