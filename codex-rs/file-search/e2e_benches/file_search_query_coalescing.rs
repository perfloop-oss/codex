#![allow(clippy::expect_used)]

mod app_event {
    use codex_file_search::FileMatch;

    pub(crate) enum AppEvent {
        FileSearchResult {
            query: String,
            matches: Vec<FileMatch>,
        },
    }
}

mod app_event_sender {
    use super::app_event::AppEvent;
    use tokio::sync::mpsc::UnboundedSender;

    #[derive(Clone)]
    pub(crate) struct AppEventSender {
        app_event_tx: UnboundedSender<AppEvent>,
    }

    impl AppEventSender {
        pub(crate) fn new(app_event_tx: UnboundedSender<AppEvent>) -> Self {
            Self { app_event_tx }
        }

        pub(crate) fn send(&self, event: AppEvent) {
            let _ = self.app_event_tx.send(event);
        }
    }
}

#[allow(dead_code)]
#[path = "../../tui/src/file_search.rs"]
mod tui_file_search;

use app_event::AppEvent;
use app_event_sender::AppEventSender;
use divan::Bencher;
use std::hint::black_box;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc::UnboundedReceiver;

const FILE_COUNT: usize = 32_768;
const PREFIX_COUNT: usize = 32;
const BURST_COUNT: usize = 4;

fn main() {
    divan::main();
}

/// Enters the same manager and app-event path used by the TUI's `@` search.
#[divan::bench(sample_count = 1, sample_size = 1)]
fn file_search_query_coalescing(bencher: Bencher) {
    let root = tempfile::tempdir().expect("temporary search root");
    let final_query = burst_query(BURST_COUNT - 1)[..PREFIX_COUNT].to_string();
    for index in 0..FILE_COUNT {
        std::fs::write(
            root.path().join(format!("{final_query}-{index:05}.rs")),
            "contents",
        )
        .expect("fixture file");
    }

    bencher.bench_local(move || {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
        let manager = tui_file_search::FileSearchManager::new(
            root.path().to_path_buf(),
            AppEventSender::new(event_tx),
        );

        for burst in 0..BURST_COUNT {
            let query = burst_query(burst);
            for prefix_len in 1..=PREFIX_COUNT {
                manager.on_user_query(query[..prefix_len].to_string());
                thread::sleep(Duration::from_millis(1));
            }
        }

        let matches = wait_for_query(&mut event_rx, &final_query);
        assert!(!matches.is_empty(), "final query should match the fixture");
        black_box(matches);
    });
}

fn burst_query(burst: usize) -> String {
    format!("burst-{burst:02}-current-query-result-abcdef")
}

fn wait_for_query(
    event_rx: &mut UnboundedReceiver<AppEvent>,
    expected_query: &str,
) -> Vec<codex_file_search::FileMatch> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match event_rx.try_recv() {
            Ok(AppEvent::FileSearchResult { query, matches })
                if query == expected_query && !matches.is_empty() =>
            {
                return matches;
            }
            Ok(AppEvent::FileSearchResult { .. }) => {}
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                panic!("timed out waiting for query {expected_query}");
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                panic!("file search event channel closed");
            }
        }
    }
}
