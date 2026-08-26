use super::*;
use crate::app_event::AppEvent;
use crate::app_event_sender::AppEventSender;
use codex_file_search::FileSearchSnapshot;
use pretty_assertions::assert_eq;
use std::fs;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::error::TryRecvError;

#[test]
fn reporter_discards_snapshot_for_superseded_query() {
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let state = Arc::new(Mutex::new(SearchState {
        latest_query: "current".to_string(),
        session: None,
        session_token: 1,
    }));
    let reporter = TuiSessionReporter {
        state,
        app_tx: AppEventSender::new(event_tx),
        session_token: 1,
    };

    reporter.send_snapshot(&FileSearchSnapshot {
        query: "stale".to_string(),
        ..Default::default()
    });
    assert!(matches!(event_rx.try_recv(), Err(TryRecvError::Empty)));

    reporter.send_snapshot(&FileSearchSnapshot {
        query: "current".to_string(),
        ..Default::default()
    });
    let AppEvent::FileSearchResult { query, matches } = event_rx.try_recv().expect("current event")
    else {
        panic!("expected file search result");
    };
    assert_eq!(query, "current");
    assert_eq!(matches, Vec::new());
}

#[test]
fn manager_burst_updates_publish_the_final_query() {
    const BURST_COUNT: usize = 4;
    const FILE_COUNT: usize = 32_768;
    const PREFIX_COUNT: usize = 32;

    let root = tempfile::tempdir().expect("temporary search root");
    let final_query = query_for_burst(BURST_COUNT - 1)[..PREFIX_COUNT].to_string();
    for index in 0..FILE_COUNT {
        fs::write(
            root.path().join(format!("{final_query}-{index:05}.rs")),
            "contents",
        )
        .expect("fixture file");
    }

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let manager = FileSearchManager::new(root.path().to_path_buf(), AppEventSender::new(event_tx));
    for burst in 0..BURST_COUNT {
        let query = query_for_burst(burst);
        for prefix_len in 1..=PREFIX_COUNT {
            manager.on_user_query(query[..prefix_len].to_string());
            thread::sleep(Duration::from_millis(1));
        }
    }

    let matches = wait_for_query(&mut event_rx, &final_query);
    assert!(!matches.is_empty());
}

fn query_for_burst(burst: usize) -> String {
    format!("burst-{burst:02}-current-query-result-abcdef")
}

fn wait_for_query(
    event_rx: &mut UnboundedReceiver<AppEvent>,
    expected_query: &str,
) -> Vec<file_search::FileMatch> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match event_rx.try_recv() {
            Ok(AppEvent::FileSearchResult { query, matches })
                if query == expected_query && !matches.is_empty() =>
            {
                return matches;
            }
            Ok(_) => {}
            Err(TryRecvError::Empty) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(TryRecvError::Empty) => panic!("timed out waiting for query {expected_query}"),
            Err(TryRecvError::Disconnected) => panic!("file search event channel closed"),
        }
    }
}
