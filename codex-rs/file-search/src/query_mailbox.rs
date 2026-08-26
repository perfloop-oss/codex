use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use crossbeam_channel::TrySendError;
use crossbeam_channel::bounded;
use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingQuery {
    pub(super) query: String,
    pub(super) update_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum QueryMailboxUpdateError {
    Closed,
}

#[derive(Debug)]
pub(super) struct QueryMailbox {
    state: Mutex<QueryMailboxState>,
    wake_tx: Sender<()>,
}

#[derive(Debug, Default)]
struct QueryMailboxState {
    pending: Option<PendingQuery>,
    closed: bool,
}

impl QueryMailbox {
    pub(super) fn new() -> (Self, Receiver<()>) {
        let (wake_tx, wake_rx) = bounded(1);
        (
            Self {
                state: Mutex::new(QueryMailboxState::default()),
                wake_tx,
            },
            wake_rx,
        )
    }

    /// Replaces any unprocessed query and ensures the matcher will observe it.
    pub(super) fn update(&self, query: &str) -> Result<(), QueryMailboxUpdateError> {
        #[expect(clippy::unwrap_used)]
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(QueryMailboxUpdateError::Closed);
        }

        match state.pending.as_mut() {
            Some(pending) => {
                pending.query.clear();
                pending.query.push_str(query);
                pending.update_count = pending.update_count.saturating_add(1);
            }
            None => {
                state.pending = Some(PendingQuery {
                    query: query.to_string(),
                    update_count: 1,
                });
            }
        }

        match self.wake_tx.try_send(()) {
            Ok(()) | Err(TrySendError::Full(())) => Ok(()),
            Err(TrySendError::Disconnected(())) => {
                // The worker closes the mailbox before it drops the receiver.
                // A disconnected receiver can therefore only come from a caller
                // that did not coordinate shutdown, so reject this update rather
                // than claiming it will be processed.
                state.pending = None;
                state.closed = true;
                Err(QueryMailboxUpdateError::Closed)
            }
        }
    }

    /// Takes all coalesced updates for the matcher worker.
    pub(super) fn take(&self) -> Option<PendingQuery> {
        #[expect(clippy::unwrap_used)]
        self.state.lock().unwrap().pending.take()
    }

    /// Prevents future updates while allowing the worker to drain pending work.
    pub(super) fn close(&self) {
        #[expect(clippy::unwrap_used)]
        let mut state = self.state.lock().unwrap();
        state.closed = true;
    }
}
