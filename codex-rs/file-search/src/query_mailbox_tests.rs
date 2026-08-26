use super::query_mailbox::PendingQuery;
use super::query_mailbox::QueryMailbox;
use super::query_mailbox::QueryMailboxUpdateError;
use crossbeam_channel::TryRecvError;
use pretty_assertions::assert_eq;
use std::time::Duration;

#[test]
fn updates_coalesce_to_the_latest_query() {
    let (mailbox, wake_rx) = QueryMailbox::new();

    assert_eq!(mailbox.update("first"), Ok(()));
    assert_eq!(mailbox.update("second"), Ok(()));

    wake_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("matcher wake");
    assert_eq!(
        mailbox.take(),
        Some(PendingQuery {
            query: "second".to_string(),
            update_count: 2,
        })
    );
    assert_eq!(wake_rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn take_clears_the_pending_query_for_the_next_wake() {
    let (mailbox, wake_rx) = QueryMailbox::new();

    mailbox.update("first").expect("first query accepted");
    wake_rx.recv().expect("first wake");
    assert_eq!(
        mailbox.take(),
        Some(PendingQuery {
            query: "first".to_string(),
            update_count: 1,
        })
    );

    mailbox.update("second").expect("second query accepted");
    wake_rx.recv().expect("second wake");
    assert_eq!(
        mailbox.take(),
        Some(PendingQuery {
            query: "second".to_string(),
            update_count: 1,
        })
    );
}

#[test]
fn close_rejects_new_queries_but_keeps_pending_work() {
    let (mailbox, wake_rx) = QueryMailbox::new();

    mailbox.update("pending").expect("query accepted");
    mailbox.close();

    assert_eq!(
        mailbox.update("rejected"),
        Err(QueryMailboxUpdateError::Closed)
    );
    wake_rx.recv().expect("pending wake");
    assert_eq!(
        mailbox.take(),
        Some(PendingQuery {
            query: "pending".to_string(),
            update_count: 1,
        })
    );
}

#[test]
fn receiver_disconnect_rejects_the_update() {
    let (mailbox, wake_rx) = QueryMailbox::new();
    drop(wake_rx);

    assert_eq!(
        mailbox.update("dropped"),
        Err(QueryMailboxUpdateError::Closed)
    );
    assert_eq!(
        mailbox.update("also dropped"),
        Err(QueryMailboxUpdateError::Closed)
    );
    assert_eq!(mailbox.take(), None);
}

#[test]
fn update_then_close_keeps_accepted_query_drainable() {
    let (mailbox, wake_rx) = QueryMailbox::new();

    mailbox.update("accepted").expect("query accepted");
    mailbox.close();

    assert_eq!(
        mailbox.update("rejected"),
        Err(QueryMailboxUpdateError::Closed)
    );
    wake_rx.recv().expect("wake for accepted query");
    assert_eq!(
        mailbox.take(),
        Some(PendingQuery {
            query: "accepted".to_string(),
            update_count: 1,
        })
    );
}

#[test]
fn close_then_update_rejects_query_without_pending_work() {
    let (mailbox, wake_rx) = QueryMailbox::new();

    mailbox.close();

    assert_eq!(
        mailbox.update("rejected"),
        Err(QueryMailboxUpdateError::Closed)
    );
    assert_eq!(wake_rx.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(mailbox.take(), None);
}

#[test]
fn take_then_update_delivers_the_new_query() {
    let (mailbox, wake_rx) = QueryMailbox::new();

    mailbox.update("first").expect("first query accepted");
    wake_rx.recv().expect("first wake");
    assert!(mailbox.take().is_some());

    mailbox.update("second").expect("second query accepted");
    wake_rx.recv().expect("second wake");
    assert_eq!(
        mailbox.take(),
        Some(PendingQuery {
            query: "second".to_string(),
            update_count: 1,
        })
    );
}

#[test]
fn accepted_query_remains_pending_when_receiver_closes_after_update() {
    let (mailbox, wake_rx) = QueryMailbox::new();

    mailbox.update("accepted").expect("query accepted");
    drop(wake_rx);

    assert_eq!(
        mailbox.take(),
        Some(PendingQuery {
            query: "accepted".to_string(),
            update_count: 1,
        })
    );
}
