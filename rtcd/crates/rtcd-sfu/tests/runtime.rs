//! The SFU on real sockets: browsers (real `rtc` peers) connect over loopback UDP to the running
//! `Sfu` and its shard thread.

mod live;

use std::time::Duration;

use live::Live;
use rtcd_proto::MessageType;
use rtcd_sfu::engine::{CloseReason, Event};

#[test]
fn two_browsers_connect_and_hear_each_other_over_real_sockets() {
    let mut live = Live::start(1);
    let a = live.join("alice", "call1");
    let b = live.join("bob", "call1");
    assert_eq!(live.sfu.session_count(), 2);

    live.speak(a, 60);
    live.speak(b, 60);
    live.run(Duration::from_millis(500));

    assert!(
        live.browsers[b].received("voice_session-alice") > 40,
        "bob heard {}",
        live.browsers[b].received("voice_")
    );
    assert!(
        live.browsers[a].received("voice_session-bob") > 40,
        "alice heard {}",
        live.browsers[a].received("voice_")
    );
}

#[test]
fn the_http_lookups_see_sessions_by_group_call_and_id() {
    let mut live = Live::start(1);
    live.join("alice", "call1");
    live.join("bob", "call1");
    live.join("carol", "call2");
    let cfg = live
        .sfu
        .session("group", "call1", "session-alice")
        .expect("alice is in call1");
    assert_eq!(cfg.user_id, "user-alice");
    assert!(
        live.sfu
            .session("group", "call2", "session-alice")
            .is_none(),
        "wrong call"
    );
    assert!(
        live.sfu
            .session("other", "call1", "session-alice")
            .is_none(),
        "wrong group"
    );
    assert_eq!(live.sfu.call_sessions("group", "call1").len(), 2);
    assert_eq!(live.sfu.call_sessions("group", "call2").len(), 1);
    assert!(live.sfu.call_sessions("group", "nothing").is_empty());
}

#[test]
fn leaving_ends_the_session_and_reports_it() {
    let mut live = Live::start(1);
    live.join("alice", "call1");
    live.sfu.leave("session-alice");
    live.run_until(Duration::from_secs(3), |l| {
        l.closed.iter().any(|(id, _)| id == "session-alice")
    });
    assert!(
        live.closed
            .contains(&("session-alice".to_owned(), CloseReason::Left))
    );
    assert_eq!(live.sfu.session_count(), 0);
    assert!(
        live.sfu
            .session("group", "call1", "session-alice")
            .is_none()
    );
}

#[test]
fn messages_for_a_session_that_does_not_exist_are_refused() {
    let live = Live::start(1);
    let msg = rtcd_proto::Message {
        group_id: "g".into(),
        user_id: "u".into(),
        session_id: "ghost".into(),
        call_id: "c".into(),
        kind: MessageType::Mute,
        data: vec![],
    };
    assert!(live.sfu.signal(msg).is_err());
}

#[test]
fn a_duplicate_session_id_is_refused_and_the_first_is_untouched() {
    let mut live = Live::start(1);
    live.join("alice", "call1");
    let cfg = live.sfu.session("group", "call1", "session-alice").unwrap();
    assert!(live.sfu.join(cfg).is_err());
    live.run(Duration::from_millis(200));
    assert_eq!(live.sfu.session_count(), 1);
    assert!(live.closed.is_empty());
}

#[test]
fn shutdown_waits_for_nobody_when_the_drain_is_zero_and_ends_everyone() {
    let mut live = Live::start(1);
    live.join("alice", "call1");
    live.join("bob", "call1");
    live.sfu.shutdown(Duration::ZERO);
    live.drain_events();
    assert_eq!(live.sfu.session_count(), 0);
    assert_eq!(
        live.closed
            .iter()
            .filter(|(_, r)| *r == CloseReason::Shutdown)
            .count(),
        2
    );
    // And nothing new is admitted afterwards.
    let cfg = rtcd_proto::SessionConfig {
        group_id: "g".into(),
        call_id: "c".into(),
        user_id: "u".into(),
        session_id: "late".into(),
        ..Default::default()
    };
    assert!(live.sfu.join(cfg).is_err());
}

#[test]
fn shutdown_gives_running_sessions_time_to_end_on_their_own() {
    let mut live = Live::start(1);
    live.join("alice", "call1");
    let sfu = live.sfu.clone();
    let handle = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        sfu.shutdown(Duration::from_secs(10));
        start.elapsed()
    });
    std::thread::sleep(Duration::from_millis(300));
    live.sfu.leave("session-alice");
    let took = handle.join().unwrap();
    assert!(
        took < Duration::from_secs(5),
        "the drain ended when the last session did: {took:?}"
    );
    live.drain_events();
    assert!(
        live.closed
            .contains(&("session-alice".to_owned(), CloseReason::Left))
    );
}

#[allow(dead_code)]
fn _unused(_: Event) {}
