//! Integration tests against a real etcd instance.
//!
//! These require a running etcd reachable at the endpoints listed in
//! `KADEDB_TEST_ETCD_ENDPOINTS` (comma-separated, e.g. `http://127.0.0.1:2379`). They are
//! `#[ignore]`d so `cargo test` stays hermetic by default; run them explicitly once etcd
//! is up, e.g.:
//!
//! ```sh
//! docker run -d --rm -p 2379:2379 --name kadedb-test-etcd \
//!   gcr.io/etcd-development/etcd:v3.5.9 \
//!   /usr/local/bin/etcd --advertise-client-urls http://0.0.0.0:2379 \
//!   --listen-client-urls http://0.0.0.0:2379
//! KADEDB_TEST_ETCD_ENDPOINTS=http://127.0.0.1:2379 \
//!   cargo test -p kadedb-services-cluster --manifest-path services/Cargo.toml \
//!   -- --ignored --test-threads=1
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kadedb_services_cluster::{ClusterMembership, MembershipConfig, MembershipEvent, NodeInfo};

fn etcd_endpoints() -> Option<Vec<String>> {
    let raw = std::env::var("KADEDB_TEST_ETCD_ENDPOINTS").ok()?;
    Some(raw.split(',').map(|s| s.trim().to_string()).collect())
}

/// A key prefix unique to this test run, so parallel/repeated runs never see stale keys
/// left behind by a previous run.
fn unique_prefix(test_name: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("/kadedb/cluster/test-{test_name}-{nanos}-{n}/")
}

macro_rules! require_etcd {
    () => {
        match etcd_endpoints() {
            Some(endpoints) => endpoints,
            None => {
                eprintln!(
                    "skipping: set KADEDB_TEST_ETCD_ENDPOINTS to run cluster membership integration tests"
                );
                return;
            }
        }
    };
}

#[tokio::test]
#[ignore]
async fn join_registers_node_and_members_lists_it() {
    let endpoints = require_etcd!();
    let mut config = MembershipConfig::new(endpoints);
    config.key_prefix = unique_prefix("join-list");

    let node = NodeInfo {
        node_id: "node-a".to_string(),
        address: "127.0.0.1:9001".to_string(),
    };
    let mut membership = ClusterMembership::join(&config, node.clone())
        .await
        .expect("join");

    let members = membership.members().await.expect("members");
    assert_eq!(members, vec![node]);

    membership.leave().await.expect("leave");
}

#[tokio::test]
#[ignore]
async fn graceful_leave_removes_node_immediately() {
    let endpoints = require_etcd!();
    let mut config = MembershipConfig::new(endpoints);
    config.key_prefix = unique_prefix("graceful-leave");

    let node = NodeInfo {
        node_id: "node-a".to_string(),
        address: "127.0.0.1:9001".to_string(),
    };
    let mut membership = ClusterMembership::join(&config, node).await.expect("join");
    assert_eq!(membership.members().await.expect("members").len(), 1);

    membership.leave().await.expect("leave");

    // Re-join under the same prefix just to get a client for the follow-up read.
    let node_b = NodeInfo {
        node_id: "node-b".to_string(),
        address: "127.0.0.1:9002".to_string(),
    };
    let mut observer = ClusterMembership::join(&config, node_b.clone())
        .await
        .expect("join observer");
    let members = observer.members().await.expect("members");
    assert_eq!(members, vec![node_b]);

    observer.leave().await.expect("leave observer");
}

#[tokio::test]
#[ignore]
async fn watch_observes_join_and_leave_events() {
    let endpoints = require_etcd!();
    let mut config = MembershipConfig::new(endpoints);
    config.key_prefix = unique_prefix("watch");

    let node_id = "node-a".to_string();
    let node = NodeInfo {
        node_id: node_id.clone(),
        address: "127.0.0.1:9001".to_string(),
    };

    // A separate connection just to hold the watch, independent of the joining node.
    let mut watcher_conn = ClusterMembership::join(
        &config,
        NodeInfo {
            node_id: "watcher".to_string(),
            address: "127.0.0.1:9999".to_string(),
        },
    )
    .await
    .expect("join watcher placeholder");
    let mut watch = watcher_conn.watch().await.expect("watch");

    let membership = ClusterMembership::join(&config, node.clone())
        .await
        .expect("join");

    let joined = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("timed out waiting for join event")
        .expect("watch stream ended")
        .expect("join event");
    assert_eq!(joined, MembershipEvent::Joined(node));

    membership.leave().await.expect("leave");

    let left = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("timed out waiting for leave event")
        .expect("watch stream ended")
        .expect("leave event");
    assert_eq!(left, MembershipEvent::Left { node_id });

    watcher_conn
        .leave()
        .await
        .expect("leave watcher placeholder");
}

#[tokio::test]
#[ignore]
async fn heartbeat_failure_is_observed_after_lease_ttl_expires() {
    let endpoints = require_etcd!();
    let mut config = MembershipConfig::new(endpoints);
    config.key_prefix = unique_prefix("heartbeat-failure");
    config.lease_ttl_secs = 2;

    let node = NodeInfo {
        node_id: "node-a".to_string(),
        address: "127.0.0.1:9001".to_string(),
    };
    let mut membership = ClusterMembership::join(&config, node.clone())
        .await
        .expect("join");
    assert_eq!(membership.members().await.expect("members"), vec![node]);

    // Simulate a crash: heartbeats stop, but the lease is never explicitly revoked.
    membership.stop_heartbeat();

    let deadline = Duration::from_secs((config.lease_ttl_secs as u64) * 3);
    let poll_interval = Duration::from_millis(200);
    let mut waited = Duration::ZERO;
    loop {
        let members = membership.members().await.expect("members");
        if members.is_empty() {
            break;
        }
        if waited >= deadline {
            panic!(
                "node still present {waited:?} after heartbeat stopped (lease_ttl={:?})",
                Duration::from_secs(config.lease_ttl_secs as u64)
            );
        }
        tokio::time::sleep(poll_interval).await;
        waited += poll_interval;
    }
}
