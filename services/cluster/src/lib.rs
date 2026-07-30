//! Cluster membership for KadeDB's distributed mode.
//!
//! Node discovery, liveness (heartbeat), and cluster configuration (the current member
//! list) are delegated to etcd, per `docs/spec/00-project-spec.md`'s Core Rules and Open
//! Decisions: KadeDB does not self-implement a membership/consensus protocol. Each node
//! registers itself under a key prefix, bound to a TTL lease it renews with periodic
//! keep-alives; if a node stops renewing (crash, network partition), etcd expires the
//! lease and deletes the key, which callers observe via [`ClusterMembership::members`] or
//! [`ClusterMembership::watch`].
//!
//! Leader election, shard routing, and WAL replication are out of scope here — see
//! Plans.md Tasks 1.4/1.5.

use std::time::Duration;

use etcd_client::{Client, EventType, GetOptions, PutOptions, WatchOptions, WatchStream, Watcher};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

/// Default etcd key prefix under which nodes register themselves.
pub const DEFAULT_KEY_PREFIX: &str = "/kadedb/cluster/nodes/";

/// Default lease TTL, in seconds, used to detect a node as failed once heartbeats stop.
pub const DEFAULT_LEASE_TTL_SECS: i64 = 5;

#[derive(Debug, thiserror::Error)]
pub enum MembershipError {
    // Boxed because `etcd_client::Error` is large; boxing keeps `Result<_, MembershipError>`
    // cheap to return and pass around (see clippy::result_large_err).
    #[error("etcd error: {0}")]
    Etcd(Box<etcd_client::Error>),

    #[error("failed to (de)serialize node info: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("received a membership entry with no value: key={0}")]
    EmptyValue(String),
}

impl From<etcd_client::Error> for MembershipError {
    fn from(err: etcd_client::Error) -> Self {
        MembershipError::Etcd(Box::new(err))
    }
}

/// Static metadata a node advertises to the rest of the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub address: String,
}

/// Connection and registration parameters for joining a cluster.
#[derive(Debug, Clone)]
pub struct MembershipConfig {
    pub etcd_endpoints: Vec<String>,
    pub key_prefix: String,
    pub lease_ttl_secs: i64,
}

impl MembershipConfig {
    pub fn new(etcd_endpoints: Vec<String>) -> Self {
        Self {
            etcd_endpoints,
            key_prefix: DEFAULT_KEY_PREFIX.to_string(),
            lease_ttl_secs: DEFAULT_LEASE_TTL_SECS,
        }
    }
}

fn node_key(key_prefix: &str, node_id: &str) -> String {
    format!("{key_prefix}{node_id}")
}

/// A membership change observed via [`ClusterMembership::watch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipEvent {
    Joined(NodeInfo),
    Left { node_id: String },
}

/// A live handle on the cluster's node-join/leave event stream.
pub struct MembershipWatcher {
    _watcher: Watcher,
    stream: WatchStream,
    key_prefix: String,
    // etcd may batch several key events into one `WatchResponse`; these are decoded ones
    // still waiting to be returned from `next()`, in order.
    pending: std::collections::VecDeque<MembershipEvent>,
}

impl MembershipWatcher {
    /// Waits for and returns the next membership event, or `Ok(None)` if the watch
    /// stream ended (e.g. the etcd connection was closed).
    pub async fn next(&mut self) -> Result<Option<MembershipEvent>, MembershipError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }
            let Some(resp) = self.stream.message().await? else {
                return Ok(None);
            };
            for event in resp.events() {
                let Some(kv) = event.kv() else { continue };
                let key = String::from_utf8_lossy(kv.key()).into_owned();
                let node_id = key
                    .strip_prefix(&self.key_prefix)
                    .unwrap_or(&key)
                    .to_string();
                match event.event_type() {
                    EventType::Put => {
                        let node = decode_node_info(&key, kv.value())?;
                        self.pending.push_back(MembershipEvent::Joined(node));
                    }
                    EventType::Delete => {
                        self.pending.push_back(MembershipEvent::Left { node_id });
                    }
                }
            }
        }
    }
}

fn decode_node_info(key: &str, value: &[u8]) -> Result<NodeInfo, MembershipError> {
    if value.is_empty() {
        return Err(MembershipError::EmptyValue(key.to_string()));
    }
    Ok(serde_json::from_slice(value)?)
}

/// A background task that periodically renews a node's etcd lease. Renewal is driven by
/// etcd's own keep-alive cadence recommendation (roughly a third of the TTL, floored at 1s
/// for small TTLs) so a couple of dropped heartbeats don't immediately expire the node.
struct HeartbeatHandle {
    task: Option<JoinHandle<()>>,
}

impl HeartbeatHandle {
    fn spawn(mut client: Client, lease_id: i64, lease_ttl_secs: i64) -> Self {
        let period = Duration::from_secs((lease_ttl_secs.max(1) as u64 / 3).max(1));
        let task = tokio::spawn(async move {
            let (mut keeper, mut stream) = match client.lease_keep_alive(lease_id).await {
                Ok(pair) => pair,
                Err(err) => {
                    tracing::warn!(error = %err, lease_id, "failed to start lease keep-alive");
                    return;
                }
            };
            let mut interval = tokio::time::interval(period);
            interval.tick().await; // first tick fires immediately; lease_grant already covers t=0
            loop {
                interval.tick().await;
                if let Err(err) = keeper.keep_alive().await {
                    tracing::warn!(error = %err, lease_id, "lease keep-alive send failed");
                    return;
                }
                match stream.message().await {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        tracing::warn!(lease_id, "lease keep-alive stream closed by etcd");
                        return;
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, lease_id, "lease keep-alive ack failed");
                        return;
                    }
                }
            }
        });
        Self { task: Some(task) }
    }

    /// Stops sending heartbeats without revoking the lease. Used both for clean shutdown
    /// (immediately followed by an explicit lease revoke) and, in tests, to simulate a
    /// node crash: the key then stays registered until the lease's TTL naturally expires.
    fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Drop for HeartbeatHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A node's active membership in the cluster: it owns a leased etcd registration and a
/// background heartbeat task that keeps that lease alive.
pub struct ClusterMembership {
    client: Client,
    node: NodeInfo,
    key_prefix: String,
    lease_id: i64,
    heartbeat: HeartbeatHandle,
}

impl ClusterMembership {
    /// Registers `node` under `config.key_prefix`, bound to a fresh lease with
    /// `config.lease_ttl_secs` TTL, and starts the background heartbeat that renews it.
    pub async fn join(config: &MembershipConfig, node: NodeInfo) -> Result<Self, MembershipError> {
        let mut client = Client::connect(&config.etcd_endpoints, None).await?;

        let lease = client.lease_grant(config.lease_ttl_secs, None).await?;
        let lease_id = lease.id();

        let key = node_key(&config.key_prefix, &node.node_id);
        let value = serde_json::to_vec(&node)?;
        client
            .put(
                key.clone().into_bytes(),
                value,
                Some(PutOptions::new().with_lease(lease_id)),
            )
            .await?;

        let heartbeat = HeartbeatHandle::spawn(client.clone(), lease_id, config.lease_ttl_secs);

        Ok(Self {
            client,
            node,
            key_prefix: config.key_prefix.clone(),
            lease_id,
            heartbeat,
        })
    }

    /// This node's own advertised metadata.
    pub fn node(&self) -> &NodeInfo {
        &self.node
    }

    /// Lists all nodes currently registered under the cluster's key prefix.
    pub async fn members(&mut self) -> Result<Vec<NodeInfo>, MembershipError> {
        let resp = self
            .client
            .get(
                self.key_prefix.clone().into_bytes(),
                Some(GetOptions::new().with_prefix()),
            )
            .await?;
        resp.kvs()
            .iter()
            .map(|kv| decode_node_info(&String::from_utf8_lossy(kv.key()), kv.value()))
            .collect()
    }

    /// Starts watching the cluster's key prefix for join/leave events, starting from now.
    pub async fn watch(&mut self) -> Result<MembershipWatcher, MembershipError> {
        let (watcher, stream) = self
            .client
            .watch(
                self.key_prefix.clone().into_bytes(),
                Some(WatchOptions::new().with_prefix()),
            )
            .await?;
        Ok(MembershipWatcher {
            _watcher: watcher,
            stream,
            key_prefix: self.key_prefix.clone(),
            pending: std::collections::VecDeque::new(),
        })
    }

    /// Stops the heartbeat without revoking the lease, simulating an unclean node
    /// departure (crash / network partition): the node's key remains visible to
    /// [`members`](Self::members) and other watchers until the lease's TTL elapses.
    pub fn stop_heartbeat(&mut self) {
        self.heartbeat.stop();
    }

    /// Gracefully leaves the cluster: stops the heartbeat and immediately revokes the
    /// lease, which deletes the node's key right away instead of waiting out the TTL.
    pub async fn leave(mut self) -> Result<(), MembershipError> {
        self.heartbeat.stop();
        self.client.lease_revoke(self.lease_id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_key_joins_prefix_and_id() {
        assert_eq!(
            node_key("/kadedb/cluster/nodes/", "node-1"),
            "/kadedb/cluster/nodes/node-1"
        );
    }

    #[test]
    fn node_info_round_trips_through_json() {
        let node = NodeInfo {
            node_id: "node-1".to_string(),
            address: "127.0.0.1:9000".to_string(),
        };
        let encoded = serde_json::to_vec(&node).unwrap();
        let decoded = decode_node_info("any-key", &encoded).unwrap();
        assert_eq!(node, decoded);
    }

    #[test]
    fn decode_node_info_rejects_empty_value() {
        let err = decode_node_info("/kadedb/cluster/nodes/node-1", b"").unwrap_err();
        assert!(matches!(err, MembershipError::EmptyValue(_)));
    }

    #[test]
    fn membership_config_new_uses_defaults() {
        let config = MembershipConfig::new(vec!["http://127.0.0.1:2379".to_string()]);
        assert_eq!(config.key_prefix, DEFAULT_KEY_PREFIX);
        assert_eq!(config.lease_ttl_secs, DEFAULT_LEASE_TTL_SECS);
    }
}
