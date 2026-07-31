//! Distributed query routing for KadeDB's Rust services layer.
//!
//! See `docs/spec/00-project-spec.md`'s "Task 1.4 Design" for the shard topology model,
//! the routing heuristic, and the map-reduce merge rules this module implements.

use std::collections::BTreeMap;

use kadedb_services_grpc::kadedb::{query_service_client::QueryServiceClient, QueryRequest};

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("failed to connect to shard {shard_id} at {address}: {source}")]
    Connect {
        shard_id: u64,
        address: String,
        #[source]
        source: tonic::transport::Error,
    },
    #[error("query to shard {shard_id} failed: {source}")]
    Query {
        shard_id: u64,
        #[source]
        source: tonic::Status,
    },
}

/// One shard's static range and address. `range_start` is inclusive, `range_end` is
/// exclusive; use `i64::MIN`/`i64::MAX` for an unbounded first/last shard.
#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub shard_id: u64,
    pub range_start: i64,
    pub range_end: i64,
    pub address: String,
}

/// Static shard topology for one sharded table's key column.
#[derive(Debug, Clone)]
pub struct ShardTopology {
    pub shard_key_column: String,
    pub shards: Vec<ShardInfo>,
}

impl ShardTopology {
    pub fn new(shard_key_column: impl Into<String>, shards: Vec<ShardInfo>) -> Self {
        Self {
            shard_key_column: shard_key_column.into(),
            shards,
        }
    }

    /// Shards whose range can hold a row matching `query`'s bound (if any) on
    /// `shard_key_column`. Falls back to every shard when no bound is recognized —
    /// broadcasting is always correct, just not minimal.
    pub fn shards_for_query(&self, query: &str) -> Vec<&ShardInfo> {
        match extract_bound(&self.shard_key_column, query) {
            Some((lo, hi)) => self
                .shards
                .iter()
                .filter(|s| ranges_overlap(s.range_start, s.range_end, lo, hi))
                .collect(),
            None => self.shards.iter().collect(),
        }
    }
}

fn ranges_overlap(a_start: i64, a_end: i64, b_start: i64, b_end: i64) -> bool {
    a_start < b_end && b_start < a_end
}

/// Scans `query`'s tokens for a bound on `column` (`col >= N`, `col > N`, `col <= N`,
/// `col < N`, `col = N`, `col BETWEEN a AND b`, conjoined with `AND`). Not a KadeQL
/// parse — a documented heuristic (see Task 1.4 Design). Returns a half-open
/// `[lo, hi)` bound, or `None` if no recognizable bound on `column` was found.
fn extract_bound(column: &str, query: &str) -> Option<(i64, i64)> {
    let normalized = query.replace(['(', ')', ','], " ");
    let tokens: Vec<&str> = normalized.split_whitespace().collect();

    let mut lo = i64::MIN;
    let mut hi = i64::MAX;
    let mut found = false;

    for (i, tok) in tokens.iter().enumerate() {
        if !tok.eq_ignore_ascii_case(column) {
            continue;
        }

        if tokens
            .get(i + 1)
            .is_some_and(|t| t.eq_ignore_ascii_case("BETWEEN"))
        {
            if let (Some(a), Some(b)) = (
                tokens.get(i + 2).and_then(|t| t.parse::<i64>().ok()),
                tokens.get(i + 4).and_then(|t| t.parse::<i64>().ok()),
            ) {
                lo = lo.max(a);
                hi = hi.min(b.saturating_add(1));
                found = true;
            }
            continue;
        }

        let (Some(op), Some(n)) = (
            tokens.get(i + 1),
            tokens.get(i + 2).and_then(|t| t.parse::<i64>().ok()),
        ) else {
            continue;
        };

        match *op {
            ">=" => {
                lo = lo.max(n);
                found = true;
            }
            ">" => {
                lo = lo.max(n.saturating_add(1));
                found = true;
            }
            "<=" => {
                hi = hi.min(n.saturating_add(1));
                found = true;
            }
            "<" => {
                hi = hi.min(n);
                found = true;
            }
            "=" => {
                lo = lo.max(n);
                hi = hi.min(n.saturating_add(1));
                found = true;
            }
            _ => {}
        }
    }

    found.then_some((lo, hi))
}

/// How to combine per-shard rows into one result set. `Concat` fits plain `SELECT`s;
/// the `TimeBucketFirst`/`TimeBucketLast` variants fit KadeQL's only aggregate shape,
/// `TIME_BUCKET(...) ... FIRST(...)`/`LAST(...)` (see Task 1.4 Design for why the
/// bucket key is caller-supplied rather than inferred from row JSON).
#[derive(Debug, Clone)]
pub enum MergeStrategy {
    Concat,
    TimeBucketFirst { bucket_key: String },
    TimeBucketLast { bucket_key: String },
}

/// Merges each shard's rows per `strategy`. `per_shard` need not be pre-sorted by
/// shard id — the `TimeBucket*` variants sort internally since the merge rule depends
/// on shard order (see Task 1.4 Design's FIRST/LAST merge rule).
pub fn merge_rows(strategy: MergeStrategy, per_shard: Vec<(u64, Vec<String>)>) -> Vec<String> {
    match strategy {
        MergeStrategy::Concat => per_shard.into_iter().flat_map(|(_, rows)| rows).collect(),
        MergeStrategy::TimeBucketFirst { bucket_key } => {
            merge_time_bucket(per_shard, &bucket_key, false)
        }
        MergeStrategy::TimeBucketLast { bucket_key } => {
            merge_time_bucket(per_shard, &bucket_key, true)
        }
    }
}

fn merge_time_bucket(
    mut per_shard: Vec<(u64, Vec<String>)>,
    bucket_key: &str,
    prefer_last: bool,
) -> Vec<String> {
    per_shard.sort_by_key(|(shard_id, _)| *shard_id);

    let mut by_bucket: BTreeMap<String, (u64, String)> = BTreeMap::new();
    for (shard_id, rows) in per_shard {
        for row in rows {
            let Some(key) = bucket_key_of(&row, bucket_key) else {
                continue;
            };
            match by_bucket.get(&key) {
                None => {
                    by_bucket.insert(key, (shard_id, row));
                }
                Some((existing_shard_id, _)) => {
                    let replace = if prefer_last {
                        shard_id > *existing_shard_id
                    } else {
                        shard_id < *existing_shard_id
                    };
                    if replace {
                        by_bucket.insert(key, (shard_id, row));
                    }
                }
            }
        }
    }

    by_bucket.into_values().map(|(_, row)| row).collect()
}

fn bucket_key_of(row_json: &str, bucket_key: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(row_json).ok()?;
    value.as_object()?.get(bucket_key).map(|v| v.to_string())
}

/// Fans a query out to the shards `ShardTopology::shards_for_query` selects, then
/// merges the results per the caller-chosen [`MergeStrategy`].
pub struct QueryRouter {
    topology: ShardTopology,
}

impl QueryRouter {
    pub fn new(topology: ShardTopology) -> Self {
        Self { topology }
    }

    pub async fn execute(
        &self,
        query: &str,
        merge: MergeStrategy,
    ) -> Result<Vec<String>, RouterError> {
        let targets: Vec<ShardInfo> = self
            .topology
            .shards_for_query(query)
            .into_iter()
            .cloned()
            .collect();

        let mut set = tokio::task::JoinSet::new();
        for shard in targets {
            let query = query.to_string();
            set.spawn(async move { query_shard(shard, query).await });
        }

        let mut per_shard = Vec::new();
        while let Some(result) = set.join_next().await {
            per_shard.push(result.expect("shard query task panicked")?);
        }

        Ok(merge_rows(merge, per_shard))
    }
}

async fn query_shard(shard: ShardInfo, query: String) -> Result<(u64, Vec<String>), RouterError> {
    let endpoint = format!("http://{}", shard.address);
    let mut client = QueryServiceClient::connect(endpoint)
        .await
        .map_err(|source| RouterError::Connect {
            shard_id: shard.shard_id,
            address: shard.address.clone(),
            source,
        })?;

    let mut stream = client
        .query(QueryRequest { query })
        .await
        .map_err(|source| RouterError::Query {
            shard_id: shard.shard_id,
            source,
        })?
        .into_inner();

    let mut rows = Vec::new();
    while let Some(row) = stream
        .message()
        .await
        .map_err(|source| RouterError::Query {
            shard_id: shard.shard_id,
            source,
        })?
    {
        rows.push(row.json);
    }

    Ok((shard.shard_id, rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology() -> ShardTopology {
        ShardTopology::new(
            "ts",
            vec![
                ShardInfo {
                    shard_id: 1,
                    range_start: i64::MIN,
                    range_end: 100,
                    address: "shard-1".to_string(),
                },
                ShardInfo {
                    shard_id: 2,
                    range_start: 100,
                    range_end: 200,
                    address: "shard-2".to_string(),
                },
                ShardInfo {
                    shard_id: 3,
                    range_start: 200,
                    range_end: i64::MAX,
                    address: "shard-3".to_string(),
                },
            ],
        )
    }

    #[test]
    fn shards_for_query_narrows_to_overlapping_range() {
        let topo = topology();
        let targets = topo.shards_for_query("SELECT * FROM points WHERE ts >= 100 AND ts < 200");
        assert_eq!(
            targets.iter().map(|s| s.shard_id).collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[test]
    fn shards_for_query_handles_between() {
        let topo = topology();
        let targets = topo.shards_for_query("SELECT * FROM points WHERE ts BETWEEN 50 AND 150");
        assert_eq!(
            targets.iter().map(|s| s.shard_id).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn shards_for_query_broadcasts_when_no_bound_recognized() {
        let topo = topology();
        let targets = topo.shards_for_query("SELECT * FROM points");
        assert_eq!(targets.len(), 3);
    }

    #[test]
    fn merge_concat_preserves_shard_order() {
        let per_shard = vec![
            (2u64, vec!["{\"b\":2}".to_string()]),
            (1u64, vec!["{\"a\":1}".to_string()]),
        ];
        let merged = merge_rows(MergeStrategy::Concat, per_shard);
        assert_eq!(
            merged,
            vec!["{\"b\":2}".to_string(), "{\"a\":1}".to_string()]
        );
    }

    #[test]
    fn merge_time_bucket_first_prefers_lowest_shard() {
        let per_shard = vec![
            (1u64, vec!["{\"bucket\":\"b0\",\"from\":1}".to_string()]),
            (2u64, vec!["{\"bucket\":\"b0\",\"from\":2}".to_string()]),
            (2u64, vec!["{\"bucket\":\"b1\",\"from\":2}".to_string()]),
        ];
        let merged = merge_rows(
            MergeStrategy::TimeBucketFirst {
                bucket_key: "bucket".to_string(),
            },
            per_shard,
        );
        assert_eq!(
            merged,
            vec![
                "{\"bucket\":\"b0\",\"from\":1}".to_string(),
                "{\"bucket\":\"b1\",\"from\":2}".to_string(),
            ]
        );
    }

    #[test]
    fn merge_time_bucket_last_prefers_highest_shard() {
        let per_shard = vec![
            (1u64, vec!["{\"bucket\":\"b0\",\"from\":1}".to_string()]),
            (2u64, vec!["{\"bucket\":\"b0\",\"from\":2}".to_string()]),
        ];
        let merged = merge_rows(
            MergeStrategy::TimeBucketLast {
                bucket_key: "bucket".to_string(),
            },
            per_shard,
        );
        assert_eq!(merged, vec!["{\"bucket\":\"b0\",\"from\":2}".to_string()]);
    }
}
