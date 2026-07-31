use kadedb_services_auth::AuthConfig;
use kadedb_services_grpc::QueryServiceImpl;
use kadedb_services_router::{MergeStrategy, QueryRouter, ShardInfo, ShardTopology};

async fn spawn_shard(rows: Vec<String>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        kadedb_services_grpc::serve_with_listener_and_service(
            listener,
            AuthConfig {
                enabled: false,
                jwt_secret: None,
            },
            QueryServiceImpl::with_rows(rows),
        )
        .await;
    });
    addr.to_string()
}

#[tokio::test]
async fn routes_range_query_to_the_owning_shard_only() {
    let shard1_addr = spawn_shard(vec![
        "{\"shard\":1,\"marker\":\"should-not-appear\"}".to_string()
    ])
    .await;
    let shard2_addr = spawn_shard(vec![
        "{\"shard\":2,\"marker\":\"expected-a\"}".to_string(),
        "{\"shard\":2,\"marker\":\"expected-b\"}".to_string(),
    ])
    .await;

    let topology = ShardTopology::new(
        "ts",
        vec![
            ShardInfo {
                shard_id: 1,
                range_start: i64::MIN,
                range_end: 100,
                address: shard1_addr,
            },
            ShardInfo {
                shard_id: 2,
                range_start: 100,
                range_end: i64::MAX,
                address: shard2_addr,
            },
        ],
    );
    let router = QueryRouter::new(topology);

    let rows = router
        .execute(
            "SELECT * FROM points WHERE ts >= 150",
            MergeStrategy::Concat,
        )
        .await
        .expect("execute");

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.contains("\"shard\":2")));
    assert!(!rows.iter().any(|r| r.contains("should-not-appear")));
}

#[tokio::test]
async fn merges_time_bucket_first_across_shards_over_the_wire() {
    let shard1_addr = spawn_shard(vec![
        "{\"bucket\":\"b0\",\"value\":\"from-shard-1\"}".to_string()
    ])
    .await;
    let shard2_addr = spawn_shard(vec![
        "{\"bucket\":\"b0\",\"value\":\"from-shard-2\"}".to_string(),
        "{\"bucket\":\"b1\",\"value\":\"from-shard-2\"}".to_string(),
    ])
    .await;

    let topology = ShardTopology::new(
        "ts",
        vec![
            ShardInfo {
                shard_id: 1,
                range_start: i64::MIN,
                range_end: 100,
                address: shard1_addr,
            },
            ShardInfo {
                shard_id: 2,
                range_start: 100,
                range_end: i64::MAX,
                address: shard2_addr,
            },
        ],
    );
    let router = QueryRouter::new(topology);

    let rows = router
        .execute(
            "SELECT TIME_BUCKET(ts, 60), FIRST(value, ts) FROM points",
            MergeStrategy::TimeBucketFirst {
                bucket_key: "bucket".to_string(),
            },
        )
        .await
        .expect("execute");

    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .any(|r| r.contains("\"bucket\":\"b0\"") && r.contains("from-shard-1")));
    assert!(rows
        .iter()
        .any(|r| r.contains("\"bucket\":\"b1\"") && r.contains("from-shard-2")));
}
