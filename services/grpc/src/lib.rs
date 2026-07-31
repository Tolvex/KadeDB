use std::pin::Pin;

use kadedb_services_auth::{authorize_bearer_header, AuthConfig, AuthError, Permission};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};

pub mod kadedb {
    tonic::include_proto!("kadedb");
}

fn map_auth_error(err: AuthError) -> Status {
    match err {
        AuthError::Forbidden => Status::permission_denied("forbidden"),
        _ => Status::unauthenticated("unauthenticated"),
    }
}

use kadedb::query_service_server::{QueryService, QueryServiceServer};
use kadedb::{QueryRequest, QueryRow};

/// `QueryService` implementation. Not yet wired to `kadedb_ffi`/`kadedb_core` — by
/// default it echoes the incoming query back as canned rows. `with_rows` overrides
/// those canned rows, so callers (e.g. `kadedb-services-router`'s tests) can stand up
/// several instances with distinguishable per-instance data to simulate shards.
#[derive(Default)]
pub struct QueryServiceImpl {
    rows: Option<Vec<String>>,
}

impl QueryServiceImpl {
    pub fn with_rows(rows: Vec<String>) -> Self {
        Self { rows: Some(rows) }
    }
}

#[tonic::async_trait]
impl QueryService for QueryServiceImpl {
    type QueryStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<QueryRow, Status>> + Send>>;

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let query = request.into_inner().query;

        let (tx, rx) = tokio::sync::mpsc::channel(8);

        let rows = self.rows.clone().unwrap_or_else(|| {
            vec![
                serde_json::json!({"echo": query, "row": 1}).to_string(),
                serde_json::json!({"echo": query, "row": 2}).to_string(),
                serde_json::json!({"echo": query, "row": 3}).to_string(),
            ]
        });

        tokio::spawn(async move {
            for json in rows {
                if tx.send(Ok(QueryRow { json })).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::QueryStream
        ))
    }
}

pub async fn serve(addr: std::net::SocketAddr, auth_cfg: AuthConfig) {
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    serve_with_listener(listener, auth_cfg).await;
}

pub async fn serve_with_listener(listener: tokio::net::TcpListener, auth_cfg: AuthConfig) {
    serve_with_listener_and_service(listener, auth_cfg, QueryServiceImpl::default()).await;
}

/// Same as [`serve_with_listener`], but with a caller-supplied `QueryServiceImpl`
/// (e.g. one built via [`QueryServiceImpl::with_rows`]) instead of the default echo.
pub async fn serve_with_listener_and_service(
    listener: tokio::net::TcpListener,
    auth_cfg: AuthConfig,
    service: QueryServiceImpl,
) {
    let interceptor = move |req: Request<()>| -> Result<Request<()>, Status> {
        if !auth_cfg.enabled {
            return Ok(req);
        }

        let header = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok());

        authorize_bearer_header(&auth_cfg, header, Permission::Read)
            .map(|_| req)
            .map_err(map_auth_error)
    };

    let svc = QueryServiceServer::with_interceptor(service, interceptor);

    Server::builder()
        .add_service(svc)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
        .expect("serve");
}
