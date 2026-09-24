//! Development sidecar: the same engine core behind Arrow Flight.
//!
//! - `DoAction` with type `call` carries the JSON request envelope (identical to `mb_engine_call`).
//! - `DoGet` will stream query results (not implemented yet).

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use engine_core::Engine;
use futures::stream::{self, Stream};
use tokio::net::TcpListener;
use tonic::{Request, Response, Status, Streaming};

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct EngineFlightService {
    engine: Arc<Engine>,
}

impl EngineFlightService {
    pub fn new(engine: Arc<Engine>) -> EngineFlightService {
        EngineFlightService { engine }
    }
}

#[tonic::async_trait]
impl FlightService for EngineFlightService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;
    type DoExchangeStream = BoxStream<FlightData>;

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(&self, _: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get: query execution lands in milestone 3",
        ))
    }

    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        let body = match action.r#type.as_str() {
            "call" => {
                let json = std::str::from_utf8(&action.body).map_err(|e| {
                    Status::invalid_argument(format!("call body is not UTF-8: {e}"))
                })?;
                self.engine.handle_json_async(json).await.to_json()
            }
            "version" => serde_json::json!({ "version": Engine::version() }).to_string(),
            other => return Err(Status::unimplemented(format!("action {other}"))),
        };
        let result = arrow_flight::Result {
            body: body.into_bytes().into(),
        };
        Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
    }

    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            ActionType {
                r#type: "call".into(),
                description: "JSON request envelope, same as mb_engine_call".into(),
            },
            ActionType {
                r#type: "version".into(),
                description: "engine version".into(),
            },
        ];
        Ok(Response::new(Box::pin(stream::iter(
            actions.into_iter().map(Ok),
        ))))
    }
}

/// Serve Flight on an already-bound listener until `shutdown` resolves.
pub async fn serve(
    engine: Arc<Engine>,
    listener: TcpListener,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(EngineFlightService::new(engine)))
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await
}
