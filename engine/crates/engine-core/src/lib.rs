//! Metabase query engine core: parse DataFusion SQL, apply policy, unparse to the warehouse
//! dialect, execute through a connector, stream Arrow back. Knows nothing about MBQL.

pub mod api;
pub mod catalog;
pub mod connector;
pub mod engine;
pub mod error;
pub mod planner;
pub mod policy;
pub mod pools;
pub mod schema_cast;
pub mod stream;

pub use api::{Request, Response, SessionContext};
pub use engine::{Engine, EngineConfig, VERSION};
pub use error::{EngineError, ErrorKind, Result};
