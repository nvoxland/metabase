//! C ABI for the engine, loaded into the Metabase JVM through Panama.
//!
//! Rules at the boundary:
//! - Only `*const c_char` (UTF-8, NUL-terminated), opaque handles, integers, and a raw
//!   pointer to a caller-allocated `ArrowArrayStream` cross it.
//! - Every export catches panics. Return codes: 0 ok, 1 error (see `mb_engine_last_error`),
//!   2 panic.
//! - Strings the engine returns through `char**` are freed only by `mb_engine_free_string`.
//! - Input strings are borrowed for the duration of the call.

use std::cell::RefCell;
use std::ffi::{c_char, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use engine_core::api::{Request, Response, SessionContext};
use engine_core::connector::Connector;
use engine_core::{Engine, EngineConfig, EngineError, ErrorKind};

/// The warehouse connectors compiled into this library.
pub fn connectors() -> Vec<Arc<dyn Connector>> {
    vec![
        Arc::new(engine_connector_postgres::PostgresConnector),
        Arc::new(engine_connector_mysql::MySqlConnector),
        Arc::new(engine_connector_jdbc_bridge::JdbcBridgeConnector),
    ]
}

pub const MB_OK: i32 = 0;
pub const MB_ERROR: i32 = 1;
pub const MB_PANIC: i32 = 2;

/// Opaque engine handle.
pub struct MbEngine {
    inner: Arc<Engine>,
}

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(error: &EngineError) {
    let json = error.to_json();
    let c = CString::new(json).unwrap_or_else(|_| CString::new("{\"kind\":\"internal_panic\",\"error_type\":\"qp\",\"message\":\"error contained NUL\"}").unwrap());
    LAST_ERROR.with(|slot| *slot.borrow_mut() = Some(c));
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| *slot.borrow_mut() = None);
}

/// Run `f`, converting errors and panics into return codes.
fn guarded(f: impl FnOnce() -> Result<(), EngineError>) -> i32 {
    clear_last_error();
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => MB_OK,
        Ok(Err(e)) => {
            set_last_error(&e);
            MB_ERROR
        }
        Err(payload) => {
            let message = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "panic with non-string payload".to_string()
            };
            set_last_error(&EngineError::new(
                ErrorKind::InternalPanic,
                format!("engine panicked: {message}"),
            ));
            MB_PANIC
        }
    }
}

/// # Safety
/// `p` must be null or a valid NUL-terminated string that outlives the call.
unsafe fn borrow_str<'a>(p: *const c_char, what: &str) -> Result<&'a str, EngineError> {
    if p.is_null() {
        return Err(EngineError::invalid_request(format!("{what} is null")));
    }
    CStr::from_ptr(p)
        .to_str()
        .map_err(|e| EngineError::invalid_request(format!("{what} is not UTF-8: {e}")))
}

/// # Safety
/// `out` must be null or a valid pointer to a `*mut c_char` slot.
unsafe fn write_out_string(out: *mut *mut c_char, s: String) -> Result<(), EngineError> {
    if out.is_null() {
        return Err(EngineError::invalid_request("output pointer is null"));
    }
    let c = CString::new(s).map_err(|_| EngineError::invalid_request("output contained NUL"))?;
    *out = c.into_raw();
    Ok(())
}

/// # Safety
/// `e` must be a handle returned by `mb_engine_init` that has not been shut down.
unsafe fn engine<'a>(e: *const MbEngine) -> Result<&'a Arc<Engine>, EngineError> {
    if e.is_null() {
        return Err(EngineError::invalid_request("engine handle is null"));
    }
    Ok(&(*e).inner)
}

fn response_to_result(response: Response) -> Result<serde_json::Value, EngineError> {
    match response {
        Response::Ok { result } => Ok(result),
        Response::Err { error } => Err(error),
    }
}

/// Crate version. Static; never free it.
#[no_mangle]
pub extern "C" fn mb_engine_version() -> *const c_char {
    static VERSION: OnceLock<CString> = OnceLock::new();
    VERSION
        .get_or_init(|| CString::new(Engine::version()).unwrap())
        .as_ptr()
}

/// Create an engine from a JSON `EngineConfig` (may be null or empty for defaults).
///
/// # Safety
/// `out` must point to a writable `*mut MbEngine` slot.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_init(
    config_json: *const c_char,
    out: *mut *mut MbEngine,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return Err(EngineError::invalid_request("output pointer is null"));
        }
        let config = if config_json.is_null() {
            EngineConfig::default()
        } else {
            let s = borrow_str(config_json, "config_json")?;
            if s.trim().is_empty() {
                EngineConfig::default()
            } else {
                serde_json::from_str(s)?
            }
        };
        let inner = Engine::with_connectors(config, connectors())?;
        *out = Box::into_raw(Box::new(MbEngine { inner }));
        Ok(())
    })
}

/// Shut the engine down and free the handle. Idempotent for a null handle.
///
/// # Safety
/// `e` must be a handle from `mb_engine_init` not previously shut down.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_shutdown(e: *mut MbEngine, timeout_ms: u32) -> i32 {
    guarded(|| {
        if e.is_null() {
            return Ok(());
        }
        let boxed = Box::from_raw(e);
        boxed
            .inner
            .shutdown(Duration::from_millis(u64::from(timeout_ms)));
        Ok(())
    })
}

/// The generic JSON channel: any `Request`, returns a `Response` envelope in `out_json`.
/// Returns 0 when the response is `ok`, 1 when it is `err` (also set as last error).
///
/// # Safety
/// See crate docs.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_call(
    e: *const MbEngine,
    request_json: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    guarded(|| {
        let engine = engine(e)?;
        let request = borrow_str(request_json, "request_json")?;
        let response = engine.handle_json(request);
        let is_err = !response.is_ok();
        write_out_string(out_json, response.to_json())?;
        if let Response::Err { error } = response {
            if is_err {
                return Err(error);
            }
        }
        Ok(())
    })
}

/// Last error on this thread as JSON, or null. Valid until the next call on this thread.
#[no_mangle]
pub extern "C" fn mb_engine_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| match slot.borrow().as_ref() {
        Some(c) => c.as_ptr(),
        None => std::ptr::null(),
    })
}

/// Free a string returned through any `char**` output.
///
/// # Safety
/// `s` must be null or a pointer produced by this library.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_free_string(s: *mut c_char) {
    if !s.is_null() {
        drop(CString::from_raw(s));
    }
}

/// # Safety
/// See crate docs.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_register_catalog(
    e: *const MbEngine,
    snapshot_json: *const c_char,
) -> i32 {
    guarded(|| {
        let engine = engine(e)?;
        let snapshot = serde_json::from_str(borrow_str(snapshot_json, "snapshot_json")?)?;
        response_to_result(engine.handle(Request::RegisterCatalog { snapshot })).map(|_| ())
    })
}

/// # Safety
/// See crate docs.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_register_database(
    e: *const MbEngine,
    registration_json: *const c_char,
) -> i32 {
    guarded(|| {
        let engine = engine(e)?;
        let registration =
            serde_json::from_str(borrow_str(registration_json, "registration_json")?)?;
        response_to_result(engine.handle(Request::RegisterDatabase { registration })).map(|_| ())
    })
}

/// # Safety
/// See crate docs.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_invalidate_database(
    e: *const MbEngine,
    database_id: i64,
) -> i32 {
    guarded(|| {
        let engine = engine(e)?;
        response_to_result(engine.handle(Request::InvalidateDatabase { database_id })).map(|_| ())
    })
}

/// # Safety
/// `out_session_id` must point to a writable u64.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_session_create(
    e: *const MbEngine,
    context_json: *const c_char,
    out_session_id: *mut u64,
) -> i32 {
    guarded(|| {
        if out_session_id.is_null() {
            return Err(EngineError::invalid_request("output pointer is null"));
        }
        let engine = engine(e)?;
        let context: SessionContext =
            serde_json::from_str(borrow_str(context_json, "context_json")?)?;
        let id = engine.create_session(context)?;
        *out_session_id = id;
        Ok(())
    })
}

/// # Safety
/// See crate docs.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_session_destroy(e: *const MbEngine, session_id: u64) -> i32 {
    guarded(|| {
        let engine = engine(e)?;
        response_to_result(engine.handle(Request::DestroySession { session_id })).map(|_| ())
    })
}

/// Plan `sql` in the session and return `{"warehouse_sql", "warehouse", "schema", "warnings"}`.
///
/// # Safety
/// See crate docs.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_plan_and_unparse(
    e: *const MbEngine,
    session_id: u64,
    sql: *const c_char,
    params_json: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    guarded(|| {
        let engine = engine(e)?;
        let sql = borrow_str(sql, "sql")?.to_string();
        let params = if params_json.is_null() {
            vec![]
        } else {
            serde_json::from_str(borrow_str(params_json, "params_json")?)?
        };
        let result = response_to_result(engine.handle(Request::PlanAndUnparse {
            session_id,
            sql,
            params,
        }))?;
        write_out_string(out_json, result.to_string())
    })
}

/// # Safety
/// See crate docs.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_pool_stats(
    e: *const MbEngine,
    database_id: i64,
    out_json: *mut *mut c_char,
) -> i32 {
    guarded(|| {
        let engine = engine(e)?;
        let database_id = if database_id < 0 {
            None
        } else {
            Some(database_id)
        };
        let result = response_to_result(engine.handle(Request::PoolStats { database_id }))?;
        write_out_string(out_json, result.to_string())
    })
}

/// Plan and start `sql` in the session; rows arrive through `mb_engine_stream_export`.
///
/// # Safety
/// `out_stream_id` must point to a writable u64.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_execute(
    e: *const MbEngine,
    session_id: u64,
    sql: *const c_char,
    params_json: *const c_char,
    out_stream_id: *mut u64,
) -> i32 {
    guarded(|| {
        if out_stream_id.is_null() {
            return Err(EngineError::invalid_request("output pointer is null"));
        }
        let engine = engine(e)?;
        let sql = borrow_str(sql, "sql")?.to_string();
        let params = if params_json.is_null() {
            vec![]
        } else {
            serde_json::from_str(borrow_str(params_json, "params_json")?)?
        };
        let id = engine.execute_blocking(session_id, sql, params)?;
        *out_stream_id = id;
        Ok(())
    })
}

/// Run warehouse-dialect `sql` without planning (Metabase's own metadata queries). Rows arrive
/// through `mb_engine_stream_export` like any other statement.
///
/// # Safety
/// `out_stream_id` must point to a writable u64.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_execute_raw(
    e: *const MbEngine,
    session_id: u64,
    sql: *const c_char,
    params_json: *const c_char,
    out_stream_id: *mut u64,
) -> i32 {
    guarded(|| {
        if out_stream_id.is_null() {
            return Err(EngineError::invalid_request("output pointer is null"));
        }
        let engine = engine(e)?;
        let sql = borrow_str(sql, "sql")?.to_string();
        let params = if params_json.is_null() {
            vec![]
        } else {
            serde_json::from_str(borrow_str(params_json, "params_json")?)?
        };
        let id = engine.execute_raw_blocking(session_id, sql, params)?;
        *out_stream_id = id;
        Ok(())
    })
}

/// Export a stream as an `ArrowArrayStream` into caller-allocated memory (40 bytes, zeroed).
/// The caller pulls batches through the struct's callbacks and must call its `release` exactly
/// once; releasing before the end cancels the statement. Only the first export of a stream succeeds.
///
/// # Safety
/// `out_stream` must point to at least 40 writable, 8-byte-aligned bytes that stay valid until released.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_stream_export(
    e: *const MbEngine,
    stream_id: u64,
    out_stream: *mut c_void,
) -> i32 {
    guarded(|| {
        if out_stream.is_null() {
            return Err(EngineError::invalid_request("output pointer is null"));
        }
        let engine = engine(e)?;
        let reader = engine.blocking_reader(stream_id)?;
        let exported = FFI_ArrowArrayStream::new(Box::new(reader));
        std::ptr::write(out_stream as *mut FFI_ArrowArrayStream, exported);
        Ok(())
    })
}

/// Stop a running statement. Safe from any thread.
///
/// # Safety
/// See crate docs.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_stream_cancel(e: *const MbEngine, stream_id: u64) -> i32 {
    guarded(|| {
        let engine = engine(e)?;
        if engine.cancel_stream(stream_id) {
            Ok(())
        } else {
            Err(EngineError::invalid_request(format!(
                "no stream {stream_id}"
            )))
        }
    })
}

/// Forget a stream handle (after the exported stream was released). Cancels it if still running.
///
/// # Safety
/// See crate docs.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_stream_close(e: *const MbEngine, stream_id: u64) -> i32 {
    guarded(|| {
        let engine = engine(e)?;
        engine.close_stream(stream_id);
        Ok(())
    })
}

/// `{"rows", "batches", "bytes", "elapsed_ms", "exported", "cancelled"}` for a stream.
///
/// # Safety
/// See crate docs.
#[no_mangle]
pub unsafe extern "C" fn mb_engine_stream_stats(
    e: *const MbEngine,
    stream_id: u64,
    out_json: *mut *mut c_char,
) -> i32 {
    guarded(|| {
        let engine = engine(e)?;
        let stats = engine.stream_stats(stream_id)?;
        write_out_string(out_json, serde_json::to_string(&stats)?)
    })
}
