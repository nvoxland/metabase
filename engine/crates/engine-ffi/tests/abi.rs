//! Exercise the C ABI the way the JVM will: through the exported functions, with raw
//! pointers, checking return codes and the thread-local last error.

use std::ffi::{c_char, CStr, CString};
use std::ptr;

use metabase_engine::*;

fn c(s: &str) -> CString {
    CString::new(s).unwrap()
}

unsafe fn take_string(p: *mut c_char) -> String {
    assert!(!p.is_null());
    let s = CStr::from_ptr(p).to_string_lossy().into_owned();
    mb_engine_free_string(p);
    s
}

unsafe fn last_error() -> serde_json::Value {
    let p = mb_engine_last_error();
    assert!(!p.is_null(), "expected a last error");
    serde_json::from_str(&CStr::from_ptr(p).to_string_lossy()).unwrap()
}

unsafe fn init(config: &str) -> *mut MbEngine {
    let mut handle: *mut MbEngine = ptr::null_mut();
    assert_eq!(mb_engine_init(c(config).as_ptr(), &mut handle), MB_OK);
    assert!(!handle.is_null());
    handle
}

const SNAPSHOT: &str = r#"{"database_id":42,"catalog_version":"v1","connector":"postgres",
  "tables":[{"schema":"public","name":"Orders","columns":[
    {"name":"ID","type":"Int64","nullable":false},{"name":"Total","type":"Float64"}]}]}"#;

#[test]
fn version_is_the_crate_version() {
    let v = unsafe { CStr::from_ptr(mb_engine_version()) }
        .to_str()
        .unwrap();
    assert_eq!(v, env!("CARGO_PKG_VERSION"));
}

#[test]
fn full_lifecycle_through_the_abi() {
    unsafe {
        let e = init("{}");

        let mut out: *mut c_char = ptr::null_mut();
        assert_eq!(
            mb_engine_call(e, c(r#"{"op":"version"}"#).as_ptr(), &mut out),
            MB_OK
        );
        let response: serde_json::Value = serde_json::from_str(&take_string(out)).unwrap();
        assert_eq!(response["status"], "ok");
        assert_eq!(response["result"]["version"], env!("CARGO_PKG_VERSION"));

        assert_eq!(mb_engine_register_catalog(e, c(SNAPSHOT).as_ptr()), MB_OK);

        let mut session_id = 0u64;
        assert_eq!(
            mb_engine_session_create(
                e,
                c(r#"{"database_id":42,"remark":"Metabase:: userID: 1"}"#).as_ptr(),
                &mut session_id
            ),
            MB_OK
        );
        assert!(session_id > 0);

        let mut plan: *mut c_char = ptr::null_mut();
        let sql = c(
            r#"SELECT "ID", "Total" FROM "public"."Orders" WHERE "Total" > $1 ORDER BY "ID" LIMIT 10"#,
        );
        assert_eq!(
            mb_engine_plan_and_unparse(e, session_id, sql.as_ptr(), ptr::null(), &mut plan),
            MB_OK
        );
        let plan: serde_json::Value = serde_json::from_str(&take_string(plan)).unwrap();
        assert_eq!(plan["warehouse"], "postgres");
        let warehouse_sql = plan["warehouse_sql"].as_str().unwrap();
        assert!(
            warehouse_sql.starts_with("-- Metabase:: userID: 1\nSELECT"),
            "{warehouse_sql}"
        );
        assert!(warehouse_sql.contains("$1"), "{warehouse_sql}");
        assert_eq!(plan["schema"][0]["name"], "ID");
        assert_eq!(plan["schema"][0]["type"], "Int64");

        assert_eq!(mb_engine_session_destroy(e, session_id), MB_OK);
        assert_eq!(mb_engine_shutdown(e, 1000), MB_OK);
    }
}

#[test]
fn errors_come_back_as_code_1_with_structured_json() {
    unsafe {
        let e = init("{}");
        let mut session_id = 0u64;
        assert_eq!(
            mb_engine_session_create(e, c(r#"{"database_id":99}"#).as_ptr(), &mut session_id),
            MB_ERROR
        );
        let err = last_error();
        assert_eq!(err["kind"], "catalog_missing");
        assert_eq!(err["error_type"], "bad-configuration");

        assert_eq!(mb_engine_register_catalog(e, c(SNAPSHOT).as_ptr()), MB_OK);
        assert_eq!(
            mb_engine_session_create(e, c(r#"{"database_id":42}"#).as_ptr(), &mut session_id),
            MB_OK
        );
        let mut plan: *mut c_char = ptr::null_mut();
        let sql = c(r#"SELECT arrow_cast("Total", 'Utf8') FROM "public"."Orders""#);
        assert_eq!(
            mb_engine_plan_and_unparse(e, session_id, sql.as_ptr(), ptr::null(), &mut plan),
            MB_ERROR
        );
        assert!(plan.is_null());
        let err = last_error();
        assert_eq!(err["kind"], "unsupported");
        assert_eq!(err["error_type"], "unsupported-feature");
        assert_eq!(err["construct"], "function arrow_cast");
        assert_eq!(err["warehouse"], "postgres");

        // A successful call clears the last error on this thread.
        let mut out: *mut c_char = ptr::null_mut();
        assert_eq!(
            mb_engine_call(e, c(r#"{"op":"version"}"#).as_ptr(), &mut out),
            MB_OK
        );
        take_string(out);
        assert!(mb_engine_last_error().is_null());

        assert_eq!(mb_engine_shutdown(e, 1000), MB_OK);
    }
}

#[test]
fn panics_are_contained_and_reported_as_code_2() {
    unsafe {
        let e = init(r#"{"allow_debug_ops":true}"#);
        let mut out: *mut c_char = ptr::null_mut();
        assert_eq!(
            mb_engine_call(e, c(r#"{"op":"debug_panic"}"#).as_ptr(), &mut out),
            MB_PANIC
        );
        assert!(out.is_null());
        let err = last_error();
        assert_eq!(err["kind"], "internal_panic");
        assert_eq!(err["error_type"], "qp");
        assert!(err["message"]
            .as_str()
            .unwrap()
            .contains("debug_panic requested"));

        // The engine still works afterwards.
        assert_eq!(
            mb_engine_call(e, c(r#"{"op":"version"}"#).as_ptr(), &mut out),
            MB_OK
        );
        take_string(out);
        assert_eq!(mb_engine_shutdown(e, 1000), MB_OK);
    }
}

#[test]
fn null_and_bad_inputs_are_errors_not_crashes() {
    unsafe {
        let mut handle: *mut MbEngine = ptr::null_mut();
        assert_eq!(
            mb_engine_init(c("{not json").as_ptr(), &mut handle),
            MB_ERROR
        );
        assert_eq!(last_error()["kind"], "invalid_request");

        let e = init("");
        let mut out: *mut c_char = ptr::null_mut();
        assert_eq!(mb_engine_call(e, ptr::null(), &mut out), MB_ERROR);
        assert_eq!(
            mb_engine_call(ptr::null(), c("{}").as_ptr(), &mut out),
            MB_ERROR
        );
        assert_eq!(
            mb_engine_call(e, c(r#"{"op":"nope"}"#).as_ptr(), &mut out),
            MB_ERROR
        );
        let envelope: serde_json::Value = serde_json::from_str(&take_string(out)).unwrap();
        assert_eq!(envelope["status"], "err");
        assert_eq!(mb_engine_shutdown(e, 1000), MB_OK);
        assert_eq!(mb_engine_shutdown(ptr::null_mut(), 0), MB_OK);
        mb_engine_free_string(ptr::null_mut());
    }
}

#[test]
fn execute_needs_a_session_and_stream_calls_need_a_stream() {
    unsafe {
        let e = init("{}");
        let mut stream_id = 0u64;
        assert_eq!(
            mb_engine_execute(e, 1, c("SELECT 1").as_ptr(), ptr::null(), &mut stream_id),
            MB_ERROR
        );
        assert_eq!(last_error()["kind"], "session_missing");
        assert_eq!(mb_engine_stream_cancel(e, 7), MB_ERROR);
        assert_eq!(last_error()["kind"], "invalid_request");
        let mut out: *mut c_char = ptr::null_mut();
        assert_eq!(mb_engine_stream_stats(e, 7, &mut out), MB_ERROR);
        assert!(out.is_null());
        assert_eq!(
            mb_engine_stream_close(e, 7),
            MB_OK,
            "closing an unknown stream is harmless"
        );
        assert_eq!(mb_engine_shutdown(e, 1000), MB_OK);
    }
}

/// Load the built shared library the way the JVM will, if cargo produced it.
#[test]
fn shared_library_exports_the_abi() {
    let target_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
    let name = if cfg!(target_os = "macos") {
        "libmetabase_engine.dylib"
    } else if cfg!(target_os = "windows") {
        "metabase_engine.dll"
    } else {
        "libmetabase_engine.so"
    };
    let candidates = ["debug", "release"].map(|p| target_dir.join(p).join(name));
    let Some(path) = candidates.iter().find(|p| p.exists()) else {
        eprintln!("shared library not built yet; skipping dlopen check");
        return;
    };
    unsafe {
        let lib = libloading::Library::new(path).expect("dlopen");
        let version: libloading::Symbol<unsafe extern "C" fn() -> *const c_char> = lib
            .get(b"mb_engine_version\0")
            .expect("mb_engine_version symbol");
        let v = CStr::from_ptr(version()).to_str().unwrap();
        assert_eq!(v, env!("CARGO_PKG_VERSION"));
        for symbol in [
            "mb_engine_init",
            "mb_engine_shutdown",
            "mb_engine_call",
            "mb_engine_last_error",
            "mb_engine_free_string",
            "mb_engine_register_catalog",
            "mb_engine_register_database",
            "mb_engine_invalidate_database",
            "mb_engine_session_create",
            "mb_engine_session_destroy",
            "mb_engine_plan_and_unparse",
            "mb_engine_pool_stats",
            "mb_engine_execute",
            "mb_engine_execute_raw",
            "mb_engine_stream_export",
            "mb_engine_stream_cancel",
            "mb_engine_stream_close",
            "mb_engine_stream_stats",
        ] {
            let name = CString::new(symbol).unwrap();
            let sym: Result<libloading::Symbol<*const ()>, _> = lib.get(name.as_bytes_with_nul());
            assert!(sym.is_ok(), "missing export {symbol}");
        }
    }
}
