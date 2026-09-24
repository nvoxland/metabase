fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let out = std::path::Path::new(&crate_dir).join("../../include/metabase_engine.h");
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    match cbindgen::generate(&crate_dir) {
        Ok(bindings) => {
            bindings.write_to_file(out);
        }
        Err(e) => println!("cargo:warning=cbindgen failed: {e}"),
    }
    println!("cargo:rerun-if-changed=src/lib.rs");
}
