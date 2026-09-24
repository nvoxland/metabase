//! The sqllogictest suite as a cargo test: every `.slt` file must pass on DataFusion (the oracle)
//! and on the engine planning for and executing on Postgres. Skips the Postgres half when no
//! server is reachable.

use engine_slt::{
    load_postgres, postgres_config, postgres_details, postgres_required, slt_files, DataFusionDb,
    EngineDb,
};
use sqllogictest::Runner;

#[tokio::test]
async fn every_slt_file_passes_on_datafusion_and_on_the_engine() {
    let details = postgres_details();
    let postgres = match load_postgres(&postgres_config(&details)).await {
        Ok(()) => true,
        Err(e) if postgres_required() => {
            panic!("MB_ENGINE_TESTS_REQUIRED but no Postgres reachable ({e})")
        }
        Err(e) => {
            eprintln!("skipping the engine half: no Postgres reachable ({e})");
            false
        }
    };
    let mut failures = Vec::new();
    for file in slt_files() {
        let mut oracle = Runner::new(|| async { DataFusionDb::new().await });
        if let Err(e) = oracle.run_file_async(&file).await {
            failures.push(format!("datafusion {}: {e}", file.display()));
        }
        if postgres {
            let details = details.clone();
            let mut candidate = Runner::new(move || {
                let details = details.clone();
                async move { EngineDb::connect(details).await }
            });
            if let Err(e) = candidate.run_file_async(&file).await {
                failures.push(format!("engine+postgres {}: {e}", file.display()));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
