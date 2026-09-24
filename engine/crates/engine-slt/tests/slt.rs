//! The sqllogictest suite as a cargo test: every `.slt` file must pass on DataFusion (the oracle)
//! and on the engine planning for and executing on each reachable warehouse. A warehouse that
//! isn't reachable is skipped, unless `MB_ENGINE_TESTS_REQUIRED` says otherwise.

use engine_slt::{
    load_mysql, load_postgres, mysql_details, postgres_config, postgres_details, postgres_required,
    slt_files, Candidate, DataFusionDb, EngineDb,
};
use sqllogictest::Runner;

#[tokio::test]
async fn every_slt_file_passes_on_datafusion_and_on_the_engine() {
    let mut candidates = Vec::new();
    let postgres = postgres_details();
    match load_postgres(&postgres_config(&postgres)).await {
        Ok(()) => candidates.push((Candidate::Postgres, postgres)),
        Err(e) if postgres_required() => {
            panic!("MB_ENGINE_TESTS_REQUIRED but no Postgres reachable ({e})")
        }
        Err(e) => eprintln!("skipping engine+postgres: no Postgres reachable ({e})"),
    }
    let mysql = mysql_details();
    match load_mysql(&mysql).await {
        Ok(()) => candidates.push((Candidate::MySql, mysql)),
        Err(e) if postgres_required() => {
            panic!("MB_ENGINE_TESTS_REQUIRED but no MySQL reachable ({e})")
        }
        Err(e) => eprintln!("skipping engine+mysql: no MySQL reachable ({e})"),
    }
    let mut failures = Vec::new();
    for file in slt_files() {
        let mut oracle = Runner::new(|| async { DataFusionDb::new().await });
        if let Err(e) = oracle.run_file_async(&file).await {
            failures.push(format!("datafusion {}: {e}", file.display()));
        }
        for (candidate, details) in &candidates {
            let (candidate, details) = (*candidate, details.clone());
            let mut runner = Runner::new(move || {
                let details = details.clone();
                async move { EngineDb::connect(candidate, details).await }
            });
            if let Err(e) = runner.run_file_async(&file).await {
                failures.push(format!(
                    "{} {}: {e}",
                    candidate.engine_name(),
                    file.display()
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}
