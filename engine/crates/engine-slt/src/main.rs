//! `engine-slt`: run the sqllogictest suite.
//!
//! ```text
//! cargo run -p engine-slt -- check            # DataFusion, then the engine on Postgres
//! cargo run -p engine-slt -- update           # rewrite expected results from DataFusion (the oracle)
//! cargo run -p engine-slt -- unparse "SELECT ..."   # the Postgres SQL the engine would send
//! ```
//! Postgres comes from `MB_ENGINE_TEST_PG_*` (defaults to the local development server).

use engine_slt::{
    load_postgres, postgres_config, postgres_details, slt_files, DataFusionDb, EngineDb,
};
use sqllogictest::Runner;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("update") => update().await,
        Some("check") | None => check().await,
        Some("unparse") => unparse(args[1..].join(" ")).await,
        Some(other) => Err(format!("unknown command {other:?}").into()),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn update() -> Result<(), Box<dyn std::error::Error>> {
    for file in slt_files() {
        let mut runner = Runner::new(|| async { DataFusionDb::new().await });
        runner
            .update_test_file(
                &file,
                " ",
                sqllogictest::default_validator,
                sqllogictest::default_normalizer,
                sqllogictest::default_column_validator,
            )
            .await?;
        println!("updated {}", file.display());
    }
    Ok(())
}

async fn check() -> Result<(), Box<dyn std::error::Error>> {
    let details = postgres_details();
    load_postgres(&postgres_config(&details)).await?;
    let mut failures = 0;
    for file in slt_files() {
        let mut oracle = Runner::new(|| async { DataFusionDb::new().await });
        match oracle.run_file_async(&file).await {
            Ok(()) => println!("datafusion       ok   {}", file.display()),
            Err(e) => {
                failures += 1;
                println!("datafusion       FAIL {}\n{e}", file.display());
            }
        }
        let details = details.clone();
        let mut candidate = Runner::new(move || {
            let details = details.clone();
            async move { EngineDb::connect(details).await }
        });
        match candidate.run_file_async(&file).await {
            Ok(()) => println!("engine+postgres  ok   {}", file.display()),
            Err(e) => {
                failures += 1;
                println!("engine+postgres  FAIL {}\n{e}", file.display());
            }
        }
    }
    if failures > 0 {
        return Err(format!("{failures} file(s) failed").into());
    }
    Ok(())
}

async fn unparse(sql: String) -> Result<(), Box<dyn std::error::Error>> {
    let db = EngineDb::connect(postgres_details()).await?;
    let sql = tokio::task::spawn_blocking(move || db.unparse(&sql)).await??;
    println!("{sql}");
    Ok(())
}
