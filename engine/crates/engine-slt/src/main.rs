//! `engine-slt`: run the sqllogictest suite.
//!
//! ```text
//! cargo run -p engine-slt -- check            # DataFusion, then the engine on Postgres
//! cargo run -p engine-slt -- update           # rewrite expected results from DataFusion (the oracle)
//! cargo run -p engine-slt -- unparse "SELECT ..."   # the Postgres SQL the engine would send
//! cargo run -p engine-slt -- unparse mysql "SELECT ..."   # the MySQL SQL
//! ```
//! Postgres comes from `ENGINE_TEST_POSTGRES_DETAILS` and MySQL from `ENGINE_TEST_MYSQL_DETAILS`
//! (both default to the local development servers). A warehouse that isn't reachable is skipped.

use engine_slt::{
    load_mysql, load_postgres, mysql_details, postgres_config, postgres_details, slt_files,
    Candidate, DataFusionDb, EngineDb,
};
use sqllogictest::Runner;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("update") => update().await,
        Some("check") | None => check().await,
        Some("unparse") if args.get(1).map(String::as_str) == Some("mysql") => {
            unparse(Candidate::MySql, args[2..].join(" ")).await
        }
        Some("unparse") => unparse(Candidate::Postgres, args[1..].join(" ")).await,
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
    let mut candidates = Vec::new();
    let postgres = postgres_details();
    match load_postgres(&postgres_config(&postgres)).await {
        Ok(()) => candidates.push((Candidate::Postgres, postgres)),
        Err(e) => println!("skipping engine+postgres: {e}"),
    }
    let mysql = mysql_details();
    match load_mysql(&mysql).await {
        Ok(()) => candidates.push((Candidate::MySql, mysql)),
        Err(e) => println!("skipping engine+mysql: {e}"),
    }
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
        for (candidate, details) in &candidates {
            let (candidate, details) = (*candidate, details.clone());
            let mut runner = Runner::new(move || {
                let details = details.clone();
                async move { EngineDb::connect(candidate, details).await }
            });
            match runner.run_file_async(&file).await {
                Ok(()) => println!("{:<16} ok   {}", candidate.engine_name(), file.display()),
                Err(e) => {
                    failures += 1;
                    println!(
                        "{:<16} FAIL {}\n{e}",
                        candidate.engine_name(),
                        file.display()
                    );
                }
            }
        }
    }
    if failures > 0 {
        return Err(format!("{failures} file(s) failed").into());
    }
    Ok(())
}

async fn unparse(candidate: Candidate, sql: String) -> Result<(), Box<dyn std::error::Error>> {
    let details = match candidate {
        Candidate::Postgres => postgres_details(),
        Candidate::MySql => mysql_details(),
    };
    let db = EngineDb::connect(candidate, details).await?;
    let sql = tokio::task::spawn_blocking(move || db.unparse(&sql)).await??;
    println!("{sql}");
    Ok(())
}
