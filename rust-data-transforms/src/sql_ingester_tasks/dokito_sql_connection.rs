use std::{
    env,
    sync::{LazyLock, OnceLock},
    time::Duration,
};

use sqlx::{PgPool, postgres::PgPoolOptions};
use thiserror::Error;

pub static DEFAULT_POSTGRES_CONNECTION_URL: LazyLock<String> = LazyLock::new(|| {
    env::var("POSTGRES_CONNECTION")
        .or(env::var("DATABASE_URL"))
        .expect("POSTGRES_CONNECTION or DATABASE_URL should be set.")
});

#[derive(Error, Debug)]
#[error("Could not initialize postgres pool")]
pub struct InitializePostgresError {}

static DOKITO_POOL_CELL: OnceLock<PgPool> = OnceLock::new();
pub async fn get_dokito_pool() -> Result<&'static PgPool, InitializePostgresError> {
    if let Some(inital_pool) = DOKITO_POOL_CELL.get() {
        return Ok(inital_pool);
    }
    let db_url = &**DEFAULT_POSTGRES_CONNECTION_URL;
    let pool = PgPoolOptions::new()
        .max_connections(40)
        .acquire_timeout(Duration::from_secs(600))
        .connect(db_url)
        .await;
    match pool {
        Ok(pool_value) => {
            let pool_ref = DOKITO_POOL_CELL.get_or_init(|| pool_value);
            Ok(pool_ref)
        }
        Err(err) => {
            eprintln!("Failed to connect to database:");
            eprintln!("Error: {}", err);
            eprintln!("Debug: {:?}", err);
            eprintln!("Connection URL pattern: {}",
                if db_url.contains("@") {
                    let parts: Vec<&str> = db_url.splitn(2, '@').collect();
                    format!("{}@<REDACTED>", parts[0].chars().take(10).collect::<String>())
                } else {
                    "<NO_AUTH_INFO>".to_string()
                });
            eprintln!("Expected environment variables: POSTGRES_CONNECTION or DATABASE_URL");
            eprintln!("");
            eprintln!("IMPORTANT: If you see 'MAC tag mismatch' error above, this is misleading!");
            eprintln!("This version of sqlx reports 'MAC tag mismatch' when the actual issue");
            eprintln!("is typically an incorrect password or authentication failure.");
            eprintln!("Check your database credentials and connection string.");

            Err(InitializePostgresError {})
        }
    }
}
