use sqlx::{MySql, Pool};

pub async fn get_pool() -> Pool<MySql> {
    let host = std::env::var("ISUCON_DB_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("ISUCON_DB_PORT")
        .map(|port_str| {
            port_str.parse().expect(
                "failed to convert DB port number from ISUCON_DB_PORT environment variable into u16",
            )
        })
        .unwrap_or(3306);
    let user = std::env::var("ISUCON_DB_USER").unwrap_or_else(|_| "isucon".to_owned());
    let password = std::env::var("ISUCON_DB_PASSWORD").unwrap_or_else(|_| "isucon".to_owned());
    let db_name = std::env::var("ISUCON_DB_NAME").unwrap_or_else(|_| "isuride".to_owned());

    tracing::info!("connecting to mysql://{user}:{password}@{host}:{port}/{db_name}");

    sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(128)
        .connect_with(
            sqlx::mysql::MySqlConnectOptions::default()
                .host(&host)
                .port(port)
                .username(&user)
                .password(&password)
                .database(&db_name),
        )
        .await
        .unwrap()
}
