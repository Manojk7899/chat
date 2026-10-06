use rusqlite::{Connection, params};
use sha2::{Sha256, Digest};

pub fn init_db() -> Connection {
    let conn = Connection::open("chat_users.db").unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL
        )",
        [],
    ).unwrap();
    conn
}

pub fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    format!("{:x}", hasher.finalize())
}

// Returns true if registration succeeded (username was free)
pub fn register_user(conn: &Connection, username: &str, password: &str) -> bool {
    let hash = hash_password(password);
    conn.execute(
        "INSERT INTO users (username, password_hash) VALUES (?1, ?2)",
        params![username, hash],
    ).is_ok() // fails if username already exists (PRIMARY KEY constraint)
}

// Returns true if username exists and password hash matches
pub fn verify_login(conn: &Connection, username: &str, password: &str) -> bool {
    let hash = hash_password(password);
    let result: Result<String, _> = conn.query_row(
        "SELECT password_hash FROM users WHERE username = ?1",
        params![username],
        |row| row.get(0),
    );
    match result {
        Ok(stored_hash) => stored_hash == hash,
        Err(_) => false, // username not found
    }
}