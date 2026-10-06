mod db;

use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc, Mutex};
use std::sync::Arc;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use futures_util::{StreamExt, SinkExt};
use rusqlite::Connection;

const DEFAULT_ROOM: &str = "general";
const HISTORY_LIMIT: usize = 20;
const RATE_LIMIT_COUNT: usize = 5;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(5);

type UserMap = Arc<Mutex<HashMap<String, mpsc::Sender<String>>>>;
type RoomMap = Arc<Mutex<HashMap<String, Vec<String>>>>;
type HistoryMap = Arc<Mutex<HashMap<String, Vec<String>>>>;
type Db = Arc<Mutex<Connection>>;

#[tokio::main]
async fn main() {
    let users: UserMap = Arc::new(Mutex::new(HashMap::new()));
    let rooms: RoomMap = Arc::new(Mutex::new(HashMap::new()));
    let history: HistoryMap = Arc::new(Mutex::new(HashMap::new()));
    let database: Db = Arc::new(Mutex::new(db::init_db()));
    let (tx, _rx) = broadcast::channel(100);

    let tcp_listener = TcpListener::bind("127.0.0.1:8082").await.unwrap();
    println!("TCP server listening on 127.0.0.1:8082");

    let ws_listener = TcpListener::bind("127.0.0.1:8083").await.unwrap();
    println!("WebSocket server listening on 127.0.0.1:8083");

    let (tcp_users, tcp_rooms, tcp_history, tcp_tx, tcp_db) =
        (users.clone(), rooms.clone(), history.clone(), tx.clone(), database.clone());
    let tcp_task = tokio::spawn(async move {
        loop {
            let (mut socket, addr) = tcp_listener.accept().await.unwrap();
            let (tx, users, rooms, history, database) =
                (tcp_tx.clone(), tcp_users.clone(), tcp_rooms.clone(), tcp_history.clone(), tcp_db.clone());

            tokio::spawn(async move {
                let username = match authenticate_tcp(&mut socket, &database).await {
                    Some(u) => u,
                    None => return,
                };
                println!("Authenticated TCP connection from {:?} as {}", addr, username);
                handle_tcp_connection(socket, username, tx, users, rooms, history).await;
            });
        }
    });

    let (ws_users, ws_rooms, ws_history, ws_tx, ws_db) =
        (users.clone(), rooms.clone(), history.clone(), tx.clone(), database.clone());
    let ws_task = tokio::spawn(async move {
        loop {
            let (stream, addr) = ws_listener.accept().await.unwrap();
            let (tx, users, rooms, history, database) =
                (ws_tx.clone(), ws_users.clone(), ws_rooms.clone(), ws_history.clone(), ws_db.clone());

            tokio::spawn(async move {
                let mut ws_stream = match accept_async(stream).await {
                    Ok(ws) => ws,
                    Err(e) => { println!("WS handshake failed for {:?}: {:?}", addr, e); return; }
                };

                let username = match authenticate_ws(&mut ws_stream, &database).await {
                    Some(u) => u,
                    None => return,
                };
                println!("Authenticated WS connection from {:?} as {}", addr, username);
                handle_ws_connection(ws_stream, username, tx, users, rooms, history).await;
            });
        }
    });

    tokio::select! {
        _ = tcp_task => {},
        _ = ws_task => {},
        _ = tokio::signal::ctrl_c() => {
            println!("\nCtrl+C received, shutting down...");
            let _ = tx.send("SYSTEM::Server is shutting down. Goodbye!".to_string());
            tokio::time::sleep(Duration::from_millis(300)).await;
            std::process::exit(0);
        }
    }
}

// ---------- Authentication ----------

async fn authenticate_tcp(socket: &mut tokio::net::TcpStream, database: &Db) -> Option<String> {
    let mut buf = [0; 1024];

    socket.write_all(b"Type 'login' or 'register': ").await.ok()?;
    let n = socket.read(&mut buf).await.ok()?;
    let choice = String::from_utf8_lossy(&buf[0..n]).trim().to_string();

    socket.write_all(b"Username: ").await.ok()?;
    let n = socket.read(&mut buf).await.ok()?;
    let username = String::from_utf8_lossy(&buf[0..n]).trim().to_string();

    socket.write_all(b"Password: ").await.ok()?;
    let n = socket.read(&mut buf).await.ok()?;
    let password = String::from_utf8_lossy(&buf[0..n]).trim().to_string();

    let conn = database.lock().await;
    if choice == "register" {
        if db::register_user(&conn, &username, &password) {
            socket.write_all(b"Registered! You're in.\n").await.ok()?;
            Some(username)
        } else {
            socket.write_all(b"Username already taken.\n").await.ok()?;
            None
        }
    } else {
        if db::verify_login(&conn, &username, &password) {
            socket.write_all(b"Login successful!\n").await.ok()?;
            Some(username)
        } else {
            socket.write_all(b"Wrong username or password.\n").await.ok()?;
            None
        }
    }
}

async fn authenticate_ws(
    ws_stream: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    database: &Db,
) -> Option<String> {
    async fn ask(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        prompt: &str,
    ) -> Option<String> {
        ws.send(Message::Text(prompt.to_string())).await.ok()?;
        match ws.next().await {
            Some(Ok(Message::Text(t))) => Some(t.trim().to_string()),
            _ => None,
        }
    }

    let choice = ask(ws_stream, "Type 'login' or 'register':").await?;
    let username = ask(ws_stream, "Username:").await?;
    let password = ask(ws_stream, "Password:").await?;

    let conn = database.lock().await;
    if choice == "register" {
        if db::register_user(&conn, &username, &password) {
            ws_stream.send(Message::Text("Registered! You're in.".to_string())).await.ok()?;
            Some(username)
        } else {
            ws_stream.send(Message::Text("Username already taken.".to_string())).await.ok()?;
            None
        }
    } else {
        if db::verify_login(&conn, &username, &password) {
            ws_stream.send(Message::Text("Login successful!".to_string())).await.ok()?;
            Some(username)
        } else {
            ws_stream.send(Message::Text("Wrong username or password.".to_string())).await.ok()?;
            None
        }
    }
}

// ---------- Shared room/history helpers ----------

async fn push_history(history: &HistoryMap, room: &str, msg: String) {
    let mut map = history.lock().await;
    let entry = map.entry(room.to_string()).or_insert_with(Vec::new);
    entry.push(msg);
    if entry.len() > HISTORY_LIMIT { entry.remove(0); }
}

async fn join_room(rooms: &RoomMap, room: &str, username: &str) {
    rooms.lock().await.entry(room.to_string()).or_insert_with(Vec::new).push(username.to_string());
}

async fn leave_room(rooms: &RoomMap, room: &str, username: &str) {
    if let Some(members) = rooms.lock().await.get_mut(room) {
        members.retain(|u| u != username);
    }
}

async fn cleanup(
    users: &UserMap, rooms: &RoomMap, history: &HistoryMap,
    tx: &broadcast::Sender<String>, username: &str, current_room: &str,
) {
    users.lock().await.remove(username);
    leave_room(rooms, current_room, username).await;
    let leave_msg = format!("{}::{} has left the chat!", current_room, username);
    let _ = tx.send(leave_msg.clone());
    push_history(history, current_room, leave_msg).await;
}

// ---------- Shared command logic (used by both TCP and WS) ----------

async fn process_line(
    received: &str, username: &str, current_room: &mut String,
    users: &UserMap, rooms: &RoomMap, history: &HistoryMap,
    tx: &broadcast::Sender<String>, recent_sends: &mut VecDeque<Instant>,
) -> Result<Option<String>, ()> {
    if received == "quit" { return Err(()); }

    if received == "/list" {
        let names = rooms.lock().await.get(current_room).cloned().unwrap_or_default();
        return Ok(Some(format!("Users in {}: {}", current_room, names.join(", "))));
    }

    if let Some(new_room) = received.strip_prefix("/join ") {
        let new_room = new_room.trim().to_string();
        if new_room == *current_room {
            return Ok(Some("You're already in that room".to_string()));
        }
        leave_room(rooms, current_room, username).await;
        let leave_msg = format!("{}::{} has left {}", current_room, username, current_room);
        let _ = tx.send(leave_msg.clone());
        push_history(history, current_room, leave_msg).await;

        *current_room = new_room;
        join_room(rooms, current_room, username).await;

        let join_msg = format!("{}::{} has joined {}!", current_room, username, current_room);
        let _ = tx.send(join_msg.clone());
        push_history(history, current_room, join_msg).await;

        let hist = history.lock().await;
        let mut reply = String::new();
        if let Some(past) = hist.get(current_room) {
            for line in past { reply.push_str(line); reply.push('\n'); }
        }
        return Ok(Some(reply));
    }

    if let Some(rest) = received.strip_prefix("/msg ") {
        if let Some((target, body)) = rest.split_once(' ') {
            let map = users.lock().await;
            if let Some(target_tx) = map.get(target) {
                let _ = target_tx.send(format!("[private] {}: {}", username, body)).await;
            } else {
                return Ok(Some("User not found".to_string()));
            }
        }
        return Ok(None);
    }

    let now = Instant::now();
    recent_sends.retain(|&t| now.duration_since(t) < RATE_LIMIT_WINDOW);
    if recent_sends.len() >= RATE_LIMIT_COUNT {
        return Ok(Some("You're sending messages too fast, slow down!".to_string()));
    }
    recent_sends.push_back(now);

    let msg = format!("{}::{}: {}", current_room, username, received);
    let _ = tx.send(msg.clone());
    push_history(history, current_room, msg).await;
    Ok(None)
}

// ---------- TCP connection handler ----------

async fn handle_tcp_connection(
    mut socket: tokio::net::TcpStream, username: String, tx: broadcast::Sender<String>,
    users: UserMap, rooms: RoomMap, history: HistoryMap,
) {
    let mut rx = tx.subscribe();
    let mut buf = [0; 1024];
    let mut current_room = DEFAULT_ROOM.to_string();
    let mut recent_sends: VecDeque<Instant> = VecDeque::new();

    let (personal_tx, mut personal_rx) = mpsc::channel::<String>(32);
    users.lock().await.insert(username.clone(), personal_tx);
    join_room(&rooms, &current_room, &username).await;

    {
        let hist = history.lock().await;
        if let Some(past) = hist.get(&current_room) {
            for line in past {
                socket.write_all(line.as_bytes()).await.unwrap();
                socket.write_all(b"\n").await.unwrap();
            }
        }
    }

    let join_msg = format!("{}::{} has joined {}!", current_room, username, current_room);
    let _ = tx.send(join_msg.clone());
    push_history(&history, &current_room, join_msg).await;

    loop {
        tokio::select! {
            result = socket.read(&mut buf) => {
                let n = result.unwrap();
                if n == 0 {
                    cleanup(&users, &rooms, &history, &tx, &username, &current_room).await;
                    return;
                }
                let received = String::from_utf8_lossy(&buf[0..n]).trim().to_string();

                match process_line(&received, &username, &mut current_room, &users, &rooms, &history, &tx, &mut recent_sends).await {
                    Err(()) => {
                        cleanup(&users, &rooms, &history, &tx, &username, &current_room).await;
                        return;
                    }
                    Ok(Some(reply)) => {
                        socket.write_all(reply.as_bytes()).await.unwrap();
                        socket.write_all(b"\n").await.unwrap();
                    }
                    Ok(None) => {}
                }
            }
            result = rx.recv() => {
                let msg = result.unwrap();
                if let Some((room, content)) = msg.split_once("::") {
                    if room == "SYSTEM" || room == current_room {
                        socket.write_all(content.as_bytes()).await.unwrap();
                        socket.write_all(b"\n").await.unwrap();
                    }
                }
            }
            Some(private_msg) = personal_rx.recv() => {
                socket.write_all(private_msg.as_bytes()).await.unwrap();
                socket.write_all(b"\n").await.unwrap();
            }
        }
    }
}

// ---------- WebSocket connection handler ----------

async fn handle_ws_connection(
    ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    username: String,
    tx: broadcast::Sender<String>, users: UserMap, rooms: RoomMap, history: HistoryMap,
) {
    let (mut write, mut read) = ws_stream.split();

    let mut rx = tx.subscribe();
    let mut current_room = DEFAULT_ROOM.to_string();
    let mut recent_sends: VecDeque<Instant> = VecDeque::new();

    let (personal_tx, mut personal_rx) = mpsc::channel::<String>(32);
    users.lock().await.insert(username.clone(), personal_tx);
    join_room(&rooms, &current_room, &username).await;

    {
        let hist = history.lock().await;
        if let Some(past) = hist.get(&current_room) {
            for line in past { let _ = write.send(Message::Text(line.clone())).await; }
        }
    }

    let join_msg = format!("{}::{} has joined {}!", current_room, username, current_room);
    let _ = tx.send(join_msg.clone());
    push_history(&history, &current_room, join_msg).await;

    loop {
        tokio::select! {
            incoming = read.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let received = text.trim().to_string();
                        match process_line(&received, &username, &mut current_room, &users, &rooms, &history, &tx, &mut recent_sends).await {
                            Err(()) => {
                                cleanup(&users, &rooms, &history, &tx, &username, &current_room).await;
                                return;
                            }
                            Ok(Some(reply)) => { let _ = write.send(Message::Text(reply)).await; }
                            Ok(None) => {}
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        cleanup(&users, &rooms, &history, &tx, &username, &current_room).await;
                        return;
                    }
                    _ => {}
                }
            }
            result = rx.recv() => {
                let msg = result.unwrap();
                if let Some((room, content)) = msg.split_once("::") {
                    if room == "SYSTEM" || room == current_room {
                        let _ = write.send(Message::Text(content.to_string())).await;
                    }
                }
            }
            Some(private_msg) = personal_rx.recv() => {
                let _ = write.send(Message::Text(private_msg)).await;
            }
        }
    }
}