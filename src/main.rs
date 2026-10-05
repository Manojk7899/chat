use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc, Mutex};
use std::sync::Arc;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

const DEFAULT_ROOM: &str = "general";
const HISTORY_LIMIT: usize = 20;      // how many past messages a room remembers
const RATE_LIMIT_COUNT: usize = 5;    // max messages
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(5); // per this many seconds

type UserMap = Arc<Mutex<HashMap<String, mpsc::Sender<String>>>>;
type RoomMap = Arc<Mutex<HashMap<String, Vec<String>>>>;       // room -> usernames in it
type HistoryMap = Arc<Mutex<HashMap<String, Vec<String>>>>;    // room -> recent messages

#[tokio::main]
async fn main() {
    let users: UserMap = Arc::new(Mutex::new(HashMap::new()));
    let rooms: RoomMap = Arc::new(Mutex::new(HashMap::new()));
    let history: HistoryMap = Arc::new(Mutex::new(HashMap::new()));

    let listener = TcpListener::bind("127.0.0.1:8082").await.unwrap();
    let (tx, _rx) = broadcast::channel(100);

    loop {
        let (mut socket, addr) = listener.accept().await.unwrap();

        socket.write_all(b"Enter a username: ").await.unwrap();
        let mut buf = [0; 1024];
        let n = socket.read(&mut buf).await.unwrap();
        let username = String::from_utf8_lossy(&buf[0..n]).trim().to_string();

        let tx = tx.clone();
        let users = users.clone();
        let rooms = rooms.clone();
        let history = history.clone();

        println!("Accepted connection from {:?} as {}", addr, username);

        tokio::spawn(async move {
            handle_connection(socket, username, tx, users, rooms, history).await;
        });
    }
}

// Adds a message to a room's history, trimming to the last HISTORY_LIMIT entries.
async fn push_history(history: &HistoryMap, room: &str, msg: String) {
    let mut map = history.lock().await;
    let entry = map.entry(room.to_string()).or_insert_with(Vec::new);
    entry.push(msg);
    if entry.len() > HISTORY_LIMIT {
        entry.remove(0); // drop the oldest message
    }
}

// Adds username to a room's member list.
async fn join_room(rooms: &RoomMap, room: &str, username: &str) {
    let mut map = rooms.lock().await;
    map.entry(room.to_string()).or_insert_with(Vec::new).push(username.to_string());
}

// Removes username from a room's member list.
async fn leave_room(rooms: &RoomMap, room: &str, username: &str) {
    let mut map = rooms.lock().await;
    if let Some(members) = map.get_mut(room) {
        members.retain(|u| u != username);
    }
}

async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    username: String,
    tx: broadcast::Sender<String>,
    users: UserMap,
    rooms: RoomMap,
    history: HistoryMap,
) {
    let mut rx = tx.subscribe();
    let mut buf = [0; 1024];
    let mut current_room = DEFAULT_ROOM.to_string();

    // rate limiting: remember timestamps of this client's recent messages
    let mut recent_sends: VecDeque<Instant> = VecDeque::new();

    let (personal_tx, mut personal_rx) = mpsc::channel::<String>(32);
    users.lock().await.insert(username.clone(), personal_tx);

    // join the default room on connect
    join_room(&rooms, &current_room, &username).await;

    // send this room's recent history to the NEW client only (not broadcast)
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

                if received == "quit" {
                    cleanup(&users, &rooms, &history, &tx, &username, &current_room).await;
                    return;
                }

                // --- /list : show who's in the current room ---
                if received == "/list" {
                    let map = rooms.lock().await;
                    let names = map.get(&current_room).cloned().unwrap_or_default();
                    let listing = format!("Users in {}: {}", current_room, names.join(", "));
                    socket.write_all(listing.as_bytes()).await.unwrap();
                    socket.write_all(b"\n").await.unwrap();
                    continue;
                }

                // --- /join roomname : switch rooms ---
                if let Some(new_room) = received.strip_prefix("/join ") {
                    let new_room = new_room.trim().to_string();
                    if new_room == current_room {
                        socket.write_all(b"You're already in that room\n").await.unwrap();
                        continue;
                    }

                    // leave old room
                    leave_room(&rooms, &current_room, &username).await;
                    let leave_msg = format!("{}::{} has left {}", current_room, username, current_room);
                    let _ = tx.send(leave_msg.clone());
                    push_history(&history, &current_room, leave_msg).await;

                    // join new room
                    current_room = new_room.clone();
                    join_room(&rooms, &current_room, &username).await;

                    // send new room's history to this client only
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
                    continue;
                }

                // --- /msg username text : private message ---
                if let Some(rest) = received.strip_prefix("/msg ") {
                    if let Some((target, body)) = rest.split_once(' ') {
                        let map = users.lock().await;
                        if let Some(target_tx) = map.get(target) {
                            let _ = target_tx.send(format!("[private] {}: {}", username, body)).await;
                        } else {
                            socket.write_all(b"User not found\n").await.unwrap();
                        }
                    }
                    continue;
                }

                // --- Rate limiting check (only applies to normal chat messages) ---
                let now = Instant::now();
                recent_sends.retain(|&t| now.duration_since(t) < RATE_LIMIT_WINDOW);
                if recent_sends.len() >= RATE_LIMIT_COUNT {
                    socket.write_all(b"You're sending messages too fast, slow down!\n").await.unwrap();
                    continue;
                }
                recent_sends.push_back(now);

                // --- Normal message: broadcast to current room only ---
                let msg = format!("{}::{}: {}", current_room, username, received);
                let _ = tx.send(msg.clone());
                push_history(&history, &current_room, msg).await;
            }

            result = rx.recv() => {
                let msg = result.unwrap();
                // messages are tagged "room::content" — only show ones for OUR current room
                if let Some((room, content)) = msg.split_once("::") {
                    if room == current_room {
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

// Shared cleanup logic for both disconnect paths (quit, and abrupt n==0 disconnect)
async fn cleanup(
    users: &UserMap,
    rooms: &RoomMap,
    history: &HistoryMap,
    tx: &broadcast::Sender<String>,
    username: &str,
    current_room: &str,
) {
    users.lock().await.remove(username);
    leave_room(rooms, current_room, username).await;
    let leave_msg = format!("{}::{} has left the chat!", current_room, username);
    let _ = tx.send(leave_msg.clone());
    push_history(history, current_room, leave_msg).await;
}