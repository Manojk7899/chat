
    use tokio::net::{
        TcpListener, 
        };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::broadcast;

    #[tokio::main]
    async fn main() {
        let listener = TcpListener::bind("127.0.0.1:8082").await.unwrap();
        let (tx, _rx) = broadcast::channel(32);
        loop{ 
            let ( socket,addr)=listener.accept().await.unwrap();
            let tx=tx.clone();
            println!("Accepted the connection from {:?}",addr );
            tokio::spawn(async move {
                handle_connection(socket,addr,tx).await;
            });
            async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    tx: broadcast::Sender<String>,
) {
    let mut rx = tx.subscribe(); // this client's own "ear" on the shared intercom
    let mut buf = [0; 1024];

    loop {
        tokio::select! {
            // Job 1: something arrived from THIS client's keyboard
            result = socket.read(&mut buf) => {
                let n = result.unwrap();
                if n == 0 {
                    return; // client disconnected
                }

                let received = String::from_utf8_lossy(&buf[0..n]).trim().to_string();

                if received == "quit" {
                    println!("Client {} requested to quit", addr);
                    return;
                }

                let msg = format!("{}: {}", addr, received);
                let _ = tx.send(msg); // push it onto the shared intercom
            }

            // Job 2: something arrived on the shared intercom (from ANY client)
            result = rx.recv() => {
                let msg = result.unwrap();
                socket.write_all(msg.as_bytes()).await.unwrap();
                socket.write_all(b"\n").await.unwrap();
            }
        }
    }
}
        }
    }
    