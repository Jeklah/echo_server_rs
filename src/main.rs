use anyhow::Result;
use bytes::BytesMut;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    net::{SocketAddr, TcpListener},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream},
};

/// Tuneables
/// The size of the buffer used for reading and writing data
const BUF_SIZE: usize = 64 * 1024; // 64 KiB
const TCP_RCVBUF: usize = 1 << 20; // 1 MiB
const TCP_SNDBUF: usize = 1 << 20; // 1 MiB
const BACKLOG: i32 = 1024; // Maximum number of pending connections

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Read address and optional SO_REUSEPORT workers from env/args if you like.
    // For simplicity, bind to 0.0.0.0:9000 here.
    let addr: SocketAddr = "0.0.0.0:9000".parse().unwrap();

    // Create and tune a socket with socket2, and then hand it to Tokio.
    let listener = create_tuned_listener(addr)?;
    let listener = TokioTcpListener::from_std(listener)?;

    println!("echo_server_rs listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        // Spawn a new task for each connection (cheap on Tokio).
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream).await {
                eprintln!("Error handling connection from {peer}: {e}");
            }
        });
    }
}

fn create_tuned_listener(addr: SocketAddr) -> Result<TcpListener> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

    // Put it in nonblocking mode so Tokio can work with it.
    socket.set_nonblocking(true)?;

    // Allow quick restarts and multi-process/multi-instance scaling.
    socket.set_reuse_address(true)?;
    #[cfg(target_os = "linux")]
    socket.set_reuse_port(true)?;

    // Buffer sizes; tune to your workload/latency goals.
    socket.set_recv_buffer_size(TCP_RCVBUF as i32)?;
    socket.set_send_buffer_size(TCP_SNDBUF as i32)?;

    // Disable Nagle on the listening socket so accepted sockets inherit it on some OSes.
    socket.set_nodelay(true)?;

    // Bind and listen on the socket.
    socket.bind(&addr.into())?;
    socket.listen(BACKLOG)?;

    Ok(socket.into())
}

async fn handle_conn(mut stream: TokioTcpStream) -> Result<()> {
    // Per-connection TCP_NODELAY reduces small-packet latency.
    stream.set_nodelay(true)?;

    // Optional: quick timeouts to fail fast in benchmarks with dead peers.
    stream.set_linger(Some(Duration::from_millis(0)))?;

    // Reusable buffer to avoid per-iteration allocations.
    // BytesMut lets us read directly into uninitialized capacity.
    let mut buf = BytesMut::with_capacity(BUF_SIZE);

    loop {
        // Ensure capacity, then read into it.
        buf.reserve(BUF_SIZE);
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            // EOF, peer closed connection
            return Ok(());
        }

        // Echo back exactly what we received in this read.
        // We write only the last 'n' bytes appended.
        let start = buf.len() - n;
        stream.write_all(&buf[start..]).await?;

        // If you want ultra-low latency for tiny bursts, you can flush;
        // Usually write_all is enough (Tokio flushes when needed).
        // stream.flush().await?;

        // Compact buffer if it has grown too large to keep memory in check
        if buf.capacity() > 2 * BUF_SIZE && buf.len() == 0 {
            buf = BytesMut::with_capacity(BUF_SIZE);
        }

        // For a pure echo, the buffer content is only needed for the write above.
        // Drop consumed bytes by truncating back (no need to keep them).
        buf.truncate(start);
    }
}
