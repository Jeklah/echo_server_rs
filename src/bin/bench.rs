use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::Instant,
};

/// Benchmark settings
const SERVER_ADDR: &str = "127.0.0.1:9000";
const MESSAGE_SIZE: usize = 1024; // 1 KB
const ITERATIONS: usize = 10_000;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    println!("Connecting to echo_server_rs at {SERVER_ADDR}...");
    let mut stream = TcpStream::connect(SERVER_ADDR).await?;

    // Prepare a buffer with dummy data
    let mut buf = vec![0u8; MESSAGE_SIZE];
    for i in 0..MESSAGE_SIZE {
        buf[i] = (i % 256) as u8;
    }

    let mut echo_buf = vec![0u8; MESSAGE_SIZE];

    println!("Running benchmark with {ITERATIONS} iterations of {MESSAGE_SIZE} bytes each...");
    let start = Instant::now();

    for i in 0..ITERATIONS {
        // Stamp iteration number into first 8 bytes for traceability
        buf[0..8].copy_from_slice(&(i as u64).to_le_bytes());

        // Send the message
        stream.write_all(&buf).await?;

        // Read the echoed message
        stream.read_exact(&mut echo_buf).await?;

        // Optional sanity check
        if echo_buf != buf {
            eprintln!("Mismatch at iteration {i}");
            break;
        }
    }

    let duration = start.elapsed();
    let total_bytes = ITERATIONS * MESSAGE_SIZE * 2; // sent + received
    let throughput = (total_bytes as f64) / duration.as_secs_f64() / 1_048_576.0; // MB/s

    println!("Benchmark completed in {:.2?}", duration);
    println!("Throughput: {:.2} MB/s", throughput);

    Ok(())
}
