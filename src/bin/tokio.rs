use std::env;
use std::time;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt, AsyncSeekExt};
use tokio::task::JoinSet;

const FILE_SIZE: u64 = 10 * 1024 * 1024 * 1024;
const BLOCK_SIZE: u64 = 16 * 4096;
const PAGE_SIZE: u64 = 4096;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let shards = args[1].parse::<u64>().expect("Need one number argument");
    let mut join_set = JoinSet::new();
    let los = time::SystemTime::now();
    for s in 0..shards {
        let shard_size = FILE_SIZE / BLOCK_SIZE / shards;
        let start = shard_size * s;
        join_set.spawn(async move {
            let mut f = File::open("datei").await.expect("Cannot open");
            let mut buf = vec![0 as u8; PAGE_SIZE as usize];
            let mut sum = 0 as u64;
            for i in 0..shard_size {
                f.seek(std::io::SeekFrom::Start((start + i) * BLOCK_SIZE))
                    .await
                    .expect("Cannot seek");
                let n = f.read(&mut buf[..]).await.expect("Cannot read");
                if n != PAGE_SIZE as usize {
                    panic!("Could not read!");
                }
                sum += buf[0] as u64;
            }
            sum
        });
    }
    let mut sum: u64 = 0;
    while let Some(res) = join_set.join_next().await {
        let r = res.unwrap();
        sum += r;
    }

    println!("Done, elapsed: {:?}, sum: {}", los.elapsed(), sum);
    Ok(())
}
