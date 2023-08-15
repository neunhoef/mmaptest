use io_uring::{opcode, types, IoUring};
use madvise::madvise;
use memmap::MmapOptions;
use std::fs::File;
use std::io::prelude::*;
use std::io::stdin;
use std::io::SeekFrom;
use std::os::unix::io::AsRawFd;
use std::time;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let nr_threads = if args.len() >= 2 {
        args[1].parse::<u64>().expect("Cannot parse number")
    } else {
        8 as u64
    };
    println!("This is mmaptest.");

    {
        let testfile = File::open("datei");
        if let Err(_) = testfile {
            let los = time::SystemTime::now();
            println!("Creating a 10 GB large file...");
            let mut file = File::create("datei").expect("Cannot create file");
            let mut data: Vec<u8> = Vec::with_capacity(1024 * 1024);
            for i in 0..1024 * 1024 {
                data.push((i % 47) as u8);
            }
            for _i in 0..1024 * 10 {
                file.write_all(&data[..]).expect("Could not write block");
            }
            file.sync_all().expect("Could not sync");
            println!("Done writing after {:?}.", los.elapsed());
        }
    }

    let los = time::SystemTime::now();
    println!("Mapping file...");
    let file = File::open("datei").expect("Cannot open file");
    let mmap = unsafe { MmapOptions::new().map(&file).expect("Cannot map file") };
    println!("Done mmapping after {:?}.", los.elapsed());
    assert_eq!(b"\x00\x01\x02\x03\x04\x05\x06\x07", &mmap[0..8]);

    let los = time::SystemTime::now();
    println!("Advising madvise...");
    unsafe {
        madvise(&mmap[0], mmap.len(), madvise::AccessPattern::Random).expect("Cannot madvise");
    }
    println!("Done madvise after {:?}.", los.elapsed());

    println!("Trying mmap accesses...");

    loop {
        let mut buffer = String::with_capacity(100);
        stdin().read_line(&mut buffer).expect("Cannot read line");
        println!("Got: {}", buffer);
        if buffer == "q\n" {
            break;
        }
        let los = time::SystemTime::now();
        println!("Racing to access some data...");
        let mut sum: u64 = 0;
        for i in 0..1024 * 1024 * 1024 * 10 / (16 * 4096) {
            let j = (i * 12373) % (1024 * 1024 * 1024 * 10 / (16 * 4096));
            sum += mmap[j * 16 * 4096] as u64;
        }
        println!("Done summation {} after {:?}.", sum, los.elapsed());
    }

    drop(mmap);
    drop(file);

    println!("Map dropped, trying conventional I/O");
    loop {
        let mut buffer = String::with_capacity(100);
        stdin().read_line(&mut buffer).expect("Cannot read line");
        println!("Got: {}", buffer);
        if buffer == "q\n" {
            break;
        }
        let los = time::SystemTime::now();
        println!("Racing to access some data with seek...");
        let mut file = File::open("datei").expect("Cannot open file");
        let mut sum: u64 = 0;
        let mut buf: Vec<u8> = Vec::with_capacity(4096);
        buf.resize(4096, 0);
        for i in 0..1024 * 1024 * 1024 * 10 / (16 * 4096) {
            //let j = (i * 12373) % (1024 * 1024 * 1024 * 10 / (16 * 4096));
            file.seek(SeekFrom::Start(i * 16 * 4096))
                .expect("Could not seek");
            file.read(&mut buf).expect("Could not read page");
            sum += buf[0] as u64;
        }
        println!("Done summation {} via I/O after {:?}.", sum, los.elapsed());
    }

    println!("Trying to read all");
    loop {
        let mut buffer = String::with_capacity(100);
        stdin().read_line(&mut buffer).expect("Cannot read line");
        println!("Got: {}", buffer);
        if buffer == "q\n" {
            break;
        }
        let los = time::SystemTime::now();
        println!("Racing to access some data with seek...");
        let mut file = File::open("datei").expect("Cannot open file");
        let mut sum: u64 = 0;
        let mut buf: Vec<u8> = Vec::with_capacity(4096);
        buf.resize(16 * 4096 * 1024, 0);
        for i in 0..1024 * 1024 * 1024 * 10 / (16 * 4096 * 1024) {
            file.seek(SeekFrom::Start(i * 16 * 4096 * 1024))
                .expect("Could not seek");
            file.read(&mut buf).expect("Could not read page");
            for j in 0..1024 {
                sum += buf[j * 16 * 4096] as u64;
            }
        }
        println!(
            "Done summation {} via full I/O after {:?}.",
            sum,
            los.elapsed()
        );
    }

    println!("Using io_uring...");
    let mut ring = IoUring::new(4096).expect("Cannot create ring");
    loop {
        let mut buffer = String::with_capacity(100);
        stdin().read_line(&mut buffer).expect("Cannot read line");
        println!("Got: {}", buffer);
        if buffer == "q\n" {
            break;
        }
        let los = time::SystemTime::now();
        println!("Racing to access some data with io_uring...");
        let file = File::open("datei").expect("Cannot open file");
        // Prepare some buffers:
        let mut bufs: Vec<Vec<u8>> = vec![];
        bufs.reserve(4096);
        for _ in 0..4096 {
            bufs.push(vec![0; 4096]);
        }
        let mut sum: u64 = 0;
        for i in 0..1024 * 1024 * 1024 * 10 / (16 * 4096 * 4096) {
            for j in 0..4096 {
                let read_e = opcode::Read::new(
                    types::Fd(file.as_raw_fd()),
                    bufs[j].as_mut_ptr(),
                    bufs[j].len() as _,
                )
                .offset((i * 16 * 4096 * 4096 + j * 16 * 4096) as u64)
                .build()
                .user_data(j as u64);

                unsafe {
                    ring.submission()
                        .push(&read_e)
                        .expect("submission queue is full");
                }
            }

            let c = ring
                .submit_and_wait(4096)
                .expect("Could not submit_and_wait");
            if c != 4096 {
                panic!("Did not get 4096 responses");
            }

            for _ in 0..4096 {
                let cqe = ring.completion().next().expect("completion queue is empty");

                let jj = cqe.user_data();
                if cqe.result() < 0 {
                    panic!("read error: {}", cqe.result());
                }

                sum += bufs[jj as usize][0] as u64;
                /*
                let k = j % 16;
                let millpos = k * 16 * 4096;
                for l in 0..4096 {
                    if bufs[j][l] != ((millpos + l) % 47) as u8 {
                        println!(
                            "Alarm: i = {}, j = {}, k = {}, l = {}, found: {}, expected: {}",
                            i,
                            j,
                            k,
                            l,
                            bufs[j][l],
                            ((millpos + l) % 47) as u8
                        );
                    }
                }
                */
            }
        }
        println!(
            "Done summation {} via io_uring after {:?}.",
            sum,
            los.elapsed()
        );
    }

    println!("Using io_uring better...");
    loop {
        let mut buffer = String::with_capacity(100);
        stdin().read_line(&mut buffer).expect("Cannot read line");
        println!("Got: {}", buffer);
        if buffer == "q\n" {
            break;
        }
        let los = time::SystemTime::now();
        println!("Racing to access some data with io_uring...");
        let file = File::open("datei").expect("Cannot open file");
        // Prepare some buffers:
        let mut bufs: Vec<Vec<u8>> = vec![];
        bufs.reserve(4096);
        for _ in 0..4096 {
            bufs.push(vec![0; 4096]);
        }
        let mut sum: u64 = 0;

        let submit_one = |i: &mut usize, j: usize, ring: &mut IoUring, bufs: &mut Vec<Vec<u8>>| {
            let read_e = opcode::Read::new(
                types::Fd(file.as_raw_fd()),
                bufs[j].as_mut_ptr(),
                bufs[j].len() as _,
            )
            .offset((*i) as u64)
            .build()
            .user_data(j as u64);

            unsafe {
                ring.submission()
                    .push(&read_e)
                    .expect("submission queue is full");
            }
            *i += 16 * 4096;
        };

        let mut i: usize = 0;
        let mut inflight: u32 = 0;

        for j in 0..4096 {
            submit_one(&mut i, j, &mut ring, &mut bufs);
        }
        inflight += 4096;

        while inflight > 0 {
            /*
            println!(
                "Beginning of loop, inflight = {}, i={}, target={}",
                inflight,
                i,
                10 * 1024 * 1024 * 1024 as u64
            );
            */

            let _ = ring
                .submit_and_wait(std::cmp::min(inflight as usize, 1024 as usize))
                .expect("Could not submit_and_wait");

            // println!("Got {} entries back", c);
            loop {
                let cqe = ring.completion().next();
                if cqe.is_none() {
                    break;
                }
                let cqe = cqe.unwrap();

                let jj = cqe.user_data();
                if cqe.result() < 0 {
                    panic!("read error: {}", cqe.result());
                }

                sum += bufs[jj as usize][0] as u64;
                if i < 10 * 1024 * 1024 * 1024 {
                    submit_one(&mut i, jj as usize, &mut ring, &mut bufs);
                } else {
                    inflight -= 1;
                }
                /*
                let k = j % 16;
                let millpos = k * 16 * 4096;
                for l in 0..4096 {
                    if bufs[j][l] != ((millpos + l) % 47) as u8 {
                        println!(
                            "Alarm: i = {}, j = {}, k = {}, l = {}, found: {}, expected: {}",
                            i,
                            j,
                            k,
                            l,
                            bufs[j][l],
                            ((millpos + l) % 47) as u8
                        );
                    }
                }
                */
            }
        }
        println!(
            "Done summation {} via io_uring better after {:?}.",
            sum,
            los.elapsed()
        );
    }

    const PAGE_SIZE: u64 = 4096;
    const DATA_SIZE: u64 = 10 * 1024 * 1024 * 1024;
    const BLOCK_SIZE: u64 = 4096 * 16;
    let shards: u64 = nr_threads; // needs to be a power of 2

    println!("Using io_uring better and multithreaded...");
    loop {
        let mut buffer = String::with_capacity(100);
        stdin().read_line(&mut buffer).expect("Cannot read line");
        println!("Got: {}", buffer);
        if buffer == "q\n" {
            break;
        }
        println!("Racing to access some data with io_uring in threads...");
        let los = time::SystemTime::now();

        let mut j: Vec<std::thread::JoinHandle<u64>> = vec![];
        for s in 0..shards {
            let nr_blocks = DATA_SIZE / BLOCK_SIZE;
            let blocks_per_shard = nr_blocks / shards;
            let start = blocks_per_shard * s;
            j.push(std::thread::spawn(move || -> u64 {
                let mut ring = IoUring::new(4096).expect("Cannot create ring");
                let file = File::open("datei").expect("Cannot open file");
                // Prepare some buffers:
                let mut bufs: Vec<Vec<u8>> = vec![];
                bufs.reserve(4096);
                for _ in 0..4096 {
                    bufs.push(vec![0; PAGE_SIZE as usize]);
                }
                let mut sum: u64 = 0;

                let submit_one =
                    |i: &mut u64, j: usize, ring: &mut IoUring, bufs: &mut Vec<Vec<u8>>| {
                        let read_e = opcode::Read::new(
                            types::Fd(file.as_raw_fd()),
                            bufs[j].as_mut_ptr(),
                            bufs[j].len() as _,
                        )
                        .offset((*i) * BLOCK_SIZE)
                        .build()
                        .user_data(j as u64);

                        unsafe {
                            ring.submission()
                                .push(&read_e)
                                .expect("submission queue is full");
                        }
                        *i += 1;
                    };

                let mut i = start;
                let mut inflight: u32 = 0;

                for j in 0..4096 {
                    submit_one(&mut i, j, &mut ring, &mut bufs);
                }
                inflight += 4096;

                while inflight > 0 {
                    let _ = ring
                        .submit_and_wait(std::cmp::min(inflight as usize, 1024 as usize))
                        .expect("Could not submit_and_wait");

                    // println!("Got {} entries back", c);
                    loop {
                        let cqe = ring.completion().next();
                        if cqe.is_none() {
                            break;
                        }
                        let cqe = cqe.unwrap();

                        let jj = cqe.user_data();
                        if cqe.result() < 0 {
                            panic!("read error: {}", cqe.result());
                        }

                        sum += bufs[jj as usize][0] as u64;
                        if i < start + blocks_per_shard {
                            submit_one(&mut i, jj as usize, &mut ring, &mut bufs);
                        } else {
                            inflight -= 1;
                        }
                    }
                }
                sum
            }));
        }
        let mut sum: u64 = 0;
        for jj in j {
            let r = jj.join().expect("Could not join");
            sum += r;
        }
        println!(
            "Done summation {} via io_uring better after {:?}.",
            sum,
            los.elapsed()
        );
    }
}
