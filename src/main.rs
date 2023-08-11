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
    println!("This is mmaptest.");

    {
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
            "Done summation {} via full I/O after {:?}.",
            sum,
            los.elapsed()
        );
    }
}