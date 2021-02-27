use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use crossbeam::sync::WaitGroup;
use kvs::{
    stop_server,
    thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool},
    KvStore, KvsClient, KvsEngine, KvsServer, SledKvsEngine,
};
use rand::Rng;
use std::usize;
use std::{sync::atomic::Ordering, thread};
use tempfile::TempDir;

const ASCII_START: u8 = 33;
const ASCII_END: u8 = 127;

fn random_gen_key(len: usize) -> String {
    let mut rng = rand::thread_rng();
    let key: String = (0..len)
        .map(|_| rng.gen_range(ASCII_START, ASCII_END) as char)
        .collect();
    key
}

fn write_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_bench");
    let mut keys = Vec::with_capacity(1000);
    for _ in 0..1000 {
        keys.push(random_gen_key(10));
    }

    for thread_num in vec![1, 2, 4, 6, 8] {
        println!("thread num {} start", thread_num);
        // KvStore with SharedQueueThreadPool
        let temp_dir = TempDir::new().unwrap();
        let (mut server, server_state) = KvsServer::new_with_state(
            KvStore::open(temp_dir.path()).unwrap(),
            SharedQueueThreadPool::new(thread_num).unwrap(),
        );
        thread::spawn(
            move || {
                while let Err(..) = server.run(format!("127.0.0.1:888{}", thread_num)) {}
            },
        );
        group.bench_with_input(
            BenchmarkId::new("write_shared_kvstore", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..1000 {
                        let wg = wg.clone();
                        let key = keys[i].clone();
                        let thread_num = thread_num.clone();
                        thread::spawn(move || {
                            match KvsClient::new(format!("127.0.0.1:888{}", thread_num)) {
                                Ok(mut client) => {
                                    if let Err(e) = client.set(key, "value".to_owned()) {
                                        eprintln!("{}", e);
                                    }
                                }
                                Err(_) => {}
                            }

                            drop(wg);
                        });
                    }
                    wg.wait();
                });
            },
        );
        stop_server(server_state, format!("127.0.0.1:888{}", thread_num));
        // KvStore with Rayon
        let temp_dir = TempDir::new().unwrap();
        let (mut server, server_state) = KvsServer::new_with_state(
            KvStore::open(temp_dir.path()).unwrap(),
            RayonThreadPool::new(thread_num).unwrap(),
        );
        thread::spawn(
            move || {
                while let Err(..) = server.run(format!("127.0.0.1:777{}", thread_num)) {}
            },
        );
        group.bench_with_input(
            BenchmarkId::new("write_rayon_kvstore", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..1000 {
                        let wg = wg.clone();
                        let key = keys[i].clone();
                        let thread_num = thread_num.clone();
                        thread::spawn(move || {
                            match KvsClient::new(format!("127.0.0.1:777{}", thread_num)) {
                                Ok(mut client) => {
                                    if let Err(e) = client.set(key, "value".to_owned()) {
                                        eprintln!("{}", e);
                                    }
                                }
                                Err(_) => {}
                            }

                            drop(wg);
                        });
                    }
                    wg.wait();
                });
            },
        );
        stop_server(server_state, format!("127.0.0.1:777{}", thread_num));
        // Sled with Rayon
        let temp_dir = TempDir::new().unwrap();
        let (mut server, server_state) = KvsServer::new_with_state(
            SledKvsEngine::open(temp_dir.path()).unwrap(),
            RayonThreadPool::new(thread_num).unwrap(),
        );
        thread::spawn(
            move || {
                while let Err(..) = server.run(format!("127.0.0.1:999{}", thread_num)) {}
            },
        );
        group.bench_with_input(
            BenchmarkId::new("write_rayon_sled", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..1000 {
                        let wg = wg.clone();
                        let key = keys[i].clone();
                        let thread_num = thread_num.clone();
                        thread::spawn(move || {
                            match KvsClient::new(format!("127.0.0.1:999{}", thread_num)) {
                                Ok(mut client) => {
                                    if let Err(e) = client.set(key, "value".to_owned()) {
                                        eprintln!("{}", e);
                                    }
                                }
                                Err(_) => {}
                            }

                            drop(wg);
                        });
                    }
                    wg.wait();
                });
            },
        );
        stop_server(server_state, format!("127.0.0.1:999{}", thread_num));
    }
    group.finish();
}

fn read_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_bench");
    let mut keys = Vec::with_capacity(1000);
    let mut values = Vec::with_capacity(1000);
    for _ in 0..1000 {
        keys.push(random_gen_key(10));
        values.push(random_gen_key(10));
    }

    for thread_num in vec![1, 2, 4, 6, 8] {
        // init data
        let temp_dir1 = TempDir::new().unwrap();
        let temp_dir2 = TempDir::new().unwrap();
        let temp_dir3 = TempDir::new().unwrap();
        let engine1 = KvStore::open(temp_dir1.path()).unwrap();
        let engine2 = KvStore::open(temp_dir2.path()).unwrap();
        let engine3 = SledKvsEngine::open(temp_dir3.path()).unwrap();
        for i in 0..1000 {
            engine1.set(keys[i].clone(), values[i].clone()).unwrap();
            engine2.set(keys[i].clone(), values[i].clone()).unwrap();
            engine3.set(keys[i].clone(), values[i].clone()).unwrap();
        }
        println!("thread num {} start", thread_num);
        // KvStore with SharedQueueThreadPool
        let (mut server, server_state) =
            KvsServer::new_with_state(engine1, SharedQueueThreadPool::new(thread_num).unwrap());
        thread::spawn(
            move || {
                while let Err(..) = server.run(format!("127.0.0.1:888{}", thread_num)) {}
            },
        );
        group.bench_with_input(
            BenchmarkId::new("read_shared_kvstore", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..1000 {
                        let wg = wg.clone();
                        let key = keys[i].clone();
                        let value = values[i].clone();
                        let thread_num = thread_num.clone();
                        thread::spawn(move || {
                            match KvsClient::new(format!("127.0.0.1:888{}", thread_num)) {
                                Ok(mut client) => match client.get(key) {
                                    Err(e) => {
                                        eprintln!("{}", e);
                                    }
                                    Ok(Some(v)) => {
                                        assert_eq!(value, v);
                                    }
                                    _ => {}
                                },
                                Err(_) => {}
                            }
                            drop(wg);
                        });
                    }
                    wg.wait();
                });
            },
        );
        stop_server(server_state, format!("127.0.0.1:888{}", thread_num));
        // KvStore with Rayon
        let (mut server, server_state) =
            KvsServer::new_with_state(engine2, RayonThreadPool::new(thread_num).unwrap());
        thread::spawn(
            move || {
                while let Err(..) = server.run(format!("127.0.0.1:777{}", thread_num)) {}
            },
        );
        group.bench_with_input(
            BenchmarkId::new("read_rayon_kvstore", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..1000 {
                        let wg = wg.clone();
                        let key = keys[i].clone();
                        let value = values[i].clone();
                        let thread_num = thread_num.clone();
                        thread::spawn(move || {
                            match KvsClient::new(format!("127.0.0.1:777{}", thread_num)) {
                                Ok(mut client) => match client.get(key) {
                                    Err(e) => {
                                        eprintln!("{}", e);
                                    }
                                    Ok(Some(v)) => {
                                        assert_eq!(value, v);
                                    }
                                    _ => {}
                                },
                                Err(_) => {}
                            }
                            drop(wg);
                        });
                    }
                    wg.wait();
                });
            },
        );
        stop_server(server_state, format!("127.0.0.1:777{}", thread_num));
        // Sled with Rayon
        let (mut server, server_state) =
            KvsServer::new_with_state(engine3, RayonThreadPool::new(thread_num).unwrap());
        thread::spawn(
            move || {
                while let Err(..) = server.run(format!("127.0.0.1:999{}", thread_num)) {}
            },
        );
        group.bench_with_input(
            BenchmarkId::new("read_rayon_sled", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..1000 {
                        let wg = wg.clone();
                        let key = keys[i].clone();
                        let value = values[i].clone();
                        let thread_num = thread_num.clone();
                        thread::spawn(move || {
                            match KvsClient::new(format!("127.0.0.1:999{}", thread_num)) {
                                Ok(mut client) => match client.get(key) {
                                    Err(e) => {
                                        eprintln!("{}", e);
                                    }
                                    Ok(Some(v)) => {
                                        assert_eq!(value, v);
                                    }
                                    _ => {}
                                },
                                Err(_) => {}
                            }
                            drop(wg);
                        });
                    }
                    wg.wait();
                });
            },
        );
        stop_server(server_state, format!("127.0.0.1:999{}", thread_num));
    }
    group.finish();
}

criterion_group!(benches, write_bench, read_bench);
criterion_main!(benches);
