use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use crossbeam::sync::WaitGroup;
use kvs::{
    async_client, async_server, sync_client, sync_server,
    thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool},
    KvStore, KvsEngine, SledKvsEngine,
};
use rand::Rng;
use std::thread;
use std::usize;
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
        let (mut server, server_state) = sync_server::KvsServer::new_with_state(
            KvStore::open(temp_dir.path()).unwrap(),
            SharedQueueThreadPool::new(thread_num).unwrap(),
        );
        thread::spawn(
            move || {
                while let Err(..) = server.run(format!("127.0.0.1:888{}", thread_num)) {}
            },
        );
        group.bench_with_input(
            BenchmarkId::new("sync_write_shared_kvstore", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..1000 {
                        let wg = wg.clone();
                        let key = keys[i].clone();
                        let thread_num = thread_num.clone();
                        thread::spawn(move || {
                            match sync_client::KvsClient::connect(format!(
                                "127.0.0.1:888{}",
                                thread_num
                            )) {
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
        sync_server::stop_server(server_state, format!("127.0.0.1:888{}", thread_num));
        // KvStore with Rayon
        let temp_dir = TempDir::new().unwrap();
        let (mut server, server_state) = sync_server::KvsServer::new_with_state(
            KvStore::open(temp_dir.path()).unwrap(),
            RayonThreadPool::new(thread_num).unwrap(),
        );
        thread::spawn(
            move || {
                while let Err(..) = server.run(format!("127.0.0.1:777{}", thread_num)) {}
            },
        );
        group.bench_with_input(
            BenchmarkId::new("sync_write_rayon_kvstore", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..1000 {
                        let wg = wg.clone();
                        let key = keys[i].clone();
                        let thread_num = thread_num.clone();
                        thread::spawn(move || {
                            match sync_client::KvsClient::connect(format!(
                                "127.0.0.1:777{}",
                                thread_num
                            )) {
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
        sync_server::stop_server(server_state, format!("127.0.0.1:777{}", thread_num));
        // Sled with Rayon
        let temp_dir = TempDir::new().unwrap();
        let (mut server, server_state) = sync_server::KvsServer::new_with_state(
            SledKvsEngine::open(temp_dir.path()).unwrap(),
            RayonThreadPool::new(thread_num).unwrap(),
        );
        thread::spawn(
            move || {
                while let Err(..) = server.run(format!("127.0.0.1:999{}", thread_num)) {}
            },
        );
        group.bench_with_input(
            BenchmarkId::new("sync_write_rayon_sled", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..1000 {
                        let wg = wg.clone();
                        let key = keys[i].clone();
                        let thread_num = thread_num.clone();
                        thread::spawn(move || {
                            match sync_client::KvsClient::connect(format!(
                                "127.0.0.1:999{}",
                                thread_num
                            )) {
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
        sync_server::stop_server(server_state, format!("127.0.0.1:999{}", thread_num));

        // async with tokio
        let rt = tokio::runtime::Runtime::new().unwrap();
        // async KvStore
        let temp_dir = TempDir::new().unwrap();
        let (mut server, server_state) =
            async_server::KvsServer::new_with_state(KvStore::open(temp_dir.path()).unwrap());
        rt.spawn(async move {
            while let Err(..) = server.run(format!("127.0.0.1:887{}", thread_num)).await {}
        });
        group.bench_with_input(
            BenchmarkId::new("async_write_kvstore", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.to_async(&rt).iter(|| async_sets(&keys, &thread_num));
            },
        );
        rt.block_on(async_server::stop_server(
            server_state,
            format!("127.0.0.1:887{}", thread_num),
        ));
        drop(rt);
        // async sled
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp_dir = TempDir::new().unwrap();
        let (mut server, server_state) =
            async_server::KvsServer::new_with_state(SledKvsEngine::open(temp_dir.path()).unwrap());
        rt.spawn(async move {
            while let Err(..) = server.run(format!("127.0.0.1:889{}", thread_num)).await {}
        });
        group.bench_with_input(
            BenchmarkId::new("async_write_sled", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.to_async(&rt).iter(|| async_sets(&keys, &thread_num));
            },
        );
        rt.block_on(async_server::stop_server(
            server_state,
            format!("127.0.0.1:889{}", thread_num),
        ));
        drop(rt);
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
        let temp_dir4 = TempDir::new().unwrap();
        let engine1 = KvStore::open(temp_dir1.path()).unwrap();
        let engine2 = KvStore::open(temp_dir2.path()).unwrap();
        let engine3 = SledKvsEngine::open(temp_dir3.path()).unwrap();
        let engine4 = KvStore::open(temp_dir4.path()).unwrap();
        for i in 0..1000 {
            engine1.set(keys[i].clone(), values[i].clone()).unwrap();
            engine2.set(keys[i].clone(), values[i].clone()).unwrap();
            engine3.set(keys[i].clone(), values[i].clone()).unwrap();
            engine4.set(keys[i].clone(), values[i].clone()).unwrap();
        }
        println!("thread num {} start", thread_num);
        // async
        // KvStore with SharedQueueThreadPool
        let (mut server, server_state) = sync_server::KvsServer::new_with_state(
            engine1,
            SharedQueueThreadPool::new(thread_num).unwrap(),
        );
        thread::spawn(
            move || {
                while let Err(..) = server.run(format!("127.0.0.1:888{}", thread_num)) {}
            },
        );
        group.bench_with_input(
            BenchmarkId::new("sync_read_shared_kvstore", thread_num),
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
                            match sync_client::KvsClient::connect(format!(
                                "127.0.0.1:888{}",
                                thread_num
                            )) {
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
        sync_server::stop_server(server_state, format!("127.0.0.1:888{}", thread_num));
        // KvStore with Rayon
        let (mut server, server_state) = sync_server::KvsServer::new_with_state(
            engine2,
            RayonThreadPool::new(thread_num).unwrap(),
        );
        thread::spawn(
            move || {
                while let Err(..) = server.run(format!("127.0.0.1:777{}", thread_num)) {}
            },
        );
        group.bench_with_input(
            BenchmarkId::new("sync_read_rayon_kvstore", thread_num),
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
                            match sync_client::KvsClient::connect(format!(
                                "127.0.0.1:777{}",
                                thread_num
                            )) {
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
        sync_server::stop_server(server_state, format!("127.0.0.1:777{}", thread_num));
        // Sled with Rayon
        let (mut server, server_state) = sync_server::KvsServer::new_with_state(
            engine3,
            RayonThreadPool::new(thread_num).unwrap(),
        );
        thread::spawn(
            move || {
                while let Err(..) = server.run(format!("127.0.0.1:999{}", thread_num)) {}
            },
        );
        group.bench_with_input(
            BenchmarkId::new("sync_read_rayon_sled", thread_num),
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
                            match sync_client::KvsClient::connect(format!(
                                "127.0.0.1:999{}",
                                thread_num
                            )) {
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
        sync_server::stop_server(server_state, format!("127.0.0.1:999{}", thread_num));
        // async
        let rt = tokio::runtime::Runtime::new().unwrap();
        // async KvStore
        let (mut server, server_state) = async_server::KvsServer::new_with_state(engine4);
        rt.spawn(async move {
            while let Err(..) = server.run(format!("127.0.0.1:887{}", thread_num)).await {}
        });
        group.bench_with_input(
            BenchmarkId::new("async_read_kvstore", thread_num),
            &thread_num,
            |b, &thread_num| {
                b.to_async(&rt)
                    .iter(|| async_gets(&keys, &values, &thread_num));
            },
        );
        rt.block_on(async_server::stop_server(
            server_state,
            format!("127.0.0.1:887{}", thread_num),
        ));
        drop(rt);
    }
    group.finish();
}

async fn async_sets(keys: &Vec<String>, thread_num: &u32) {
    {
        let wg = WaitGroup::new();
        for i in 0..1000 {
            let wg = wg.clone();
            let key = keys[i].clone();
            let thread_num = thread_num.clone();
            tokio::spawn(async move {
                match async_client::KvsClient::connect(format!("127.0.0.1:555{}", thread_num)).await
                {
                    Ok(client) => {
                        if let Err(e) = client.set(key, "value".to_owned()).await {
                            eprintln!("{}", e);
                        }
                    }
                    Err(_) => {}
                }

                drop(wg);
            });
        }
        wg.wait();
    }
}

async fn async_gets(keys: &Vec<String>, values: &Vec<String>, thread_num: &u32) {
    let wg = WaitGroup::new();
    for i in 0..1000 {
        let wg = wg.clone();
        let key = keys[i].clone();
        let value = values[i].clone();
        let thread_num = thread_num.clone();
        tokio::spawn(async move {
            match async_client::KvsClient::connect(format!("127.0.0.1:889{}", thread_num)).await {
                Ok(client) => match client.get(key).await {
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
}

criterion_group!(benches, write_bench, read_bench);
criterion_main!(benches);
