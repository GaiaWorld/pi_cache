#![feature(test)]

extern crate test;
use test::Bencher;

use std::thread;
use std::sync::{Arc,
                mpsc::channel};

use parking_lot::RwLock;
use tinyufo::TinyUfo;

use pi_cache::{Cache, Data};

#[derive(Debug, Clone, Eq, PartialEq)]
struct R1(usize, usize, u64);

impl Data for R1 {
    /// 资源的大小
    fn size(&self) -> usize {
        self.1
    }
    /// 超时时间
    fn timeout(&self) -> u64 {
        self.2
    }
}

#[bench]
fn bench_pi_cache_put(b: &mut Bencher) {
    let mut cache: Cache<usize, R1> = Default::default();
    let mut time: u64 = 0;
    let mut f = || {
        time += 1;
        time
    };

    b.iter(move || {
        for k in 0..10000 {
            cache.put(k, R1(k, k * 1000, f()));
        }
    });
}

#[bench]
fn bench_pi_cache_get(b: &mut Bencher) {
    let mut cache: Cache<usize, R1> = Default::default();
    let mut time: u64 = 0;
    let mut f = || {
        time += 1;
        time
    };

    for k in 0..10000 {
        cache.put(k, R1(k, k * 1000, f()));
    }

    b.iter(move || {
        for k in 0..10000 {
            let r = cache.get(&k);
            assert!(r.is_some());
        }
    });
}

#[bench]
fn bench_pi_cache_put_by_lock(b: &mut Bencher) {
    let mut cache: Arc<RwLock<Cache<usize, R1>>> = Arc::new(RwLock::new(Default::default()));

    b.iter(move || {
        let cache0 = cache.clone();
        let handle0 = thread::spawn(move || {
            for k in 0..10000 {
                cache0.write().put(k, R1(k, k * 1000, k as u64));
            }
        });

        let cache1 = cache.clone();
        let handle1 = thread::spawn(move || {
            for k in 0..10000 {
                cache1.write().put(k, R1(k, k * 1000, k as u64));
            }
        });

        let cache2 = cache.clone();
        let handle2 = thread::spawn(move || {
            for k in 0..10000 {
                cache2.write().put(k, R1(k, k * 1000, k as u64));
            }
        });

        let cache3 = cache.clone();
        let handle3 = thread::spawn(move || {
            for k in 0..10000 {
                cache3.write().put(k, R1(k, k * 1000, k as u64));
            }
        });

        handle0.join();
        handle1.join();
        handle2.join();
        handle3.join();
    });
}

#[bench]
fn bench_pi_cache_get_by_lock(b: &mut Bencher) {
    let mut cache: Arc<RwLock<Cache<usize, R1>>> = Arc::new(RwLock::new(Default::default()));
    let mut time: u64 = 0;
    let mut f = || {
        time += 1;
        time
    };

    {
        let mut locked = cache.write();
        for k in 0..10000 {
            locked.put(k, R1(k, k * 1000, f()));
        }
    }

    b.iter(move || {
        let cache0 = cache.clone();
        let handle0 = thread::spawn(move || {
            for k in 0..10000 {
                assert!(cache0.read().get(&k).is_some());
            }
        });

        let cache1 = cache.clone();
        let handle1 = thread::spawn(move || {
            for k in 0..10000 {
                assert!(cache1.read().get(&k).is_some());
            }
        });

        let cache2 = cache.clone();
        let handle2 = thread::spawn(move || {
            for k in 0..10000 {
                assert!(cache2.read().get(&k).is_some());
            }
        });

        let cache3 = cache.clone();
        let handle3 = thread::spawn(move || {
            for k in 0..10000 {
                assert!(cache3.read().get(&k).is_some());
            }
        });

        handle0.join();
        handle1.join();
        handle2.join();
        handle3.join();
    });
}

#[bench]
fn bench_tinyufo_put(b: &mut Bencher) {
    let mut cache = TinyUfo::new(10000, 100);
    let mut time: u64 = 0;
    let mut f = || {
        time += 1;
        time
    };

    b.iter(move || {
        for k in 0..10000 {
            cache.put(k, R1(k, k * 1000, f()), 1);
        }
    });
}

#[bench]
fn bench_tinyufo_get(b: &mut Bencher) {
    let mut cache = TinyUfo::new(10000, 100);
    let mut time: u64 = 0;
    let mut f = || {
        time += 1;
        time
    };

    for k in 0..10000 {
        cache.put(k, R1(k, k * 1000, f()), 1);
    }

    b.iter(move || {
        for k in 0..10000 {
            let r = cache.get(&k);
            assert!(r.is_some());
        }
    });
}

#[bench]
fn bench_tinyufo_put_by_multi_thread(b: &mut Bencher) {
    let mut cache = Arc::new(TinyUfo::new(50000, 100));

    b.iter(move || {
        let cache0 = cache.clone();
        let handle0 = thread::spawn(move || {
            for k in 0..10000 {
                cache0.put(k, R1(k, k * 1000, k as u64), 1);
            }
        });

        let cache1 = cache.clone();
        let handle1 = thread::spawn(move || {
            for k in 0..10000 {
                cache1.put(k, R1(k, k * 1000, k as u64), 1);
            }
        });

        let cache2 = cache.clone();
        let handle2 = thread::spawn(move || {
            for k in 0..10000 {
                cache2.put(k, R1(k, k * 1000, k as u64), 1);
            }
        });

        let cache3 = cache.clone();
        let handle3 = thread::spawn(move || {
            for k in 0..10000 {
                cache3.put(k, R1(k, k * 1000, k as u64), 1);
            }
        });

        handle0.join();
        handle1.join();
        handle2.join();
        handle3.join();
    });
}

#[bench]
fn bench_tinyufo_get_by_multi_thread(b: &mut Bencher) {
    let mut cache = Arc::new(TinyUfo::new(10000, 100));
    let mut time: u64 = 0;
    let mut f = || {
        time += 1;
        time
    };

    for k in 0..10000 {
        cache.put(k, R1(k, k * 1000, f()), 1);
    }

    b.iter(move || {
        let cache0 = cache.clone();
        let handle0 = thread::spawn(move || {
            for k in 0..10000 {
                let r = cache0.get(&k);
                assert!(r.is_some());
            }
        });

        let cache1 = cache.clone();
        let handle1 = thread::spawn(move || {
            for k in 0..10000 {
                let r = cache1.get(&k);
                assert!(r.is_some());
            }
        });

        let cache2 = cache.clone();
        let handle2 = thread::spawn(move || {
            for k in 0..10000 {
                let r = cache2.get(&k);
                assert!(r.is_some());
            }
        });

        let cache3 = cache.clone();
        let handle3 = thread::spawn(move || {
            for k in 0..10000 {
                let r = cache3.get(&k);
                assert!(r.is_some());
            }
        });

        handle0.join();
        handle1.join();
        handle2.join();
        handle3.join();
    });
}


