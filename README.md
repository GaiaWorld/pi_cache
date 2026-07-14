# pi_cache

`pi_cache` 是一个基于分段 LFU-LRU 的内存缓存。它优先淘汰低频条目，同一频次
范围内优先淘汰最早进入队列的条目，并通过周期性频次衰减避免历史热点长期占据
缓存。

## 工作原理

访问频次最大为 `15`，内部划分为五个 LRU 队列：

| 队列 | 有效频次 |
| --- | --- |
| 0 | `0` |
| 1 | `1` |
| 2 | `2..=3` |
| 3 | `4..=7` |
| 4 | `8..=15` |

新条目从频次 `0` 开始。更新已有条目的 `put` 和成功的 `active_mut` 会增加频次，
只读的 `get`、不激活条目的 `get_mut` 以及 `take` 不会增加频次。

缓存会根据 `frequency_down_rate` 周期性降低所有条目的有效频次。默认值为 `8`，
表示触发降频所需的操作数量近似为当前槽位数的八倍。降频时频次大致减半，例如
`8 → 4`、`4 → 2`、`2 → 1`、`1 → 0`。

## 快速开始

缓存值必须实现 `Data`，用来报告参与容量计算的大小和超时时间：

```rust
use pi_cache::{Cache, Data};

struct Image {
    bytes: Vec<u8>,
    expires_at_ms: u64,
}

impl Data for Image {
    fn size(&self) -> usize {
        self.bytes.len()
    }

    fn timeout(&self) -> u64 {
        self.expires_at_ms
    }
}

let mut cache = Cache::with_config(128, 8);
cache.put(
    "logo".to_owned(),
    Image {
        bytes: vec![0; 1024],
        expires_at_ms: 2_000,
    },
);

assert_eq!(cache.len(), 1);
assert_eq!(cache.size(), 1024);
assert!(cache.get(&"logo".to_owned()).is_some());

// active_mut 会提升频次；get_mut 不会。
cache.active_mut(&"logo".to_owned());

// 清理接口是惰性迭代器，必须消费它才会真正执行清理。
let removed: Vec<_> = cache.capacity_collect(512).collect();
assert_eq!(removed.len(), 1);
assert_eq!(cache.len(), 0);
```

最后一项也可以直接写成：

```rust
# use pi_cache::{Cache, Data};
# struct Value;
# impl Data for Value {}
# let mut cache: Cache<u32, Value> = Cache::default();
# cache.put(1, Value);
let _: Vec<_> = cache.capacity_collect(0).collect();
assert_eq!(cache.len(), 0);
```

## 容量不是硬限制

`Cache::with_config(map_capacity, frequency_down_rate)` 中的 `map_capacity` 只是键索引
哈希表的初始容量，不是缓存大小上限。`put` 总会先插入值，也不会自动触发淘汰。

调用方应主动消费容量清理迭代器：

```rust
# use pi_cache::{Cache, Data};
# struct Value(usize);
# impl Data for Value { fn size(&self) -> usize { self.0 } }
# let mut cache: Cache<u32, Value> = Cache::default();
# cache.put(1, Value(150));
let evicted: Vec<_> = cache.capacity_collect(100).collect();
```

清理按低频优先、同频段先进先出的顺序进行，直到 `cache.size() <= capacity`。如果
单个值就大于目标容量，该值最终也会被淘汰。

## 获取、激活与暂时取走

| 接口 | 返回值 | 是否提升频次 | 主要用途 |
| --- | --- | --- | --- |
| `get` | `&V` | 否 | 只读访问 |
| `get_mut` | `&mut V` | 否 | 修改值但不记录访问 |
| `active_mut` | `&mut V` | 是 | 修改值并记录一次访问 |
| `take` | `V` | 否 | 暂时取得值的所有权 |
| `remove` | `V` | 否 | 永久删除键和值 |

`take` 成功后会保留该键的频次信息，状态为 `FrequencyState::TakenAway`。调用方应
保证之后使用 `put` 归还同一个键；若决定丢弃该值，应使用 `remove` 清除残留记录。

通过 `get_mut` 或 `active_mut` 改变值的实际大小时，缓存无法自动获知差值，需要
调用 `adjust_size(new_size as isize - old_size as isize)` 修正大小统计。

## 两阶段引用清理

`capacity_ref_collect` 和 `timeout_ref_collect` 不立即释放值，而是把条目移出频次
队列、标记为 `FrequencyState::Garbaged`，并返回值的共享引用。引用释放后，可用
`collect` 真正删除条目；在删除前也可以用 `active_mut` 重新激活。

```rust
# use pi_cache::{Cache, Data};
# #[derive(Debug)] struct Value;
# impl Data for Value {}
# let mut cache: Cache<u32, Value> = Cache::default();
# cache.put(1, Value);
let garbage_keys: Vec<u32> = cache
    .capacity_ref_collect(0)
    .map(|(key, _value)| *key)
    .collect();

// 引用迭代器已经释放，现在可以再次可变借用 cache。
for key in garbage_keys {
    let _removed = cache.collect(key);
}
```

直接取得被清理条目所有权时，使用 `capacity_collect` 或 `timeout_collect`。

## 超时语义

超时清理使用以下条件判断条目是否过期：

```text
value.timeout() < now
```

`timeout()` 和清理接口的 `now` 必须采用相同的时间基准，通常是毫秒时间戳。默认
实现返回 `0`，所以当 `now > 0` 时会被视为过期；当前实现没有为“永不过期”保留
特殊值。

超时清理还受 `capacity` 控制：只有当前大小大于 `capacity` 时才继续清理。它不是
“删除所有已过期条目”的无条件扫描；如果需要尽可能清理过期数据，应传入 `0`。

## 保存和恢复频次

`items_metas` 返回有效条目的键、当前频次、大小和超时时间，可用于外部序列化。
恢复时可以使用 `put_with_frequency`：

- 新键的 `frequency` 表示初始频次；
- 已有键的 `frequency` 表示增加到现有频次上的增量；
- 该接口不推进全局降频计数；
- 新键的频次必须位于 `0..=15`，传入更大的值可能导致 panic。

`items_metas` 不包含被 `take` 取走或已经标记为垃圾的条目，也不包含值本身。

## 指标

`metrics()` 返回累计指标的快照，不会重置计数器。`len()` 和 `size()` 分别通过
`len_incr - len_decr`、`size_incr - size_decr` 计算当前统计值；`hit` 和 `miss`
只统计 `take` 与 `active_mut` 的结果，普通 `get` 不更新命中率。

## 线程安全

`Cache` 不提供内部同步。需要在多个线程间共享时，由调用方使用 `Mutex`、`RwLock`
或其他适合业务访问模式的同步机制。

## License

MIT OR Apache-2.0（以 `Cargo.toml` 中的声明为准）。
