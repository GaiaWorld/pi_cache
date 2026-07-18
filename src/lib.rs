//! 基于分段 LFU-LRU 的内存缓存。
//!
//! `pi_cache` 同时使用访问频次和同频次内的访问顺序决定淘汰优先级。缓存将
//! `0`、`1`、`2..=3`、`4..=7`、`8..=15` 五个频次范围分别维护为 LRU
//! 队列；清理时先处理低频队列，同一队列中先处理最早进入队列的条目。
//!
//! # 频次与老化
//!
//! 新条目的频次为 `0`。调用 [`Cache::put`] 更新已有条目或调用
//! [`Cache::active_mut`] 会提升频次，最大频次为 `15`。当会提升频次的操作数量
//! 超过“当前槽位数乘以降频率”的近似阈值时，缓存执行一次全局降频：频次大致
//! 减半，使近期热点能够逐渐替代历史热点。只读的 [`Cache::get`] 和
//! [`Cache::get_mut`] 不会提升频次。
//!
//! # 条目状态
//!
//! 除正常缓存状态外，频次表还会记录两种中间状态：
//!
//! - [`FrequencyState::TakenAway`]：值已被 [`Cache::take`] 取走，但频次信息仍被
//!   保留，调用方应使用 [`Cache::put`] 将值归还；
//! - [`FrequencyState::Garbaged`]：值已移出频次队列但仍保存在槽位中，之后可用
//!   [`Cache::collect`] 真正移除，或用 [`Cache::active_mut`] 重新激活。
//!
//! # 容量管理
//!
//! 缓存没有自动执行的硬容量上限。[`Cache::with_config`] 的 `map_capacity` 仅是
//! 哈希表的初始容量。调用方应根据 [`Data::size`] 返回的大小，主动消费
//! [`Cache::capacity_collect`]、[`Cache::capacity_ref_collect`] 或相应的超时清理
//! 迭代器。

use pi_hash::XHashMap;
use pi_null::Null;
use pi_slot_deque::{Deque, Iter as SlotIter, Slot};
use pi_slotmap::{DefaultKey, Key};
use std::collections::hash_map;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::replace;
use std::time::{SystemTime, UNIX_EPOCH};

/// 缓存支持的最大访问频次。
const FREQUENCY_MAX: u32 = 15;

/// 返回当前 Unix 毫秒时间戳。
///
/// 系统时间早于 Unix 纪元时返回 `0`；超出 `u64` 表示范围时截断为 `u64::MAX`。
fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}
/// 默认的 CuckooFilter 窗口大小。
///
/// 当前版本已不再启用 CuckooFilter；保留该公共常量仅用于兼容已有调用方。
pub const WINDOW_SIZE: usize = 1024;
/// 默认频次降级率。
///
/// 该值控制触发全局降频所需的操作数量，近似为当前槽位数的 `8` 倍。
/// 参见 [`Cache::with_config`]。
pub const FREQUENCY_DOWN_RATE: usize = 8;

/// 基于分段 LFU-LRU 的缓存。
///
/// `K` 用于定位条目和保存频次状态；`V` 通过 [`Data`] 向缓存报告大小和超时
/// 时间。类型本身不提供内部同步，多线程共享时应由调用方使用锁或其他同步机制。
pub struct Cache<K: Eq + Hash + Clone, V: Data> {
    /// 频率表
    lfu: Lfu<K, V>,
    /// 数据条目
    map: XHashMap<K, Item>,
    // /// 首次数据的bloom，判断不在缓存的数据是否被调用过， 一般指定大小，比如1024.
    // filter: CuckooFilter<K>,
}

impl<K: Eq + Hash + Clone, V: Data> Default for Cache<K, V> {
    fn default() -> Self {
        Self::with_config(0, /*WINDOW_SIZE*/ FREQUENCY_DOWN_RATE)
    }
}
impl<K: Eq + Hash + Clone, V: Data> Cache<K, V> {
    /// 使用指定的哈希表初始容量和频次降级率创建缓存。
    ///
    /// # 参数
    ///
    /// - `map_capacity`：键索引哈希表的初始容量；传入 `0` 使用默认容量。它不是
    ///   缓存大小上限，也不会使 [`put`](Self::put) 自动淘汰条目。
    /// - `frequency_down_rate`：频次老化速率。值越小，历史频次衰减越快；值越大，
    ///   热点保留越久。默认值为 [`FREQUENCY_DOWN_RATE`]。
    ///
    /// 降频阈值由当前槽位数动态计算，因此它表示近似比例，而不是固定操作次数。
    pub fn with_config(
        map_capacity: usize,
        // cuckoo_filter_window_size: usize,
        frequency_down_rate: usize,
    ) -> Self {
        let map = if map_capacity == 0 {
            Default::default()
        } else {
            XHashMap::with_capacity_and_hasher(map_capacity, Default::default())
        };
        Self {
            lfu: Lfu::new(frequency_down_rate),
            map,
            // filter: CuckooFilter::from_entries_per_index(cuckoo_filter_window_size, 0.01, 8),
        }
    }
    /// 判断指定键当前是否仍有可访问的数据。
    ///
    /// 正常条目和已标记为垃圾的条目返回 `true`；被 [`take`](Self::take) 取走或
    /// 从未存在的键返回 `false`。
    pub fn contains_key(&self, k: &K) -> bool {
        if let Some(r) = self.map.get(k) {
            return !r.key.is_null();
        }
        false
    }
    /// 查询指定键当前的频次或中间状态。
    ///
    /// 频次会根据已经发生的全局降频次数实时折算，返回值范围为 `0..=15`。
    pub fn get_frequency(&self, k: &K) -> FrequencyState {
        if let Some(r) = self.map.get(k) {
            return if r.key.is_null() {
                FrequencyState::TakenAway
            } else if r.frequency_down_count == 0 {
                FrequencyState::Garbaged
            } else {
                FrequencyState::Frequency(r.shr(self.lfu.frequency_down_count) as u8)
            };
        }
        FrequencyState::None
    }
    /// 返回指定键最后一次被激活的 Unix 毫秒时间戳。
    ///
    /// [`put`](Self::put) 插入、替换或归还条目，以及
    /// [`active_mut`](Self::active_mut) 成功激活条目时会更新时间戳。
    /// [`get`](Self::get)、[`get_mut`](Self::get_mut)、[`take`](Self::take) 和
    /// [`put_with_frequency`](Self::put_with_frequency) 不更新时间戳。
    ///
    /// 条目不存在或仅通过 `put_with_frequency` 写入且从未被激活时返回 `None`。
    /// 被 `take` 取走或已标记为垃圾的条目会保留已有时间戳，直到条目被彻底移除。
    /// 时间来自系统时钟，可能受到系统时间校准影响，不保证严格单调递增。
    pub fn get_last_active_time(&self, k: &K) -> Option<u64> {
        self.map.get(k).and_then(|item| item.last_active_time)
    }
    /// 获取指定键对应值的共享引用。
    ///
    /// 该操作不会提升频次、改变 LRU 顺序或更新命中/未命中指标。已标记为垃圾的
    /// 条目仍可通过本方法读取；被 [`take`](Self::take) 取走的条目返回 `None`。
    pub fn get(&self, k: &K) -> Option<&V> {
        if let Some(r) = self.map.get(k) {
            if !r.key.is_null() {
                return unsafe { Some(&(self.lfu.slot.get_unchecked(r.key.clone()).el.1)) };
            }
        }
        None
    }
    /// 获取指定键对应值的可变引用，但不激活条目。
    ///
    /// 该操作不会提升频次、改变 LRU 顺序或更新命中/未命中指标。如果修改会改变
    /// [`Data::size`] 的结果，调用方还必须使用 [`adjust_size`](Self::adjust_size)
    /// 修正缓存大小。需要记录一次访问时应改用 [`active_mut`](Self::active_mut)。
    pub fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        if let Some(r) = self.map.get(k) {
            if !r.key.is_null() {
                return unsafe { Some(&mut self.lfu.slot.get_unchecked_mut(r.key.clone()).el.1) };
            }
        }
        None
    }
    /// 调整缓存记录的总大小。
    ///
    /// 当调用方通过可变引用改变了条目的实际大小时，使用新旧大小的差值更新统计：
    /// 正数增加总大小，负数减少总大小。
    ///
    /// 调用方必须保证减少量不超过当前记录的大小，否则后续 [`size`](Self::size)
    /// 的无符号减法可能下溢。
    pub fn adjust_size(&mut self, size: isize) {
        if size > 0 {
            self.lfu.metrics.size_incr += size as u64;
        } else {
            self.lfu.metrics.size_decr += -size as u64;
        }
    }
    /// 暂时取走指定键的值，同时保留其频次信息。
    ///
    /// 正常条目被取走后进入 [`FrequencyState::TakenAway`] 状态，并从缓存数量和
    /// 大小中扣除。调用方应保证之后调用 [`put`](Self::put) 归还同一个键；若不再
    /// 需要该键，应调用 [`remove`](Self::remove) 清除残留的频次记录。已经标记为
    /// 垃圾的条目被取走时会直接从缓存删除。
    ///
    /// 正常条目被取走会增加命中计数，不存在的键会增加未命中计数；已经被取走的键
    /// 返回 `None`。
    pub fn take(&mut self, k: &K) -> Option<V> {
        match self.map.entry(k.clone()) {
            hash_map::Entry::Occupied(mut e) => {
                let r = e.get_mut();
                if r.key.is_null() {
                    return None;
                }
                return if r.frequency_down_count > 0 {
                    let key = replace(&mut r.key, DefaultKey::null());
                    // 获得当前该键所在的频率段
                    let i = r.get(self.lfu.frequency_down_count);
                    self.lfu.metrics.hit += 1;
                    self.lfu.delete(i, key)
                } else {
                    let r = e.remove();
                    // 垃圾回收状态，从slot中拿走
                    unsafe { Some(self.lfu.slot.remove(r.key).unwrap_unchecked().el.1) }
                };
            }
            hash_map::Entry::Vacant(_) => {
                self.lfu.metrics.miss += 1;
                None
            }
        }
    }
    /// 插入、替换或归还一个条目。
    ///
    /// 新键以频次 `0` 插入；已有键会在当前有效频次上增加 `1`，最高为 `15`。
    /// 本方法可能推进并触发全局降频，但不会依据容量自动清理条目。
    /// 无论是插入、替换还是归还条目，都会把最后激活时间更新为当前 Unix 毫秒
    /// 时间戳，可通过 [`get_last_active_time`](Self::get_last_active_time) 查询。
    ///
    /// 如果键对应的值仍在缓存中，返回被替换的旧值；插入新键或归还一个由
    /// [`take`](Self::take) 取走的键时返回 `None`。
    pub fn put(&mut self, k: K, v: V) -> Option<V> {
        // 先频降
        self.lfu.frequency_down();
        let active_time = unix_time_millis();
        match self.map.entry(k.clone()) {
            hash_map::Entry::Occupied(mut e) => {
                let r = e.get_mut();
                r.last_active_time = Some(active_time);
                // 获取新旧位置
                let (i, old_i) = r.put(self.lfu.frequency_down_count);
                if !r.key.is_null() {
                    self.lfu.metrics.replace += 1;
                    // 插入新数据
                    let key = self.lfu.insert(i, k, v);
                    // 记录新的key，及删除旧数据
                    return self.lfu.delete(old_i, replace(&mut r.key, key));
                } else {
                    self.lfu.metrics.put += 1;
                    // 插入新数据，记录新的key
                    r.key = self.lfu.insert(i, k, v);
                    None
                }
            }
            hash_map::Entry::Vacant(e) => {
                // 如果在概率过滤器中命中
                // let frequency = if self.filter.contains(e.key()) {
                //     self.lfu.metrics.insert2 += 1;
                //     1
                // } else {
                self.lfu.metrics.insert1 += 1;
                // if self.filter.is_nearly_full() {
                //     self.filter.clear();
                // }
                // self.filter.insert(&e.key());
                // 0
                // };
                // 插入新数据
                let key = self.lfu.insert(0, k, v);
                e.insert(Item {
                    key,
                    frequency: 0,
                    frequency_down_count: self.lfu.frequency_down_count,
                    last_active_time: Some(active_time),
                });
                None
            }
        }
    }

    /// 使用指定频次插入条目，且不推进全局降频计数。
    ///
    /// 对新键，`frequency` 是条目的初始频次；对已有键，它是增加到当前有效频次
    /// 上的增量。已有键的最终频次会封顶到 `15`。
    ///
    /// # 参数约束
    ///
    /// `frequency` 应位于 `0..=15`。当前实现对新键直接使用该值选择内部队列，
    /// 传入大于 `15` 的值可能因队列下标越界而 panic。
    ///
    /// # 返回值
    ///
    /// 如果键对应的值仍在缓存中，返回被替换的旧值；否则返回 `None`。
    pub fn put_with_frequency(&mut self, k: K, v: V, frequency: u32) -> Option<V> {
        match self.map.entry(k.clone()) {
            hash_map::Entry::Occupied(mut e) => {
                let r = e.get_mut();
                // 获取新旧位置
                let (i, old_i) = r.put_with_frequency(self.lfu.frequency_down_count, frequency);
                if !r.key.is_null() {
                    self.lfu.metrics.replace += 1;
                    // 插入新数据
                    let key = self.lfu.insert(i, k, v);
                    // 记录新的key，及删除旧数据
                    return self.lfu.delete(old_i, replace(&mut r.key, key));
                } else {
                    self.lfu.metrics.put += 1;
                    // 插入新数据，记录新的key
                    r.key = self.lfu.insert(i, k, v);
                    None
                }
            }
            hash_map::Entry::Vacant(e) => {
                self.lfu.metrics.insert1 += 1;
                // 计算指定频次插入位置
                let i = (u32::BITS - frequency.leading_zeros()) as usize;
                // 插入新数据
                let key = self.lfu.insert(i, k, v);
                e.insert(Item {
                    key,
                    frequency,
                    frequency_down_count: self.lfu.frequency_down_count,
                    last_active_time: None,
                });
                None
            }
        }
    }

    /// 激活指定条目并获取其可变引用。
    ///
    /// 正常条目的频次增加 `1` 并移动到对应队列尾部；已标记为垃圾的条目会以
    /// 频次 `1` 重新加入缓存。正常或垃圾状态的条目会增加命中指标，不存在的键会
    /// 增加未命中指标，并可能触发全局降频。被 [`take`](Self::take) 取走的条目
    /// 不能通过本方法激活，也不会增加未命中指标。
    /// 成功激活时，最后激活时间会更新为当前 Unix 毫秒时间戳；返回 `None` 时不会
    /// 更新时间。时间可通过 [`get_last_active_time`](Self::get_last_active_time) 查询。
    ///
    /// 如果修改导致 [`Data::size`] 的返回值发生变化，调用方还必须使用
    /// [`adjust_size`](Self::adjust_size) 修正缓存记录的大小。
    pub fn active_mut(&mut self, k: &K) -> Option<&mut V> {
        if let Some(r) = self.map.get_mut(k) {
            if r.key.is_null() {
                return None;
            }
            r.last_active_time = Some(unix_time_millis());
            // 先频降
            self.lfu.frequency_down();
            self.lfu.metrics.hit += 1;
            self.lfu.metrics.put += 1;
            return if r.frequency_down_count > 0 {
                // 获取新旧位置
                let (i, old_i) = r.put(self.lfu.frequency_down_count);
                let (prev, next) = unsafe {
                    let n = self.lfu.slot.get_unchecked(r.key);
                    (n.prev(), n.next())
                };
                // 从旧队列中删除
                self.lfu.arr[old_i].repair(prev, next, &mut self.lfu.slot);
                // 添加进新队列的尾部
                self.lfu.arr[i].push_key_back(r.key, &mut self.lfu.slot);
                unsafe { Some(&mut (self.lfu.slot.get_unchecked_mut(r.key).el.1)) }
            } else {
                r.frequency_down_count = self.lfu.frequency_down_count;
                r.frequency = 1;
                // 垃圾回收状态，重新添加进队列1的尾部
                self.lfu.arr[1].push_key_back(r.key, &mut self.lfu.slot);
                let v = unsafe { &mut (self.lfu.slot.get_unchecked_mut(r.key).el.1) };
                self.lfu.metrics.len_incr += 1;
                self.lfu.metrics.size_incr += v.size() as u64;
                Some(v)
            };
        }
        self.lfu.metrics.miss += 1;
        None
    }
    /// 从缓存和频次表中彻底移除指定键。
    ///
    /// 正常条目或已标记为垃圾的条目会返回其值；被 [`take`](Self::take) 取走的
    /// 条目只有频次记录可删除，因此返回 `None`。不存在的键同样返回 `None`。
    pub fn remove(&mut self, k: &K) -> Option<V> {
        if let Some(r) = self.map.remove(k) {
            // 已经被拿走，则只移除频率
            if r.key.is_null() {
                return None;
            }
            self.lfu.metrics.remove += 1;
            return if r.frequency_down_count > 0 {
                // 获得当前该键所在的频率段
                let i = r.get(self.lfu.frequency_down_count);
                self.lfu.delete(i, r.key)
            } else {
                // 垃圾回收状态，从slot中拿走
                unsafe { Some(self.lfu.slot.remove(r.key).unwrap_unchecked().el.1) }
            };
        }
        None
    }
    /// 将指定条目标记为垃圾并返回其共享引用。
    ///
    /// 标记会把条目移出频次队列，但值和键索引仍被保留，其状态变为
    /// [`FrequencyState::Garbaged`]。之后可以调用 [`collect`](Self::collect) 真正
    /// 移除，也可以调用 [`active_mut`](Self::active_mut) 重新加入频次队列。
    ///
    /// 已被取走、已经标记为垃圾或不存在的条目返回 `None`。
    pub fn garbage(&mut self, k: &K) -> Option<&V> {
        if let Some(r) = self.map.get_mut(&k) {
            // 已经被拿走，或垃圾回收状态，不可标记
            if r.key.is_null() || r.frequency_down_count == 0 {
                return None;
            }
            let i = r.get(self.lfu.frequency_down_count);
            r.frequency_down_count = 0;
            let (prev, next) = unsafe {
                let n = self.lfu.slot.get_unchecked(r.key);
                (n.prev(), n.next())
            };
            // 从队列中删除
            self.lfu.arr[i].repair(prev, next, &mut self.lfu.slot);
            self.lfu.metrics.garbage += 1;
            let r = unsafe { &(self.lfu.slot.get_unchecked(r.key).el) };
            self.lfu.metrics.len_incr += 1;
            self.lfu.metrics.size_decr += r.1.size() as u64;
            return Some(&r.1);
        }
        None
    }
    /// 真正移除一个已标记为垃圾的条目。
    ///
    /// 只有状态为 [`FrequencyState::Garbaged`] 的条目会被移除并返回值；正常条目、
    /// 已被取走的条目和不存在的键均返回 `None`。每次调用都会增加
    /// [`Metrics::collect`]，无论是否成功移除条目。
    pub fn collect(&mut self, k: K) -> Option<V> {
        self.lfu.metrics.collect += 1;
        match self.map.entry(k) {
            hash_map::Entry::Occupied(e) => {
                if e.get().frequency_down_count == 0 {
                    let r = e.remove();
                    unsafe { Some(self.lfu.slot.remove(r.key).unwrap_unchecked().el.1) }
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    /// 返回键索引中保存的记录总数。
    ///
    /// 该数量包括正常条目、被 [`take`](Self::take) 取走后保留的频次记录，以及
    /// 已标记为垃圾但尚未 [`collect`](Self::collect) 的条目，因此可能大于
    /// [`len`](Self::len)。
    pub fn frequency_len(&self) -> usize {
        self.map.len()
    }
    /// 返回统计指标记录的当前缓存条目数。
    ///
    /// 结果由 [`Metrics::len_incr`] 减去 [`Metrics::len_decr`] 得到，并不等同于
    /// 键索引记录数；需要包含中间状态记录的数量时使用 [`frequency_len`](Self::frequency_len)。
    pub fn len(&self) -> usize {
        self.lfu.metrics.len_incr - self.lfu.metrics.len_decr
    }
    /// 返回统计指标记录的当前缓存总大小。
    ///
    /// 大小由各条目的 [`Data::size`] 以及 [`adjust_size`](Self::adjust_size) 的手动
    /// 调整累计得到，单位由 `Data` 实现自行约定，通常使用字节。
    pub fn size(&self) -> usize {
        (self.lfu.metrics.size_incr - self.lfu.metrics.size_decr) as usize
    }
    /// 返回当前累计指标的快照。
    ///
    /// 指标不会因读取而清零；返回的是 [`Metrics`] 的克隆值。
    pub fn metrics(&self) -> Metrics {
        self.lfu.metrics.clone()
    }
    /// 按淘汰优先级遍历当前位于频次队列中的条目。
    ///
    /// 遍历顺序为频次从低到高，同一频次段内从最早进入队列到最晚进入队列。
    /// 被取走或已标记为垃圾的条目不在频次队列中，因此不会被返回。
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            cache: self,
            iter: self.lfu.arr[0].iter(&self.lfu.slot),
            index: 1,
        }
    }
    /// 创建一个超时引用清理迭代器。
    ///
    /// 当缓存大小大于 `capacity` 时，迭代器按淘汰优先级检查各频次段的队首；若
    /// 队首条目的 [`Data::timeout`] 严格小于 `now`，则将其标记为垃圾并返回引用。
    /// 清理在大小降至 `capacity` 或没有更多符合条件的队首条目时结束。
    ///
    /// 返回的条目仍占用槽位，调用方需要在引用不再使用后通过
    /// [`collect`](Self::collect) 真正移除，或通过 [`active_mut`](Self::active_mut)
    /// 重新激活。
    pub fn timeout_ref_collect(&mut self, capacity: usize, now: u64) -> TimeoutRefIter<'_, K, V> {
        TimeoutRefIter {
            cache: self as *mut Self,
            index: 0,
            capacity,
            now,
            _p: PhantomData,
        }
    }
    /// 创建一个容量引用清理迭代器。
    ///
    /// 迭代器按低频优先、同频段先进先出的顺序，把条目标记为垃圾并返回引用，直到
    /// 缓存记录的大小不大于 `capacity`。返回的值尚未从槽位释放，之后需要调用
    /// [`collect`](Self::collect) 真正移除，或调用 [`active_mut`](Self::active_mut)
    /// 重新激活。
    pub fn capacity_ref_collect(&mut self, capacity: usize) -> CapacityRefIter<'_, K, V> {
        CapacityRefIter {
            cache: self as *mut Self,
            index: 0,
            capacity,
            _p: PhantomData,
        }
    }
    /// 创建一个会取得条目所有权的超时清理迭代器。
    ///
    /// 当缓存大小大于 `capacity` 时，迭代器按淘汰优先级检查各频次段的队首；若
    /// 队首条目的 [`Data::timeout`] 严格小于 `now`，则彻底移除并返回该条目。
    /// `capacity` 是停止清理的大小下限，不是条目数量。
    pub fn timeout_collect(&mut self, capacity: usize, now: u64) -> TimeoutIter<'_, K, V> {
        TimeoutIter {
            cache: self,
            index: 0,
            capacity,
            now,
        }
    }
    /// 创建一个会取得条目所有权的容量清理迭代器。
    ///
    /// 迭代器按低频优先、同频段先进先出的顺序彻底移除并返回条目，直到缓存记录的
    /// 大小不大于 `capacity`。如果单个条目就大于目标容量，它最终也会被淘汰。
    pub fn capacity_collect(&mut self, capacity: usize) -> CapacityIter<'_, K, V> {
        CapacityIter {
            cache: self,
            index: 0,
            capacity,
        }
    }

    /// 收集当前所有有效条目的核心元数据。
    ///
    /// 结果遵循 [`iter`](Self::iter) 的顺序，仅包含仍位于频次队列中的条目，不含
    /// 被取走或已标记为垃圾的条目。该方法会克隆每个键，可用于在外部序列化缓存
    /// 状态并结合 [`put_with_frequency`](Self::put_with_frequency) 重建条目。
    pub fn items_metas(&self) -> Vec<ItemMeta<K>> {
        let mut metas: Vec<ItemMeta<K>> = Vec::new();
        for r in self.iter() {
            match self.get_frequency(&r.0) {
                FrequencyState::Frequency(f) => metas.push(ItemMeta {
                    key: r.0.clone(),
                    frequency: f,
                    size: r.1.size(),
                    timeout: r.1.timeout(),
                }),
                _ => (),
            }
        }
        metas
    }
}

/// 用于序列化或重建缓存条目的核心元数据。
///
/// 元数据不包含值本身。调用方可以单独序列化值，并使用
/// [`Cache::put_with_frequency`] 恢复条目的初始频次。
#[derive(Debug)]
pub struct ItemMeta<K> {
    /// 条目的键。
    pub key: K,
    /// 已考虑全局降频后的当前有效频次，范围为 `0..=15`。
    pub frequency: u8,
    /// 由 [`Data::size`] 报告的条目大小。
    pub size: usize,
    /// 由 [`Data::timeout`] 报告的超时时间值。
    pub timeout: u64,
}

/// 缓存值需要提供的大小和超时信息。
///
/// 缓存不会校验这两个值的单位或单调性。实现方应保证同一个缓存中的所有值使用
/// 一致的大小单位和时间基准。
pub trait Data {
    /// 返回该值计入缓存容量的大小。
    ///
    /// 默认返回 `1`，此时容量清理等价于按条目数量控制容量。若返回字节数，
    /// [`Cache::size`] 和各清理接口的 `capacity` 参数也都以字节为单位。
    ///
    /// 值在缓存期间大小发生变化时，调用方应使用 [`Cache::adjust_size`] 同步差值。
    fn size(&self) -> usize {
        1
    }
    /// 返回该值的超时时间。
    ///
    /// 默认返回 `0`。超时清理接口使用 `timeout() < now` 作为过期条件，因此调用方
    /// 必须为 `timeout` 和 `now` 使用相同的时间基准；在当前实现中，当 `now > 0`
    /// 时，默认值 `0` 会被视为已经过期。
    fn timeout(&self) -> u64 {
        0
    }
}

/// 将过期条目标记为垃圾并借用返回的惰性迭代器。
///
/// 仅创建迭代器不会执行清理，调用方必须消费迭代器。每次迭代可能修改缓存的队列
/// 和统计信息，但值仍保存在缓存槽位中。通常通过
/// [`Cache::timeout_ref_collect`] 创建。
pub struct TimeoutRefIter<'a, K: Eq + Hash + Clone, V: Data> {
    cache: *mut Cache<K, V>,
    index: usize,
    capacity: usize,
    now: u64,
    _p: PhantomData<&'a Cache<K, V>>,
}
impl<'a, K: Eq + Hash + Clone, V: Data> Iterator for TimeoutRefIter<'a, K, V> {
    type Item = &'a (K, V);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let cache = unsafe { &mut *self.cache };
        if cache.size() <= self.capacity {
            return None;
        }
        while self.index < cache.lfu.arr.len() {
            if let Some(r) = cache.lfu.slot.get(cache.lfu.arr[self.index].head()) {
                if r.el.1.timeout() < self.now {
                    let item = cache.map.get_mut(&r.el.0).unwrap();
                    item.frequency_down_count = 0;
                    cache.lfu.metrics.timeout += 1;
                    cache.lfu.metrics.len_decr += 1;
                    cache.lfu.metrics.size_decr += r.el.1.size() as u64;
                    let k = cache.lfu.pop_key(self.index).unwrap();
                    return Some(unsafe { &(cache.lfu.slot.get_unchecked(k).el) });
                }
            }
            self.index += 1;
        }
        None
    }
}

/// 将超出目标容量的条目标记为垃圾并借用返回的惰性迭代器。
///
/// 仅创建迭代器不会执行清理，调用方必须消费迭代器。值在后续调用
/// [`Cache::collect`] 前仍保存在缓存槽位中。通常通过
/// [`Cache::capacity_ref_collect`] 创建。
pub struct CapacityRefIter<'a, K: Eq + Hash + Clone, V: Data> {
    cache: *mut Cache<K, V>,
    index: usize,
    capacity: usize,
    _p: PhantomData<&'a Cache<K, V>>,
}
impl<'a, K: Eq + Hash + Clone, V: Data> Iterator for CapacityRefIter<'a, K, V> {
    type Item = &'a (K, V);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let cache = unsafe { &mut *self.cache };
        if cache.size() <= self.capacity {
            return None;
        }
        while self.index < cache.lfu.arr.len() {
            if let Some(k) = cache.lfu.pop_key(self.index) {
                let r = unsafe { &(cache.lfu.slot.get_unchecked(k).el) };
                let item = cache.map.get_mut(&r.0).unwrap();
                item.frequency_down_count = 0;
                cache.lfu.metrics.evict += 1;
                cache.lfu.metrics.len_decr += 1;
                cache.lfu.metrics.size_decr += r.1.size() as u64;
                return Some(r);
            }
            self.index += 1;
        }
        None
    }
}

/// 彻底移除并返回过期条目所有权的惰性迭代器。
///
/// 仅创建迭代器不会执行清理，调用方必须消费迭代器。通常通过
/// [`Cache::timeout_collect`] 创建。
pub struct TimeoutIter<'a, K: Eq + Hash + Clone, V: Data> {
    cache: &'a mut Cache<K, V>,
    index: usize,
    capacity: usize,
    now: u64,
}
impl<'a, K: Eq + Hash + Clone, V: Data> Iterator for TimeoutIter<'a, K, V> {
    type Item = (K, V);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.cache.size() <= self.capacity {
            return None;
        }
        while self.index < self.cache.lfu.arr.len() {
            if let Some(r) = self
                .cache
                .lfu
                .slot
                .get(self.cache.lfu.arr[self.index].head())
            {
                if r.el.1.timeout() < self.now {
                    self.cache.map.remove(&r.el.0);
                    self.cache.lfu.metrics.timeout += 1;
                    self.cache.lfu.metrics.len_decr += 1;
                    self.cache.lfu.metrics.size_decr += r.el.1.size() as u64;
                    return self.cache.lfu.pop(self.index);
                }
            }
            self.index += 1;
        }
        None
    }
}

/// 彻底移除并返回超出目标容量条目所有权的惰性迭代器。
///
/// 仅创建迭代器不会执行清理，调用方必须消费迭代器。通常通过
/// [`Cache::capacity_collect`] 创建。
pub struct CapacityIter<'a, K: Eq + Hash + Clone, V: Data> {
    cache: &'a mut Cache<K, V>,
    index: usize,
    capacity: usize,
}
impl<'a, K: Eq + Hash + Clone, V: Data> Iterator for CapacityIter<'a, K, V> {
    type Item = (K, V);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.cache.size() <= self.capacity {
            return None;
        }
        while self.index < self.cache.lfu.arr.len() {
            if let Some(r) = self.cache.lfu.pop(self.index) {
                self.cache.map.remove(&r.0);
                self.cache.lfu.metrics.evict += 1;
                self.cache.lfu.metrics.len_decr += 1;
                self.cache.lfu.metrics.size_decr += r.1.size() as u64;
                return Some(r);
            }
            self.index += 1;
        }
        None
    }
}
/// 按淘汰优先级借用缓存条目的迭代器。
///
/// 该迭代器只访问频次队列，不返回被取走或已标记为垃圾的条目。通常通过
/// [`Cache::iter`] 创建。
pub struct Iter<'a, K: Eq + Hash + Clone, V: Data> {
    cache: &'a Cache<K, V>,
    iter: SlotIter<'a, DefaultKey, (K, V)>,
    index: usize,
}
impl<'a, K: Eq + Hash + Clone, V: Data> Iterator for Iter<'a, K, V> {
    type Item = &'a (K, V);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(r) = self.iter.next() {
                return Some(r);
            }
            if self.index >= self.cache.lfu.arr.len() {
                return None;
            }
            self.iter = self.cache.lfu.arr[self.index].iter(&self.cache.lfu.slot);
            self.index += 1;
        }
    }
}
/// 键在频次表中的当前状态。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrequencyState {
    /// 键不存在，且没有保留频次信息。
    None,
    /// 值已通过 [`Cache::take`] 取走，但键和频次信息仍被保留。
    TakenAway,
    /// 条目已移出频次队列，但值仍等待 [`Cache::collect`] 清理。
    Garbaged,
    /// 正常条目的当前有效频次，范围为 `0..=15`。
    Frequency(u8),
}
/// 缓存生命周期内累计的操作和容量指标。
///
/// [`Cache::metrics`] 返回该结构的快照。计数器不会自动清零；其中 `len_*` 和
/// `size_*` 是增减累计值，[`Cache::len`] 与 [`Cache::size`] 通过两者相减得到
/// 当前统计值。
#[derive(Clone, Default, Debug)]
pub struct Metrics {
    /// 用于计算当前条目数的累计增加量。
    pub len_incr: usize,
    /// 用于计算当前条目数的累计减少量。
    pub len_decr: usize,
    /// 插入值及手动调整产生的累计大小增加量。
    pub size_incr: u64,
    /// 移除值及手动调整产生的累计大小减少量。
    pub size_decr: u64,
    /// `take` 或 `active_mut` 成功命中的累计次数。
    pub hit: usize,
    /// `take` 或 `active_mut` 未命中的累计次数。
    pub miss: usize,
    /// 新键首次插入的累计次数。
    pub insert1: usize,
    /// CuckooFilter 二次命中插入次数；当前实现未启用过滤器，通常保持为 `0`。
    pub insert2: usize,
    /// 已存在值被新值替换的累计次数。
    pub replace: usize,
    /// 被取走条目的归还及条目激活累计次数。
    pub put: usize,
    /// 通过 [`Cache::remove`] 移除有值条目的累计次数。
    pub remove: usize,
    /// 通过 [`Cache::garbage`] 主动标记垃圾的累计次数。
    pub garbage: usize,
    /// 调用 [`Cache::collect`] 的累计次数，包括未移除条目的调用。
    pub collect: usize,
    /// 两种超时清理迭代器产出的累计条目数。
    pub timeout: usize,
    /// 两种容量清理迭代器产出的累计条目数。
    pub evict: usize,
}

/// 频率表
struct Lfu<K: Eq + Hash + Clone, V: Data> {
    /// 不同数据访问频次的LRU，频次为0,1,2-3,4-7,8-15
    arr: [Deque<DefaultKey>; 5],
    /// SlotMap
    slot: Slot<DefaultKey, (K, V)>,
    /// 统计数据
    metrics: Metrics,
    /// 频降率：放入次数/总数量，默认为8
    frequency_down_rate: usize,
    /// 频降次数
    frequency_down_count: u32,
    /// 这个频降周期的放入次数，
    put_count: usize,
}

impl<K: Eq + Hash + Clone, V: Data> Lfu<K, V> {
    pub fn new(frequency_down_rate: usize) -> Self {
        Self {
            arr: Default::default(),
            slot: Default::default(),
            frequency_down_rate,
            metrics: Default::default(),
            frequency_down_count: 1,
            put_count: 0,
        }
    }
    /// 删除数据
    fn delete(&mut self, i: usize, k: DefaultKey) -> Option<V> {
        let v = unsafe { self.arr[i].remove(k, &mut self.slot).unwrap_unchecked().1 };
        self.metrics.len_decr += 1;
        self.metrics.size_decr += v.size() as u64;
        Some(v)
    }
    /// 插入数据
    fn insert(&mut self, i: usize, k: K, v: V) -> DefaultKey {
        self.metrics.len_incr += 1;
        self.metrics.size_incr += v.size() as u64;
        self.arr[i].push_back((k, v), &mut self.slot)
    }
    /// 频降
    fn frequency_down(&mut self) {
        if self.put_count <= (self.slot.len() + 1) * self.frequency_down_rate {
            self.put_count += 1;
            return;
        }
        // 如果放入次数达到上限，进行频降， 增加频降次数，并清空放入次数
        self.frequency_down_count += 1;
        self.put_count = 0;
        // 先将1合并到0
        let d = replace(&mut self.arr[1], Default::default());
        self.arr[0].merge_back(d, &mut self.slot);
        // 调换位置
        self.arr[1..5].rotate_left(1);
    }
    /// 弹出
    fn pop(&mut self, i: usize) -> Option<(K, V)> {
        self.arr[i].pop_front(&mut self.slot)
    }
    /// 弹出key
    fn pop_key(&mut self, i: usize) -> Option<DefaultKey> {
        self.arr[i].pop_key_front(&mut self.slot)
    }
}
/// 数据条目
struct Item {
    /// 数据的slot键
    key: DefaultKey,
    /// 数据的频次
    frequency: u32,
    /// 当前频次所在的频降周期数，为0表示在垃圾回收状态
    frequency_down_count: u32,
    /// 最近一次由 `put` 或 `active_mut` 更新的 Unix 毫秒时间戳。
    last_active_time: Option<u64>,
}
impl Item {
    #[inline]
    fn shr(&self, frequency_down_count: u32) -> u32 {
        let count = frequency_down_count - self.frequency_down_count;
        if count < 4 {
            self.frequency >> count
        } else {
            0
        }
    }

    /// 获得频次所在的位置
    #[inline]
    fn get(&self, frequency_down_count: u32) -> usize {
        let i = self.shr(frequency_down_count);
        (u32::BITS - i.leading_zeros()) as usize
    }
    /// 增加频次，设置当前频降数，并获得新旧频次所在的位置
    #[inline]
    fn put(&mut self, frequency_down_count: u32) -> (usize, usize) {
        let old = if frequency_down_count > self.frequency_down_count {
            let old = self.shr(frequency_down_count);
            self.frequency_down_count = frequency_down_count;
            old
        } else {
            self.frequency
        };
        if old >= FREQUENCY_MAX {
            self.frequency = FREQUENCY_MAX;
        } else {
            self.frequency = old + 1;
        }
        (
            (u32::BITS - self.frequency.leading_zeros()) as usize,
            (u32::BITS - old.leading_zeros()) as usize,
        )
    }

    /// 增加频次，设置当前频降数，并获得新旧频次所在的位置
    #[inline]
    fn put_with_frequency(&mut self, frequency_down_count: u32, frequency: u32) -> (usize, usize) {
        let old = if frequency_down_count > self.frequency_down_count {
            let old = self.shr(frequency_down_count);
            self.frequency_down_count = frequency_down_count;
            old
        } else {
            self.frequency
        };

        if old >= FREQUENCY_MAX {
            self.frequency = FREQUENCY_MAX;
        } else {
            self.frequency = old + frequency;
            if self.frequency >= FREQUENCY_MAX {
                self.frequency = FREQUENCY_MAX;
            }
        }
        (
            (u32::BITS - self.frequency.leading_zeros()) as usize,
            (u32::BITS - old.leading_zeros()) as usize,
        )
    }
}

#[cfg(test)]
mod test_mod {

    extern crate pcg_rand;
    extern crate rand_core;

    use std::time::{SystemTime, UNIX_EPOCH};

    use self::rand_core::{RngCore, SeedableRng};
    use crate::*;

    #[derive(Debug, Eq, PartialEq)]
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

    #[test]
    fn last_active_time_is_updated_only_by_put_and_active_mut() {
        let mut cache: Cache<usize, R1> = Default::default();

        assert_eq!(cache.get_last_active_time(&1), None);

        cache.put_with_frequency(1, R1(1, 1, 1), 3);
        assert_eq!(cache.get_last_active_time(&1), None);

        cache.map.get_mut(&1).unwrap().last_active_time = Some(1);
        cache.put_with_frequency(1, R1(1, 1, 2), 1);
        assert_eq!(cache.get_last_active_time(&1), Some(1));

        assert!(cache.get(&1).is_some());
        assert_eq!(cache.get_last_active_time(&1), Some(1));
        assert!(cache.get_mut(&1).is_some());
        assert_eq!(cache.get_last_active_time(&1), Some(1));

        assert!(cache.active_mut(&1).is_some());
        assert!(cache.get_last_active_time(&1).unwrap() > 1);

        cache.map.get_mut(&1).unwrap().last_active_time = Some(1);
        cache.put(1, R1(1, 1, 3));
        assert!(cache.get_last_active_time(&1).unwrap() > 1);

        cache.map.get_mut(&1).unwrap().last_active_time = Some(1);
        assert!(cache.take(&1).is_some());
        assert_eq!(cache.get_last_active_time(&1), Some(1));

        cache.put(2, R1(2, 1, 1));
        assert!(cache.get_last_active_time(&2).unwrap() > 1);
    }

    #[test]
    pub fn test() {
        let mut cache: Cache<usize, R1> = Default::default();
        let mut time: u64 = 0;
        let mut f = || {
            time += 1;
            time
        };

        cache.put(1, R1(1, 1000, f()));
        cache.put(2, R1(2, 2000, f()));
        cache.put(3, R1(3, 3000, f()));
        cache.put(4, R1(4, 3000, f()));
        // for r in cache.capacity_collect(7000) {
        //     println!("result = {},r1 = {}", r.0, r.1 .0);
        // }

        assert(&cache, vec![1, 2, 3, 4]);
        assert_eq!(cache.get(&1), Some(&R1(1, 1000, 1)));
        assert_eq!(cache.get(&2), Some(&R1(2, 2000, 2)));
        assert_eq!(cache.get_frequency(&3), FrequencyState::Frequency(0));
        assert_eq!(cache.get_frequency(&5), FrequencyState::None);
        cache.take(&3);
        assert(&cache, vec![1, 2, 4]);
        assert_eq!(cache.get_frequency(&3), FrequencyState::TakenAway);
        cache.put(3, R1(3, 3000, f()));
        assert(&cache, vec![1, 2, 4, 3]);
        assert_eq!(cache.get_frequency(&3), FrequencyState::Frequency(1));
        {
            let mut r = cache.active_mut(&1);
            r.as_mut().unwrap().2 = f();
            assert_eq!(r.unwrap(), &R1(1, 1000, 6));
        };
        assert(&cache, vec![2, 4, 3, 1]);
        cache.put(3, R1(3, 3100, f()));
        assert(&cache, vec![2, 4, 1, 3]);
        let mut r = cache.active_mut(&4);
        r.as_mut().unwrap().2 = f();
        assert(&cache, vec![2, 1, 4, 3]);
        assert_eq!(cache.get_frequency(&2), FrequencyState::Frequency(0));
        assert_eq!(cache.get_frequency(&1), FrequencyState::Frequency(1));
        assert_eq!(cache.get_frequency(&4), FrequencyState::Frequency(1));
        assert_eq!(cache.get_frequency(&3), FrequencyState::Frequency(2));
        // 测试移除后，在过滤器命中的情况下，数据频次应为1
        cache.remove(&2);
        assert_eq!(cache.get_frequency(&2), FrequencyState::None);
        cache.put(2, R1(2, 2100, f()));
        assert_eq!(cache.get_frequency(&2), FrequencyState::Frequency(0));
        assert(&cache, vec![2, 1, 4, 3]);
        // 测试最大频次为15
        for i in 1..32 {
            let mut r = cache.active_mut(&2);
            r.as_mut().unwrap().2 = f();
            assert_eq!(
                cache.get_frequency(&2),
                FrequencyState::Frequency(if i > 15 { 15 } else { i })
            );
        }
        assert(&cache, vec![1, 4, 3, 2]);
        assert_eq!(cache.get_frequency(&1), FrequencyState::Frequency(1));
        assert_eq!(cache.get_frequency(&2), FrequencyState::Frequency(15));
        assert_eq!(cache.get_frequency(&3), FrequencyState::Frequency(2));
        assert_eq!(cache.get_frequency(&4), FrequencyState::Frequency(1));
        cache.put(5, R1(5, 5000, f()));
        println!("---------, 1:{:?}", cache.get(&1));
        println!("---------, 2:{:?}", cache.get(&2));
        println!("---------, 3:{:?}", cache.get(&3));
        println!("---------, 4:{:?}", cache.get(&4));
        println!("---------, 5:{:?}", cache.get(&5));
        assert_eq!(cache.get_frequency(&5), FrequencyState::Frequency(0));
        assert_eq!(cache.get_frequency(&1), FrequencyState::Frequency(1));

        // 测试频降后的数据正确性
        assert_eq!(cache.take(&2).unwrap().0, 2);
        cache.put(2, R1(2, 2200, f()));
        println!("2---------");
        assert_eq!(cache.get_frequency(&1), FrequencyState::Frequency(0));
        assert_eq!(cache.get_frequency(&2), FrequencyState::Frequency(8));
        assert_eq!(cache.get_frequency(&3), FrequencyState::Frequency(1));
        assert_eq!(cache.get_frequency(&4), FrequencyState::Frequency(0));
        assert_eq!(cache.get_frequency(&5), FrequencyState::Frequency(0));
        assert(&cache, vec![5, 1, 4, 3, 2]);
        println!("cache size:{}, len:{}", cache.size(), cache.len(),);
        for i in cache.timeout_ref_collect(0, 8) {
            println!("timeout_ref_collect, {}", i.0);
        }
        for i in cache.capacity_ref_collect(9000) {
            println!("capacity_ref_collect, {}", i.0);
        }
        assert_eq!(cache.get_frequency(&5), FrequencyState::Garbaged);
        let mut r = cache.active_mut(&5);
        r.as_mut().unwrap().2 = f();
        assert_eq!(cache.get_frequency(&3), FrequencyState::Garbaged);
        cache.collect(3).unwrap();
        assert(&cache, vec![1, 4, 5, 2]);
        cache.put(3, R1(3, 3330, f()));
        assert(&cache, vec![1, 4, 3, 5, 2]);

        for i in 6..100 {
            cache.put(i, R1(i, i * 1000, f()));
        }
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        println!("---------------seed:{:?}", seed);
        let mut rng = pcg_rand::Pcg32::seed_from_u64(seed);
        let mut vec = vec![];
        let mut kvec = vec![];
        for _ in 1..10000 {
            if cache.len() > 0 {
                let t = (rng.next_u32() % cache.len() as u32) as usize;
                if t % 2 == 0 {
                    let k = key(&cache, t);
                    //println!("---------------t1:{:?}, k:{:?}", t, k);
                    let r = cache.take(&k).unwrap();
                    vec.push(r);
                } else {
                    for r in cache.capacity_ref_collect(0) {
                        //println!("---------------t2:{:?}, k:{:?}", t, r.0);
                        kvec.push(r.0);
                        break;
                    }
                }
                check(&cache);
            }
            let mut i = (rng.next_u32() % (vec.len() + 5) as u32) as usize;
            let mut j = (rng.next_u32() % (vec.len() + 5) as u32) as usize;
            if i > j {
                j = replace(&mut i, j);
            }

            if i % 2 == 0 {
                if i >= vec.len() {
                    continue;
                }
                if j >= vec.len() {
                    j = vec.len();
                }
                //println!("---------------add1, i:{:?}, len:{:?}, vec_len:{:?}", i, j, vec.len());
                for _ in i..j {
                    let mut r = vec.remove(i);
                    r.2 = f();
                    cache.put(r.0, r);
                    check(&cache);
                }
            } else {
                if i >= kvec.len() {
                    continue;
                }
                if j >= kvec.len() {
                    j = kvec.len();
                }
                //println!("---------------add2, i:{:?}, len:{:?}, vec_len:{:?}", i, j, kvec.len());
                for _ in i..j {
                    let k = kvec.remove(i);
                    let mut r = cache.collect(k).unwrap();
                    r.2 = f();
                    cache.put(r.0, r);
                    check(&cache);
                }
            }
        }
    }
    fn key(c: &Cache<usize, R1>, mut index: usize) -> usize {
        for i in c.iter() {
            if index == 0 {
                return i.0;
            }
            index -= 1;
        }
        0
    }
    fn assert(c: &Cache<usize, R1>, vec: Vec<usize>) {
        let mut i = 0;
        println!("assert, vec:{:?}", vec);
        for n in 0..5 {
            for r in c.lfu.arr[n].iter(&c.lfu.slot) {
                assert_eq!(r.0, vec[i]);
                if let FrequencyState::Frequency(x) = c.get_frequency(&r.0) {
                    //println!("assert n:{}, f:{:?}, k:{:?}", n, c.get_frequency(&r.0), r.0);
                    assert_eq!(u32::BITS - (x as u32).leading_zeros(), n as u32);
                } else {
                    //panic!("invalid: n:{}, f:{:?}, k:{:?}", n, c.get_frequency(&r.0), r.0)
                }

                i += 1;
            }
        }
    }
    fn check(c: &Cache<usize, R1>) {
        //println!("------------check");
        for n in 0..5 {
            for r in c.lfu.arr[n].iter(&c.lfu.slot) {
                if let FrequencyState::Frequency(x) = c.get_frequency(&r.0) {
                    //println!("assert n:{}, f:{:?}, k:{:?}", n, c.get_frequency(&r.0), r.0);
                    assert_eq!(u32::BITS - (x as u32).leading_zeros(), n as u32);
                } else {
                    panic!(
                        "invalid: n:{}, f:{:?}, k:{:?}",
                        n,
                        c.get_frequency(&r.0),
                        r.0
                    )
                }
            }
        }
    }

    #[test]
    pub fn test_with_frequency() {
        let mut cache: Cache<usize, R1> = Default::default();
        let mut time: u64 = 0;
        let mut f = || {
            time += 1;
            time
        };

        cache.put_with_frequency(1, R1(1, 1000, f()), 5);
        cache.put(2, R1(2, 2000, f()));
        cache.put(2, R1(2, 2000, f()));
        cache.put(3, R1(3, 3000, f()));
        cache.put(4, R1(4, 3000, f()));
        assert_eq!(cache.get_frequency(&1), FrequencyState::Frequency(5));
        cache.put(1, R1(1, 1000, f()));
        cache.put_with_frequency(1, R1(1, 1000, f()), 3);
        assert_eq!(cache.get_frequency(&1), FrequencyState::Frequency(9));

        let items_metas = cache.items_metas();
        println!("==== items_metas {:?}", items_metas);
    }
}
