//! Cache缓存
//! 基于LFU-LRU
//! 预设数据访问频次最高为15， 然后按照访问频次0,1,2-3,4-7,8-15分成5段，每段为一个LRU，如果数据增加频次，则可能迁移到更高段的LRU上。
//! 为了减少缓存击穿的情况，增加了一个CuckooFilter。
//! 当数据放入时，首先查找频次表，获得原频次，
//!     如果没有该键，则为首次放入，接着查找CuckooFilter，如果在，则频次提升为1，如果不在则记录到过滤器。
//!     将数据放入到指定频次LRU中后，从0频次开始进行LRU淘汰，然后依次向更大频次的LRU进行淘汰。
//!     当过滤器的接近满的时候，清空过滤器，重新开始。默认过滤器条目数量为1024.
//! 频降率: 总放入次数/缓存总条目数，每当频降率达到阈值后，将所有频次减半，8降为4， 4降为2，2降为1， 将1频次LRU的数据放入0频次的LRU中。
//! 为了记录被take拿走的数据频次，并且为了快速找到指定key所在的频次LRU，需要维护一个key的频次表。
//! 支持垃圾标记和引用整理，将数据弹出频率队列，但保留数据本身，用collect方法真正移除。 如果在collect调用前，有其他的take和active_mut，则将数据重新放入频率队列。
//!

use pi_hash::XHashMap;
use pi_slot_deque::{Deque, Iter as SlotIter, Slot};
use probabilistic_collections::cuckoo::CuckooFilter;
use slotmap::{DefaultKey, Key};
use std::collections::hash_map;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::replace;

/// 最大频次
const FREQUENCY_MAX: u32 = 15;
// 默认CuckooFilter窗口大小
const WINDOW_SIZE: usize = 1024;
// 默认的整理率，总放入次数/缓存总条目数
const FREQUENCY_DOWN_RATE: usize = 8;

/// Cache缓存， 基于LFU-LRU
pub struct Cache<K: Eq + Hash + Clone, V: Data> {
    /// 频率表
    lfu: Lfu<K, V>,
    /// 数据条目
    map: XHashMap<K, Item>,
    /// 首次数据的bloom，判断不在缓存的数据是否被调用过， 一般指定大小，比如1024.
    filter: CuckooFilter<K>,
}

impl<K: Eq + Hash + Clone, V: Data> Default for Cache<K, V> {
    fn default() -> Self {
        Self::with_config(0, WINDOW_SIZE, FREQUENCY_DOWN_RATE)
    }
}
impl<K: Eq + Hash + Clone, V: Data> Cache<K, V> {
    /// 用初始表大小，CuckooFilter窗口大小，整理率创建Cache
    pub fn with_config(
        map_capacity: usize,
        cuckoo_filter_window_size: usize,
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
            filter: CuckooFilter::from_entries_per_index(cuckoo_filter_window_size, 0.01, 8),
        }
    }
    /// 判断是否有指定键的数据
    pub fn contains_key(&self, k: &K) -> bool {
        if let Some(r) = self.map.get(k) {
            return !r.key.is_null();
        }
        false
    }
    /// 获得指定键的频次
    pub fn get_frequency(&self, k: &K) -> FrequencyState {
        if let Some(r) = self.map.get(k) {
            return if r.key.is_null() {
                FrequencyState::TakenAway
            } else if r.frequency_down_count == 0 {
                FrequencyState::Garbaged
            } else {
                FrequencyState::Frequency(
                    (r.frequency >> (self.lfu.frequency_down_count - r.frequency_down_count)) as u8,
                )
            };
        }
        FrequencyState::None
    }
    /// 获得指定键的数据
    pub fn get(&self, k: &K) -> Option<&V> {
        if let Some(r) = self.map.get(k) {
            if !r.key.is_null() {
                return unsafe { Some(&(self.lfu.slot.get_unchecked(r.key.clone()).el.1)) };
            }
        }
        None
    }
    /// GetMut by key
    pub fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        if let Some(r) = self.map.get(k) {
            if !r.key.is_null() {
                return unsafe { Some(&mut self.lfu.slot.get_unchecked_mut(r.key.clone()).el.1) };
            }
        }
        None
    }
    /// 拿走的数据， 如果拿到了数据，就必须保证会调用put还回来
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
    /// 放入的数据，返回Some(V)表示被替换的数据
    pub fn put(&mut self, k: K, v: V) -> Option<V> {
        // 先频降
        self.lfu.frequency_down();
        match self.map.entry(k.clone()) {
            hash_map::Entry::Occupied(mut e) => {
                let r = e.get_mut();
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
                let frequency = if self.filter.contains(e.key()) {
                    self.lfu.metrics.insert2 += 1;
                    1
                } else {
                    self.lfu.metrics.insert1 += 1;
                    if self.filter.is_nearly_full() {
                        self.filter.clear();
                    }
                    self.filter.insert(&e.key());
                    0
                };
                // 插入新数据
                let key = self.lfu.insert(frequency as usize, k, v);
                e.insert(Item {
                    key,
                    frequency,
                    frequency_down_count: self.lfu.frequency_down_count,
                });
                None
            }
        }
    }
    /// 激活并获取可写应用，会增加频次和最后使用时间，等于拿走并立即还回来，但性能更高
    pub fn active_mut(&mut self, k: &K) -> Option<&mut V> {
        if let Some(r) = self.map.get_mut(k) {
            if r.key.is_null() {
                return None;
            }
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
                self.lfu.size += v.size();
                Some(v)
            };
        }
        self.lfu.metrics.miss += 1;
        None
    }
    /// 移走
    pub fn remove(&mut self, k: &K) -> Option<V> {
        if let Some(r) = self.map.remove(k) {
            // 已经被拿走，不可移除
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
    /// 将指定键的数据标记为垃圾回收，从频率队列中移除，但保留数据本身， 返回数据引用
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
            self.lfu.size -= r.1.size();
            return Some(&r.1);
        }
        None
    }
    /// 移走垃圾回收的数据
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
    /// 全部数量，包括被拿走的数据
    pub fn len(&self) -> usize {
        self.map.len()
    }
    /// 当前数量，被缓存的数据数量
    pub fn count(&self) -> usize {
        self.lfu.slot.len()
    }
    /// 当前缓存的数据总大小
    pub fn size(&self) -> usize {
        self.lfu.size
    }
    /// 获得当前的统计
    pub fn metrics(&self) -> Metrics {
        self.lfu.metrics.clone()
    }
    /// 迭代器，按频率由低到高，同频率先进先出的顺序迭代
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            cache: self,
            iter: self.lfu.arr[0].iter(&self.lfu.slot),
            index: 1,
        }
    }
    /// 超时整理方法， 参数为最小容量及毫秒时间，清理最小容量外的超时数据
    pub fn timeout_ref_collect(&mut self, capacity: usize, now: u64) -> TimeoutRefIter<'_, K, V> {
        TimeoutRefIter {
            cache: self as *mut Self,
            index: 0,
            capacity,
            now,
            _p: PhantomData,
        }
    }
    /// 超量整理方法， 参数为容量， 按照频率优先， 同频先进先出的原则，清理超出容量的数据
    pub fn capacity_ref_collect(&mut self, capacity: usize) -> CapacityRefIter<'_, K, V> {
        CapacityRefIter {
            cache: self as *mut Self,
            index: 0,
            capacity,
            _p: PhantomData,
        }
    }
    /// 超时整理方法， 参数为最小容量及毫秒时间，清理最小容量外的超时数据
    pub fn timeout_collect(&mut self, capacity: usize, now: u64) -> TimeoutIter<'_, K, V> {
        TimeoutIter {
            cache: self,
            index: 0,
            capacity,
            now,
        }
    }
    /// 超量整理方法， 参数为容量， 按照频率优先， 同频先进先出的原则，清理超出容量的数据
    pub fn capacity_collect(&mut self, capacity: usize) -> CapacityIter<'_, K, V> {
        CapacityIter {
            cache: self,
            index: 0,
            capacity,
        }
    }
}

/// 数据，放入数据表的数据必须实现该trait
pub trait Data {
    /// 数据的大小
    fn size(&self) -> usize {
        1
    }
    /// 数据的超时时间，毫秒时间
    fn timeout(&self) -> u64 {
        0
    }
}

/// 超时引用迭代器
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
        if cache.lfu.size <= self.capacity {
            return None;
        }
        while self.index < cache.lfu.arr.len() {
            if let Some(r) = cache.lfu.slot.get(cache.lfu.arr[self.index].head()) {
                if r.el.1.timeout() > self.now {
                    if let Some(item) = cache.map.get_mut(&r.el.0) {
                        item.frequency_down_count = 0;
                    }
                    cache.lfu.metrics.timeout += 1;
                    cache.lfu.size -= r.el.1.size();
                    let k = cache.lfu.pop_key(self.index).unwrap();
                    return Some(unsafe { &(cache.lfu.slot.get_unchecked(k).el) });
                }
            }
            self.index += 1;
        }
        None
    }
}

/// 容量引用迭代器
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
        if cache.lfu.size <= self.capacity {
            return None;
        }
        while self.index < cache.lfu.arr.len() {
            if let Some(k) = cache.lfu.pop_key(self.index) {
                let r = unsafe { &(cache.lfu.slot.get_unchecked(k).el) };
                if let Some(item) = cache.map.get_mut(&r.0) {
                    item.frequency_down_count = 0;
                }
                cache.lfu.metrics.evict += 1;
                cache.lfu.size -= r.1.size();
                return Some(r);
            }
            self.index += 1;
        }
        None
    }
}

/// 超时迭代器
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
        if self.cache.lfu.size <= self.capacity {
            return None;
        }
        while self.index < self.cache.lfu.arr.len() {
            if let Some(r) = self
                .cache
                .lfu
                .slot
                .get(self.cache.lfu.arr[self.index].head())
            {
                if r.el.1.timeout() > self.now {
                    self.cache.map.remove(&r.el.0);
                    self.cache.lfu.metrics.timeout += 1;
                    self.cache.lfu.size -= r.el.1.size();
                    return self.cache.lfu.pop(self.index);
                }
            }
            self.index += 1;
        }
        None
    }
}

/// 容量迭代器
pub struct CapacityIter<'a, K: Eq + Hash + Clone, V: Data> {
    cache: &'a mut Cache<K, V>,
    index: usize,
    capacity: usize,
}
impl<'a, K: Eq + Hash + Clone, V: Data> Iterator for CapacityIter<'a, K, V> {
    type Item = (K, V);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.cache.lfu.size <= self.capacity {
            return None;
        }
        while self.index < self.cache.lfu.arr.len() {
            if let Some(r) = self.cache.lfu.pop(self.index) {
                self.cache.map.remove(&r.0);
                self.cache.lfu.metrics.evict += 1;
                self.cache.lfu.size -= r.1.size();
                return Some(r);
            }
            self.index += 1;
        }
        None
    }
}
/// 迭代器
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
/// 数据的频率状态
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrequencyState {
    /// 不存在
    None,
    /// 被拿走
    TakenAway,
    /// 被标记为垃圾
    Garbaged,
    /// 频率
    Frequency(u8),
}
/// 统计数据
#[derive(Clone, Default, Debug)]
pub struct Metrics {
    /// 命中次数
    pub hit: usize,
    /// 未命中次数
    pub miss: usize,
    /// 首次插入的次数
    pub insert1: usize,
    /// 二次插入的次数
    pub insert2: usize,
    /// 替换次数
    pub replace: usize,
    /// 归还次数
    pub put: usize,
    /// 移除次数
    pub remove: usize,
    /// 垃圾标记次数
    pub garbage: usize,
    /// 垃圾清理次数
    pub collect: usize,
    /// 超时清理次数
    pub timeout: usize,
    /// 超过容量的驱除次数
    pub evict: usize,
}

/// 频率表
struct Lfu<K: Eq + Hash + Clone, V: Data> {
    /// 不同数据访问频次的LRU，频次为0,1,2-3,4-7,8-15
    arr: [Deque<DefaultKey>; 5],
    /// SlotMap
    slot: Slot<DefaultKey, (K, V)>,
    /// 数据的总大小
    size: usize,
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
            size: 0,
            frequency_down_rate,
            metrics: Default::default(),
            frequency_down_count: 1,
            put_count: 0,
        }
    }
    /// 删除数据
    fn delete(&mut self, i: usize, k: DefaultKey) -> Option<V> {
        let v = unsafe { self.arr[i].remove(k, &mut self.slot).unwrap_unchecked().1 };
        self.size -= v.size();
        Some(v)
    }
    /// 插入数据
    fn insert(&mut self, i: usize, k: K, v: V) -> DefaultKey {
        self.size += v.size();
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
}
impl Item {
    /// 获得频次所在的位置
    #[inline]
    fn get(&self, frequency_down_count: u32) -> usize {
        let i = self.frequency >> (frequency_down_count - self.frequency_down_count);
        (u32::BITS - i.leading_zeros()) as usize
    }
    /// 增加频次，设置当前频降数，并获得新旧频次所在的位置
    #[inline]
    fn put(&mut self, frequency_down_count: u32) -> (usize, usize) {
        let old = if frequency_down_count > self.frequency_down_count {
            let old = self.frequency >> (frequency_down_count - self.frequency_down_count);
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
}

#[cfg(test)]
mod test_mod {
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
    pub fn test() {
        let mut cache: Cache<usize, R1> = Default::default();
        cache.put(1, R1(1, 1000, 0));
        cache.put(2, R1(2, 2000, 0));
        cache.put(3, R1(3, 3000, 0));
        cache.put(4, R1(4, 3000, 0));
        assert(&cache, vec![1, 2, 3, 4]);
        assert_eq!(cache.get(&1), Some(&R1(1, 1000, 0)));
        assert_eq!(cache.get(&2), Some(&R1(2, 2000, 0)));
        assert_eq!(cache.get_frequency(&3), FrequencyState::Frequency(0));
        assert_eq!(cache.get_frequency(&5), FrequencyState::None);
        cache.take(&3);
        assert(&cache, vec![1, 2, 4]);
        assert_eq!(cache.get_frequency(&3), FrequencyState::TakenAway);
        cache.put(3, R1(3, 3000, 0));
        assert(&cache, vec![1, 2, 4, 3]);
        assert_eq!(cache.get_frequency(&3), FrequencyState::Frequency(1));
        {
            let r = cache.active_mut(&1);
            assert_eq!(r.unwrap(), &R1(1, 1000, 0));
        };
        assert(&cache, vec![2, 4, 3, 1]);
        cache.put(3, R1(3, 3100, 0));
        assert(&cache, vec![2, 4, 1, 3]);
        cache.active_mut(&4);
        assert(&cache, vec![2, 1, 4, 3]);
        assert_eq!(cache.get_frequency(&2), FrequencyState::Frequency(0));
        assert_eq!(cache.get_frequency(&1), FrequencyState::Frequency(1));
        assert_eq!(cache.get_frequency(&4), FrequencyState::Frequency(1));
        assert_eq!(cache.get_frequency(&3), FrequencyState::Frequency(2));
        // 测试移除后，在过滤器命中的情况下，数据频次应为1
        cache.remove(&2);
        assert_eq!(cache.get_frequency(&2), FrequencyState::None);
        cache.put(2, R1(2, 2100, 0));
        assert_eq!(cache.get_frequency(&2), FrequencyState::Frequency(1));
        assert(&cache, vec![1, 4, 2, 3]);
        // 测试最大频次为15
        for i in 2..33 {
            cache.active_mut(&2);
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
        cache.put(5, R1(5, 5000, 0));
        println!("1---------");
        assert_eq!(cache.get_frequency(&5), FrequencyState::Frequency(0));
        assert_eq!(cache.get_frequency(&1), FrequencyState::Frequency(1));

        // 测试频降后的数据正确性
        assert_eq!(cache.take(&2).unwrap().0, 2);
        cache.put(2, R1(2, 2200, 0));
        println!("2---------");
        assert_eq!(cache.get_frequency(&1), FrequencyState::Frequency(0));
        assert_eq!(cache.get_frequency(&2), FrequencyState::Frequency(8));
        assert_eq!(cache.get_frequency(&3), FrequencyState::Frequency(1));
        assert_eq!(cache.get_frequency(&4), FrequencyState::Frequency(0));
        assert_eq!(cache.get_frequency(&5), FrequencyState::Frequency(0));
        assert(&cache, vec![5, 1, 4, 3, 2]);
        println!(
            "cache size:{}, len:{}, count:{}",
            cache.size(),
            cache.len(),
            cache.count()
        );
        for i in cache.timeout_ref_collect(0, 1000) {
            println!("timeout_ref_collect, {}", i.0);
        }
        for i in cache.capacity_ref_collect(9000) {
            println!("capacity_ref_collect, {}", i.0);
        }
        assert_eq!(cache.get_frequency(&5), FrequencyState::Garbaged);
        cache.active_mut(&5);
        assert_eq!(cache.get_frequency(&1), FrequencyState::Garbaged);
        cache.collect(1).unwrap();
        assert(&cache, vec![4, 3, 5, 2]);
    }
    fn assert(c: &Cache<usize, R1>, vec: Vec<usize>) {
        let mut i = 0;
        for r in c.iter() {
            assert_eq!(r.0, vec[i]);
            i += 1;
        }
    }
}
