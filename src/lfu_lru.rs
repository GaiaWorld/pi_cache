/**
 * 预设数据访问频次最高为15， 然后按照访问频次0,1,2-3,4-7,8-15分成5段，每段为一个LRU，如果数据增加频次，则可能迁移到更高段的LRU上。
 * 为了减少缓存击穿的情况，增加了一个CuckooFilter。
 * 当数据放入时，首先查找频次表，获得原频次，
 *      如果没有该键，则为首次放入，接着查找CuckooFilter，如果在，则频次提升为1，如果不在则记录到过滤器。
 *      将数据放入到指定频次LRU中后，从0频次开始进行LRU淘汰，然后依次向更大频次的LRU进行淘汰。
 *      当过滤器的大小超过1024次后，清空过滤器，重新开始。
 * 频降率: 总放入次数/缓存总条目数，每当频降率达到阈值后，将所有频次减半，8降为4， 4降为2，2降为1， 将1频次LRU的数据放入0频次的LRU中。
 * 为了记录被take拿走的数据频次，并且为了快速找到指定key所在的频次LRU，需要维护一个key的频次表。
 */

use probabilistic_collections::cuckoo::CuckooFilter;
use std::collections::hash_map;
use std::hash::{Hash};
use pi_hash::XHashMap;
use crate::deque_map::DequeMap;

/// 最大频次
const FREQUENCY_MAX: u8 = 15;
// 默认CuckooFilter窗口大小
const WINDOW_SIZE: usize = 1024;
// 默认的整理率，总放入次数/缓存总条目数
const FREQUENCY_DOWN_RATE: usize = 8;

/// 数据，放入数据表的数据必须实现该trait
pub trait Data {
    /// 数据的大小
    fn size(&self) -> usize {
        1
    }
    /// 数据的超时时间，毫秒时间
    fn timeout(&self) -> usize {
        0
    }
}
/// 临时的数据条目
pub struct Entry<'a, K: Eq + Hash + Clone, V: Data> {
    k: K,
    v: Option<V>,
    f: u8,
    lrus: &'a mut Lrus<K, V>,
}
impl<'a, K: Eq + Hash + Clone, V: Data> Entry<'a, K, V> {
    pub fn value(&mut self) -> &mut V {
        unsafe { self.v.as_mut().unwrap_unchecked() }
    }
}
impl<'a, K: Eq + Hash + Clone, V: Data> Drop for Entry<'a, K, V> {
    fn drop(&mut self) {
        let v = unsafe { self.v.take().unwrap_unchecked() };
        self.lrus.put(self.f, self.k.clone(), v);
    }
}
/// 超时迭代器
pub struct TimeoutIter<'a, K: Eq + Hash + Clone, V: Data> {
	lrus: &'a mut Lrus<K, V>,
    index: usize,
    now: usize,
    capacity: usize,
}
impl<'a, K: Eq + Hash + Clone, V: Data> Iterator for TimeoutIter<'a, K, V>
{
    type Item = V;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while self.lrus.size > self.capacity && self.index < self.lrus.arr.len() {
            if let Some(r) = self.lrus.arr[self.index].head() {
                if r.1.timeout() < self.now {
                    self.lrus.size -= r.1.size();
                    self.lrus.metrics.timeout += 1;
                    return Some(self.lrus.arr[self.index].pop_front().unwrap().1)
                }else{
                    return None
                }
            }else{
                self.index += 1;
            }
        }
        None
    }
}

/// 容量迭代器
pub struct CapacityIter<'a, K: Eq + Hash + Clone, V: Data> {
	lrus: &'a mut Lrus<K, V>,
    index: usize,
    capacity: usize,
}
impl<'a, K: Eq + Hash + Clone, V: Data> Iterator for CapacityIter<'a, K, V>
{
    type Item = V;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while self.lrus.size > self.capacity && self.index < self.lrus.arr.len() {
            if let Some(r) = self.lrus.arr[self.index].head() {
                self.lrus.size -= r.1.size();
                self.lrus.metrics.evict += 1;
                return Some(self.lrus.arr[self.index].pop_front().unwrap().1)
            }else{
                self.index += 1;
            }
        }
        None
    }
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
    /// 归还次数
    pub put: usize,
    /// 移除次数
    pub remove: usize,
    /// 超时清理次数
    pub timeout: usize,
    /// 超过容量的驱除次数
    pub evict: usize,
}
/// LFU-LRU缓存
pub struct LfuLru<K: Eq + Hash + Clone, V: Data> {
    /// 不同数据访问频次的LRU，频次为0,1,2-3,4-7,8-15
    lrus: Lrus<K, V>,
    /// 每个数据的频次及频降周期数
    frequency_map: XHashMap<K, (u8, usize)>,
    /// 首次数据的bloom，判断不在缓存的数据是否被调用过， 一般指定大小，比如1024.
    filter: CuckooFilter<K>,
}
impl<K: Eq + Hash + Clone, V: Data> Default for LfuLru<K, V> {
    fn default() -> Self {
        Self::with_config(
            WINDOW_SIZE,
            FREQUENCY_DOWN_RATE,
        )
    }
}
impl<K: Eq + Hash + Clone, V: Data> LfuLru<K, V> {
    pub fn new() -> Self {
        Self::with_config(
            WINDOW_SIZE,
            FREQUENCY_DOWN_RATE,
        )
    }
    /// 用CuckooFilter窗口大小，整理率，
    pub fn with_config(
        cuckoo_filter_window_size: usize,
        frequency_down_rate: usize,
    ) -> Self {
        LfuLru {
            lrus: Lrus::new(frequency_down_rate),
            frequency_map: Default::default(),
            filter: CuckooFilter::from_entries_per_index(cuckoo_filter_window_size, 0.01, 8),
        }
    }
    /// 拿走的数据， 如果拿到了数据，就必须保证会调用put还回来
    pub fn take(&mut self, k: &K) -> Option<V> {
        match self.frequency_map.get(k) {
            Some(r) => {
                // 获得当前该键所在的频率段
                let f = self.lrus.frequency(r);
                self.lrus.take(k, f)
            }
            _ => {
                self.lrus.metrics.miss += 1;
                None
            }
        }
    }
    /// 判断是否有指定键的数据， 返回None表示没有，Some(true)表示数据在，Some(false)表示数据已被拿走
    pub fn contains_key(&self, k: &K) -> Option<bool> {
        match self.frequency_map.get(k) {
            Some(r) => self.lrus.contains_key(k, self.lrus.frequency(r)),
            None => None,
        }
    }
    /// 获得指定键的数据
    pub fn get(&self, k: &K) -> Option<&V> {
        match self.frequency_map.get(k) {
            Some(r) => self.lrus.get(k, self.lrus.frequency(r)),
            None => None,
        }
    }
    /// 放入的数据，返回Some(V)表示被替换的数据
    pub fn put(&mut self, k: K, v: V) -> Option<V> {
        self.lrus.frequency_down();
        // 获得该数据的频次
        let f = match self.frequency_map.entry(k.clone()) {
            hash_map::Entry::Occupied(mut e) => self.lrus.put_frequency(e.get_mut()),
            hash_map::Entry::Vacant(e) => {
                // 如果在窗口区中命中
                let f = if self.filter.contains(e.key()) {
                    self.lrus.metrics.insert2 += 1;
                    1
                } else {
                    self.filter.insert(&e.key());
                    self.lrus.metrics.insert1 += 1;
                    0
                };
                e.insert((f, self.lrus.frequency_down_count));
                f
            }
        };
        self.lrus.put(f, k, v)
    }
    /// 激活并获取可写应用，等于拿走并立即还回来，但少一次hash查找，会增加频次和最后使用时间
    pub fn active_mut(&mut self, k: &K) -> Option<Entry<'_, K, V>> {
        match self.frequency_map.entry(k.clone()) {
            hash_map::Entry::Occupied(mut e) => {
                // 如果放入次数达到上限，则增加频降次数，并清空放入次数
                self.lrus.frequency_down();
                // 获得当前该键放入的频率段
                let f = self.lrus.put_frequency(e.get_mut());
                let v = self.lrus.take(k, f);
                if v.is_some() {
                    Some(Entry {
                        k: k.clone(),
                        v,
                        f,
                        lrus: &mut self.lrus,
                    })
                } else {
                    None
                }
            }
            _ => {
                self.lrus.metrics.miss += 1;
                None
            }
        }
    }
    /// 移走
    pub fn remove(&mut self, k: &K) -> Option<V> {
        match self.frequency_map.remove(k) {
            Some(r) => {
                // 获得当前该键所在的频率段
                let f = self.lrus.frequency(&r);
                self.lrus.remove(k, f)
            }
            None => None,
        }
    }
    /// 全部数量，包括被拿走的数据
    pub fn len(&self) -> usize {
        self.frequency_map.len()
    }
    /// 当前数量，被缓存的数据数量
    pub fn count(&self) -> usize {
        let mut r = 0;
        for i in self.lrus.arr.iter() {
            r += i.len();
        }
        r
    }
    /// 当前缓存的数据总大小
    pub fn size(&self) -> usize {
        self.lrus.size
    }
    /// 获得当前的统计
    pub fn metrics(&self) -> Metrics {
        self.lrus.metrics.clone()
    }
    /// 获得当前的缓存数据分布，0-4分别为频次为0,1,2-3,4-7,8-15的数据数量
    pub fn distribution(&self) -> [usize; 5] {
        let mut r = [0, 0, 0, 0, 0];
        for i in 0..self.lrus.arr.len() {
            r[i] = self.lrus.arr[i].len()
        }
        r
    }
    /// 超时整理方法， 参数为毫秒时间及最小容量，清理最小容量外的超时数据
    pub fn timeout_collect(&mut self, now: usize, capacity: usize) -> TimeoutIter<'_, K, V> {
        TimeoutIter { lrus: &mut self.lrus, index: 0, now, capacity }

    }
    /// 超量整理方法， 参数为容量， 按照频率优先， 同频先进先出的原则，清理超出容量的数据
    pub fn capacity_collect(&mut self, capacity: usize) -> CapacityIter<'_, K, V> {
        CapacityIter { lrus: &mut self.lrus, index: 0, capacity }
    }
}

#[derive(Default)]
struct Lrus<K: Eq + Hash + Clone, V: Data> {
    /// 不同数据访问频次的LRU，频次为0,1,2-3,4-7,8-15
    arr: [DequeMap<K, V>; 5],
    /// 数据的总大小
    size: usize,
    /// 频降率：放入次数/总数量，默认为8
    frequency_down_rate: usize,
    /// 统计数据
    metrics: Metrics,
    /// 频降次数
    frequency_down_count: usize,
    /// 这个频降周期的放入次数，
    put_count: usize,
}
impl<K: Eq + Hash + Clone, V: Data> Lrus<K, V> {
    #[inline]
    fn new(frequency_down_rate: usize) -> Self {
        Lrus {
            arr: Default::default(),
            size: 0,
            frequency_down_rate,
            metrics: Default::default(),
            frequency_down_count: 0,
            put_count: 0,
        }
    }
    /// 获得频次
    #[inline]
    fn frequency(&self, (frequency, frequency_down): &(u8, usize)) -> u8 {
        frequency >> (self.frequency_down_count - frequency_down)
    }
    /// 拿走的数据， 如果拿到了数据，就必须保证会调用put还回来
    fn take(&mut self, k: &K, f: u8) -> Option<V> {
        match self.arr[f as usize].remove(k) {
            Some(v) => {
                self.metrics.hit += 1;
                self.size -= v.1.size();
                Some(v.1)
            }
            _ => {
                self.metrics.miss += 1;
                None
            }
        }
    }
    /// 判断是否有指定键的数据， 返回None表示没有，Some(true)表示数据在，Some(false)表示数据已被拿走
    #[inline]
    fn contains_key(&self, k: &K, f: u8) -> Option<bool> {
        Some(self.arr[f as usize].contains_key(k))
    }
    /// 获得指定键的数据
    #[inline]
    fn get(&self, k: &K, f: u8) -> Option<&V> {
        self.arr[f as usize].get(k)
    }
    /// 移走
    #[inline]
    fn remove(&mut self, k: &K, f: u8) -> Option<V> {
        self.metrics.remove += 1;
        if let Some(r) = self.arr[f as usize].remove(k) {
            self.size -= r.1.size();
            Some(r.1)
        }else{
            None
        }
    }
    /// 频降
    fn frequency_down(&mut self) {
        let mut n = self.put_count / self.frequency_down_rate;

        for map in &self.arr {
            if n > map.len() {
                n -= map.len();
            }else {
                self.put_count += 1;
                return;
            }
        }
        // 如果放入次数达到上限，则增加频降次数，并清空放入次数
        self.frequency_down_count += 1;
        self.put_count = 0;
        // 进行频降
        // 先将1合并到0
        while let Some(e) = self.arr[1].pop_front() {
            self.arr[0].push_back(e.0, e.1);
        }
        // 调换位置
        self.arr[1..5].rotate_left(1);
    }
    /// 获得频次
    fn put_frequency(&mut self, r: &mut (u8, usize)) -> u8 {
        let mut f = r.0 >> (self.frequency_down_count - r.1);
        if f >= FREQUENCY_MAX {
            f = FREQUENCY_MAX;
        } else {
            f += 1;
        }
        self.metrics.put += 1;
        // 修正频次及频降数
        r.0 = f;
        r.1 = self.frequency_down_count;
        f
    }
    /// 放入数据
    fn put(&mut self, f: u8, k: K, v: V) -> Option<V> {
        self.size += v.size();
        if let Some(r) = self.arr[f as usize].push_back(k, v) {
            self.size -= r.1.size();
            Some(r.1)
        }else{
            None
        }
    }
}
