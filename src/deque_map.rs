//! deque hashmap

use std::hash::Hash;
use std::collections::hash_map::Entry;
use pi_hash::XHashMap;
use pi_slot_deque::{Deque, Slot};
use slotmap::{SlotMap, DefaultKey};

/// deque hashmap
#[derive(Debug)]
pub struct DequeMap<K: Eq + Hash + Clone, V> {
    /// deque
    deque: Deque<DefaultKey>,
    /// hashmap
    map: XHashMap<K, DefaultKey>,
    /// SlotMap
    slot: Slot<DefaultKey, (K, V)>,
}
impl<K: Eq + Hash + Clone, V> Default for DequeMap<K, V> {
    fn default() -> Self {
        DequeMap {
            deque: Default::default(),
            map: Default::default(),
            slot: Default::default(),
        }
    }
}

impl<K: Eq + Hash + Clone, V> DequeMap<K, V> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            deque: Default::default(),
            map: Default::default(),//XHashMap::with_capacity(capacity),
            slot: SlotMap::with_capacity(capacity),
        }
    }
    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn contains_key(&self, k: &K) -> bool {
        self.map.contains_key(k)
    }
    /// Get by key
    pub fn get(&self, k: &K) -> Option<&V> {
        if let Some(k) = self.map.get(k) {
            unsafe {Some(&(self.slot.get_unchecked(k.clone()).el.1))}
        }else{
            None
        }
    }
    /// GetMut by key
    pub fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        if let Some(k) = self.map.get(k) {
            unsafe {Some(&mut (self.slot.get_unchecked_mut(k.clone()).el.1))}
        }else{
            None
        }
    }
    /// remove by key
    pub fn remove(&mut self, k: &K) -> Option<(K, V)> {
        if let Some(k) = self.map.remove(k) {
           self.deque.remove(k, &mut self.slot)
        }else{
            None
        }
    }
    /// Get head
    pub fn head(&self) -> Option<&(K, V)> {
        if let Some(node) = self.slot.get(self.deque.head()) {
            Some(&node.el)
        }else{
            None
        }
    }
    /// Get tail key
    pub fn tail(&self) -> Option<&(K, V)> {
        if let Some(node) = self.slot.get(self.deque.tail()) {
            Some(&node.el)
        }else{
            None
        }
    }
    /// Get head
    pub fn head_mut(&mut self) -> Option<&mut (K, V)> {
        if let Some(node) = self.slot.get_mut(self.deque.head()) {
            Some(&mut node.el)
        }else{
            None
        }
    }
    /// Get tail key
    pub fn tail_mut(&mut self) -> Option<&mut (K, V)> {
        if let Some(node) = self.slot.get_mut(self.deque.tail()) {
            Some(&mut node.el)
        }else{
            None
        }
    }
    /// Append an element to the Deque. return key value
    pub fn push_back(&mut self, k: K, v: V) -> Option<(K, V)> {
        let sk = self.deque.push_back((k.clone(), v), &mut self.slot);
        match self.map.entry(k) {
            Entry::Occupied(mut e) => {
                self.deque.remove(std::mem::replace(e.get_mut(), sk), &mut self.slot)
            }
            Entry::Vacant(e) => {
                e.insert(sk);
                None
            }
        }
    }

    /// Prepend an element to the Deque. return key value
    pub fn push_front(&mut self, k: K, v: V) -> Option<(K, V)> {
        let sk = self.deque.push_front((k.clone(), v), &mut self.slot);
        match self.map.entry(k) {
            Entry::Occupied(mut e) => {
                self.deque.remove(std::mem::replace(e.get_mut(), sk), &mut self.slot)
            }
            Entry::Vacant(e) => {
                e.insert(sk);
                None
            }
        }
    }
    /// Removes the last element from the Deque and returns it, or None if it is empty.
    pub fn pop_back(&mut self) -> Option<(K, V)> {
        let r = self.deque.pop_back(&mut self.slot);
        if let Some((k, _)) = &r {
            self.map.remove(k);
        }
        r
    }

    /// Removes the first element from the Deque and returns it, or None if it is empty.
    pub fn pop_front(&mut self) -> Option<(K, V)> {
        let r = self.deque.pop_front(&mut self.slot);
        if let Some((k, _)) = &r {
            self.map.remove(k);
        }
        r
    }

}