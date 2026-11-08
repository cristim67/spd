//! Un shard: hash map cheie → slot + listă dublu înlănțuită intrusivă peste slab.
//!
//! Capul listei (`head`) = cel mai recent inserat/accesat; coada (`tail`) =
//! candidatul la evacuare.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use bytes::Bytes;

use crate::Policy;

const NIL: u32 = u32::MAX;

#[derive(Debug)]
struct Entry {
    key: Box<str>,
    value: Bytes,
    expires_at: Option<Instant>,
    tags: Box<[Box<str>]>,
    referenced: bool,
}

#[derive(Debug)]
struct Slot {
    entry: Option<Entry>,
    prev: u32,
    next: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Lookup {
    Hit(Bytes),
    Miss,
    Expired,
}

#[derive(Debug)]
pub(crate) struct Shard {
    map: HashMap<Box<str>, u32>,
    slots: Vec<Slot>,
    free: Vec<u32>,
    head: u32,
    tail: u32,
    capacity: usize,
    policy: Policy,
    tags: HashMap<Box<str>, HashSet<u32>>,
}

impl Shard {
    pub(crate) fn new(capacity: usize, policy: Policy) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            slots: Vec::with_capacity(capacity),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
            capacity,
            policy,
            tags: HashMap::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn clear(&mut self) {
        *self = Self::new(self.capacity, self.policy);
    }

    fn entry(&self, i: u32) -> &Entry {
        self.slots[i as usize]
            .entry
            .as_ref()
            .expect("linked slot must hold an entry")
    }

    fn entry_mut(&mut self, i: u32) -> &mut Entry {
        self.slots[i as usize]
            .entry
            .as_mut()
            .expect("linked slot must hold an entry")
    }

    fn unlink(&mut self, i: u32) {
        let (prev, next) = {
            let s = &self.slots[i as usize];
            (s.prev, s.next)
        };
        if prev == NIL {
            self.head = next;
        } else {
            self.slots[prev as usize].next = next;
        }
        if next == NIL {
            self.tail = prev;
        } else {
            self.slots[next as usize].prev = prev;
        }
        let s = &mut self.slots[i as usize];
        s.prev = NIL;
        s.next = NIL;
    }

    fn push_front(&mut self, i: u32) {
        let old_head = self.head;
        {
            let s = &mut self.slots[i as usize];
            s.prev = NIL;
            s.next = old_head;
        }
        if old_head == NIL {
            self.tail = i;
        } else {
            self.slots[old_head as usize].prev = i;
        }
        self.head = i;
    }

    fn attach_tags(&mut self, i: u32) {
        let tags = self.entry(i).tags.clone();
        for t in tags {
            self.tags.entry(t).or_default().insert(i);
        }
    }

    fn detach_tags(&mut self, i: u32, tags: &[Box<str>]) {
        for t in tags {
            if let Some(set) = self.tags.get_mut(t) {
                set.remove(&i);
                if set.is_empty() {
                    self.tags.remove(t);
                }
            }
        }
    }

    fn remove(&mut self, i: u32) {
        self.unlink(i);
        let e = self.slots[i as usize]
            .entry
            .take()
            .expect("removing an empty slot");
        self.map.remove(&e.key);
        self.detach_tags(i, &e.tags);
        self.free.push(i);
    }

    fn touch(&mut self, i: u32) {
        match self.policy {
            Policy::Lru => {
                if self.head != i {
                    self.unlink(i);
                    self.push_front(i);
                }
            }
            Policy::Clock => self.entry_mut(i).referenced = true,
            Policy::Fifo => {}
        }
    }

    /// Evacuează o intrare conform politicii. `false` dacă shard-ul e gol.
    fn evict_one(&mut self) -> bool {
        loop {
            let t = self.tail;
            if t == NIL {
                return false;
            }
            // CLOCK: intrările referențiate primesc o a doua șansă (bit resetat,
            // mutate în cap). Bucla se termină după cel mult o trecere completă.
            if self.policy == Policy::Clock && self.entry(t).referenced {
                self.entry_mut(t).referenced = false;
                self.unlink(t);
                self.push_front(t);
                continue;
            }
            self.remove(t);
            return true;
        }
    }

    pub(crate) fn get(&mut self, key: &str, now: Instant) -> Lookup {
        let Some(&i) = self.map.get(key) else {
            return Lookup::Miss;
        };
        if self.entry(i).expires_at.is_some_and(|e| e <= now) {
            self.remove(i);
            return Lookup::Expired;
        }
        self.touch(i);
        Lookup::Hit(self.entry(i).value.clone())
    }

    /// Returnează numărul de intrări evacuate pentru a face loc.
    pub(crate) fn set(
        &mut self,
        key: &str,
        value: Bytes,
        expires_at: Option<Instant>,
        tags: &[&str],
    ) -> usize {
        let tags: Box<[Box<str>]> = tags.iter().map(|t| Box::from(*t)).collect();
        if let Some(&i) = self.map.get(key) {
            let old_tags = std::mem::take(&mut self.entry_mut(i).tags);
            self.detach_tags(i, &old_tags);
            let e = self.entry_mut(i);
            e.value = value;
            e.expires_at = expires_at;
            e.tags = tags;
            self.attach_tags(i);
            self.touch(i);
            return 0;
        }

        let mut evicted = 0;
        while self.map.len() >= self.capacity && self.evict_one() {
            evicted += 1;
        }

        let entry = Entry {
            key: Box::from(key),
            value,
            expires_at,
            tags,
            referenced: false,
        };
        let i = if let Some(i) = self.free.pop() {
            self.slots[i as usize].entry = Some(entry);
            i
        } else {
            self.slots.push(Slot {
                entry: Some(entry),
                prev: NIL,
                next: NIL,
            });
            (self.slots.len() - 1) as u32
        };
        self.push_front(i);
        self.map.insert(Box::from(key), i);
        self.attach_tags(i);
        evicted
    }

    pub(crate) fn delete(&mut self, key: &str) -> bool {
        match self.map.get(key) {
            Some(&i) => {
                self.remove(i);
                true
            }
            None => false,
        }
    }

    pub(crate) fn invalidate_tag(&mut self, tag: &str) -> usize {
        let Some(set) = self.tags.remove(tag) else {
            return 0;
        };
        let n = set.len();
        for i in set {
            self.remove(i);
        }
        n
    }

    pub(crate) fn purge_expired(&mut self, now: Instant) -> usize {
        let expired: Vec<u32> = self
            .map
            .values()
            .copied()
            .filter(|&i| self.entry(i).expires_at.is_some_and(|e| e <= now))
            .collect();
        for &i in &expired {
            self.remove(i);
        }
        expired.len()
    }

    #[cfg(test)]
    fn keys_mru_to_lru(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut i = self.head;
        while i != NIL {
            out.push(self.entry(i).key.to_string());
            i = self.slots[i as usize].next;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn fill(s: &mut Shard, keys: &[&str]) {
        for k in keys {
            s.set(k, Bytes::new(), None, &[]);
        }
    }

    #[test]
    fn lru_evicts_least_recently_used() {
        let now = Instant::now();
        let mut s = Shard::new(3, Policy::Lru);
        fill(&mut s, &["a", "b", "c"]);
        assert!(matches!(s.get("a", now), Lookup::Hit(_)));
        fill(&mut s, &["d"]);
        assert_eq!(s.keys_mru_to_lru(), ["d", "a", "c"]);
    }

    #[test]
    fn fifo_ignores_access() {
        let now = Instant::now();
        let mut s = Shard::new(3, Policy::Fifo);
        fill(&mut s, &["a", "b", "c"]);
        let _ = s.get("a", now);
        fill(&mut s, &["d"]);
        assert_eq!(s.keys_mru_to_lru(), ["d", "c", "b"]);
    }

    #[test]
    fn clock_gives_second_chance() {
        let now = Instant::now();
        let mut s = Shard::new(3, Policy::Clock);
        fill(&mut s, &["a", "b", "c"]);
        let _ = s.get("a", now); // a referențiat
        fill(&mut s, &["d"]); // a primește a doua șansă, b e evacuat
        let mut keys = s.keys_mru_to_lru();
        keys.sort();
        assert_eq!(keys, ["a", "c", "d"]);
    }

    #[test]
    fn clock_all_referenced_terminates() {
        let now = Instant::now();
        let mut s = Shard::new(2, Policy::Clock);
        fill(&mut s, &["a", "b"]);
        let _ = s.get("a", now);
        let _ = s.get("b", now);
        assert_eq!(s.set("c", Bytes::new(), None, &[]), 1);
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn expired_entries_are_misses_and_purged() {
        let now = Instant::now();
        let mut s = Shard::new(10, Policy::Lru);
        s.set("x", Bytes::new(), Some(now + Duration::from_secs(1)), &[]);
        s.set("y", Bytes::new(), Some(now + Duration::from_secs(10)), &[]);
        assert!(matches!(s.get("x", now), Lookup::Hit(_)));
        assert_eq!(s.get("x", now + Duration::from_secs(2)), Lookup::Expired);
        assert_eq!(s.purge_expired(now + Duration::from_secs(11)), 1);
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn overwrite_replaces_tags_and_reuses_slots() {
        let mut s = Shard::new(10, Policy::Lru);
        s.set("k", Bytes::new(), None, &["t1"]);
        s.set("k", Bytes::new(), None, &["t2"]);
        assert_eq!(s.invalidate_tag("t1"), 0);
        assert_eq!(s.invalidate_tag("t2"), 1);
        s.set("k2", Bytes::new(), None, &[]);
        assert_eq!(s.slots.len(), 1, "freed slot must be reused");
    }
}
