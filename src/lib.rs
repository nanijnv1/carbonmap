//! carbonmap
//!
//! Concurrent hash map for Rust.
//!
//! ⚠️ Early alpha.

use std::collections::HashMap;
use std::hash::Hash;

use parking_lot::{RwLock, RwLockWriteGuard, MappedRwLockWriteGuard};

/// Concurrent hash map
pub struct CarbonMap<K, V> {
    inner: RwLock<HashMap<K, V>>,
}

/* ================= Entry Types ================= */

pub enum Entry<'a, K, V> {
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}

pub struct OccupiedEntry<'a, K, V> {
    key: K,
    guard: RwLockWriteGuard<'a, HashMap<K, V>>,
}

pub struct VacantEntry<'a, K, V> {
    key: K,
    guard: RwLockWriteGuard<'a, HashMap<K, V>>,
}

/* ================= Impl ================= */

impl<K, V> CarbonMap<K, V>
where
    K: Eq + Hash + Clone,
{
    /// New map
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Insert or overwrite
    pub fn insert(&self, key: K, val: V) {
        let mut map = self.inner.write();
        map.insert(key, val);
    }

    /// Get cloned value
    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let map = self.inner.read();
        map.get(key).cloned()
    }

    /// Remove key
    pub fn remove(&self, key: &K) -> Option<V> {
        let mut map = self.inner.write();
        map.remove(key)
    }

    /// Entry API
    pub fn entry(&self, key: K) -> Entry<'_, K, V> {
        let guard = self.inner.write();

        if guard.contains_key(&key) {
            Entry::Occupied(OccupiedEntry { key, guard })
        } else {
            Entry::Vacant(VacantEntry { key, guard })
        }
    }
}

/* ================= Entry Impl ================= */

impl<'a, K, V> Entry<'a, K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn or_insert(self, default: V) -> MappedRwLockWriteGuard<'a, V> {
        match self {
            Entry::Occupied(e) => e.into_guard(),
            Entry::Vacant(e) => e.insert(default),
        }
    }

    pub fn or_insert_with<F>(self, f: F) -> MappedRwLockWriteGuard<'a, V>
    where
        F: FnOnce() -> V,
    {
        match self {
            Entry::Occupied(e) => e.into_guard(),
            Entry::Vacant(e) => e.insert(f()),
        }
    }

    pub fn and_modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut V),
    {
        match self {
            Entry::Occupied(mut e) => {
                f(e.get_mut());
                Entry::Occupied(e)
            }
            e => e,
        }
    }
}

impl<'a, K, V> OccupiedEntry<'a, K, V>
where
    K: Eq + Hash + Clone,
{
    fn into_guard(self) -> MappedRwLockWriteGuard<'a, V> {
        RwLockWriteGuard::map(self.guard, |m| {
            m.get_mut(&self.key).unwrap()
        })
    }

    pub fn get(&self) -> &V {
        self.guard.get(&self.key).unwrap()
    }

    pub fn get_mut(&mut self) -> &mut V {
        self.guard.get_mut(&self.key).unwrap()
    }
}

impl<'a, K, V> VacantEntry<'a, K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn insert(mut self, val: V) -> MappedRwLockWriteGuard<'a, V> {
        self.guard.insert(self.key.clone(), val);

        RwLockWriteGuard::map(self.guard, |m| {
            m.get_mut(&self.key).unwrap()
        })
    }
}

/* ================= Tests ================= */

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn basic_insert_get() {
        let map = CarbonMap::new();

        map.insert("a", 1);
        map.insert("b", 2);

        assert_eq!(map.get(&"a"), Some(1));
        assert_eq!(map.get(&"b"), Some(2));
        assert_eq!(map.get(&"c"), None);
    }

    #[test]
    fn overwrite() {
        let map = CarbonMap::new();

        map.insert("x", 10);
        map.insert("x", 20);

        assert_eq!(map.get(&"x"), Some(20));
    }

    #[test]
    fn remove_basic() {
        let map = CarbonMap::new();

        map.insert("a", 1);

        let v = map.remove(&"a");

        assert_eq!(v, Some(1));
        assert_eq!(map.get(&"a"), None);
    }

    #[test]
    fn entry_or_insert() {
        let map = CarbonMap::new();

        let v = map.entry("a").or_insert(10);

        assert_eq!(*v, 10);
    }

    #[test]
    fn entry_and_modify() {
        let map = CarbonMap::new();

        let _ = map.entry("a").or_insert(1);

        let _ = map.entry("a")
            .and_modify(|v| *v += 5)
            .or_insert(0);

        assert_eq!(map.get(&"a"), Some(6));
    }

    #[test]
    fn entry_or_insert_with() {
        let map = CarbonMap::new();

        let v = map.entry("k").or_insert_with(|| 42);

        assert_eq!(*v, 42);
    }

    #[test]
    fn concurrent_inserts() {
        let map = Arc::new(CarbonMap::new());

        let mut handles = vec![];

        for i in 0..8 {
            let m = map.clone();

            handles.push(thread::spawn(move || {
                for j in 0..1000 {
                    let k = format!("{}-{}", i, j);
                    m.insert(k, j);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert!(map.get(&"0-0".to_string()).is_some());
        assert!(map.get(&"7-999".to_string()).is_some());
    }

    #[test]
    fn concurrent_entry_updates() {
        let map = Arc::new(CarbonMap::new());

        let mut handles = vec![];

        for _ in 0..10 {
            let m = map.clone();

            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    let _ = m.entry("counter")
                        .and_modify(|v| *v += 1)
                        .or_insert(1);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let val = map.get(&"counter");

        assert_eq!(val, Some(10000));
    }
}
