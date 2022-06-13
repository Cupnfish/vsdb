/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[cfg(any(
    feature = "rocks_engine",
    all(feature = "rocks_engine", feature = "sled_engine"),
    all(not(feature = "rocks_engine"), not(feature = "sled_engine")),
))]
mod rocks_db;

#[cfg(all(feature = "sled_engine", not(feature = "rocks_engine")))]
mod sled_db;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[cfg(any(
    feature = "rocks_engine",
    all(feature = "rocks_engine", feature = "sled_engine"),
    all(not(feature = "rocks_engine"), not(feature = "sled_engine")),
))]
pub(crate) use rocks_db::RocksEngine as RocksDB;

#[cfg(all(feature = "sled_engine", not(feature = "rocks_engine")))]
pub(crate) use sled_db::SledEngine as Sled;

#[cfg(any(
    feature = "rocks_engine",
    all(feature = "rocks_engine", feature = "sled_engine"),
    all(not(feature = "rocks_engine"), not(feature = "sled_engine")),
))]
type EngineIter = rocks_db::RocksIter;

#[cfg(all(feature = "sled_engine", not(feature = "rocks_engine")))]
type EngineIter = sled_db::SledIter;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

use crate::common::{
    ende::{SimpleVisitor, ValueEnDe},
    BranchID, Pre, PreBytes, RawKey, RawValue, VersionID, VSDB,
};
use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut, RangeBounds},
    result::Result as StdResult,
};

static LEN_LK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// Low-level database interface.
pub trait Engine: Sized {
    fn new() -> Result<Self>;
    fn alloc_prefix(&self) -> Pre;
    fn alloc_branch_id(&self) -> BranchID;
    fn alloc_version_id(&self) -> VersionID;
    fn area_count(&self) -> usize;

    // NOTE:
    // do NOT make the number of areas bigger than `u8::MAX`
    fn area_idx(&self, meta_prefix: PreBytes) -> usize {
        meta_prefix[0] as usize % self.area_count()
    }

    fn flush(&self);

    fn iter(&self, meta_prefix: PreBytes) -> EngineIter;

    fn range<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> EngineIter;

    fn get(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<&RawValue>;

    fn insert(
        &self,
        meta_prefix: PreBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<RawValue>;

    fn remove(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue>;

    fn get_instance_len(&self, instance_prefix: PreBytes) -> u64;

    fn set_instance_len(&self, instance_prefix: PreBytes, new_len: u64);

    #[allow(unused_variables)]
    fn increase_instance_len(&self, instance_prefix: PreBytes) {
        let x = LEN_LK.lock();
        let l = self.get_instance_len(instance_prefix);
        self.set_instance_len(instance_prefix, l + 1)
    }

    #[allow(unused_variables)]
    fn decrease_instance_len(&self, instance_prefix: PreBytes) {
        let x = LEN_LK.lock();
        let l = self.get_instance_len(instance_prefix);
        self.set_instance_len(instance_prefix, l - 1)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

const LRU_CAP: usize = 10_0000;

#[derive(Eq, Debug)]
pub(crate) struct Mapx {
    // the unique ID of each instance
    prefix: PreBytes,

    lru: LruCache<RawKey, RawValue>,
}

impl Clone for Mapx {
    fn clone(&self) -> Self {
        let mut new_instance = Self::new();
        for (k, v) in self.iter() {
            new_instance.insert(k, v);
        }
        new_instance
    }
}

impl Mapx {
    pub(crate) unsafe fn shadow(&self) -> Self {
        Self {
            prefix: self.prefix,
            lru: LruCache::new(0),
        }
    }

    #[inline(always)]
    pub(crate) fn new() -> Self {
        let prefix = VSDB.db.alloc_prefix();

        let prefix_bytes = prefix.to_be_bytes();

        assert!(VSDB.db.iter(prefix_bytes).next().is_none());

        VSDB.db.set_instance_len(prefix_bytes, 0);

        Mapx {
            prefix: prefix_bytes,
            lru: LruCache::new(LRU_CAP),
        }
    }

    fn get_instance_cfg(&self) -> InstanceCfg {
        InstanceCfg::from(self)
    }

    #[inline(always)]
    pub(crate) fn get(&self, key: &[u8]) -> Option<&RawValue> {
        let k = key.to_vec().into_boxed_slice();

        unsafe {
            let mut lru = self.lru as *const LruCache<RawKey, RawValue>
                as *mut LruCache<RawKey, RawValue>;
            if let Some(v) = lru.get(&k) {
                return Some(v);
            }

            let v = VSDB.db.get(self.prefix, key)?;
            lru.put(k, v);
            lru.peek(&k)
        }
    }

    #[inline(always)]
    pub(crate) fn get_mut(&mut self, key: &[u8]) -> Option<&mut RawValue> {
        let k = key.to_vec().into_boxed_slice();

        unsafe {
            let mut lru = self.lru as *const LruCache<RawKey, RawValue>
                as *mut LruCache<RawKey, RawValue>;
            if let Some(v) = lru.get_mut(&k) {
                return Some(v);
            }

            let v = VSDB.db.get(self.prefix, key)?;
            lru.put(k, v);
            lru.peek_mut(&k)
        }
    }

    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        VSDB.db.get_instance_len(self.prefix) as usize
    }

    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        0 == self.len()
    }

    #[inline(always)]
    pub(crate) fn iter(&self) -> MapxIter {
        MapxIter {
            db_iter: VSDB.db.iter(self.prefix),
            kv_cache: Default::default(),
            hdr: self,
        }
    }

    #[inline(always)]
    fn iter_mut<'a>(&'a mut self) -> MapxIterMut<'a> {
        MapxIterMut {
            prefix: self.prefix,
            db_iter: VSDB.db.iter(self.prefix),
            lru: &mut self.lru,
        }
    }

    #[inline(always)]
    pub(crate) fn into_iter(self) -> MapxIntoIter {
        MapxIntoIter {
            db_iter: VSDB.db.iter(self.prefix),
            hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn range<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        bounds: R,
    ) -> MapxIter<'a> {
        MapxIter {
            db_iter: VSDB.db.range(self.prefix, bounds),
            kv_cache: Default::default(),
            hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn range_mut<'a, R: RangeBounds<&'a [u8]>>(
        &'a mut self,
        bounds: R,
    ) -> MapxIterMut<'a> {
        MapxIterMut {
            prefix: self.prefix,
            db_iter: VSDB.db.range(self.prefix, bounds),
            lru: &mut self.lru,
        }
    }

    #[inline(always)]
    pub(crate) fn insert(&mut self, key: &[u8], value: &[u8]) -> Option<RawValue> {
        let ret = VSDB.db.insert(self.prefix, key, value);
        if ret.is_none() {
            VSDB.db.increase_instance_len(self.prefix);
        }
        ret
    }

    #[inline(always)]
    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<RawValue> {
        let ret = VSDB.db.remove(self.prefix, key);
        if ret.is_some() {
            VSDB.db.decrease_instance_len(self.prefix);
        }
        ret
    }

    #[inline(always)]
    pub(crate) fn clear(&mut self) {
        VSDB.db.iter(self.prefix).for_each(|(k, _)| {
            if VSDB.db.remove(self.prefix, &k).is_some() {
                VSDB.db.decrease_instance_len(self.prefix);
            }
        });
    }
}

impl PartialEq for Mapx {
    fn eq(&self, other: &Mapx) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .zip(other.iter())
                .all(|((k, v), (ko, vo))| k == ko && v == vo)
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct InstanceCfg {
    prefix: PreBytes,
}

impl From<InstanceCfg> for Mapx {
    fn from(cfg: InstanceCfg) -> Self {
        Self { prefix: cfg.prefix }
    }
}

impl From<&Mapx> for InstanceCfg {
    fn from(x: &Mapx) -> Self {
        Self { prefix: x.prefix }
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl Serialize for Mapx {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&<InstanceCfg as ValueEnDe>::encode(
            &self.get_instance_cfg(),
        ))
    }
}

impl<'de> Deserialize<'de> for Mapx {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SimpleVisitor).map(|meta| {
            let meta = pnk!(<InstanceCfg as ValueEnDe>::decode(&meta));
            Mapx::from(meta)
        })
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

pub struct MapxIter<'a> {
    db_iter: EngineIter,
    kv_cache: (RawKey, RawValue),
    hdr: PhantomData<&'a Mapx>,
}

impl<'a> Iterator for MapxIter<'a> {
    type Item = (&'a [u8], &'a [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        let (k, v) = self.db_iter.next()?;
        unsafe {
            *((&self.kv_cache) as *const (RawKey, RawValue)
                as *mut (RawKey, RawValue)) = (k, v);
        }
        Some((&self.kv_cache.0[..], &self.kv_cache.0[..]))
    }
}

impl<'a> DoubleEndedIterator for MapxIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let (k, v) = self.db_iter.next_back()?;
        unsafe {
            *((&self.kv_cache) as *const (RawKey, RawValue)
                as *mut (RawKey, RawValue)) = (k, v);
        }
        Some((&self.kv_cache.0[..], &self.kv_cache.0[..]))
    }
}

pub struct MapxIntoIter {
    db_iter: EngineIter,
    hdr: Mapx,
}

impl IntoIterator for MapxIntoIter {
    type Item = (RawKey, RawValue);

    fn into_iter(self) -> Self::IntoIter {
        self.db_iter
    }
}

impl Drop for MapxIntoIter {
    fn drop(&mut self) {
        let hdr = unsafe { self.hdr.shadow() };
        self.hdr.iter().for_each(|(k, _)| {
            hdr.remove(k);
        });
    }
}

pub struct MapxIterMut<'a> {
    prefix: PreBytes,
    db_iter: EngineIter,
    lru: &'a mut LruCache<RawKey, RawValue>,
}

impl<'a> Iterator for MapxIterMut<'a> {
    type Item = (&'a [u8], ValueMut<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let (k, v) = self.db_iter.next()?;

        let v = unsafe {
            let mut lru = self.lru as *const LruCache<RawKey, RawValue>
                as *mut LruCache<RawKey, RawValue>;
            if let Some(v) = lru.get_mut(&k) {
                v
            } else {
                lru.put(k.clone(), v);
                lru.get_mut(&k).unwrap()
            }
        };

        let vmut = ValueMut {
            prefix: self.prefix,
            key: k,
            value: v,
        };

        Some((&vmut.key[..], vmut))
    }
}

impl<'a> DoubleEndedIterator for MapxIterMut<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let (k, v) = self.db_iter.next_back()?;

        let v = unsafe {
            let mut lru = self.lru as *const LruCache<RawKey, RawValue>
                as *mut LruCache<RawKey, RawValue>;
            if let Some(v) = lru.get_mut(&k) {
                v
            } else {
                lru.put(k.clone(), v);
                lru.get_mut(&k).unwrap()
            }
        };

        let vmut = ValueMut {
            prefix: self.prefix,
            key: k,
            value: v,
        };

        Some((&vmut.key[..], vmut))
    }
}

pub struct ValueMut<'a> {
    prefix: PreBytes,
    key: RawKey,
    value: &'a mut RawValue,
}

impl<'a> Drop for ValueMut<'a> {
    fn drop(&'a mut self) {
        VSDB.db.insert(self.prefix, &self.key[..], &self.value[..]);
    }
}

impl<'a> Deref for ValueMut<'a> {
    type Target = RawValue;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<'a> DerefMut for ValueMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
