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
pub type MapxIter = rocks_db::RocksIter;

#[cfg(all(feature = "sled_engine", not(feature = "rocks_engine")))]
pub type MapxIter = sled_db::SledIter;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

use crate::common::{
    ende::{SimpleVisitor, ValueEnDe},
    BranchID, Pre, PreBytes, RawValue, VersionID, VSDB,
};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{ops::RangeBounds, result::Result as StdResult};

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

    fn iter(&self, meta_prefix: PreBytes) -> MapxIter;

    fn range<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> MapxIter;

    fn get(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue>;

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

#[derive(Clone, Copy, Eq, Debug)]
pub(crate) struct Mapx {
    // the unique ID of each instance
    prefix: PreBytes,
}

impl Mapx {
    #[inline(always)]
    pub(crate) fn new() -> Self {
        let prefix = VSDB.db.alloc_prefix();

        let prefix_bytes = prefix.to_be_bytes();

        assert!(VSDB.db.iter(prefix_bytes).next().is_none());

        VSDB.db.set_instance_len(prefix_bytes, 0);

        Mapx {
            prefix: prefix_bytes,
        }
    }

    fn get_instance_cfg(&self) -> InstanceCfg {
        InstanceCfg::from(self)
    }

    #[inline(always)]
    pub(crate) fn get(&self, key: &[u8]) -> Option<RawValue> {
        VSDB.db.get(self.prefix, key)
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
        VSDB.db.iter(self.prefix)
    }

    #[inline(always)]
    pub(crate) fn range<'a, R: RangeBounds<&'a [u8]>>(&'a self, bounds: R) -> MapxIter {
        VSDB.db.range(self.prefix, bounds)
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
