//!
//! A `BTreeMap`-like structure but storing data in disk.
//!
//! NOTE:
//! - Keys will **NOT** be encoded in this structure, but values will be
//!     - Values will be encoded by some `serde`-like methods
//! - It's your duty to ensure that the encoded key keeps a same order with the original key
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_ord_rawkey::MapxOrdRawKey;
//!
//! let dir = format!("/tmp/__vsdb__{}", rand::random::<u128>());
//! vsdb::vsdb_set_base_dir(&dir);
//!
//! let mut l = MapxOrdRawKey::new();
//!
//! l.insert_ref(&[1], &0);
//! l.insert(vec![1].into_boxed_slice(), 0);
//! l.insert_ref(&[2], &0);
//!
//! l.iter().for_each(|(_, v)| {
//!     assert_eq!(v, 0);
//! });
//!
//! l.remove(&[2]);
//! assert_eq!(l.len(), 1);
//!
//! l.clear();
//! assert_eq!(l.len(), 0);
//! ```
//!

#[cfg(test)]
mod test;

use crate::{
    basic::mapx_raw::{self, MapxRaw, MapxRawIter},
    common::{ende::ValueEnDe, RawKey},
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Bound, Deref, DerefMut, RangeBounds},
};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxOrdRawKey<V> {
    inner: MapxRaw,
    p: PhantomData<V>,
}

impl<V> MapxOrdRawKey<V>
where
    V: ValueEnDe,
{
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        Self {
            inner: self.inner.shadow(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn new() -> Self {
        MapxOrdRawKey {
            inner: MapxRaw::new(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<V> {
        self.inner
            .get(key)
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<ValueMut<'_, V>> {
        self.inner.get_mut(key).map(|inner| ValueMut {
            value: <V as ValueEnDe>::decode(&*inner).unwrap(),
            inner,
        })
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.inner.contains_key(key)
    }

    #[inline(always)]
    pub fn get_le(&self, key: &[u8]) -> Option<(RawKey, V)> {
        self.inner
            .get_le(key)
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn get_ge(&self, key: &[u8]) -> Option<(RawKey, V)> {
        self.inner
            .get_ge(key)
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    pub fn insert(&mut self, key: RawKey, value: V) -> Option<V> {
        self.insert_ref(&key, &value)
    }

    #[inline(always)]
    pub fn insert_ref(&mut self, key: &[u8], value: &V) -> Option<V> {
        self.inner
            .insert(key, &value.encode())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    // used to support efficient versioned-implementations
    #[inline(always)]
    pub(crate) fn insert_ref_encoded_value(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Option<V> {
        self.inner
            .insert(key, value)
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn set_value(&mut self, key: RawKey, value: V) {
        self.set_value_ref(&key, &value);
    }

    #[inline(always)]
    pub fn set_value_ref(&mut self, key: &[u8], value: &V) {
        self.inner.insert(key, &value.encode());
    }

    #[inline(always)]
    pub fn entry(&mut self, key: RawKey) -> Entry<'_, V> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn entry_ref<'a>(&'a mut self, key: &'a [u8]) -> EntryRef<'a, V> {
        EntryRef { key, hdr: self }
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxOrdRawKeyIter<V> {
        MapxOrdRawKeyIter {
            inner: self.inner.iter(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range<R: RangeBounds<RawKey>>(&self, bounds: R) -> MapxOrdRawKeyIter<V> {
        let start = match bounds.start_bound() {
            Bound::Included(s) => Bound::Included(&s[..]),
            Bound::Excluded(s) => Bound::Excluded(&s[..]),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = match bounds.end_bound() {
            Bound::Included(e) => Bound::Included(&e[..]),
            Bound::Excluded(e) => Bound::Excluded(&e[..]),
            Bound::Unbounded => Bound::Unbounded,
        };

        self.range_ref((start, end))
    }

    #[inline(always)]
    pub fn range_ref<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdRawKeyIter<V> {
        MapxOrdRawKeyIter {
            inner: self.inner.range(bounds),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn iter_mut(&self) -> MapxOrdRawKeyIterMut<V> {
        MapxOrdRawKeyIterMut {
            inner: self.inner.iter(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_mut<R: RangeBounds<RawKey>>(
        &mut self,
        bounds: R,
    ) -> MapxOrdRawKeyIter<V> {
        let start = match bounds.start_bound() {
            Bound::Included(s) => Bound::Included(&s[..]),
            Bound::Excluded(s) => Bound::Excluded(&s[..]),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = match bounds.end_bound() {
            Bound::Included(e) => Bound::Included(&e[..]),
            Bound::Excluded(e) => Bound::Excluded(&e[..]),
            Bound::Unbounded => Bound::Unbounded,
        };

        self.range_ref_mut((start, end))
    }

    #[inline(always)]
    pub fn range_ref_mut<'a, R: RangeBounds<&'a [u8]>>(
        &'a mut self,
        bounds: R,
    ) -> MapxOrdRawKeyIter<V> {
        MapxOrdRawKeyIterMut {
            inner: self.inner.range_mut(bounds),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first(&self) -> Option<(RawKey, V)> {
        self.iter().next()
    }

    #[inline(always)]
    pub fn last(&self) -> Option<(RawKey, V)> {
        self.iter().next_back()
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        self.inner
            .remove(key)
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn unset_value(&mut self, key: &[u8]) {
        self.inner.remove(key);
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<V> Clone for MapxOrdRawKey<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            p: PhantomData,
        }
    }
}

impl<V> Default for MapxOrdRawKey<V>
where
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct ValueMut<'a, V>
where
    V: ValueEnDe,
{
    value: V,
    inner: mapx_raw::ValueMut<'a>,
}

impl<'a, V> Drop for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        *self.inner = self.value.encode();
    }
}

impl<'a, V> Deref for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, V> DerefMut for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub struct Entry<'a, V>
where
    V: 'a + ValueEnDe,
{
    pub key: RawKey,
    pub hdr: &'a mut MapxOrdRawKey<V>,
}

impl<'a, V> Entry<'a, V>
where
    V: ValueEnDe,
{
    pub fn or_insert(self, default: V) -> ValueMut<'a, V> {
        if !self.hdr.contains_key(&self.key) {
            self.hdr.set_value_ref(&self.key, &default);
        }
        pnk!(self.hdr.get_mut(&self.key))
    }
}

pub struct EntryRef<'a, V>
where
    V: ValueEnDe,
{
    key: &'a [u8],
    hdr: &'a mut MapxOrdRawKey<V>,
}

impl<'a, V> EntryRef<'a, V>
where
    V: ValueEnDe,
{
    pub fn or_insert_ref(self, default: &V) -> ValueMut<'a, V> {
        if !self.hdr.contains_key(self.key) {
            self.hdr.set_value_ref(self.key, default);
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}

pub struct MapxOrdRawKeyIter<'a, V>
where
    V: ValueEnDe,
{
    inner: MapxRawIter<'a>,
    p: PhantomData<V>,
}

impl<'a, V> Iterator for MapxOrdRawKeyIter<'a, V>
where
    V: ValueEnDe,
{
    type Item = (RawKey, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }
}

impl<'a, V> DoubleEndedIterator for MapxOrdRawKeyIter<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }
}

impl<'a, V> ExactSizeIterator for MapxOrdRawKeyIter<'a, V> where V: ValueEnDe {}

pub struct MapxOrdRawKeyValues<'a, V>
where
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyIter<'a, V>,
}

impl<'a, V> Iterator for MapxOrdRawKeyValues<'a, V>
where
    V: ValueEnDe,
{
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<'a, V> DoubleEndedIterator for MapxOrdRawKeyValues<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| v)
    }
}

impl<'a, V> ExactSizeIterator for MapxOrdRawKeyValues<'a, V> where V: ValueEnDe {}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub struct MapxOrdRawKeyIterMut<'a, V>
where
    V: ValueEnDe,
{
    inner: mapx_raw::MapxRawIterMut<'a>,
    p: PhantomData<V>,
}

impl<'a, V> Iterator for MapxOrdRawKeyIterMut<'a, V>
where
    V: ValueEnDe,
{
    type Item = (RawKey, ValueIterMut<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| {
            (
                k,
                ValueIterMut {
                    value: <V as ValueEnDe>::decode(&*v).unwrap(),
                    inner: v,
                },
            )
        })
    }
}

impl<'a, V> DoubleEndedIterator for MapxOrdRawKeyIterMut<'a, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(k, v)| {
            (
                k,
                ValueIterMut {
                    value: <V as ValueEnDe>::decode(&*v).unwrap(),
                    inner: v,
                },
            )
        })
    }
}

impl<'a, V> ExactSizeIterator for MapxOrdRawKeyIter<'a, V> where V: ValueEnDe {}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    value: V,
    inner: mapx_raw::ValueIterMut<'a>,
}

impl<'a, V> Drop for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        *self.inner = self.value.encode();
    }
}

impl<'a, V> Deref for ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, V> DerefMut for ValueIterMut<'a, V>
where
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
