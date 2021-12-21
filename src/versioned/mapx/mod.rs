//!
//! Documents => [MapxRawVs](crate::versioned::mapx_raw)
//!

use crate::{
    common::ende::{KeyEnDe, ValueEnDe},
    versioned::mapx_ord_rawkey::{MapxOrdRawKeyVs, MapxOrdRawKeyVsIter},
    BranchName, ParentBranchName, VerChecksum, VersionName,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

/// Documents => [MapxRawVs](crate::versioned::mapx_raw::MapxRawVs)
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxVs<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyVs<V>,
    pk: PhantomData<K>,
}

impl<K, V> Default for MapxVs<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> MapxVs<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    #[inline(always)]
    pub fn new() -> Self {
        MapxVs {
            inner: MapxOrdRawKeyVs::new(),
            pk: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&key.encode())
    }

    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_le(&key.encode())
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), v))
    }

    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_ge(&key.encode())
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), v))
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
    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>> {
        self.insert_ref(&key, &value).c(d!())
    }

    #[inline(always)]
    pub fn insert_ref(&mut self, key: &K, value: &V) -> Result<Option<V>> {
        self.inner.insert_ref(&key.encode(), value).c(d!())
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxVsIter<K, V> {
        MapxVsIter {
            iter: self.inner.iter(),
            pk: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range<'a, R: 'a + RangeBounds<K>>(
        &'a self,
        bounds: R,
    ) -> MapxVsIter<'a, K, V> {
        let l = match bounds.start_bound() {
            Bound::Included(i) => Bound::Included(i.encode()),
            Bound::Excluded(i) => Bound::Excluded(i.encode()),
            _ => Bound::Unbounded,
        };
        let h = match bounds.end_bound() {
            Bound::Included(i) => Bound::Included(i.encode()),
            Bound::Excluded(i) => Bound::Excluded(i.encode()),
            _ => Bound::Unbounded,
        };

        MapxVsIter {
            iter: self.inner.range((l, h)),
            pk: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first(&self) -> Option<(K, V)> {
        self.iter().next()
    }

    #[inline(always)]
    pub fn last(&self) -> Option<(K, V)> {
        self.iter().next_back()
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(&key.encode())
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Result<Option<V>> {
        self.inner.remove(&key.encode()).c(d!())
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline(always)]
    pub fn get_by_branch(&self, key: &K, branch_name: BranchName) -> Option<V> {
        self.inner.get_by_branch(&key.encode(), branch_name)
    }

    #[inline(always)]
    pub fn get_le_by_branch(&self, key: &K, branch_name: BranchName) -> Option<(K, V)> {
        self.inner
            .get_le_by_branch(&key.encode(), branch_name)
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), v))
    }

    #[inline(always)]
    pub fn get_ge_by_branch(&self, key: &K, branch_name: BranchName) -> Option<(K, V)> {
        self.inner
            .get_ge_by_branch(&key.encode(), branch_name)
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), v))
    }

    #[inline(always)]
    pub fn len_by_branch(&self, branch_name: BranchName) -> usize {
        self.inner.len_by_branch(branch_name)
    }

    #[inline(always)]
    pub fn is_empty_by_branch(&self, branch_name: BranchName) -> bool {
        self.inner.is_empty_by_branch(branch_name)
    }

    #[inline(always)]
    pub fn insert_by_branch(
        &mut self,
        key: K,
        value: V,
        branch_name: BranchName,
    ) -> Result<Option<V>> {
        self.insert_ref_by_branch(&key, &value, branch_name).c(d!())
    }

    #[inline(always)]
    pub fn insert_ref_by_branch(
        &mut self,
        key: &K,
        value: &V,
        branch_name: BranchName,
    ) -> Result<Option<V>> {
        self.inner
            .insert_ref_by_branch(&key.encode(), value, branch_name)
            .c(d!())
    }

    #[inline(always)]
    pub fn iter_by_branch(&self, branch_name: BranchName) -> MapxVsIter<K, V> {
        MapxVsIter {
            iter: self.inner.iter_by_branch(branch_name),
            pk: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_by_branch<'a, R: 'a + RangeBounds<K>>(
        &'a self,
        bounds: R,
        branch_name: BranchName,
    ) -> MapxVsIter<'a, K, V> {
        let l = match bounds.start_bound() {
            Bound::Included(i) => Bound::Included(i.encode()),
            Bound::Excluded(i) => Bound::Excluded(i.encode()),
            _ => Bound::Unbounded,
        };
        let h = match bounds.end_bound() {
            Bound::Included(i) => Bound::Included(i.encode()),
            Bound::Excluded(i) => Bound::Excluded(i.encode()),
            _ => Bound::Unbounded,
        };

        MapxVsIter {
            iter: self.inner.range_by_branch(branch_name, (l, h)),
            pk: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first_by_branch(&self, branch_name: BranchName) -> Option<(K, V)> {
        self.iter_by_branch(branch_name).next()
    }

    #[inline(always)]
    pub fn last_by_branch(&self, branch_name: BranchName) -> Option<(K, V)> {
        self.iter_by_branch(branch_name).next_back()
    }

    #[inline(always)]
    pub fn contains_key_by_branch(&self, key: &K, branch_name: BranchName) -> bool {
        self.inner
            .contains_key_by_branch(&key.encode(), branch_name)
    }

    #[inline(always)]
    pub fn remove_by_branch(
        &mut self,
        key: &K,
        branch_name: BranchName,
    ) -> Result<Option<V>> {
        self.inner
            .remove_by_branch(&key.encode(), branch_name)
            .c(d!())
    }

    #[inline(always)]
    pub fn get_by_branch_version(
        &self,
        key: &K,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<V> {
        self.inner
            .get_by_branch_version(&key.encode(), branch_name, version_name)
    }

    #[inline(always)]
    pub fn get_le_by_branch_version(
        &self,
        key: &K,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<(K, V)> {
        self.inner
            .get_le_by_branch_version(&key.encode(), branch_name, version_name)
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), v))
    }

    #[inline(always)]
    pub fn get_ge_by_branch_version(
        &self,
        key: &K,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<(K, V)> {
        self.inner
            .get_ge_by_branch_version(&key.encode(), branch_name, version_name)
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), v))
    }

    #[inline(always)]
    pub fn len_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> usize {
        self.inner.len_by_branch_version(branch_name, version_name)
    }

    #[inline(always)]
    pub fn is_empty_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> bool {
        self.inner
            .is_empty_by_branch_version(branch_name, version_name)
    }

    #[inline(always)]
    pub fn iter_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> MapxVsIter<K, V> {
        MapxVsIter {
            iter: self.inner.iter_by_branch_version(branch_name, version_name),
            pk: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_by_branch_version<'a, R: 'a + RangeBounds<K>>(
        &'a self,
        bounds: R,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> MapxVsIter<'a, K, V> {
        let l = match bounds.start_bound() {
            Bound::Included(i) => Bound::Included(i.encode()),
            Bound::Excluded(i) => Bound::Excluded(i.encode()),
            _ => Bound::Unbounded,
        };
        let h = match bounds.end_bound() {
            Bound::Included(i) => Bound::Included(i.encode()),
            Bound::Excluded(i) => Bound::Excluded(i.encode()),
            _ => Bound::Unbounded,
        };

        MapxVsIter {
            iter: self
                .inner
                .range_by_branch_version(branch_name, version_name, (l, h)),
            pk: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<(K, V)> {
        self.iter_by_branch_version(branch_name, version_name)
            .next()
    }

    #[inline(always)]
    pub fn last_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<(K, V)> {
        self.iter_by_branch_version(branch_name, version_name)
            .next_back()
    }

    #[inline(always)]
    pub fn contains_key_by_branch_version(
        &self,
        key: &K,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> bool {
        self.inner.contains_key_by_branch_version(
            &key.encode(),
            branch_name,
            version_name,
        )
    }

    crate::impl_vs_methods!();
}

pub struct MapxVsIter<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    iter: MapxOrdRawKeyVsIter<'a, V>,
    pk: PhantomData<K>,
}

impl<'a, K, V> Iterator for MapxVsIter<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), v))
    }
}

impl<'a, K, V> DoubleEndedIterator for MapxVsIter<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|(k, v)| (pnk!(<K as KeyEnDe>::decode(&k)), v))
    }
}

impl<'a, K, V> ExactSizeIterator for MapxVsIter<'a, K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
}
