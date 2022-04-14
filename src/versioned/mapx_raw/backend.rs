//!
//! Core logic of the version managements.
//!

use crate::{
    basic::{
        mapx_ord::MapxOrd,
        mapx_ord_rawkey::{MapxOrdRawKey, MapxOrdRawKeyIter},
        mapx_ord_rawvalue::MapxOrdRawValue,
        mapx_raw::MapxRaw,
    },
    common::{
        ende::encode_optioned_bytes, BranchID, BranchName, RawKey, RawValue, VersionID,
        VersionName, INITIAL_BRANCH_ID, INITIAL_BRANCH_NAME, INITIAL_VERSION, NULL,
        RESERVED_VERSION_NUM_DEFAULT, VSDB,
    },
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashSet, ops::RangeBounds};

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct MapxRawVs {
    default_branch: BranchID,

    branch_name_to_branch_id: MapxOrdRawKey<BranchID>,
    version_name_to_version_id: MapxOrdRawKey<VersionID>,

    branch_id_to_branch_name: MapxOrdRawValue<BranchID>,
    version_id_to_version_name: MapxOrdRawValue<VersionID>,

    // versions on this branch,
    // created dirctly by it or inherited from its ancestors
    branch_to_its_versions: MapxOrd<BranchID, MapxOrd<VersionID, ()>>,

    // globally ever changed keys(no value is stored here!) within each version
    version_to_change_set: MapxOrd<VersionID, MapxRaw>,

    // key -> multi-version(global unique) -> multi-value
    layered_kv: MapxOrdRawKey<MapxOrd<VersionID, Option<RawValue>>>,
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

impl MapxRawVs {
    #[inline(always)]
    pub(super) fn new() -> Self {
        let mut ret = Self {
            default_branch: BranchID::default(),
            branch_name_to_branch_id: MapxOrdRawKey::new(),
            version_name_to_version_id: MapxOrdRawKey::new(),
            branch_id_to_branch_name: MapxOrdRawValue::new(),
            version_id_to_version_name: MapxOrdRawValue::new(),
            branch_to_its_versions: MapxOrd::new(),
            version_to_change_set: MapxOrd::new(),
            layered_kv: MapxOrdRawKey::new(),
        };
        ret.init();
        ret
    }

    #[inline(always)]
    fn init(&mut self) {
        self.default_branch = INITIAL_BRANCH_ID;
        self.branch_name_to_branch_id
            .insert_ref(INITIAL_BRANCH_NAME.0, &INITIAL_BRANCH_ID);
        self.branch_id_to_branch_name
            .insert_ref(&INITIAL_BRANCH_ID, INITIAL_BRANCH_NAME.0);
        self.branch_to_its_versions
            .insert(INITIAL_BRANCH_ID, MapxOrd::new());
        self.version_create(INITIAL_VERSION.0).unwrap();
    }

    #[inline(always)]
    pub(super) fn insert(&self, key: &[u8], value: &[u8]) -> Result<Option<RawValue>> {
        self.insert_by_branch(key, value, self.branch_get_default())
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn insert_by_branch(
        &self,
        key: &[u8],
        value: &[u8],
        branch_id: BranchID,
    ) -> Result<Option<RawValue>> {
        self.branch_to_its_versions
            .get(&branch_id)
            .c(d!("branch not found"))?
            .last()
            .c(d!("no version on this branch, create a version first"))
            .and_then(|(version_id, _)| {
                self.insert_by_branch_version(key, value, branch_id, version_id)
                    .c(d!())
            })
    }

    // This function should **NOT** be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    #[inline(always)]
    fn insert_by_branch_version(
        &self,
        key: &[u8],
        value: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_branch_version(key, Some(value), branch_id, version_id)
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn remove(&self, key: &[u8]) -> Result<Option<RawValue>> {
        self.remove_by_branch(key, self.branch_get_default())
            .c(d!())
    }

    #[inline(always)]
    pub(super) fn remove_by_branch(
        &self,
        key: &[u8],
        branch_id: BranchID,
    ) -> Result<Option<RawValue>> {
        self.branch_to_its_versions
            .get(&branch_id)
            .c(d!("branch not found"))?
            .last()
            .c(d!("no version on this branch, create a version first"))
            .and_then(|(version_id, _)| {
                self.remove_by_branch_version(key, branch_id, version_id)
                    .c(d!())
            })
    }

    // This function should **NOT** be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    //
    // The `remove` is essentially assign a `None` value to the key.
    fn remove_by_branch_version(
        &self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        self.write_by_branch_version(key, None, branch_id, version_id)
            .c(d!())
    }

    // This function should **NOT** be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    fn write_by_branch_version(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Result<Option<RawValue>> {
        let ret = self.get_by_branch_version(key, branch_id, version_id);

        // remove a non-existing value
        if value.is_none() && ret.is_none() {
            return Ok(None);
        }

        // NOTE: the value needs not to be stored here
        self.version_to_change_set
            .get_mut(&version_id)
            .c(d!())?
            .insert(key, &[]);

        self.layered_kv
            .entry_ref(key)
            .or_insert_ref(&MapxOrd::new())
            .insert_ref_encoded_value(&version_id, &encode_optioned_bytes(&value)[..]);

        Ok(ret)
    }

    #[inline(always)]
    pub(super) fn get(&self, key: &[u8]) -> Option<RawValue> {
        self.get_by_branch(key, self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn get_by_branch(
        &self,
        key: &[u8],
        branch_id: BranchID,
    ) -> Option<RawValue> {
        if let Some(vers) = self.branch_to_its_versions.get(&branch_id) {
            if let Some(version_id) = vers.last().map(|(id, _)| id) {
                return self.get_by_branch_version(key, branch_id, version_id);
            }
        }
        None
    }

    #[inline(always)]
    pub(super) fn get_by_branch_version(
        &self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Option<RawValue> {
        let vers = self.branch_to_its_versions.get(&branch_id)?;
        self.layered_kv
            .get(key)?
            .range(..=version_id)
            .rev()
            .find(|(ver, _)| vers.contains_key(ver))
            .and_then(|(_, value)| value)
    }

    #[inline(always)]
    pub(super) fn get_ge(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range_ref(key..).next()
    }

    #[inline(always)]
    pub(super) fn get_ge_by_branch(
        &self,
        key: &[u8],
        branch_id: BranchID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_ref_by_branch(branch_id, key..).next()
    }

    #[inline(always)]
    pub(super) fn get_ge_by_branch_version(
        &self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_ref_by_branch_version(branch_id, version_id, key..)
            .next()
    }

    #[inline(always)]
    pub(super) fn get_le(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range_ref(..=key).next_back()
    }

    #[inline(always)]
    pub(super) fn get_le_by_branch(
        &self,
        key: &[u8],
        branch_id: BranchID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_ref_by_branch(branch_id, ..=key).next_back()
    }

    #[inline(always)]
    pub(super) fn get_le_by_branch_version(
        &self,
        key: &[u8],
        branch_id: BranchID,
        version_id: VersionID,
    ) -> Option<(RawKey, RawValue)> {
        self.range_ref_by_branch_version(branch_id, version_id, ..=key)
            .next_back()
    }

    #[inline(always)]
    pub(super) fn iter(&self) -> MapxRawVsIter {
        self.iter_by_branch(self.branch_get_default())
    }

    #[inline(always)]
    pub(super) fn iter_by_branch(&self, branch_id: BranchID) -> MapxRawVsIter {
        if let Some(vers) = self.branch_to_its_versions.get(&branch_id) {
            if let Some((version_id, _)) = vers.last() {
                return self.iter_by_branch_version(branch_id, version_id);
            }
        }

        MapxRawVsIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            branch_id: NULL,
            version_id: NULL,
        }
    }

    #[inline(always)]
    pub(super) fn iter_by_branch_version(
        &self,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> MapxRawVsIter {
        MapxRawVsIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            branch_id,
            version_id,
        }
    }

    #[inline(always)]
    pub(super) fn range<'a, R: 'a + RangeBounds<RawKey>>(
        &'a self,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        self.range_by_branch(self.branch_get_default(), bounds)
    }

    #[inline(always)]
    pub(super) fn range_by_branch<'a, R: 'a + RangeBounds<RawKey>>(
        &'a self,
        branch_id: BranchID,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        if let Some(vers) = self.branch_to_its_versions.get(&branch_id) {
            if let Some((version_id, _)) = vers.last() {
                return self.range_by_branch_version(branch_id, version_id, bounds);
            }
        }

        // An empty `Iter`
        MapxRawVsIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            branch_id,
            version_id: NULL,
        }
    }

    #[inline(always)]
    pub(super) fn range_by_branch_version<'a, R: 'a + RangeBounds<RawKey>>(
        &'a self,
        branch_id: BranchID,
        version_id: VersionID,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        MapxRawVsIter {
            hdr: self,
            iter: self.layered_kv.range(bounds),
            branch_id,
            version_id,
        }
    }

    #[inline(always)]
    pub(super) fn range_ref<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        self.range_ref_by_branch(self.branch_get_default(), bounds)
    }

    #[inline(always)]
    pub(super) fn range_ref_by_branch<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        branch_id: BranchID,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        if let Some(vers) = self.branch_to_its_versions.get(&branch_id) {
            if let Some((version_id, _)) = vers.last() {
                return self.range_ref_by_branch_version(branch_id, version_id, bounds);
            }
        }

        // An empty `Iter`
        MapxRawVsIter {
            hdr: self,
            iter: self.layered_kv.iter(),
            branch_id,
            version_id: NULL,
        }
    }

    #[inline(always)]
    pub(super) fn range_ref_by_branch_version<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        branch_id: BranchID,
        version_id: VersionID,
        bounds: R,
    ) -> MapxRawVsIter<'a> {
        MapxRawVsIter {
            hdr: self,
            iter: self.layered_kv.range_ref(bounds),
            branch_id,
            version_id,
        }
    }

    // NOTE: just a stupid O(n) counter, very slow!
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        self.iter().count()
    }

    // NOTE: just a stupid O(n) counter, very slow!
    #[inline(always)]
    pub(super) fn len_by_branch(&self, branch_id: BranchID) -> usize {
        self.iter_by_branch(branch_id).count()
    }

    // NOTE: just a stupid O(n) counter, very slow!
    #[inline(always)]
    pub(super) fn len_by_branch_version(
        &self,
        branch_id: BranchID,
        version_id: VersionID,
    ) -> usize {
        self.iter_by_branch_version(branch_id, version_id).count()
    }

    // Clear all data, for testing purpose.
    #[inline(always)]
    pub(super) fn clear(&mut self) {
        self.branch_name_to_branch_id.clear();
        self.version_name_to_version_id.clear();
        self.branch_id_to_branch_name.clear();
        self.version_id_to_version_name.clear();
        self.branch_to_its_versions.clear();
        self.version_to_change_set.clear();
        self.layered_kv.clear();

        self.init();
    }

    #[inline(always)]
    pub(super) fn version_create(&self, version_name: &[u8]) -> Result<()> {
        self.version_create_by_branch(version_name, self.branch_get_default())
            .c(d!())
    }

    pub(super) fn version_create_by_branch(
        &self,
        version_name: &[u8],
        branch_id: BranchID,
    ) -> Result<()> {
        if self.version_name_to_version_id.get(version_name).is_some() {
            return Err(eg!("version already exists"));
        }

        let vers = self
            .branch_to_its_versions
            .get_mut(&branch_id)
            .c(d!("branch not found"))?;

        let version_id = VSDB.alloc_version_id();
        vers.insert(version_id, ());

        self.version_name_to_version_id
            .insert_ref(version_name, &version_id);
        self.version_id_to_version_name
            .insert_ref(&version_id, version_name);
        self.version_to_change_set
            .insert(version_id, MapxRaw::new());

        Ok(())
    }

    // Check if a verison exists on the default branch
    #[inline(always)]
    pub(super) fn version_exists(&self, version_id: BranchID) -> bool {
        self.version_exists_on_branch(version_id, self.branch_get_default())
    }

    // Check if a version exists on a specified branch
    #[inline(always)]
    pub(super) fn version_exists_on_branch(
        &self,
        version_id: VersionID,
        branch_id: BranchID,
    ) -> bool {
        self.branch_to_its_versions
            .get(&branch_id)
            .map(|vers| vers.contains_key(&version_id))
            .unwrap_or(false)
    }

    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn version_pop(&self) -> Result<()> {
        self.version_pop_by_branch(self.branch_get_default())
            .c(d!())
    }

    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn version_pop_by_branch(&self, branch_id: BranchID) -> Result<()> {
        if let Some((version_id, _)) = self
            .branch_to_its_versions
            .get(&branch_id)
            .c(d!("branch not found"))?
            .iter()
            .rev()
            .next()
        {
            self.version_remove_by_branch(version_id, branch_id).c(d!())
        } else {
            Ok(())
        }
    }

    // This function should **NOT** be public,
    // `write`-like operations should only be applied
    // on the latest version of every branch,
    // historical data version should be immutable in the user view.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    fn version_remove_by_branch(
        &self,
        version_id: VersionID,
        branch_id: BranchID,
    ) -> Result<()> {
        if self
            .branch_to_its_versions
            .get(&branch_id)
            .c(d!("branch not found"))?
            .remove(&version_id)
            .is_none()
        {
            return Err(eg!("version is not on this branch"));
        }

        self.clean_up_version_if_orphaned(version_id).c(d!())
    }

    // # Safety
    //
    // It's the caller's duty to ensure that
    // the `base_version` was created directly by the `branch_id`,
    // or the data records of other branches may be corrupted.
    #[inline(always)]
    pub(super) unsafe fn version_rebase(&self, base_version: VersionID) -> Result<()> {
        self.version_rebase_by_branch(base_version, self.branch_get_default())
            .c(d!())
    }

    // # Safety
    //
    // It's the caller's duty to ensure that
    // the `base_version` was created directly by the `branch_id`,
    // or the data records of other branches may be corrupted.
    pub(super) unsafe fn version_rebase_by_branch(
        &self,
        base_version: VersionID,
        branch_id: BranchID,
    ) -> Result<()> {
        let vers_hdr = self
            .branch_to_its_versions
            .get(&branch_id)
            .c(d!("branch not found"))?;
        let mut vers = vers_hdr.range(base_version..).map(|(ver, _)| ver);

        if let Some(ver) = vers.next() {
            if base_version != ver {
                return Err(eg!("base version is not on this branch"));
            }
        } else {
            return Err(eg!("base version is not on this branch"));
        };

        let base_ver_chg_set = self.version_to_change_set.get(&base_version).c(d!())?;
        let vers_to_be_merged = vers.collect::<Vec<_>>();

        for verid in vers_to_be_merged.iter() {
            // we do not call `clear()` on the discarded instance for performance reason.
            for (k, _) in self.version_to_change_set.remove(verid).c(d!())?.iter() {
                base_ver_chg_set.insert(&k, &[]);
                self.layered_kv.get(&k).c(d!()).and_then(|hdr| {
                    hdr.remove(verid)
                        .c(d!())
                        .map(|v| hdr.insert(base_version, v))
                })?;
            }

            vers_hdr.remove(verid).c(d!())?;

            self.version_id_to_version_name
                .remove(verid)
                .c(d!())
                .and_then(|vername| {
                    self.version_name_to_version_id.remove(&vername).c(d!())
                })?;
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_create(
        &self,
        branch_name: &[u8],
        version_name: &[u8],
    ) -> Result<()> {
        self.branch_create_by_base_branch(
            branch_name,
            version_name,
            self.branch_get_default(),
        )
        .c(d!())
    }

    #[inline(always)]
    pub(super) fn branch_create_by_base_branch(
        &self,
        branch_name: &[u8],
        version_name: &[u8],
        base_branch_id: BranchID,
    ) -> Result<()> {
        if self.version_name_to_version_id.contains_key(version_name) {
            return Err(eg!("this version already exists"));
        }

        let base_version_id = self
            .branch_to_its_versions
            .get(&base_branch_id)
            .c(d!("base branch not found"))?
            .last()
            .map(|(version_id, _)| version_id)
            .c(d!("base version not found"))?;

        self.branch_create_by_base_branch_version(
            branch_name,
            version_name,
            base_branch_id,
            base_version_id,
        )
        .c(d!())
    }

    pub(super) fn branch_create_by_base_branch_version(
        &self,
        branch_name: &[u8],
        version_name: &[u8],
        base_branch_id: BranchID,
        base_version_id: VersionID,
    ) -> Result<()> {
        if self.branch_name_to_branch_id.contains_key(branch_name) {
            return Err(eg!("branch already exists"));
        }

        let vers = self
            .branch_to_its_versions
            .get(&base_branch_id)
            .c(d!("base branch not exist"))?;
        if !vers.contains_key(&base_version_id) {
            return Err(eg!("version is not on the base branch"));
        }

        let branch_id = VSDB.alloc_branch_id();

        self.branch_name_to_branch_id
            .insert_ref(branch_name, &branch_id);
        self.branch_id_to_branch_name
            .insert_ref(&branch_id, branch_name);

        let vers_copied =
            vers.range(..=base_version_id)
                .fold(MapxOrd::new(), |acc, (k, v)| {
                    acc.insert(k, v);
                    acc
                });
        self.branch_to_its_versions.insert(branch_id, vers_copied);

        // create the first version of the new branch
        self.version_create_by_branch(version_name, branch_id)
            .c(d!())
    }

    // Check if a branch exists or not.
    #[inline(always)]
    pub(super) fn branch_exists(&self, branch_id: BranchID) -> bool {
        let condition_1 = self.branch_id_to_branch_name.contains_key(&branch_id);
        let condition_2 = self.branch_to_its_versions.contains_key(&branch_id);
        assert_eq!(condition_1, condition_2);
        condition_1
    }

    // Check if a branch exists and has versions on it.
    #[inline(always)]
    pub(super) fn branch_has_versions(&self, branch_id: BranchID) -> bool {
        self.branch_exists(branch_id) && !self.branch_to_its_versions.is_empty()
    }

    // Remove all changes directly made by this branch, and delete the branch itself.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn branch_remove(&self, branch_id: BranchID) -> Result<()> {
        // if self.branch_get_default() == branch_id {
        //     return Err(eg!("the default branch can NOT be removed"));
        // }

        self.branch_truncate(branch_id).c(d!())?;

        self.branch_id_to_branch_name
            .get(&branch_id)
            .c(d!())
            .and_then(|brname| self.branch_name_to_branch_id.remove(&brname).c(d!()))?;

        // we do not call `clear()` on the discarded instance for performance reason.
        self.branch_to_its_versions
            .remove(&branch_id)
            .c(d!())
            .map(|_| ())
    }

    // Remove all changes directly made by this branch, but keep its meta infomation.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn branch_truncate(&self, branch_id: BranchID) -> Result<()> {
        self.branch_truncate_to(branch_id, VersionID::MIN).c(d!())
    }

    // Remove all changes directly made by versions(bigger than `last_version_id`) of this branch.
    //
    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    pub(super) fn branch_truncate_to(
        &self,
        branch_id: BranchID,
        last_version_id: VersionID,
    ) -> Result<()> {
        if let Some(vers) = self.branch_to_its_versions.get(&branch_id) {
            // version id must be in descending order
            for (version_id, _) in vers.range((1 + last_version_id)..).rev() {
                self.version_remove_by_branch(version_id, branch_id)
                    .c(d!())?;
            }
            Ok(())
        } else {
            Err(eg!("branch not found"))
        }
    }

    // 'Write'-like operations on branches and versions are different from operations on data.
    //
    // 'Write'-like operations on data require recursive tracing of all parent nodes,
    // while operations on branches and versions are limited to their own perspective,
    // and should not do any tracing.
    #[inline(always)]
    pub(super) fn branch_pop_version(&self, branch_id: BranchID) -> Result<()> {
        self.version_pop_by_branch(branch_id).c(d!())
    }

    #[inline(always)]
    pub(super) fn branch_merge_to(
        &self,
        branch_id: BranchID,
        target_branch_id: BranchID,
    ) -> Result<()> {
        unsafe { self.do_branch_merge_to(branch_id, target_branch_id, false) }
    }

    // # Safety
    //
    // If new different versions have been created on the target branch,
    // the data records referenced by other branches may be corrupted.
    #[inline(always)]
    pub(super) unsafe fn branch_merge_to_force(
        &self,
        branch_id: BranchID,
        target_branch_id: BranchID,
    ) -> Result<()> {
        self.do_branch_merge_to(branch_id, target_branch_id, true)
    }

    // Merge a branch into another,
    // even if new different versions have been created on the target branch.
    //
    // # Safety
    //
    // If new different versions have been created on the target branch,
    // the data records referenced by other branches may be corrupted.
    unsafe fn do_branch_merge_to(
        &self,
        branch_id: BranchID,
        target_branch_id: BranchID,
        force: bool,
    ) -> Result<()> {
        let vers = self
            .branch_to_its_versions
            .get(&branch_id)
            .c(d!("branch not found"))?;
        let target_vers = self
            .branch_to_its_versions
            .get(&target_branch_id)
            .c(d!("target branch not found"))?;

        if !force {
            if let Some((ver, _)) = target_vers.last() {
                if !vers.contains_key(&ver) {
                    // Some new versions have been generated on the target branch
                    return Err(eg!("unable to merge safely"));
                }
            }
        }

        if let Some(fork_point) = vers
            .iter()
            .zip(target_vers.iter())
            .find(|(a, b)| a.0 != b.0)
        {
            vers.range(fork_point.0.0..).for_each(|(ver, _)| {
                target_vers.insert(ver, ());
            });
        } else if let Some((latest_ver, _)) = vers.last() {
            if let Some((target_latest_ver, _)) = target_vers.last() {
                match latest_ver.cmp(&target_latest_ver) {
                    Ordering::Equal => {
                        // no differences between the two branches
                        return Ok(());
                    }
                    Ordering::Greater => {
                        vers.range((1 + target_latest_ver)..)
                            .map(|(ver, _)| ver)
                            .for_each(|ver| {
                                target_vers.insert(ver, ());
                            });
                    }
                    _ => {}
                }
            } else {
                // target branch is empty, copy all versions to it
                vers.iter().for_each(|(ver, _)| {
                    target_vers.insert(ver, ());
                });
            }
        } else {
            // nothing to be merges
            return Ok(());
        };

        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_set_default(&mut self, branch_id: BranchID) -> Result<()> {
        if !self.branch_exists(branch_id) {
            return Err(eg!("branch not found"));
        }
        self.default_branch = branch_id;
        Ok(())
    }

    #[inline(always)]
    pub(super) fn branch_get_default(&self) -> BranchID {
        self.default_branch
    }

    #[inline(always)]
    pub(super) fn prune(&self, reserved_ver_num: Option<usize>) -> Result<()> {
        let reserved_ver_num = reserved_ver_num.unwrap_or(RESERVED_VERSION_NUM_DEFAULT);
        if 0 == reserved_ver_num {
            return Err(eg!("reserved version number should NOT be zero"));
        }

        let mut br_vers = self
            .branch_to_its_versions
            .iter()
            .map(|(_, vers)| vers.iter())
            .collect::<Vec<_>>();

        let last_idx = br_vers.len().saturating_sub(1);

        // filter out the longest common prefix
        let mut guard = 0;
        let mut vers_to_be_merged = vec![];
        'x: loop {
            for (idx, vers) in br_vers.iter_mut().enumerate() {
                if let Some((ver, _)) = vers.next() {
                    alt!(0 == idx, guard = ver);
                    alt!(guard != ver, break 'x);
                    alt!(last_idx == idx, vers_to_be_merged.push(guard));
                } else {
                    break 'x;
                }
            }
        }

        let (vers_to_be_merged, rewrite_ver) = {
            let l = vers_to_be_merged.len();
            if l > reserved_ver_num {
                let guard_idx = l - reserved_ver_num;
                (
                    &vers_to_be_merged[..guard_idx],
                    &vers_to_be_merged[guard_idx],
                )
            } else {
                return Ok(());
            }
        };

        let rewrite_ver_chgset = self.version_to_change_set.get(rewrite_ver).c(d!())?;

        for (_, vers) in self.branch_to_its_versions.iter() {
            for ver in vers_to_be_merged.iter() {
                vers.remove(ver).c(d!())?;
            }
        }

        for ver in vers_to_be_merged.iter() {
            self.version_id_to_version_name
                .remove(ver)
                .c(d!())
                .and_then(|vername| {
                    self.version_name_to_version_id.remove(&vername).c(d!())
                })?;
            for (k, _) in self.version_to_change_set.remove(ver).c(d!())?.iter() {
                let k_vers = self.layered_kv.get(&k).c(d!())?;
                let value = k_vers.remove(ver).c(d!())?;

                // keep at least one version
                if k_vers.range(..=rewrite_ver).next().is_none() {
                    assert!(rewrite_ver_chgset.insert(&k, &[]).is_none());
                    assert!(k_vers.insert_ref(rewrite_ver, &value).is_none());
                }
            }
        }

        let valid_vers = self
            .branch_to_its_versions
            .iter()
            .flat_map(|(_, vers)| vers.iter().map(|(ver, _)| ver))
            .collect::<HashSet<_>>();

        // clean up orphaned versions
        for (ver, chgset) in self
            .version_to_change_set
            .iter()
            .filter(|(ver, _)| !valid_vers.contains(ver))
        {
            for (k, _) in chgset.iter() {
                self.layered_kv.get(&k).c(d!())?.remove(&ver).c(d!())?;
            }
        }

        Ok(())
    }

    #[inline(always)]
    pub(super) fn get_branch_id(&self, branch_name: BranchName) -> Option<BranchID> {
        self.branch_name_to_branch_id.get(branch_name.0)
    }

    #[inline(always)]
    pub(super) fn get_version_id(&self, version_name: VersionName) -> Option<VersionID> {
        self.version_name_to_version_id.get(version_name.0)
    }

    #[inline(always)]
    fn clean_up_version_if_orphaned(&self, version_id: VersionID) -> Result<()> {
        if self
            .branch_to_its_versions
            .iter()
            .all(|(_, vers)| !vers.contains_key(&version_id))
        {
            if let Some(vername) = self.version_id_to_version_name.remove(&version_id) {
                self.version_name_to_version_id.remove(&vername).c(d!())?;
            }
        }
        Ok(())
    }
}

impl Default for MapxRawVs {
    fn default() -> Self {
        Self::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

pub struct MapxRawVsIter<'a> {
    hdr: &'a MapxRawVs,
    iter: MapxOrdRawKeyIter<MapxOrd<VersionID, Option<RawValue>>>,
    branch_id: BranchID,
    version_id: VersionID,
}

impl<'a> Iterator for MapxRawVsIter<'a> {
    type Item = (RawKey, RawValue);

    #[allow(clippy::while_let_on_iterator)]
    fn next(&mut self) -> Option<Self::Item> {
        if NULL == self.branch_id || NULL == self.version_id {
            return None;
        }

        while let Some((k, _)) = self.iter.next() {
            if let Some(v) =
                self.hdr
                    .get_by_branch_version(&k, self.branch_id, self.version_id)
            {
                return Some((k, v));
            }
        }

        None
    }
}

impl DoubleEndedIterator for MapxRawVsIter<'_> {
    #[allow(clippy::while_let_on_iterator)]
    fn next_back(&mut self) -> Option<Self::Item> {
        if NULL == self.branch_id || NULL == self.version_id {
            return None;
        }

        while let Some((k, _)) = self.iter.next_back() {
            if let Some(v) =
                self.hdr
                    .get_by_branch_version(&k, self.branch_id, self.version_id)
            {
                return Some((k, v));
            }
        }

        None
    }
}

impl ExactSizeIterator for MapxRawVsIter<'_> {}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
