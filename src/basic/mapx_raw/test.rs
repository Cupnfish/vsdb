//!
//! # Test Cases
//!

use super::*;

#[test]
fn t_mapx_raw() {
    let cnt = 200;

    let hdr = {
        let mut hdr_i = crate::MapxRaw::new();

        assert_eq!(0, hdr_i.len());
        (0..cnt).for_each(|i: usize| {
            assert!(hdr_i.get(&i.to_be_bytes()).is_none());
        });

        (0..cnt)
            .map(|i: usize| (i.to_be_bytes(), i.to_be_bytes()))
            .for_each(|(i, b)| {
                hdr_i.entry(&i).or_insert(&b);
                assert_eq!(pnk!(hdr_i.get(&i)).as_ref(), &i);
                assert_eq!(hdr_i.remove(&i).unwrap().as_ref(), &b);
                assert!(hdr_i.get(&i).is_none());
                assert!(hdr_i.insert(&i, &b).is_none());
                assert!(hdr_i.insert(&i, &b).is_some());
            });

        assert_eq!(cnt, hdr_i.len());

        pnk!(bincode::serialize(&hdr_i))
    };

    let mut reloaded = pnk!(bincode::deserialize::<MapxRaw>(&hdr));

    assert_eq!(cnt, reloaded.len());

    (0..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        assert_eq!(&i, reloaded.get(&i).unwrap().as_ref());
    });

    (1..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        *pnk!(reloaded.get_mut(&i)) = IVec::from(&i);
        assert_eq!(pnk!(reloaded.get(&i)).as_ref(), &i);
        assert!(reloaded.contains_key(&i));
        assert!(reloaded.remove(&i).is_some());
        assert!(!reloaded.contains_key(&i));
    });

    assert_eq!(1, reloaded.len());
    reloaded.clear();
    assert!(reloaded.is_empty());

    reloaded.insert(&[1], &[1]);
    reloaded.insert(&[4], &[4]);
    reloaded.insert(&[6], &[6]);
    reloaded.insert(&[80], &[80]);

    assert!(reloaded.range(&[][..]..&[1][..]).next().is_none());
    assert_eq!(
        &[4],
        reloaded
            .range(&[2][..]..&[10][..])
            .next()
            .unwrap()
            .1
            .as_ref()
    );

    assert_eq!(&[80], reloaded.get_ge(&[79]).unwrap().1.as_ref());
    assert_eq!(&[80], reloaded.get_ge(&[80]).unwrap().1.as_ref());
    assert_eq!(&[80], reloaded.get_le(&[80]).unwrap().1.as_ref());
    assert_eq!(&[80], reloaded.get_le(&[100]).unwrap().1.as_ref());
}