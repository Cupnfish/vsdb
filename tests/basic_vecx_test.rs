use ruc::*;
use serde::{Deserialize, Serialize};
use vsdb::{vsdb_set_base_dir, ValueEnDe, Vecx};

#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SampleBlock {
    idx: usize,
    data: Vec<usize>,
}

fn gen_sample(idx: usize) -> SampleBlock {
    SampleBlock {
        idx,
        data: vec![idx],
    }
}

#[test]
fn basic_cases() {
    let cnt = 200;
    vsdb_set_base_dir("/tmp/.vsdb/basic_vecx_test/basic_cases").unwrap();
    let hdr = {
        let mut hdr = Vecx::new();

        assert_eq!(0, hdr.len());
        (0..cnt).for_each(|i| {
            assert!(hdr.get(i).is_none());
        });

        (0..cnt).map(|i| (i, gen_sample(i))).for_each(|(i, b)| {
            hdr.push_ref(&b);
            assert_eq!(1 + i as usize, hdr.len());
            assert_eq!(pnk!(hdr.get(i as usize)), b);
            assert_eq!(pnk!(hdr.last()), b);
        });

        assert_eq!(cnt, hdr.len());

        <Vecx<SampleBlock> as ValueEnDe>::encode(&hdr)
    };

    let mut reloaded = pnk!(<Vecx<SampleBlock> as ValueEnDe>::decode(&hdr));

    (0..cnt).for_each(|i| {
        assert_eq!(i, reloaded.get(i).unwrap().idx);
    });

    assert_eq!(cnt, reloaded.len());

    reloaded.update_ref(0, &gen_sample(100 * cnt)).unwrap();
    assert_eq!(cnt, reloaded.len());
    *reloaded.get_mut(0).unwrap() = gen_sample(999 * cnt);
    assert_eq!(reloaded.get(0).unwrap(), gen_sample(999 * cnt));

    reloaded.pop();
    assert_eq!(cnt - 1, reloaded.len());

    reloaded.clear();
    assert!(reloaded.is_empty());
}

#[test]
fn write() {
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let mut hdr = Vecx::new();

    hdr.insert(0, 0);
    assert_eq!(1, hdr.len());
    hdr.insert(0, 0);
    assert_eq!(2, hdr.len());

    hdr.update_ref(0, &1);
    assert_eq!(1, hdr.get(0).unwrap());
    hdr.update_ref(1, &1);
    assert_eq!(1, hdr.get(1).unwrap());

    hdr.push(2);
    assert_eq!(1, hdr.swap_remove(0));
    assert_eq!(2, hdr.len());
    assert_eq!(2, hdr.get(0).unwrap());

    hdr.push_ref(&3);
    assert_eq!(2, hdr.remove(0));
    assert_eq!(2, hdr.len());
    assert_eq!(3, hdr.get(1).unwrap());
}

#[test]
#[should_panic]
fn write_out_of_index_0() {
    let mut hdr = Vecx::new();
    hdr.insert_ref(100, &0);
}

#[test]
#[should_panic]
fn write_out_of_index_1() {
    let mut hdr = Vecx::new();
    hdr.insert(0, 0);
    hdr.insert_ref(100, &0);
}

#[test]
#[should_panic]
fn write_out_of_index_2() {
    let mut hdr = Vecx::new();
    hdr.update_ref(100, &0);
    hdr.insert(0, 0);
}

#[test]
#[should_panic]
fn write_out_of_index_3() {
    let mut hdr = Vecx::new();
    hdr.insert(0, 0);
    hdr.update_ref(100, &0);
}

#[test]
#[should_panic]
fn write_out_of_index_4() {
    let mut hdr = Vecx::new();
    hdr.remove(100);
    hdr.insert(0, 0);
}

#[test]
#[should_panic]
fn write_out_of_index_5() {
    let mut hdr = Vecx::new();
    hdr.insert(0, 0);
    hdr.remove(100);
}

#[test]
#[should_panic]
fn write_out_of_index_6() {
    let mut hdr = Vecx::new();
    hdr.swap_remove(100);
    hdr.insert(0, 0);
}

#[test]
#[should_panic]
fn write_out_of_index_7() {
    let mut hdr = Vecx::new();
    hdr.insert(0, 0);
    hdr.swap_remove(100);
}
