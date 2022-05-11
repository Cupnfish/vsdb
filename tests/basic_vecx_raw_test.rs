use ruc::*;
use vsdb::{basic::vecx_raw::VecxRaw, ValueEnDe};

fn gen_sample(idx: usize) -> Box<[u8]> {
    idx.to_be_bytes().to_vec().into_boxed_slice()
}

#[test]
fn basic_cases() {
    let cnt = 200;

    let hdr = {
        let hdr = VecxRaw::new();

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

        <VecxRaw as ValueEnDe>::encode(&hdr)
    };

    let reloaded = pnk!(<VecxRaw as ValueEnDe>::decode(&hdr));

    (0..cnt).for_each(|i| {
        assert_eq!(gen_sample(i), reloaded.get(i).unwrap());
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
    let hdr = VecxRaw::new();

    hdr.insert(0, gen_sample(0));
    assert_eq!(1, hdr.len());
    hdr.insert(0, gen_sample(0));
    assert_eq!(2, hdr.len());

    hdr.update_ref(0, &gen_sample(1));
    assert_eq!(gen_sample(1), hdr.get(0).unwrap());
    hdr.update_ref(1, &gen_sample(1));
    assert_eq!(gen_sample(1), hdr.get(1).unwrap());

    hdr.push(gen_sample(2));
    assert_eq!(gen_sample(1), hdr.swap_remove(0));
    assert_eq!(2, hdr.len());
    assert_eq!(gen_sample(2), hdr.get(0).unwrap());

    hdr.push_ref(&gen_sample(3));
    assert_eq!(gen_sample(2), hdr.remove(0));
    assert_eq!(2, hdr.len());
    assert_eq!(gen_sample(3), hdr.get(1).unwrap());
}

#[test]
#[should_panic]
fn write_out_of_index_0() {
    let hdr = VecxRaw::new();
    hdr.insert_ref(100, &gen_sample(0));
}

#[test]
#[should_panic]
fn write_out_of_index_1() {
    let hdr = VecxRaw::new();
    hdr.insert(0, gen_sample(0));
    hdr.insert_ref(100, &gen_sample(0));
}

#[test]
#[should_panic]
fn write_out_of_index_2() {
    let hdr = VecxRaw::new();
    hdr.update_ref(100, &gen_sample(0));
    hdr.insert(0, gen_sample(0));
}

#[test]
#[should_panic]
fn write_out_of_index_3() {
    let hdr = VecxRaw::new();
    hdr.insert(0, gen_sample(0));
    hdr.update_ref(100, &gen_sample(0));
}

#[test]
#[should_panic]
fn write_out_of_index_4() {
    let hdr = VecxRaw::new();
    hdr.remove(100);
    hdr.insert(0, gen_sample(0));
}

#[test]
#[should_panic]
fn write_out_of_index_5() {
    let hdr = VecxRaw::new();
    hdr.insert(0, gen_sample(0));
    hdr.remove(100);
}

#[test]
#[should_panic]
fn write_out_of_index_6() {
    let hdr = VecxRaw::new();
    hdr.swap_remove(100);
    hdr.insert(0, gen_sample(0));
}

#[test]
#[should_panic]
fn write_out_of_index_7() {
    let hdr = VecxRaw::new();
    hdr.insert(0, gen_sample(0));
    hdr.swap_remove(100);
}
