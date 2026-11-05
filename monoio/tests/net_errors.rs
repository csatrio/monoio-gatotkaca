//! Submit paths return errors instead of panicking. A failing flush is hard to force, so this
//! covers the path around it: ops submitted while the SQ is full flush it first and still
//! complete with their buffers.

use monoio::{
    io::{AsyncReadRentExt, AsyncWriteRent},
    net::UnixStream,
    IoUringDriver, RuntimeBuilder,
};

#[test]
fn ops_past_a_full_sq_complete() {
    // The smallest ring: 256 SQ entries.
    let mut rt = RuntimeBuilder::<IoUringDriver>::new()
        .with_entries(256)
        .build()
        .unwrap();
    rt.block_on(async {
        let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut b = UnixStream::from_std(b).unwrap();
        const N: usize = 300;
        // One stream per write (dups of one socket), so all futures can be alive at once.
        let mut writers: Vec<UnixStream> = (0..N)
            .map(|_| UnixStream::from_std(a.try_clone().unwrap()).unwrap())
            .collect();
        // `write` submits when called, so this queues N SQEs before anything is awaited.
        let futs: Vec<_> = writers
            .iter_mut()
            .enumerate()
            .map(|(i, w)| w.write(vec![(i % 251) as u8]))
            .collect();
        let mut total = 0;
        for f in futs {
            let (res, buf) = f.await;
            assert_eq!(buf.len(), 1);
            total += res.unwrap();
        }
        assert_eq!(total, N);
        let (res, buf) = b.read_exact(vec![0u8; N]).await;
        res.unwrap();
        let mut sorted = buf.clone();
        sorted.sort_unstable();
        let mut expected: Vec<u8> = (0..N).map(|i| (i % 251) as u8).collect();
        expected.sort_unstable();
        assert_eq!(sorted, expected);
    });
}
