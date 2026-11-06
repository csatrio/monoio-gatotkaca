//! The bulk neighbour (`--bulk M`): the server writes `--chunk` buffers without pause, the
//! client reads and discards. Each server round carries one chunk per bulk connection, whose
//! send completes at submit as long as the client keeps up.

use std::rc::Rc;

use monoio::{
    io::{AsyncReadRent, AsyncWriteRentExt},
    net::TcpStream,
};

use crate::paths::Ctx;

/// Client read buffer.
const READ_BUF: usize = 64 * 1024;

/// Server side: an endless `write_all` loop. `work` passes over the chunk in user space come
/// before each write (a stand-in for encrypting it: time between two enters that the kernel
/// does not see).
pub async fn serve(mut stream: TcpStream, chunk: usize, work: u32, ctx: Rc<Ctx>) {
    let mut buf = vec![0xA5_u8; chunk];
    let mut key = 0x9E_u8;
    loop {
        for _ in 0..work {
            // A byte-serial pass (each byte depends on the one before), so it is not vectorised.
            for b in buf.iter_mut() {
                key = key.rotate_left(1) ^ *b;
                *b = key;
            }
            std::hint::black_box(&mut buf);
        }
        let (res, b) = stream.write_all(buf).await;
        buf = b;
        if res.is_err() {
            return;
        }
        let n = &ctx.count.bulk_bytes;
        n.set(n.get() + chunk as u64);
    }
}

/// Client side: read and discard.
pub async fn drain(mut stream: TcpStream) {
    let mut buf = Vec::with_capacity(READ_BUF);
    loop {
        let (res, b) = stream.read(buf).await;
        buf = b;
        if !matches!(res, Ok(n) if n > 0) {
            return;
        }
    }
}
