//! The clipboard transfer engine both peers run: it turns
//! [`ClipboardMsg`]s from the peer into local reads and writes and back,
//! honouring the chunk size and ack window from `gliff_proto::clipboard`.
//!
//! The owner feeds every clipboard message it reads to [`Transfers::on_msg`]
//! and gets back the two events it must act on: the peer's offer (put a proxy
//! on the local clipboard) and the peer's request (find the item locally and
//! [`Transfers::serve`] it). To paste something the peer offered, the owner
//! calls [`Transfers::fetch`] with a sink. Everything runs on one thread in a
//! tokio `LocalSet`; the engine's own tasks are spawned locally.

pub mod files;
pub mod progress;

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use gliff_proto::clipboard::{Assembler, ChunkError, SendWindow, CHUNK, WINDOW};
use gliff_proto::{ClipboardFile, ClipboardItem, ClipboardMsg};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};
use tokio::task::AbortHandle;

/// A transfer that makes no progress for this long is abandoned, so a paste
/// target or a source application that hangs cannot pin a transfer forever.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Where the engine puts messages for the peer; the owner writes them to the
/// socket. Bounded so a fast source cannot outrun the link.
pub type Outbound = mpsc::Sender<(ClipboardMsg, Bytes)>;

pub fn outbound_channel() -> (Outbound, mpsc::Receiver<(ClipboardMsg, Bytes)>) {
    mpsc::channel(WINDOW as usize * 2)
}

/// What the owner must act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Offer {
        serial: u32,
        mime_types: Vec<String>,
        files: Vec<ClipboardFile>,
    },
    Request {
        id: u32,
        serial: u32,
        item: ClipboardItem,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    #[error("the peer aborted the transfer")]
    Aborted,
    #[error("the peer went away")]
    PeerGone,
    #[error("no progress for {} s", IDLE_TIMEOUT.as_secs())]
    Timeout,
    #[error(transparent)]
    Chunk(#[from] ChunkError),
    #[error("io: {0}")]
    Io(#[from] io::Error),
}

/// Destination of a fetched item.
pub trait ChunkSink {
    fn write(&mut self, chunk: Bytes) -> impl std::future::Future<Output = io::Result<()>>;
    fn finish(&mut self) -> impl std::future::Future<Output = io::Result<()>>;
}

/// Source of a served item, read chunk by chunk until `None`.
pub trait ChunkSource {
    fn next(&mut self) -> impl std::future::Future<Output = io::Result<Option<Bytes>>>;
}

/// Any `AsyncWrite` (a paste pipe, a spool file) as a sink.
pub struct WriteSink<W>(pub W);

impl<W: AsyncWrite + Unpin> ChunkSink for WriteSink<W> {
    async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
        self.0.write_all(&chunk).await
    }

    async fn finish(&mut self) -> io::Result<()> {
        self.0.shutdown().await
    }
}

/// A channel to another thread as a sink; dropping the receiver fails it.
impl ChunkSink for mpsc::Sender<Bytes> {
    async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
        self.send(chunk)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "chunk receiver gone"))
    }

    async fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Any `AsyncRead` (a source pipe, a file) as a source, in full chunks.
pub struct ReadSource<R>(pub R);

impl<R: AsyncRead + Unpin> ChunkSource for ReadSource<R> {
    async fn next(&mut self) -> io::Result<Option<Bytes>> {
        let mut buf = BytesMut::with_capacity(CHUNK);
        while buf.len() < CHUNK {
            if self.0.read_buf(&mut buf).await? == 0 {
                break;
            }
        }
        Ok((!buf.is_empty()).then(|| buf.freeze()))
    }
}

/// A channel from another thread as a source; the sender closes it at EOF.
impl ChunkSource for mpsc::Receiver<io::Result<Bytes>> {
    async fn next(&mut self) -> io::Result<Option<Bytes>> {
        self.recv().await.transpose()
    }
}

/// A fetch whose bytes are wanted in memory, bounded by the cap given to
/// [`Transfers::fetch`].
#[derive(Default)]
pub struct MemorySink(pub Vec<u8>);

impl ChunkSink for MemorySink {
    async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
        self.0.extend_from_slice(&chunk);
        Ok(())
    }

    async fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum Chunk {
    Data { bytes: Bytes, done: bool },
    Fail(TransferError),
}

struct Incoming {
    assembler: Assembler,
    chunks: mpsc::Sender<Chunk>,
}

struct Outgoing {
    window: Rc<RefCell<SendWindow>>,
    credit: Rc<Notify>,
    task: AbortHandle,
}

/// Which peer this engine runs on. Each side allocates request ids of its
/// own parity (client odd, server even), so concurrent transfers in
/// opposite directions never share an id and an `Abort` is unambiguous.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Client,
    Server,
}

struct Inner {
    out: Outbound,
    side: Side,
    next_id: u32,
    incoming: HashMap<u32, Incoming>,
    outgoing: HashMap<u32, Outgoing>,
}

impl Inner {
    fn our_parity(&self) -> u32 {
        match self.side {
            Side::Client => 1,
            Side::Server => 0,
        }
    }
}

#[derive(Clone)]
pub struct Transfers {
    inner: Rc<RefCell<Inner>>,
}

impl Transfers {
    pub fn new(out: Outbound, side: Side) -> Self {
        Self {
            inner: Rc::new(RefCell::new(Inner {
                out,
                side,
                next_id: match side {
                    Side::Client => 1,
                    Side::Server => 2,
                },
                incoming: HashMap::new(),
                outgoing: HashMap::new(),
            })),
        }
    }

    /// Handle one message from the peer. `payload` is the `Data` chunk that
    /// followed the header (empty for other messages).
    pub fn on_msg(&self, msg: ClipboardMsg, payload: Bytes) -> Option<Event> {
        match msg {
            ClipboardMsg::Offer {
                serial,
                mime_types,
                files,
            } => {
                return Some(Event::Offer {
                    serial,
                    mime_types,
                    files,
                })
            }
            ClipboardMsg::Request { id, serial, item } => {
                if id % 2 == self.inner.borrow().our_parity() {
                    tracing::warn!(id, "clipboard request with our own id parity refused");
                    self.send_later(ClipboardMsg::Abort { id });
                    return None;
                }
                return Some(Event::Request { id, serial, item });
            }
            ClipboardMsg::Data {
                id,
                offset,
                data_len,
                done,
            } => {
                if let Err(e) = self.on_data(id, offset, data_len, done, payload) {
                    tracing::warn!(id, error = %e, "clipboard chunk rejected");
                    self.fail_incoming(id, e);
                }
            }
            ClipboardMsg::Ack { id, received } => {
                let acked = {
                    let inner = self.inner.borrow();
                    inner.outgoing.get(&id).map(|o| {
                        let r = o.window.borrow_mut().on_ack(received);
                        if r.is_ok() {
                            o.credit.notify_one();
                        }
                        r
                    })
                };
                if let Some(Err(e)) = acked {
                    tracing::warn!(id, error = %e, "bad clipboard ack");
                    self.abort_outgoing(id);
                }
            }
            ClipboardMsg::Abort { id } => {
                let (outgoing, incoming) = {
                    let mut inner = self.inner.borrow_mut();
                    (inner.outgoing.remove(&id), inner.incoming.remove(&id))
                };
                if let Some(o) = outgoing {
                    o.task.abort();
                }
                if let Some(i) = incoming {
                    let _ = i.chunks.try_send(Chunk::Fail(TransferError::Aborted));
                }
            }
        }
        None
    }

    fn on_data(
        &self,
        id: u32,
        offset: u64,
        data_len: u32,
        done: bool,
        payload: Bytes,
    ) -> Result<(), TransferError> {
        let mut inner = self.inner.borrow_mut();
        let Some(i) = inner.incoming.get_mut(&id) else {
            return Ok(()); // finished or aborted already; the peer has our Abort
        };
        if payload.len() != data_len as usize {
            return Err(ChunkError::BadOffset {
                expected: offset + data_len as u64,
                got: offset + payload.len() as u64,
            }
            .into());
        }
        i.assembler.accept(offset, data_len, done)?;
        i.chunks
            .try_send(Chunk::Data {
                bytes: payload,
                done,
            })
            .map_err(|_| ChunkError::WindowExceeded.into())
    }

    /// End an incoming transfer on a protocol error and tell the peer.
    fn fail_incoming(&self, id: u32, e: TransferError) {
        let removed = self.inner.borrow_mut().incoming.remove(&id);
        if let Some(i) = removed {
            let _ = i.chunks.try_send(Chunk::Fail(e));
            self.send_later(ClipboardMsg::Abort { id });
        }
    }

    fn abort_outgoing(&self, id: u32) {
        let removed = self.inner.borrow_mut().outgoing.remove(&id);
        if let Some(o) = removed {
            o.task.abort();
            self.send_later(ClipboardMsg::Abort { id });
        }
    }

    /// Abort every transfer we are serving and tell the peer, for when the
    /// offer they came from is withdrawn.
    pub fn abort_serving(&self) {
        let ids: Vec<u32> = self.inner.borrow().outgoing.keys().copied().collect();
        for id in ids {
            self.abort_outgoing(id);
        }
    }

    /// Ask the peer for `item` and write it into `sink`. Resolves with the
    /// byte count once the final chunk is in the sink and the sink finished.
    /// `cap` bounds the size; anything over it aborts the transfer.
    pub async fn fetch<S: ChunkSink>(
        &self,
        item: ClipboardItem,
        serial: u32,
        mut sink: S,
        cap: Option<u64>,
    ) -> Result<u64, TransferError> {
        // Room for a full window plus one more chunk and a failure notice.
        let (tx, mut rx) = mpsc::channel(WINDOW as usize + 2);
        let (id, out) = {
            let mut inner = self.inner.borrow_mut();
            let id = inner.next_id;
            inner.next_id = inner.next_id.wrapping_add(2);
            if inner.next_id == 0 {
                inner.next_id = 2;
            }
            inner.incoming.insert(
                id,
                Incoming {
                    assembler: Assembler::new(cap),
                    chunks: tx,
                },
            );
            (id, inner.out.clone())
        };
        let guard = FetchGuard {
            transfers: self.clone(),
            id,
            armed: true,
        };
        out.send((ClipboardMsg::Request { id, serial, item }, Bytes::new()))
            .await
            .map_err(|_| TransferError::PeerGone)?;
        let mut received = 0u64;
        loop {
            let chunk = match tokio::time::timeout(IDLE_TIMEOUT, rx.recv()).await {
                Err(_) => return Err(TransferError::Timeout),
                Ok(None) => return Err(TransferError::PeerGone),
                Ok(Some(Chunk::Fail(e))) => {
                    guard.disarm();
                    return Err(e);
                }
                Ok(Some(Chunk::Data { bytes, done })) => (bytes, done),
            };
            received += chunk.0.len() as u64;
            tokio::time::timeout(IDLE_TIMEOUT, sink.write(chunk.0))
                .await
                .map_err(|_| TransferError::Timeout)??;
            if chunk.1 {
                break;
            }
            out.send((ClipboardMsg::Ack { id, received }, Bytes::new()))
                .await
                .map_err(|_| TransferError::PeerGone)?;
        }
        sink.finish().await?;
        guard.disarm();
        self.inner.borrow_mut().incoming.remove(&id);
        Ok(received)
    }

    /// Stream `source` to the peer as the answer to its request `id`.
    pub fn serve<S: ChunkSource + 'static>(&self, id: u32, source: S) {
        let window = Rc::new(RefCell::new(SendWindow::default()));
        let credit = Rc::new(Notify::new());
        let transfers = self.clone();
        let out = self.inner.borrow().out.clone();
        let (w, c) = (window.clone(), credit.clone());
        let task = tokio::task::spawn_local(async move {
            let result = serve_loop(id, source, out.clone(), w, c).await;
            let still_ours = transfers.inner.borrow_mut().outgoing.remove(&id).is_some();
            if let Err(e) = result {
                tracing::warn!(id, error = %e, "clipboard serve failed");
                if still_ours {
                    let _ = out.send((ClipboardMsg::Abort { id }, Bytes::new())).await;
                }
            }
        })
        .abort_handle();
        let displaced = self.inner.borrow_mut().outgoing.insert(
            id,
            Outgoing {
                window,
                credit,
                task,
            },
        );
        // A peer that repeats a request id would otherwise leave the first
        // serve running with no handle left to abort it.
        if let Some(o) = displaced {
            tracing::warn!(id, "clipboard request id reused while serving");
            o.task.abort();
        }
    }

    /// Decline the peer's request `id`.
    pub fn refuse(&self, id: u32) {
        self.send_later(ClipboardMsg::Abort { id });
    }

    /// Send the peer a message, e.g. an offer.
    pub async fn send(&self, msg: ClipboardMsg) -> Result<(), TransferError> {
        let out = self.inner.borrow().out.clone();
        out.send((msg, Bytes::new()))
            .await
            .map_err(|_| TransferError::PeerGone)
    }

    /// Enqueue a message without blocking the loop that also drains this queue.
    pub fn try_send(&self, msg: ClipboardMsg) -> Result<(), TransferError> {
        self.inner
            .borrow()
            .out
            .try_send((msg, Bytes::new()))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Closed(_) => TransferError::PeerGone,
                mpsc::error::TrySendError::Full(_) => TransferError::Io(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "clipboard send queue is full",
                )),
            })
    }

    fn send_later(&self, msg: ClipboardMsg) {
        let out = self.inner.borrow().out.clone();
        tokio::task::spawn_local(async move {
            let _ = out.send((msg, Bytes::new())).await;
        });
    }
}

async fn serve_loop<S: ChunkSource>(
    id: u32,
    source: S,
    out: Outbound,
    window: Rc<RefCell<SendWindow>>,
    credit: Rc<Notify>,
) -> Result<(), TransferError> {
    let mut source = Chunked { source, rest: None };
    loop {
        while !window.borrow().may_send() {
            tokio::time::timeout(IDLE_TIMEOUT, credit.notified())
                .await
                .map_err(|_| TransferError::Timeout)?;
        }
        let chunk = tokio::time::timeout(IDLE_TIMEOUT, source.next())
            .await
            .map_err(|_| TransferError::Timeout)??;
        let (bytes, done) = match chunk {
            Some(b) => (b, false),
            None => (Bytes::new(), true),
        };
        let msg = ClipboardMsg::Data {
            id,
            offset: window.borrow().offset,
            data_len: bytes.len() as u32,
            done,
        };
        let len = bytes.len();
        out.send((msg, bytes))
            .await
            .map_err(|_| TransferError::PeerGone)?;
        window.borrow_mut().on_sent(len);
        if done {
            return Ok(());
        }
    }
}

/// Splits whatever a source hands over into chunks of at most `CHUNK`.
struct Chunked<S> {
    source: S,
    rest: Option<Bytes>,
}

impl<S: ChunkSource> Chunked<S> {
    async fn next(&mut self) -> io::Result<Option<Bytes>> {
        let mut b = match self.rest.take() {
            Some(b) => b,
            None => loop {
                match self.source.next().await? {
                    Some(b) if b.is_empty() => continue,
                    Some(b) => break b,
                    None => return Ok(None),
                }
            },
        };
        if b.len() > CHUNK {
            self.rest = Some(b.split_off(CHUNK));
        }
        Ok(Some(b))
    }
}

/// Removes a fetch's registration if its future is dropped early and tells
/// the peer to stop sending.
struct FetchGuard {
    transfers: Transfers,
    id: u32,
    armed: bool,
}

impl FetchGuard {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for FetchGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let removed = self
            .transfers
            .inner
            .borrow_mut()
            .incoming
            .remove(&self.id)
            .is_some();
        if removed {
            self.transfers
                .send_later(ClipboardMsg::Abort { id: self.id });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::files::{fetch_files, list_files, open_source, Spool};
    use super::progress::Meter;
    use super::*;
    use std::io::Cursor;
    use tokio::task::LocalSet;

    type Events = Rc<RefCell<Vec<Event>>>;

    /// Two engines joined back to back: everything `a` sends `b` receives and
    /// the other way round. Returns the engines and the events each raised.
    fn pair() -> (Transfers, Transfers, Events, Events) {
        let (a_out, a_rx) = outbound_channel();
        let (b_out, b_rx) = outbound_channel();
        let a = Transfers::new(a_out, Side::Client);
        let b = Transfers::new(b_out, Side::Server);
        let a_events = Rc::new(RefCell::new(Vec::new()));
        let b_events = Rc::new(RefCell::new(Vec::new()));
        pump(a_rx, b.clone(), b_events.clone());
        pump(b_rx, a.clone(), a_events.clone());
        (a, b, a_events, b_events)
    }

    fn pump(mut rx: mpsc::Receiver<(ClipboardMsg, Bytes)>, to: Transfers, events: Events) {
        tokio::task::spawn_local(async move {
            while let Some((m, p)) = rx.recv().await {
                if let Some(ev) = to.on_msg(m, p) {
                    events.borrow_mut().push(ev);
                }
            }
        });
    }

    async fn serve_requests(engine: Transfers, events: Events, data: Vec<u8>) {
        loop {
            let req = events.borrow_mut().pop();
            match req {
                Some(Event::Request { id, .. }) => {
                    engine.serve(id, ReadSource(Cursor::new(data.clone())));
                    return;
                }
                _ => tokio::task::yield_now().await,
            }
        }
    }

    fn run<F: std::future::Future>(f: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        LocalSet::new().block_on(&rt, f)
    }

    #[test]
    fn fetch_streams_a_large_item_in_chunks() {
        run(async {
            let (a, b, _ae, be) = pair();
            let baseline = Rc::strong_count(&a.inner);
            let data: Vec<u8> = (0..(3 * CHUNK + 17)).map(|i| (i % 251) as u8).collect();
            tokio::task::spawn_local(serve_requests(b.clone(), be, data.clone()));
            let got = a
                .fetch(
                    ClipboardItem::Mime("image/png".into()),
                    0,
                    MemorySink::default(),
                    Some(MAX_ITEM_FOR_TEST),
                )
                .await
                .unwrap();
            assert_eq!(got, data.len() as u64);
            assert!(a.inner.borrow().incoming.is_empty());
            // A completed fetch releases its engine reference.
            assert_eq!(Rc::strong_count(&a.inner), baseline);
            tokio::task::yield_now().await;
            assert!(b.inner.borrow().outgoing.is_empty());
        });
    }

    const MAX_ITEM_FOR_TEST: u64 = 64 * 1024 * 1024;

    #[test]
    fn fetched_bytes_match_the_source() {
        run(async {
            let (a, b, _ae, be) = pair();
            let data: Vec<u8> = (0..(2 * CHUNK + 5)).map(|i| (i * 7 % 256) as u8).collect();
            tokio::task::spawn_local(serve_requests(b.clone(), be, data.clone()));
            let (tx, mut rx) = mpsc::channel::<Bytes>(2);
            let collect = tokio::task::spawn_local(async move {
                let mut v = Vec::new();
                while let Some(c) = rx.recv().await {
                    v.extend_from_slice(&c);
                }
                v
            });
            a.fetch(ClipboardItem::Mime("x".into()), 0, tx, None)
                .await
                .unwrap();
            assert_eq!(collect.await.unwrap(), data);
        });
    }

    #[test]
    fn a_sender_waits_for_acks_after_a_window() {
        run(async {
            let (b_out, mut b_rx) = outbound_channel();
            let b = Transfers::new(b_out, Side::Server);
            let data = vec![1u8; (WINDOW as usize + 3) * CHUNK];
            b.serve(9, ReadSource(Cursor::new(data)));
            let mut chunks = 0;
            while let Ok(Some((ClipboardMsg::Data { .. }, _))) =
                tokio::time::timeout(Duration::from_millis(200), b_rx.recv()).await
            {
                chunks += 1;
            }
            assert_eq!(chunks, WINDOW);
            b.on_msg(
                ClipboardMsg::Ack {
                    id: 9,
                    received: CHUNK as u64,
                },
                Bytes::new(),
            );
            let next = tokio::time::timeout(Duration::from_millis(200), b_rx.recv()).await;
            assert!(matches!(
                next,
                Ok(Some((
                    ClipboardMsg::Data {
                        id: 9,
                        done: false,
                        ..
                    },
                    _
                )))
            ));
        });
    }

    #[test]
    fn sides_allocate_ids_of_their_own_parity() {
        run(async {
            for (side, parity) in [(Side::Client, 1), (Side::Server, 0)] {
                let (out, _rx) = outbound_channel();
                let engine = Transfers::new(out, side);
                for _ in 0..3 {
                    let e = engine.clone();
                    tokio::task::spawn_local(async move {
                        let _ = e
                            .fetch(
                                ClipboardItem::Mime("x".into()),
                                0,
                                MemorySink::default(),
                                None,
                            )
                            .await;
                    });
                }
                for _ in 0..5 {
                    tokio::task::yield_now().await;
                }
                let inner = engine.inner.borrow();
                assert_eq!(inner.incoming.len(), 3);
                assert!(inner.incoming.keys().all(|id| id % 2 == parity));
            }
        });
    }

    #[test]
    fn a_request_with_our_own_parity_is_refused() {
        run(async {
            let (b_out, mut b_rx) = outbound_channel();
            let server = Transfers::new(b_out, Side::Server);
            let ev = server.on_msg(
                ClipboardMsg::Request {
                    id: 2,
                    serial: 0,
                    item: ClipboardItem::Mime("x".into()),
                },
                Bytes::new(),
            );
            assert!(ev.is_none());
            let sent = tokio::time::timeout(Duration::from_millis(200), b_rx.recv())
                .await
                .unwrap();
            assert!(matches!(sent, Some((ClipboardMsg::Abort { id: 2 }, _))));
        });
    }

    #[test]
    fn a_refused_request_fails_the_fetch() {
        run(async {
            let (a, b, _ae, be) = pair();
            tokio::task::spawn_local(async move {
                loop {
                    let req = be.borrow_mut().pop();
                    if let Some(Event::Request { id, .. }) = req {
                        b.refuse(id);
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            });
            let r = a
                .fetch(
                    ClipboardItem::Mime("x".into()),
                    0,
                    MemorySink::default(),
                    None,
                )
                .await;
            assert!(matches!(r, Err(TransferError::Aborted)), "{r:?}");
            assert!(a.inner.borrow().incoming.is_empty());
        });
    }

    #[test]
    fn an_item_over_the_cap_is_aborted_on_both_sides() {
        run(async {
            let (a, b, _ae, be) = pair();
            tokio::task::spawn_local(serve_requests(b.clone(), be, vec![0u8; CHUNK * 2]));
            let r = a
                .fetch(
                    ClipboardItem::Mime("x".into()),
                    0,
                    MemorySink::default(),
                    Some(CHUNK as u64),
                )
                .await;
            assert!(
                matches!(r, Err(TransferError::Chunk(ChunkError::OverCap(_)))),
                "{r:?}"
            );
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
            assert!(b.inner.borrow().outgoing.is_empty());
        });
    }

    #[test]
    fn a_channel_source_is_split_into_chunks() {
        run(async {
            let (a, b, _ae, be) = pair();
            let big = Bytes::from(vec![5u8; CHUNK * 2 + 1]);
            let (tx, rx) = mpsc::channel::<io::Result<Bytes>>(2);
            tokio::task::spawn_local(async move {
                loop {
                    let req = be.borrow_mut().pop();
                    if let Some(Event::Request { id, .. }) = req {
                        b.serve(id, rx);
                        tx.send(Ok(big)).await.unwrap();
                        tx.send(Ok(Bytes::from_static(b"!"))).await.unwrap();
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            });
            let (tx, mut rx) = mpsc::channel::<Bytes>(2);
            let collect = tokio::task::spawn_local(async move {
                let mut v = Vec::new();
                while let Some(c) = rx.recv().await {
                    v.extend_from_slice(&c);
                }
                v
            });
            let n = a
                .fetch(ClipboardItem::Mime("x".into()), 0, tx, None)
                .await
                .unwrap();
            assert_eq!(n as usize, CHUNK * 2 + 2);
            let got = collect.await.unwrap();
            assert_eq!(got.len(), CHUNK * 2 + 2);
            assert_eq!(got[CHUNK * 2 + 1], b'!');
        });
    }

    #[test]
    fn files_are_listed_spooled_and_removed() {
        run(async {
            let src = tempfile::tempdir().unwrap();
            let root = src.path().join("photos");
            std::fs::create_dir_all(root.join("sub/empty")).unwrap();
            std::fs::write(root.join("a.txt"), b"hello").unwrap();
            let big: Vec<u8> = (0..CHUNK + 100).map(|i| (i % 253) as u8).collect();
            std::fs::write(root.join("sub/big.bin"), &big).unwrap();
            std::fs::write(src.path().join("solo.md"), b"# solo").unwrap();
            #[cfg(unix)]
            std::os::unix::fs::symlink("/", root.join("loop")).unwrap();

            let local = list_files(&[root.clone(), src.path().join("solo.md")]).unwrap();
            let paths: Vec<&str> = local.entries.iter().map(|e| e.path.as_str()).collect();
            assert_eq!(paths[0], "photos");
            assert!(paths.contains(&"photos/a.txt"));
            assert!(paths.contains(&"photos/sub/big.bin"));
            assert!(paths.contains(&"photos/sub/empty"));
            assert_eq!(paths.last(), Some(&"solo.md"));
            assert!(!paths.iter().any(|p| p.contains("loop")));
            let big_entry = local
                .entries
                .iter()
                .find(|e| e.path == "photos/sub/big.bin")
                .unwrap();
            assert_eq!(big_entry.size as usize, big.len());

            let (a, b, _ae, be) = pair();
            let serving = local.clone();
            let b2 = b.clone();
            tokio::task::spawn_local(async move {
                loop {
                    let req = be.borrow_mut().pop();
                    if let Some(Event::Request { id, item, .. }) = req {
                        let path = serving.path_for(&item).unwrap().to_path_buf();
                        b2.serve(id, open_source(&path).await.unwrap());
                    }
                    tokio::task::yield_now().await;
                }
            });
            let base = tempfile::tempdir().unwrap();
            let spool = Spool::create_in(base.path()).unwrap();
            let tops = fetch_files(&a, &local.entries, 0, &spool, &Meter::default())
                .await
                .unwrap();
            assert_eq!(
                tops,
                vec![spool.dir().join("photos"), spool.dir().join("solo.md")]
            );
            assert_eq!(
                std::fs::read(spool.dir().join("photos/a.txt")).unwrap(),
                b"hello"
            );
            assert_eq!(
                std::fs::read(spool.dir().join("photos/sub/big.bin")).unwrap(),
                big
            );
            assert!(spool.dir().join("photos/sub/empty").is_dir());
            assert_eq!(
                std::fs::read(spool.dir().join("solo.md")).unwrap(),
                b"# solo"
            );
            let dir = spool.dir().to_path_buf();
            drop(spool);
            assert!(!dir.exists());
        });
    }

    #[cfg(unix)]
    #[test]
    fn a_spool_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let base = tempfile::tempdir().unwrap();
        let spool = Spool::create_in(base.path()).unwrap();
        let mode = std::fs::metadata(spool.dir()).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    #[test]
    fn a_new_spool_sweeps_those_of_dead_processes() {
        let base = tempfile::tempdir().unwrap();
        let dead = base.path().join("4294967295-0");
        let foreign = base.path().join("not-a-spool");
        std::fs::create_dir_all(dead.join("x")).unwrap();
        std::fs::create_dir(&foreign).unwrap();
        let ours = Spool::create_in(base.path()).unwrap();
        let ours_dir = ours.dir().to_path_buf();
        let second = Spool::create_in(base.path()).unwrap();
        assert!(!dead.exists());
        assert!(foreign.exists());
        assert!(ours_dir.exists());
        assert_ne!(ours_dir, second.dir());
    }

    #[test]
    fn a_file_longer_than_declared_is_refused() {
        run(async {
            let (a, b, _ae, be) = pair();
            let b2 = b.clone();
            tokio::task::spawn_local(async move {
                loop {
                    if let Some(Event::Request { id, .. }) = be.borrow_mut().pop() {
                        b2.serve(id, ReadSource(&b"more than one byte"[..]));
                    }
                    tokio::task::yield_now().await;
                }
            });
            let base = tempfile::tempdir().unwrap();
            let spool = Spool::create_in(base.path()).unwrap();
            let files = vec![ClipboardFile {
                path: "short".into(),
                size: 1,
                dir: false,
            }];
            let r = fetch_files(&a, &files, 0, &spool, &Meter::default()).await;
            assert!(
                matches!(r, Err(TransferError::Chunk(ChunkError::OverCap(1)))),
                "{r:?}"
            );
        });
    }

    #[test]
    fn a_file_shorter_than_declared_is_refused() {
        run(async {
            let (a, b, _ae, be) = pair();
            let b2 = b.clone();
            tokio::task::spawn_local(async move {
                loop {
                    if let Some(Event::Request { id, .. }) = be.borrow_mut().pop() {
                        b2.serve(id, ReadSource(&b"x"[..]));
                    }
                    tokio::task::yield_now().await;
                }
            });
            let base = tempfile::tempdir().unwrap();
            let spool = Spool::create_in(base.path()).unwrap();
            let files = vec![ClipboardFile {
                path: "short".into(),
                size: 5,
                dir: false,
            }];
            let r = fetch_files(&a, &files, 0, &spool, &Meter::default()).await;
            assert!(matches!(r, Err(TransferError::Io(_))), "{r:?}");
        });
    }

    #[test]
    fn a_retired_spool_lives_for_the_grace_period() {
        run(async {
            tokio::time::pause();
            let base = tempfile::tempdir().unwrap();
            let spool = Rc::new(Spool::create_in(base.path()).unwrap());
            let dir = spool.dir().to_path_buf();
            files::retire(spool);
            tokio::time::sleep(files::SPOOL_GRACE / 2).await;
            assert!(dir.exists());
            tokio::time::sleep(files::SPOOL_GRACE).await;
            tokio::task::yield_now().await;
            assert!(!dir.exists());
        });
    }

    #[test]
    fn abort_serving_stops_a_serve_displaced_by_a_reused_id() {
        run(async {
            let (out, _rx) = outbound_channel();
            let t = Transfers::new(out, Side::Client);
            // Sources that never end, so both serves stay pending.
            let (first_tx, first_rx) = mpsc::channel::<io::Result<Bytes>>(2);
            let (second_tx, second_rx) = mpsc::channel::<io::Result<Bytes>>(2);
            t.serve(2, first_rx);
            t.serve(2, second_rx);
            tokio::task::yield_now().await;
            t.abort_serving();
            for (which, tx) in [("first", first_tx), ("second", second_tx)] {
                tokio::time::timeout(Duration::from_secs(1), tx.closed())
                    .await
                    .unwrap_or_else(|_| panic!("{which} serve kept reading its source"));
            }
        });
    }

    #[test]
    fn spooling_rejects_unsafe_paths() {
        run(async {
            let (a, _b, _ae, _be) = pair();
            let base = tempfile::tempdir().unwrap();
            let spool = Spool::create_in(base.path()).unwrap();
            let files = vec![ClipboardFile {
                path: "../escape".into(),
                size: 1,
                dir: false,
            }];
            let r = fetch_files(&a, &files, 0, &spool, &Meter::default()).await;
            assert!(matches!(r, Err(TransferError::Io(_))), "{r:?}");
            assert!(!base.path().join("escape").exists());
        });
    }
}
