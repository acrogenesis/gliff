//! A lazy native text clipboard backed by the shared chunked transfer engine.
use anyhow::{ensure, Result};
use bytes::Bytes;
use gliff_proto::clipboard::{is_text_mime, TEXT_MIME};
use gliff_proto::{ClipboardItem, ClipboardMsg};
use gliff_transport::clipboard::{outbound_channel, Event, ReadSource, Side, Transfers};
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};
use tokio::sync::mpsc;

const TEXT_LIMIT: usize = 1024 * 1024;
type Native = mpsc::Sender<(u8, Vec<u8>)>;
type Outbound = mpsc::Receiver<(ClipboardMsg, Bytes)>;

pub struct Bridge {
    transfers: Transfers,
    out: RefCell<Option<Outbound>>,
    native: Native,
    serial: Cell<u32>,
    text: RefCell<Option<String>>,
    remote: Rc<RefCell<Option<(u32, String)>>>,
}
impl Bridge {
    pub fn new(side: Side, native: Native) -> Self {
        let (out, rx) = outbound_channel();
        Self {
            transfers: Transfers::new(out, side),
            out: RefCell::new(Some(rx)),
            native,
            serial: Cell::new(0),
            text: RefCell::default(),
            remote: Rc::default(),
        }
    }
    pub fn take_outbound(&self) -> Outbound {
        self.out.borrow_mut().take().unwrap()
    }
    pub fn try_local(&self, ev: &Value) -> Result<()> {
        self.transfers.try_send(self.local_offer(ev))?;
        Ok(())
    }
    #[cfg(any(target_os = "macos", test))]
    pub async fn local(&self, ev: &Value) -> Result<()> {
        self.transfers.send(self.local_offer(ev)).await?;
        Ok(())
    }
    fn local_offer(&self, ev: &Value) -> ClipboardMsg {
        let text = ev["text"]
            .as_str()
            .filter(|s| s.len() <= TEXT_LIMIT)
            .map(str::to_string);
        self.serial.set(self.serial.get().wrapping_add(1));
        let mimes = if text.is_some() {
            vec![TEXT_MIME.into()]
        } else {
            vec![]
        };
        *self.text.borrow_mut() = text;
        ClipboardMsg::Offer {
            serial: self.serial.get(),
            mime_types: mimes,
            files: vec![],
        }
    }
    pub fn peer(&self, msg: ClipboardMsg, payload: Bytes) {
        match self.transfers.on_msg(msg, payload) {
            Some(Event::Offer {
                serial, mime_types, ..
            }) => {
                let mime = mime_types.into_iter().find(|m| is_text_mime(m));
                *self.remote.borrow_mut() = mime.map(|m| (serial, m));
                let native = self.native.clone();
                let offered = self.remote.borrow().is_some();
                tokio::task::spawn_local(async move {
                    let _ = send(
                        &native,
                        json!({"type":"clipboard_offer", "serial":serial, "text":offered}),
                    )
                    .await;
                });
            }
            Some(Event::Request { id, serial, item }) => {
                let valid = matches!(item, ClipboardItem::Mime(ref m) if is_text_mime(m))
                    && serial == self.serial.get();
                if valid {
                    if let Some(text) = self.text.borrow().clone() {
                        self.transfers
                            .serve(id, ReadSource(std::io::Cursor::new(text.into_bytes())));
                        return;
                    }
                }
                self.transfers.refuse(id);
            }
            None => {}
        }
    }
    pub fn paste(&self, ev: &Value) {
        let Some(serial) = ev["serial"].as_u64().and_then(|n| u32::try_from(n).ok()) else {
            return;
        };
        let offer = self.remote.borrow().clone();
        let transfers = self.transfers.clone();
        let native = self.native.clone();
        let remote = self.remote.clone();
        tokio::task::spawn_local(async move {
            let result: Result<String> = async {
                let (actual, mime) =
                    offer.ok_or_else(|| anyhow::anyhow!("no text on remote clipboard"))?;
                ensure!(actual == serial, "clipboard was replaced");
                let (tx, mut rx) = mpsc::channel::<Bytes>(2);
                let collect = async {
                    let mut all = Vec::new();
                    while let Some(b) = rx.recv().await {
                        ensure!(
                            all.len() + b.len() <= TEXT_LIMIT,
                            "clipboard text exceeds limit"
                        );
                        all.extend_from_slice(&b);
                    }
                    Ok::<_, anyhow::Error>(String::from_utf8(all)?)
                };
                let (fetched, text) = tokio::join!(
                    transfers.fetch(
                        ClipboardItem::Mime(mime),
                        serial,
                        tx,
                        Some(TEXT_LIMIT as u64)
                    ),
                    collect
                );
                fetched?;
                let text = text?;
                ensure!(
                    remote.borrow().as_ref().is_some_and(|(s, _)| *s == serial),
                    "clipboard was replaced"
                );
                Ok(text)
            }
            .await;
            let cmd = match result {
                Ok(text) => json!({"type":"clipboard_data", "serial":serial, "text":text}),
                Err(e) => json!({"type":"clipboard_data", "serial":serial, "error":e.to_string()}),
            };
            let _ = send(&native, cmd).await;
        });
    }
}
async fn send(native: &Native, cmd: Value) -> Result<()> {
    native
        .send((crate::native::EVENT, serde_json::to_vec(&cmd)?))
        .await?;
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn native_commands(commands: mpsc::Sender<Value>) -> Native {
    let (tx, mut rx) = mpsc::channel::<(u8, Vec<u8>)>(8);
    tokio::task::spawn_local(async move {
        while let Some((_, bytes)) = rx.recv().await {
            let Ok(cmd) = serde_json::from_slice(&bytes) else {
                break;
            };
            if commands.send(cmd).await.is_err() {
                break;
            }
        }
    });
    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "current_thread")]
    async fn native_text_is_fetched_only_when_pasted_and_stale_pastes_fail() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let (local_native, _local_rx) = mpsc::channel(8);
                let (remote_native, mut remote_rx) = mpsc::channel(8);
                let source = Rc::new(Bridge::new(Side::Server, local_native));
                let dest = Rc::new(Bridge::new(Side::Client, remote_native));
                let mut from_source = source.take_outbound();
                let mut from_dest = dest.take_outbound();
                source
                    .local(&json!({"text":"hello from the Mac"}))
                    .await
                    .unwrap();
                let (offer, payload) = from_source.recv().await.unwrap();
                dest.peer(offer, payload);
                let (_, bytes) = remote_rx.recv().await.unwrap();
                assert_eq!(
                    serde_json::from_slice::<Value>(&bytes).unwrap(),
                    json!({"type":"clipboard_offer", "serial":1, "text":true})
                );
                assert!(from_dest.try_recv().is_err()); // no transfer before paste
                let send_to_source = source.clone();
                tokio::task::spawn_local(async move {
                    while let Some((msg, bytes)) = from_dest.recv().await {
                        send_to_source.peer(msg, bytes);
                    }
                });
                let send_to_dest = dest.clone();
                tokio::task::spawn_local(async move {
                    while let Some((msg, bytes)) = from_source.recv().await {
                        send_to_dest.peer(msg, bytes);
                    }
                });
                dest.paste(&json!({"serial":1}));
                let (_, bytes) =
                    tokio::time::timeout(std::time::Duration::from_secs(2), remote_rx.recv())
                        .await
                        .unwrap()
                        .unwrap();
                assert_eq!(
                    serde_json::from_slice::<Value>(&bytes).unwrap(),
                    json!({"type":"clipboard_data", "serial":1, "text":"hello from the Mac"})
                );
                dest.paste(&json!({"serial":0}));
                let (_, bytes) = remote_rx.recv().await.unwrap();
                assert!(serde_json::from_slice::<Value>(&bytes).unwrap()["error"].is_string());
            })
            .await;
    }
}
