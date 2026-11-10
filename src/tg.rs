use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};

use grammers_client::client::{LoginToken, PasswordToken};
use grammers_client::media::Media;
use grammers_client::peer::Peer;
use grammers_client::session::storages::SqliteSession;
use grammers_client::session::types::PeerRef;
use grammers_client::session::Session;
use grammers_client::{Client, SenderPool};
use grammers_client::tl;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::model::*;

const CHUNK: usize = 512 * 1024;

pub fn spawn(ctx: egui::Context) -> (tokio::sync::mpsc::UnboundedSender<Cmd>, Receiver<Ev>) {
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel::<Cmd>();
    let (ev_tx, ev_rx) = std::sync::mpsc::channel::<Ev>();

    std::thread::Builder::new()
        .name("tg-worker".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(8)
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = ev_tx.send(Ev::Err(format!("runtime: {e}")));
                    return;
                }
            };
            rt.block_on(run(cmd_rx, ev_tx, ctx));
        })
        .expect("spawn tg worker");

    (cmd_tx, ev_rx)
}

/// Persist the home DC and confirm the login on the datacenter that owns the
/// account. QR login can land on a different DC than the one we dialed, so the
/// caller passes the DC the token was finally accepted on.
async fn finish_login(
    c: &Client,
    s: &Arc<SqliteSession>,
    dc: i32,
    tx: &Sender<Ev>,
    ctx: &egui::Context,
) {
    use grammers_client::tl;

    if let Err(e) = s.set_home_dc_id(dc).await {
        eprintln!("[qr] set_home_dc({dc}) failed: {e}");
    } else {
        eprintln!("[qr] home dc saved = {dc}");
    }

    let name = match c
        .invoke_in_dc(
            dc,
            &tl::functions::users::GetUsers {
                id: vec![tl::enums::InputUser::UserSelf],
            },
        )
        .await
    {
        Ok(u) => u
            .first()
            .map(|x| match x {
                tl::enums::User::User(v) => format!(
                    "{} {}",
                    v.first_name.clone().unwrap_or_default(),
                    v.last_name.clone().unwrap_or_default()
                )
                .trim()
                .to_string(),
                _ => "signed in".to_string(),
            })
            .unwrap_or_else(|| "signed in".into()),
        Err(e) => {
            eprintln!("[qr] getusers err: {e}");
            "signed in".into()
        }
    };

    eprintln!("[qr] AUTHORIZED as {name}");
    let _ = tx.send(Ev::Authorized { me: name });
    ctx.request_repaint();
}

async fn run(
    mut rx: UnboundedReceiver<Cmd>,
    tx: Sender<Ev>,
    ctx: egui::Context,
) {
    let mut client: Option<Client> = None;
    let mut sess: Option<Arc<SqliteSession>> = None;
    let mut api_id_v: i32 = 0;
    let mut api_hash = String::new();
    let mut token: Option<LoginToken> = None;
    let mut pw_token: Option<PasswordToken> = None;
    let mut peers: Vec<PeerRef> = Vec::new();
    let mut cache: HashMap<usize, Vec<grammers_client::message::Message>> = HashMap::new();

    while let Some(cmd) = rx.recv().await {
        let res: anyhow::Result<()> = async {
            match cmd {
                Cmd::Connect { api_id, api_hash: hash, db } => {
                    api_hash = hash;
                    api_id_v = api_id;
                    let session = Arc::new(SqliteSession::open(&db).await?);
                    eprintln!("[tg] session opened");
                    let pool = SenderPool::new(Arc::clone(&session), api_id);
                    let c = Client::new(pool.handle.clone());
                    eprintln!("[tg] pool created");

                    let mut updates = pool.updates;
                    tokio::spawn(async move { while updates.recv().await.is_some() {} });
                    tokio::spawn(pool.runner.run());

                    let _ = tx.send(Ev::Log("connected".into()));
                    let auth = c.is_authorized().await;
                    eprintln!("[tg] is_authorized -> {:?}", auth.is_ok());
                    if auth? {
                        let dc = session.home_dc_id()?;
                        eprintln!("[tg] already authorized, home dc = {dc}");
                        finish_login(&c, &session, dc, &tx, &ctx).await;
                    } else {
                        let _ = tx.send(Ev::NeedQr);
                    }
                    sess = Some(session);
                    client = Some(c);
                }

                Cmd::QrLogin => {
                    let c = client.as_ref().ok_or_else(|| anyhow::anyhow!("not connected"))?.clone();
                    let s = sess.clone().ok_or_else(|| anyhow::anyhow!("no session"))?;
                    let id = api_id_v;
                    let hash = api_hash.clone();
                    let tx2 = tx.clone();
                    let ctx2 = ctx.clone();
                    tokio::spawn(async move {
                        use base64::Engine as _;
                        use grammers_client::tl;
                        let mut last: Option<Vec<u8>> = None;
                        let mut fails = 0u32;
                        loop {
                            let req = tl::functions::auth::ExportLoginToken {
                                api_id: id,
                                api_hash: hash.clone(),
                                except_ids: Vec::new(),
                            };
                            match c.invoke(&req).await {
                                Ok(tl::enums::auth::LoginToken::Token(t)) => {
                                    fails = 0;
                                    if last.as_deref() != Some(t.token.as_slice()) {
                                        last = Some(t.token.clone());
                                        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
                                            .encode(&t.token);
                                        let _ = tx2.send(Ev::Qr(format!("tg://login?token={b64}")));
                                        eprintln!("[qr] token exp={} poll=2s", t.expires);
                                        ctx2.request_repaint();
                                    }
                                    tokio::time::sleep(Duration::from_secs(2)).await;
                                }
                                Ok(tl::enums::auth::LoginToken::Success(_)) => {
                                    eprintln!("[qr] ACCEPTED (no migration)");
                                    let dc = match c.invoke(&tl::functions::help::GetNearestDc {}).await {
                                        Ok(tl::enums::NearestDc::Dc(n)) => n.this_dc,
                                        Err(e) => {
                                            eprintln!("[qr] nearest_dc err: {e}");
                                            2
                                        }
                                    };
                                    finish_login(&c, &s, dc, &tx2, &ctx2).await;
                                    return;
                                }
                                Ok(tl::enums::auth::LoginToken::MigrateTo(m)) => {
                                    eprintln!("[qr] migrate to dc {} - importing token", m.dc_id);
                                    let r = c
                                        .invoke_in_dc(
                                            m.dc_id,
                                            &tl::functions::auth::ImportLoginToken {
                                                token: m.token.clone(),
                                            },
                                        )
                                        .await;
                                    match r {
                                        Ok(tl::enums::auth::LoginToken::Success(_)) => {
                                            eprintln!("[qr] token imported on dc {}", m.dc_id);
                                            finish_login(&c, &s, m.dc_id, &tx2, &ctx2).await;
                                            // The live connection pool still points at the old
                                            // DC, so dialogs hang until it is rebuilt. Ask the
                                            // UI to reconnect; the session is already saved, so
                                            // the new pool comes up on the right DC.
                                            tokio::time::sleep(Duration::from_millis(600)).await;
                                            let _ = tx2.send(Ev::Reconnect);
                                        }
                                        Ok(_) => {
                                            eprintln!("[qr] import returned another token");
                                            let _ = tx2.send(Ev::Err(
                                                "login import returned an unexpected token".into(),
                                            ));
                                        }
                                        Err(e) => {
                                            eprintln!("[qr] import failed: {e}");
                                            let _ = tx2.send(Ev::Err(format!("login import failed: {e}")));
                                        }
                                    }
                                    return;
                                }
                                Err(e) => {
                                    fails += 1;
                                    eprintln!("[qr] poll error #{fails}: {e}");
                                    if fails >= 6 {
                                        let _ = tx2.send(Ev::Err(format!("QR polling failed: {e}")));
                                        return;
                                    }
                                    tokio::time::sleep(Duration::from_secs(2)).await;
                                }
                            }
                        }
                    });
                }

                Cmd::RequestCode { phone } => {
                    let c = client.as_ref().ok_or_else(|| anyhow::anyhow!("not connected"))?;
                    token = Some(c.request_login_code(&phone, &api_hash).await?);
                    let _ = tx.send(Ev::Log("code sent".into()));
                }

                Cmd::SignIn { code } => {
                    let c = client.as_ref().ok_or_else(|| anyhow::anyhow!("not connected"))?;
                    let t = token.as_ref().ok_or_else(|| anyhow::anyhow!("no login token"))?;
                    match c.sign_in(t, &code).await {
                        Ok(user) => {
                            let _ = tx.send(Ev::Authorized {
                                me: format!("{} (@{})", user.full_name(), user.username().unwrap_or("-")),
                            });
                        }
                        Err(grammers_client::SignInError::PasswordRequired(pt)) => {
                            let _ = tx.send(Ev::NeedPassword(pt.hint().map(|s| s.to_string())));
                            pw_token = Some(pt);
                        }
                        Err(e) => return Err(anyhow::anyhow!("{e}")),
                    }
                }

                Cmd::Password { pw } => {
                    let c = client.as_ref().ok_or_else(|| anyhow::anyhow!("not connected"))?;
                    let pt = pw_token.take().ok_or_else(|| anyhow::anyhow!("no password token"))?;
                    let user = c.check_password(pt, pw.as_bytes()).await?;
                    let _ = tx.send(Ev::Authorized {
                        me: format!("{} (@{})", user.full_name(), user.username().unwrap_or("-")),
                    });
                }

                Cmd::LoadChats => {
                    let c = client.as_ref().ok_or_else(|| anyhow::anyhow!("not connected"))?;
                    eprintln!("[chats] loading dialogs...");
                    let fut = async {
                        let mut iter = c.iter_dialogs();
                        let mut rows = Vec::new();
                        let mut ps: Vec<PeerRef> = Vec::new();
                        let mut pages = 0usize;
                        while let Some(d) = iter.next().await? {
                            pages += 1;
                            let kind = match &d.peer {
                                Peer::Channel(_) => "channel",
                                Peer::Group(_) => "group",
                                Peer::User(_) => "user",
                                Peer::Community(_) => "group",
                            };
                            let name = match d.peer.name() {
                                Some(n) => n.to_string(),
                                None => match d.peer.username() {
                                    Some(u) => format!("@{u}"),
                                    None => format!(
                                        "{kind} {}",
                                        d.peer_ref().id.bare_id().unwrap_or(0)
                                    ),
                                },
                            };
                            let idx = ps.len();
                            ps.push(d.peer_ref());
                            rows.push(ChatRow {
                                idx,
                                name,
                                kind,
                                username: d.peer.username().map(|s| s.to_string()),
                            });
                        }
                        eprintln!("[chats] iter finished after {pages} dialogs");
                        Ok::<_, anyhow::Error>((rows, ps))
                    };
                    let (mut rows, ps) = tokio::time::timeout(Duration::from_secs(45), fut)
                        .await
                        .map_err(|_| anyhow::anyhow!("dialog list timed out (home DC not answering)"))??;
                    rows.sort_by(|a, b| {
                        fn rank(k: &str) -> u8 {
                            match k {
                                "channel" => 0,
                                "group" => 1,
                                _ => 2,
                            }
                        }
                        rank(a.kind)
                            .cmp(&rank(b.kind))
                            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
                    });
                    peers = ps;
                    if let Some(dir) = dirs::config_dir() {
                        let p = dir.join("tgrab").join("chats.txt");
                        let mut out = String::new();
                        for r in &rows {
                            out.push_str(&format!("{}\t{}\t{}\n", r.kind, r.name, r.idx));
                        }
                        let _ = std::fs::write(p, out);
                    }
                    for r in rows.iter().take(15) {
                        eprintln!("[sort] {} {}", r.kind, r.name);
                    }
                    eprintln!("[chats] sending {} rows", rows.len());
                    let _ = tx.send(Ev::Chats(rows));
                }

                Cmd::LoadMedia { chat, limit } => {
                    let c = client.as_ref().ok_or_else(|| anyhow::anyhow!("not connected"))?;
                    let peer = *peers.get(chat).ok_or_else(|| anyhow::anyhow!("bad chat"))?;
                    let mut iter = c.iter_messages(peer).limit(limit);
                    let mut msgs = Vec::new();
                    let mut rows = Vec::new();
                    while let Some(m) = iter.next().await? {
                        let Some(media) = m.media() else { continue };
                        let Some((name, size, kind)) = describe(&media, m.id()) else { continue };
                        rows.push(MediaRow {
                            msg_id: m.id(),
                            name,
                            size,
                            date: fmt_date(m.date().as_second()),
                            kind,
                            selected: false,
                        });
                        msgs.push(m);
                    }
                    cache.insert(chat, msgs);
                    let _ = tx.send(Ev::Media { chat, items: rows });
                }

                Cmd::CountMedia { chat } => {
                    let c = client.as_ref().ok_or_else(|| anyhow::anyhow!("not connected"))?;
                    let peer = *peers.get(chat).ok_or_else(|| anyhow::anyhow!("bad chat"))?;
                    let res = c
                        .invoke(&tl::functions::messages::GetSearchCounters {
                            peer: peer.into(),
                            saved_peer_id: None,
                            top_msg_id: None,
                            filters: vec![
                                tl::enums::MessagesFilter::InputMessagesFilterPhotoVideo,
                                tl::enums::MessagesFilter::InputMessagesFilterDocument,
                            ],
                        })
                        .await?;
                    let (mut photos, mut files) = (0i32, 0i32);
                    for sc in res {
                        let tl::enums::messages::SearchCounter::Counter(x) = sc;
                        match x.filter {
                            tl::enums::MessagesFilter::InputMessagesFilterPhotoVideo => {
                                photos = x.count
                            }
                            tl::enums::MessagesFilter::InputMessagesFilterDocument => {
                                files = x.count
                            }
                            _ => {}
                        }
                    }
                    let _ = tx.send(Ev::MediaCount {
                        chat,
                        total: photos.saturating_add(files),
                        photos,
                        files,
                    });
                }

                Cmd::LoadMediaAll { chat } => {
                    let c = client.as_ref().ok_or_else(|| anyhow::anyhow!("not connected"))?;
                    let peer = *peers.get(chat).ok_or_else(|| anyhow::anyhow!("bad chat"))?;
                    let mut iter = c.iter_messages(peer);
                    let mut msgs = Vec::new();
                    let mut rows = Vec::new();
                    let mut scanned = 0usize;
                    while let Some(m) = iter.next().await? {
                        scanned += 1;
                        if scanned % 500 == 0 {
                            let _ = tx.send(Ev::MediaProgress {
                                chat,
                                scanned,
                                found: rows.len(),
                            });
                        }
                        let Some(media) = m.media() else { continue };
                        let Some((name, size, kind)) = describe(&media, m.id()) else {
                            continue;
                        };
                        rows.push(MediaRow {
                            msg_id: m.id(),
                            name,
                            size,
                            date: fmt_date(m.date().as_second()),
                            kind,
                            selected: false,
                        });
                        msgs.push(m);
                    }
                    eprintln!("[media] full scan: {} msgs, {} media", scanned, rows.len());
                    cache.insert(chat, msgs);
                    let _ = tx.send(Ev::Media { chat, items: rows });
                }

                Cmd::Download { chat, msg_ids, dir, parts, concurrent } => {
                    let c = client.as_ref().ok_or_else(|| anyhow::anyhow!("not connected"))?.clone();
                    let msgs = cache.get(&chat).ok_or_else(|| anyhow::anyhow!("no index for chat"))?;
                    std::fs::create_dir_all(&dir)?;
                    eprintln!("[dl] dir = {}", dir.display());
                    let mut jobs = Vec::new();
                    let mut skipped = 0usize;
                    for m in msgs.iter().filter(|m| msg_ids.contains(&m.id())) {
                        let Some(media) = m.media() else { continue };
                        let Some((name, size, _)) = describe(&media, m.id()) else { continue };
                        let key = format!("{chat}:{}", m.id());
                        let path = dir.join(format!("{}__{}", m.id(), sanitize(&name)));
                        // Already on disk with the expected size: don't fetch it again.
                        if already_complete(&path, size) {
                            skipped += 1;
                            let _ = tx.send(Ev::Skipped {
                                key,
                                path: path.display().to_string(),
                            });
                            continue;
                        }
                        jobs.push((key, media, size, path));
                    }
                    if skipped > 0 {
                        eprintln!("[dl] skipped {} already on disk", skipped);
                    }
                    let sem = Arc::new(tokio::sync::Semaphore::new(concurrent.clamp(1, 8)));
                    let mut handles = Vec::new();
                    for (key, media, size, path) in jobs {
                        let c = c.clone();
                        let sem = Arc::clone(&sem);
                        let tx2 = tx.clone();
                        let ctx2 = ctx.clone();
                        handles.push(tokio::spawn(async move {
                            let _permit = sem.acquire_owned().await;
                            let r = download_one(c, media, size, path.clone(), key.clone(), parts, tx2.clone(), ctx2).await;
                            match r {
                                Ok(()) => {
                                    let _ = tx2.send(Ev::Done { key, path: path.display().to_string() });
                                }
                                Err(e) => {
                                    let _ = tx2.send(Ev::Fail { key, err: e });
                                }
                            }
                        }));
                    }
                    let tx3 = tx.clone();
                    tokio::spawn(async move {
                        for h in handles { let _ = h.await; }
                        let _ = tx3.send(Ev::Finished);
                    });
                }
            }
            Ok(())
        }
        .await;

        if let Err(e) = res {
            let _ = tx.send(Ev::Err(format!("{e}")));
        }

        // Any event may have landed in the queue. egui only redraws when asked,
        // so without this the UI can sit on a stale frame until the mouse moves.
        ctx.request_repaint();
    }
}

fn describe(media: &Media, msg_id: i32) -> Option<(String, u64, Kind)> {
    let size = media.size().unwrap_or(0) as u64;
    match media {
        Media::Document(d) => {
            let mime = d.mime_type().unwrap_or("");
            let name = d
                .name()
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("doc_{msg_id}{}", ext_for(mime)));
            let kind = kind_for(&name, mime);
            Some((name, size, kind))
        }
        Media::Photo(_) => Some((format!("photo_{msg_id}.jpg"), size, Kind::Photo)),
        Media::Sticker(_) => Some((format!("sticker_{msg_id}.webp"), size, Kind::Sticker)),
        _ => None,
    }
}

fn kind_for(name: &str, mime: &str) -> Kind {
    let n = name.to_lowercase();
    if n.ends_with(".mp4") || n.ends_with(".mkv") || n.ends_with(".mov") || n.ends_with(".webm") || n.ends_with(".avi") {
        Kind::Video
    } else if mime.starts_with("video/") {
        Kind::Video
    } else if n.ends_with(".mp3") || n.ends_with(".m4a") || n.ends_with(".ogg") || n.ends_with(".flac") {
        Kind::Audio
    } else if mime.starts_with("audio/") {
        Kind::Audio
    } else if n.ends_with(".jpg") || n.ends_with(".jpeg") || n.ends_with(".png") || n.ends_with(".webp") {
        Kind::Photo
    } else {
        Kind::Doc
    }
}

fn ext_for(mime: &str) -> &'static str {
    match mime {
        "video/mp4" => ".mp4",
        "video/quicktime" => ".mov",
        "video/webm" => ".webm",
        "audio/mpeg" => ".mp3",
        "audio/mp4" => ".m4a",
        "image/jpeg" => ".jpg",
        "image/png" => ".png",
        "application/pdf" => ".pdf",
        "application/zip" => ".zip",
        _ => ".bin",
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c == '/' || c == '\\' || c == ':' || c == '\0' { '_' } else { c })
        .take(120)
        .collect()
}

fn fmt_date(ts: i64) -> String {
    let dt = chrono::DateTime::from_timestamp(ts, 0);
    match dt {
        Some(d) => d.format("%Y-%m-%d").to_string(),
        None => "-".into(),
    }
}

async fn download_one(
    client: Client,
    media: Media,
    size: u64,
    path: PathBuf,
    key: String,
    parts: usize,
    tx: Sender<Ev>,
    ctx: egui::Context,
) -> Result<(), String> {
    let f = std::fs::File::create(&path).map_err(|e| format!("create: {e}"))?;
    if size > 0 {
        f.set_len(size).map_err(|e| format!("alloc: {e}"))?;
    }
    let f = Arc::new(f);
    let total_chunks = ((size as usize).div_ceil(CHUNK)).max(1);
    let parts = parts.clamp(1, 8).min(total_chunks);

    let mut handles = Vec::new();
    for p in 0..parts {
        let client = client.clone();
        let media = media.clone();
        let f = Arc::clone(&f);
        let key = key.clone();
        let tx = tx.clone();
        let ctx = ctx.clone();

        handles.push(tokio::spawn(async move {
            let mut it = client.iter_download(&media).chunk_size(CHUNK as i32);
            if p > 0 {
                it = it.skip_chunks(p as i32);
            }
            let mut idx = p;
            let mut last = Instant::now();
            let mut acc = 0u64;

            while idx < total_chunks {
                match it.next().await {
                    Ok(Some(data)) => {
                        let off = (idx * CHUNK) as u64;
                        if let Err(e) = f.write_all_at(&data, off) {
                            let _ = tx.send(Ev::Fail { key, err: format!("write: {e}") });
                            return;
                        }
                        acc += data.len() as u64;
                        if last.elapsed() >= Duration::from_millis(120) {
                            let _ = tx.send(Ev::Prog { key: key.clone(), done: acc, total: size });
                            acc = 0;
                            last = Instant::now();
                            ctx.request_repaint();
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.send(Ev::Fail { key, err: format!("{e}") });
                        return;
                    }
                }
                idx += parts;
                if idx < total_chunks && parts > 1 {
                    it = it.skip_chunks((parts - 1) as i32);
                }
            }
            if acc > 0 {
                let _ = tx.send(Ev::Prog { key, done: acc, total: size });
            }
            ctx.request_repaint();
        }));
    }

    for h in handles {
        match h.await {
            Ok(()) => {}
            Err(e) if e.is_panic() => {
                let _ = std::fs::remove_file(&path);
                return Err("CDN-served file - grammers cannot follow the redirect".into());
            }
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

/// True when `path` already holds the whole file: same byte count as Telegram
/// reported. A truncated leftover (killed mid-download) fails the size check and
/// is fetched again. `expected == 0` means Telegram gave no size, so any
/// non-empty file counts as complete.
fn already_complete(path: &std::path::Path, expected: u64) -> bool {
    match std::fs::metadata(path) {
        Ok(md) => md.len() > 0 && (expected == 0 || md.len() == expected),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::already_complete;
    use std::io::Write;
    use std::path::PathBuf;

    fn tmp(name: &str, bytes: usize) -> PathBuf {
        let p = std::env::temp_dir().join(format!("tgrab-test-{name}"));
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(&vec![7u8; bytes]).unwrap();
        p
    }

    #[test]
    fn missing_file_is_not_complete() {
        let p = std::env::temp_dir().join("tgrab-test-does-not-exist.bin");
        let _ = std::fs::remove_file(&p);
        assert!(!already_complete(&p, 100));
    }

    #[test]
    fn exact_size_is_complete() {
        let p = tmp("exact", 100);
        assert!(already_complete(&p, 100));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn truncated_file_is_not_complete() {
        let p = tmp("short", 40);
        assert!(!already_complete(&p, 100));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn empty_file_is_not_complete() {
        let p = tmp("empty", 0);
        assert!(!already_complete(&p, 0));
        assert!(!already_complete(&p, 100));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn unknown_size_accepts_any_nonempty_file() {
        let p = tmp("unknown", 55);
        assert!(already_complete(&p, 0));
        let _ = std::fs::remove_file(&p);
    }
}
