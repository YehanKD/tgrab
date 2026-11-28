use std::collections::HashMap;
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
                    let me_id = c.get_me().await.ok().and_then(|u| u.id().bare_id());
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
                            let is_self = me_id.is_some()
                                && matches!(&d.peer, Peer::User(_))
                                && d.peer_ref().id.bare_id() == me_id;
                            let kind = if is_self { "saved" } else { kind };
                            let name = if is_self {
                                "📥 Saved Messages".to_string()
                            } else {
                                match d.peer.name() {
                                    Some(n) => n.to_string(),
                                    None => match d.peer.username() {
                                        Some(u) => format!("@{u}"),
                                        None => format!(
                                            "{kind} {}",
                                            d.peer_ref().id.bare_id().unwrap_or(0)
                                        ),
                                    },
                                }
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
                                "saved" => 0,
                                "channel" => 1,
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

                Cmd::Forward { chat, msg_ids, to } => {
                    let c = client.as_ref().ok_or_else(|| anyhow::anyhow!("not connected"))?.clone();
                    let from = *peers.get(chat).ok_or_else(|| anyhow::anyhow!("bad source chat"))?;
                    let dest: tl::enums::InputPeer = if to == usize::MAX {
                        tl::enums::InputPeer::PeerSelf
                    } else {
                        (*peers.get(to).ok_or_else(|| anyhow::anyhow!("bad destination"))?).into()
                    };
                    // Drop ids the index says have no media: those are what make the
                    // server reject a whole batch with MEDIA_FILE_INVALID.
                    let ids: Vec<i32> = match cache.get(&chat) {
                        Some(msgs) => msg_ids
                            .iter()
                            .copied()
                            .filter(|id| {
                                msgs.iter()
                                    .find(|m| m.id() == *id)
                                    .map(|m| m.media().is_some())
                                    .unwrap_or(true)
                            })
                            .collect(),
                        None => msg_ids.clone(),
                    };
                    let total = ids.len();
                    eprintln!("[fwd] {} msgs {} -> {} ({} filtered)", msg_ids.len(), chat, to, total);
                    let mut done = 0usize;
                    let mut failed = 0usize;
                    // Work in batches of 100, but split a failing batch in half and retry:
                    // a single unforwardable file then costs one message, not 100.
                    let mut stack: Vec<Vec<i32>> = ids.chunks(100).map(|c| c.to_vec()).collect();
                    stack.reverse();
                    while let Some(batch) = stack.pop() {
                        let seed = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_nanos() as i64)
                            .unwrap_or(1);
                        let random_id: Vec<i64> = (0..batch.len())
                            .map(|i| seed.wrapping_add(i as i64 * 7919) | 1)
                            .collect();
                        let res = c
                            .invoke(&tl::functions::messages::ForwardMessages {
                                silent: false,
                                background: false,
                                with_my_score: false,
                                drop_author: false,
                                drop_media_captions: false,
                                noforwards: false,
                                allow_paid_floodskip: false,
                                from_ephemeral: false,
                                from_peer: from.into(),
                                id: batch.clone(),
                                random_id,
                                to_peer: dest.clone(),
                                top_msg_id: None,
                                reply_to: None,
                                schedule_date: None,
                                schedule_repeat_period: None,
                                send_as: None,
                                quick_reply_shortcut: None,
                                effect: None,
                                video_timestamp: None,
                                allow_paid_stars: None,
                                suggested_post: None,
                            })
                            .await;
                        match res {
                            Ok(_) => {
                                done += batch.len();
                                let _ = tx.send(Ev::Forwarded { done, total, failed });
                                ctx.request_repaint();
                                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
                            }
                            Err(e) if is_fatal_auth(&e) => {
                                // Account banned / session revoked: stop instantly instead of
                                // splitting and hammering the API with more doomed requests.
                                eprintln!("[fwd] aborting on fatal auth error: {e}");
                                let _ = tx.send(Ev::Err(e.to_string()));
                                ctx.request_repaint();
                                break;
                            }
                            Err(e) if batch.len() > 1 => {
                                let mid = batch.len() / 2;
                                eprintln!("[fwd] batch of {} failed ({}), splitting", batch.len(), e);
                                stack.push(batch[mid..].to_vec());
                                stack.push(batch[..mid].to_vec());
                            }
                            Err(e) => {
                                failed += 1;
                                eprintln!("[fwd] id {} unforwardable: {}", batch[0], e);
                                let _ = tx.send(Ev::Forwarded { done, total, failed });
                                ctx.request_repaint();
                            }
                        }
                    }
                    eprintln!("[fwd] done {done}/{total}, {failed} failed");
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
                        jobs.push((key, m.id(), media, size, path));
                    }
                    if skipped > 0 {
                        eprintln!("[dl] skipped {} already on disk", skipped);
                    }
                    let sem = Arc::new(tokio::sync::Semaphore::new(concurrent.clamp(1, 16)));
                    // A channel that Telegram has blocked refuses every file with the
                    // same 401. Stop after a few instead of producing one failure per file.
                    let banned_hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                    let ok_hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                    let mut handles = Vec::new();
                    let dl_peer = *peers.get(chat).ok_or_else(|| anyhow::anyhow!("bad chat"))?;
                    for (key, msg_id, media, size, path) in jobs {
                        let c = c.clone();
                        let sem = Arc::clone(&sem);
                        let tx2 = tx.clone();
                        let ctx2 = ctx.clone();
                        let banned_hits = Arc::clone(&banned_hits);
                        let ok_hits = Arc::clone(&ok_hits);
                        handles.push(tokio::spawn(async move {
                            // Don't start a file while paused: no slot, no request.
                            while crate::model::paused() {
                                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                            }
                            let _permit = sem.acquire_owned().await;
                            if banned_hits.load(std::sync::atomic::Ordering::Relaxed) >= 5
                                && ok_hits.load(std::sync::atomic::Ordering::Relaxed) == 0
                            {
                                let _ = tx2.send(Ev::Fail {
                                    key,
                                    err: "channel blocked by Telegram — its files cannot be fetched"
                                        .into(),
                                });
                                return;
                            }
                            let mut attempt: u32 = 0;
                            // Five minutes of retrying per file, starting at the first failure.
                            let mut deadline: Option<std::time::Instant> = None;
                            loop {
                                // Re-fetch the message before every attempt. File references
                                // expire (~1h) and a queue this long outlives them; the official
                                // clients refresh the same way. A stale reference is what
                                // surfaces as 401 USER_DEACTIVATED_BAN / MEDIA_FILE_INVALID.
                                let fresh = match c.get_messages_by_id(dl_peer, &[msg_id]).await {
                                    Ok(list) => list
                                        .into_iter()
                                        .next()
                                        .flatten()
                                        .and_then(|m| m.media()),
                                    Err(e) => {
                                        eprintln!("[dl] {key} refresh failed: {e}");
                                        None
                                    }
                                };
                                let media_now = fresh.unwrap_or_else(|| media.clone());
                                let r = download_one(
                                    c.clone(), media_now, size, path.clone(),
                                    key.clone(), parts, tx2.clone(), ctx2.clone(),
                                ).await;
                                match r {
                                    Ok(()) => {
                                        ok_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        let _ = tx2.send(Ev::Done {
                                            key: key.clone(),
                                            path: path.display().to_string(),
                                        });
                                        break;
                                    }
                                    Err(e) => {
                                        eprintln!("[dl] {key} attempt {} failed: {e}", attempt + 1);
                                        if e.contains("USER_DEACTIVATED_BAN")
                                            || e.contains("file reference rejected")
                                        {
                                            banned_hits
                                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        }
                                        let now = std::time::Instant::now();
                                        let dl = *deadline.get_or_insert(
                                            now + std::time::Duration::from_secs(RETRY_WINDOW_SECS),
                                        );
                                        if is_fatal_auth(&e)
                                            || e.contains("CDN-served")
                                            || e.starts_with("unavailable:")
                                        {
                                            let _ = tx2.send(Ev::Fail { key, err: e });
                                            break;
                                        }
                                        if now >= dl {
                                            let _ = tx2.send(Ev::Fail {
                                                key,
                                                err: format!(
                                                    "{e} — gave up after {} min",
                                                    RETRY_WINDOW_SECS / 60
                                                ),
                                            });
                                            break;
                                        }
                                        attempt += 1;
                                        let left = dl.saturating_duration_since(now).as_secs();
                                        // Never sleep past the deadline waiting for a retry.
                                        let wait = retry_delay(attempt).min(left.max(1));
                                        let _ = tx2.send(Ev::Retry {
                                            key: key.clone(),
                                            attempt,
                                            wait,
                                            left,
                                            err: e,
                                        });
                                        ctx2.request_repaint();
                                        tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                                    }
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
    // Write into <name>.part and only rename on success. A preallocated file at
    // the final path looks "complete" to the size check even when not one byte
    // landed, which is how a killed transfer turned into a permanent zero-filled
    // stub that later runs skipped forever.
    let part_path = {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "download".into());
        path.with_file_name(format!("{name}.part"))
    };
    let _ = std::fs::remove_file(&part_path);
    let f = std::fs::File::create(&part_path).map_err(|e| format!("create: {e}"))?;
    if size > 0 {
        f.set_len(size).map_err(|e| format!("alloc: {e}"))?;
    }
    let f = Arc::new(f);
    let written = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let total_chunks = ((size as usize).div_ceil(CHUNK)).max(1);
    let parts = parts.clamp(1, 16).min(total_chunks);
    // One flag per chunk: proves every offset was written, not just that the
    // byte count adds up (a worker can write the same chunk twice and skip one).
    let first_err: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
    let seen: Arc<Vec<std::sync::atomic::AtomicBool>> = Arc::new(
        (0..total_chunks)
            .map(|_| std::sync::atomic::AtomicBool::new(false))
            .collect(),
    );

    let mut handles = Vec::new();
    for p in 0..parts {
        let client = client.clone();
        let media = media.clone();
        let f = Arc::clone(&f);
        let written_bytes = Arc::clone(&written);
        let seen = Arc::clone(&seen);
        let first_err = Arc::clone(&first_err);
        let key = key.clone();
        let tx = tx.clone();
        let ctx = ctx.clone();

        handles.push(tokio::spawn(async move {
            let mut it = client.iter_download(&media).chunk_size(CHUNK as i32);
            if p > 0 {
                it = it.skip_chunks(p as i32);
            }
            let mut idx = p;
            let mut first_read = true;
            let mut last = Instant::now();
            let mut acc = 0u64;

            while idx < total_chunks {
                // Idle here while paused: no new requests, no writes.
                while crate::model::paused() {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                match it.next().await {
                    Ok(Some(data)) => {
                        if first_read {
                            first_read = false;
                            if p == 0 {
                                eprintln!(
                                    "[dl] {key} first chunk {}B at idx {idx}/{total_chunks} (size {size}, parts {parts})",
                                    data.len()
                                );
                            }
                        }
                        let off = (idx * CHUNK) as u64;
                        if let Err(e) = write_at(&f, &data, off) {
                            let _ = tx.send(Ev::Fail { key, err: format!("write: {e}") });
                            return;
                        }
                        seen[idx].store(true, std::sync::atomic::Ordering::Relaxed);
                        acc += data.len() as u64;
                        written_bytes.fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        if last.elapsed() >= Duration::from_millis(120) {
                            let _ = tx.send(Ev::Prog { key: key.clone(), done: acc, total: size });
                            acc = 0;
                            last = Instant::now();
                            ctx.request_repaint();
                        }
                    }
                    Ok(None) => {
                        if p == 0 {
                            eprintln!("[dl] {key} iterator returned None at idx {idx}/{total_chunks} (size {size})");
                        }
                        break;
                    }
                    Err(e) => {
                        eprintln!("[dl] {key} read failed at idx {idx}/{total_chunks}: {e}");
                        if let Ok(mut slot) = first_err.lock() {
                            if slot.is_none() {
                                *slot = Some(e.to_string());
                            }
                        }
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
                let _ = std::fs::remove_file(&part_path);
                return Err("CDN-served file - grammers cannot follow the redirect".into());
            }
            Err(e) => {
                let _ = std::fs::remove_file(&part_path);
                return Err(e.to_string());
            }
        }
    }

    // Every worker can exit early (Ok(None), short chunks) without an error, so
    // trust the byte count, not the file size: set_len already faked the size.
    let got = written.load(std::sync::atomic::Ordering::Relaxed);
    if size > 0 && got == 0 {
        // Telegram returned not a single byte: deleted media, an empty document,
        // or a location the account cannot read. Waiting five minutes and asking
        // again produces exactly the same empty answer.
        let _ = std::fs::remove_file(&part_path);
        let raw = first_err
            .lock()
            .ok()
            .and_then(|g| g.clone())
            .unwrap_or_default();
        // Say why. A file whose uploader was banned is not a network problem and
        // will never become fetchable, no matter how long we retry.
        let reason = if raw.contains("USER_DEACTIVATED_BAN") {
            "file reference rejected (stale reference or dead uploader)".to_string()
        } else if raw.contains("MEDIA_FILE_INVALID") {
            "media is invalid on Telegram's side".to_string()
        } else if raw.is_empty() {
            "server returned no data".to_string()
        } else {
            raw
        };
        return Err(format!("unavailable: {reason}"));
    }
    let missing = (0..total_chunks)
        .filter(|i| !seen[*i].load(std::sync::atomic::Ordering::Relaxed))
        .count();
    if size > 0 && missing > 0 {
        let _ = std::fs::remove_file(&part_path);
        return Err(format!(
            "incomplete: {missing} of {total_chunks} chunks missing"
        ));
    }
    if size > 0 && got != size {
        let _ = std::fs::remove_file(&part_path);
        return Err(format!("incomplete: {got} of {size} bytes"));
    }
    let on_disk = std::fs::metadata(&part_path).map(|m| m.len()).unwrap_or(0);
    if size > 0 && on_disk != size {
        let _ = std::fs::remove_file(&part_path);
        return Err(format!("incomplete on disk: {on_disk} of {size} bytes"));
    }
    std::fs::rename(&part_path, &path).map_err(|e| format!("rename: {e}"))?;
    Ok(())
}

/// True when `path` already holds the whole file: same byte count as Telegram
/// reported. A truncated leftover (killed mid-download) fails the size check and
/// is fetched again. `expected == 0` means Telegram gave no size, so any
/// non-empty file counts as complete.
/// Positioned write that works on both Unix and Windows.
fn write_at(f: &std::fs::File, buf: &[u8], off: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        return f.write_all_at(buf, off);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut written = 0usize;
        while written < buf.len() {
            match f.seek_write(&buf[written..], off + written as u64) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "seek_write wrote 0 bytes",
                    ))
                }
                Ok(n) => written += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = f;
        f.seek(SeekFrom::Start(off))?;
        f.write_all(buf)
    }
}

/// Keep retrying one file for this long before giving up on it.
const RETRY_WINDOW_SECS: u64 = 300;

/// Network drops, flood waits and half-finished transfers all get retried;
/// permanent ones (bad file reference, auth) do not.
fn retry_delay(attempt: u32) -> u64 {
    match attempt {
        1 => 2,
        2 => 3,
        3 => 5,
        4 => 8,
        5 => 13,
        6 => 21,
        7 => 30,
        _ => 60,
    }
}

fn is_fatal_auth<E: std::fmt::Display>(e: &E) -> bool {
    let s = e.to_string();
    s.contains("USER_DEACTIVATED")
        || s.contains("AUTH_KEY_UNREGISTERED")
        || s.contains("SESSION_REVOKED")
        || s.contains("USER_BANNED")
        || s.contains("PHONE_NUMBER_BANNED")
}

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
