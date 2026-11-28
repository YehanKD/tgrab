// Diagnostic probe: why does upload.getFile return USER_DEACTIVATED_BAN for a
// file the official client can play?
//
//   cargo run --release --bin probe -- "FilterSEC" 11221
//
// Works on a COPY of the session so it can run while tgrab is open.
use grammers_client::session::storages::SqliteSession;
use grammers_client::session::Session;
use grammers_client::media::Downloadable;
use grammers_client::{Client, SenderPool};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let want = args.get(1).cloned().unwrap_or_else(|| "FilterSEC".into());
    let msg_id: i32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(11221);

    let cfg: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dirs::config_dir().unwrap().join("tgrab/config.json"))?)?;
    let api_id: i32 = cfg["api_id"].as_str().unwrap_or("2040").parse()?;
    let api_hash = cfg["api_hash"].as_str().unwrap_or("").to_string();

    let src = dirs::config_dir().unwrap().join("tgrab/session.db");
    let copy = std::env::temp_dir().join("tgrab-probe-session.db");
    std::fs::copy(&src, &copy)?;
    println!("session copy: {} ({} bytes)", copy.display(), std::fs::metadata(&copy)?.len());

    let session = Arc::new(SqliteSession::open(&copy).await?);
    let pool = SenderPool::new(Arc::clone(&session), api_id);
    let client = Client::new(pool.handle.clone());
    let mut updates = pool.updates;
    tokio::spawn(async move { while updates.recv().await.is_some() {} });
    tokio::spawn(pool.runner.run());

    println!("authorized: {}", client.is_authorized().await?);
    println!("home dc: {:?}", session.home_dc_id());
    println!("api_hash len: {}", api_hash.len());

    // find the chat
    let mut dialogs = client.iter_dialogs();
    let mut target = None;
    while let Some(d) = dialogs.next().await? {
        let name = d.peer.name().map(|n| n.to_string()).unwrap_or_default();
        if name.to_lowercase().contains(&want.to_lowercase()) {
            println!("chat: {:?} ", name);
            target = Some(d.peer_ref());
            break;
        }
    }
    let Some(peer) = target else {
        println!("chat not found for {want}");
        return Ok(());
    };

    // msg_id 0 = survey mode: try the first few media messages in the chat
    if msg_id == 0 {
        let mut it = client.iter_messages(peer);
        let mut tried = 0;
        while let Some(m) = it.next().await? {
            let Some(media) = m.media() else { continue };
            if tried >= 5 { break }
            tried += 1;
            let label = match &media {
                grammers_client::media::Media::Document(d) => d.name().unwrap_or("?").to_string(),
                _ => "non-document".to_string(),
            };
            let dcid = match &media {
                grammers_client::media::Media::Document(d) => d.dc_id(),
                grammers_client::media::Media::Photo(p) => p.dc_id(),
                _ => None,
            };
            print!("  msg {} dc={:?} {:>34.34} -> ", m.id(), dcid, label);
            let mut dl = client.iter_download(&media).chunk_size(512 * 1024);
            match dl.next().await {
                Ok(Some(b)) => println!("OK {}B", b.len()),
                Ok(None) => println!("EMPTY"),
                Err(e) => println!("ERR {e:?}"),
            }
        }
        return Ok(());
    }

    // Forward to Saved Messages, then try to fetch the copy. Tests whether the
    // block travels with the document or stays with the channel.
    if args.get(3).map(|s| s.as_str()) == Some("forward") {
        let me = client.get_me().await?;
        let me_id = me.id().bare_id();
        let mut self_ref = None;
        let mut d2 = client.iter_dialogs();
        while let Some(dd) = d2.next().await? {
            if matches!(&dd.peer, grammers_client::peer::Peer::User(_))
                && dd.peer_ref().id.bare_id() == me_id
            {
                self_ref = Some(dd.peer_ref());
                break;
            }
        }
        let Some(me_ref) = self_ref else {
            println!("self chat not found in dialogs");
            return Ok(());
        };
        println!("forwarding {} from {:?} to saved messages...", msg_id, want);
        match client.forward_messages(me_ref, &[msg_id], peer).await {
            Ok(list) => {
                match list.into_iter().next().flatten() {
                    Some(m) => {
                        println!("forwarded ok, new id {}", m.id());
                        if let Some(media) = m.media() {
                            let mut dl = client.iter_download(&media).chunk_size(512 * 1024);
                            match dl.next().await {
                                Ok(Some(b)) => println!("SAVED COPY: OK {} bytes", b.len()),
                                Ok(None) => println!("SAVED COPY: EMPTY"),
                                Err(e) => println!("SAVED COPY: ERR {e:?}"),
                            }
                        } else {
                            println!("forwarded message has no media");
                        }
                    }
                    None => println!("forward returned nothing"),
                }
            }
            Err(e) => println!("forward failed: {e:?}"),
        }
        return Ok(());
    }

    let msgs = client.get_messages_by_id(peer, &[msg_id]).await?;
    let Some(Some(m)) = msgs.into_iter().next() else {
        println!("message {msg_id} not returned");
        return Ok(());
    };
    println!("message {} date {}", m.id(), m.date());

    match m.media() {
        Some(media) => {
            println!("media size: {:?}", media.size());
            if let grammers_client::media::Media::Document(d) = &media {
                println!("mime: {:?} name: {:?}", d.mime_type(), d.name());
            }
            // one chunk, exactly like the downloader does it
            let mut it = client.iter_download(&media).chunk_size(512 * 1024);
            match it.next().await {
                Ok(Some(bytes)) => println!("OK first chunk: {} bytes", bytes.len()),
                Ok(None) => println!("EMPTY response (no bytes at offset 0)"),
                Err(e) => println!("ERROR on getFile: {e:?}"),
            }
            // for comparison: the same call through the raw API on home DC
        }
        None => println!("no media on this message"),
    }

    Ok(())
}
