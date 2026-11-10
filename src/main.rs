mod model;
mod tg;

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;

use eframe::egui;
use model::*;

fn main() -> eframe::Result {
    let opts = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1300.0, 840.0])
            .with_min_inner_size([960.0, 600.0])
            .with_title("tgrab"),
        ..Default::default()
    };
    eframe::run_native("tgrab", opts, Box::new(|cc| Ok(Box::new(App::new(cc)))))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Setup,
    Qr,
    Phone,
    Code,
    Password,
    Ready,
}

/// Telegram Desktop's public client identity. `tdl` uses the same one, which is
/// why it never asks for credentials. Override in the Advanced panel.
const DEF_API_ID: i32 = 2040;
const DEF_API_HASH: &str = "b18441a1ff607e10a989891a5462e627";

#[derive(Clone)]
enum QState {
    Wait,
    Run,
    Ok,
    Skip,
    Err(String),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SortMode {
    Newest,
    Oldest,
    SizeDesc,
    SizeAsc,
    Name,
}

impl SortMode {
    fn label(self) -> &'static str {
        match self {
            SortMode::Newest => "newest first",
            SortMode::Oldest => "oldest first",
            SortMode::SizeDesc => "size: largest first",
            SortMode::SizeAsc => "size: smallest first",
            SortMode::Name => "name",
        }
    }
}

struct QRow {
    key: String,
    name: String,
    done: u64,
    total: u64,
    state: QState,
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct Config {
    api_id: String,
    api_hash: String,
    phone: String,
    out_dir: String,
    parts: usize,
    concurrent: usize,
}

struct App {
    tx: tokio::sync::mpsc::UnboundedSender<Cmd>,
    rx: Receiver<Ev>,
    phase: Phase,
    cfg: Config,
    status: String,
    me: Option<String>,
    chats: Vec<ChatRow>,
    filtered: Vec<usize>,
    search: String,
    sel_chat: Option<usize>,
    media: Vec<MediaRow>,
    media_chat: Option<usize>,
    loading_media: bool,
    media_count: Option<(i32, i32)>,
    scanning_all: bool,
    scan_progress: Option<(usize, usize)>,
    sort: SortMode,
    pick_rx: Option<std::sync::mpsc::Receiver<Option<PathBuf>>>,
    dl_bytes: u64,
    speed: std::collections::VecDeque<(std::time::Instant, u64)>,
    fwd_open: bool,
    fwd_filter: String,
    fwd_target: Option<usize>,
    fwd_groups: bool,
    limit: usize,
    queue: Vec<QRow>,
    qidx: HashMap<String, usize>,
    downloading: bool,
    qr: Option<String>,
    advanced: bool,
    frames: u64,
}

impl App {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let mut visuals = egui::Visuals::dark();
        visuals.panel_fill = egui::Color32::from_rgb(18, 20, 24);
        visuals.window_fill = egui::Color32::from_rgb(24, 27, 32);
        visuals.extreme_bg_color = egui::Color32::from_rgb(12, 14, 17);
        visuals.selection.bg_fill = egui::Color32::from_rgb(38, 96, 168);
        cc.egui_ctx.set_visuals(visuals);
        setup_fonts(&cc.egui_ctx);

        let cfg = load_cfg();
        let (tx, rx) = tg::spawn(cc.egui_ctx.clone());

        let mut app = Self {
            tx,
            rx,
            phase: Phase::Setup,
            cfg,
            status: "idle".into(),
            me: None,
            chats: Vec::new(),
            filtered: Vec::new(),
            search: String::new(),
            sel_chat: None,
            media: Vec::new(),
            media_chat: None,
            loading_media: false,
            media_count: None,
            scanning_all: false,
            scan_progress: None,
            sort: SortMode::Newest,
            pick_rx: None,
            dl_bytes: 0,
            speed: std::collections::VecDeque::new(),
            fwd_open: false,
            fwd_filter: String::new(),
            fwd_target: None,
            fwd_groups: false,
            limit: 500,
            queue: Vec::new(),
            qidx: HashMap::new(),
            downloading: false,
            qr: None,
            advanced: false,
            frames: 0,
        };

        if let Ok(id) = app.cfg.api_id.trim().parse::<i32>() {
            if id != 0 && !app.cfg.api_hash.trim().is_empty() {
                let db = dirs::config_dir()
                    .unwrap_or_default()
                    .join("tgrab")
                    .join("session.db");
                if let Some(p) = db.parent() {
                    let _ = std::fs::create_dir_all(p);
                }
                let _ = app.tx.send(Cmd::Connect {
                    api_id: id,
                    api_hash: app.cfg.api_hash.trim().to_string(),
                    db,
                });
                app.status = "connecting...".into();
            }
        }

        app
    }

    fn pump(&mut self) -> bool {
        let mut any = false;
        if self.pick_rx.is_some() {
            let res = self.pick_rx.as_ref().map(|rx| rx.try_recv());
            match res {
                Some(Ok(Some(dir))) => {
                    self.pick_rx = None;
                    self.cfg.out_dir = dir.display().to_string();
                    save_cfg(&self.cfg);
                    self.status = format!("downloads go to {}", dir.display());
                }
                Some(Ok(None)) => {
                    self.pick_rx = None;
                    self.status = "folder selection cancelled".to_string();
                }
                Some(Err(std::sync::mpsc::TryRecvError::Disconnected)) => self.pick_rx = None,
                _ => {}
            }
        }

        while let Ok(ev) = self.rx.try_recv() {
            any = true;
            match ev {
                Ev::Log(s) => self.status = s,
                Ev::Err(e) => {
                    self.status = e;
                    self.loading_media = false;
                    if self.phase == Phase::Qr {
                        self.qr = None;
                    }
                }
                Ev::NeedCode => {
                    self.phase = Phase::Phone;
                    self.status = "connected - enter your phone number".into();
                }
                Ev::NeedQr => {
                    self.phase = Phase::Qr;
                    self.status = "scan the QR with your phone".into();
                    let _ = self.tx.send(Cmd::QrLogin);
                }
                Ev::Qr(url) => {
                    self.qr = Some(url);
                }
                Ev::Reconnect => {
                    self.status = "switching datacenter...".into();
                    let _ = self.tx.send(Cmd::Connect {
                        api_id: self.cfg.api_id.trim().parse().unwrap_or(2040),
                        api_hash: self.cfg.api_hash.clone(),
                        db: session_db(),
                    });
                }
                Ev::NeedPassword(_) => {
                    self.phase = Phase::Password;
                    self.status = "2FA password required".into();
                }
                Ev::Authorized { me } => {
                    self.phase = Phase::Ready;
                    self.status = "ready".into();
                    self.me = Some(me);
                    let _ = self.tx.send(Cmd::LoadChats);
                }
                Ev::Chats(c) => {
                    eprintln!("[ui] got {} chats", c.len());
                    self.chats = c;
                    self.refilter();
                    self.status = format!("{} chats", self.chats.len());
                }
                Ev::Media { chat, items } => {
                    self.media = items;
                    self.media_chat = Some(chat);
                    self.loading_media = false;
                    self.scanning_all = false;
                    self.scan_progress = None;
                    self.apply_sort();
                    self.status = format!("{} media items", self.media.len());
                }
                Ev::MediaCount {
                    chat,
                    total,
                    photos,
                    files,
                } => {
                    if self.media_chat == Some(chat) {
                        self.media_count = Some((total, files));
                        let _ = photos;
                    }
                }
                Ev::MediaProgress {
                    chat,
                    scanned,
                    found,
                } => {
                    if self.media_chat == Some(chat) {
                        self.scan_progress = Some((scanned, found));
                    }
                }
                Ev::Prog { key, done, total } => {
                    self.dl_bytes += done as u64;
                    if let Some(&i) = self.qidx.get(&key) {
                        let r = &mut self.queue[i];
                        r.done += done;
                        r.total = total;
                        r.state = QState::Run;
                    }
                }
                Ev::Forwarded { done, total } => {
                    self.status = if done >= total {
                        format!("forwarded {total} message(s)")
                    } else {
                        format!("forwarding… {done}/{total}")
                    };
                }
                Ev::Skipped { key, path: _ } => {
                    if let Some(&i) = self.qidx.get(&key) {
                        let r = &mut self.queue[i];
                        r.state = QState::Skip;
                        if r.total > 0 {
                            r.done = r.total;
                        }
                    }
                }
                Ev::Done { key, path: _ } => {
                    if let Some(&i) = self.qidx.get(&key) {
                        let r = &mut self.queue[i];
                        r.state = QState::Ok;
                        if r.total > 0 {
                            r.done = r.total;
                        }
                    }
                }
                Ev::Fail { key, err } => {
                    if let Some(&i) = self.qidx.get(&key) {
                        self.queue[i].state = QState::Err(err);
                    }
                }
                Ev::Finished => {
                    self.downloading = false;
                    self.status = "download batch finished".into();
                }
            }
        }
        any
    }

    fn refilter(&mut self) {
        let q = self.search.to_lowercase();
        // Positions in `chats`, not the peer index stored on the row: `idx` is
        // assigned before sorting, so using it here would undo the sort.
        self.filtered = self
            .chats
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                q.is_empty()
                    || c.name.to_lowercase().contains(&q)
                    || c.username
                        .as_deref()
                        .is_some_and(|u| u.to_lowercase().contains(&q))
            })
            .map(|(pos, _)| pos)
            .collect();
    }

    fn apply_sort(&mut self) {
        match self.sort {
            SortMode::Newest => self.media.sort_by(|a, b| b.msg_id.cmp(&a.msg_id)),
            SortMode::Oldest => self.media.sort_by(|a, b| a.msg_id.cmp(&b.msg_id)),
            SortMode::SizeDesc => self.media.sort_by(|a, b| b.size.cmp(&a.size)),
            SortMode::SizeAsc => self.media.sort_by(|a, b| a.size.cmp(&b.size)),
            SortMode::Name => self
                .media
                .sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase())),
        }
    }

    fn base_dir(&self) -> PathBuf {
        if self.cfg.out_dir.is_empty() {
            dirs::download_dir().unwrap_or_default().join("tgrab")
        } else {
            PathBuf::from(&self.cfg.out_dir)
        }
    }

    fn chat_dir(&self, chat: usize) -> PathBuf {
        let name = self
            .chats
            .iter()
            .find(|c| c.idx == chat)
            .map(|c| c.name.as_str())
            .unwrap_or("");
        self.base_dir().join(safe_dir_name(name, chat))
    }

    fn start_download(&mut self, ids: Vec<i32>) {
        let Some(chat) = self.media_chat else { return };
        if ids.is_empty() {
            return;
        }
        let dir = self.chat_dir(chat);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            self.status = format!("cannot create {}: {e}", dir.display());
            return;
        }

        for m in self.media.iter().filter(|m| ids.contains(&m.msg_id)) {
            let key = format!("{chat}:{}", m.msg_id);
            if self.qidx.contains_key(&key) {
                continue;
            }
            let i = self.queue.len();
            self.qidx.insert(key.clone(), i);
            self.queue.push(QRow {
                key,
                name: m.name.clone(),
                done: 0,
                total: m.size,
                state: QState::Wait,
            });
        }

        let _ = self.tx.send(Cmd::Download {
            chat,
            msg_ids: ids,
            dir,
            parts: self.cfg.parts.clamp(1, 16),
            concurrent: self.cfg.concurrent.clamp(1, 16),
        });
        self.downloading = true;
        self.status = "downloading".into();
    }
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        let had = self.pump();
        self.frames += 1;
        if self.frames % 50 == 0 {
            eprintln!("[ui] frame {} had={} phase={:?}", self.frames, had, self.phase as u8);
        }
        if had || self.phase != Phase::Ready || self.downloading || (self.chats.is_empty() && self.me.is_some()) {
            ctx.request_repaint_after(std::time::Duration::from_millis(100));
        }

        egui::Panel::top("hdr").show(ui, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.heading("tgrab");
                ui.separator();
                if let Some(me) = &self.me {
                    ui.label(egui::RichText::new(me).strong());
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(egui::RichText::new(&self.status).weak());
                });
            });
            ui.add_space(4.0);
        });

        if self.phase != Phase::Ready {
            egui::CentralPanel::default_margins().show(ui, |ui| {
                ui.add_space(40.0);
                ui.vertical_centered(|ui| {
                    ui.set_max_width(420.0);
                    ui.heading("Sign in to Telegram");
                    ui.add_space(10.0);
                    if self.phase == Phase::Setup {
                        ui.label("Optional - leave the defaults unless you have your own app.");
                    }
                    ui.add_space(12.0);

                    match self.phase {
                        Phase::Qr => {
                            ui.add_space(6.0);
                            if let Some(url) = &self.qr {
                                draw_qr(ui, url, 260.0);
                            } else {
                                ui.add_space(110.0);
                                ui.label("waiting for a code from Telegram...");
                                ui.spinner();
                                ui.add_space(110.0);
                            }
                            ui.add_space(10.0);
                            ui.label("Phone app: Settings -> Devices -> Link Desktop Device");
                            ui.label("The code refreshes itself every 30 seconds - scan the one you see now.");
                            ui.add_space(10.0);
                            ui.horizontal(|ui| {
                                if ui.button("Refresh code").clicked() {
                                    self.qr = None;
                                    let _ = self.tx.send(Cmd::QrLogin);
                                }
                                if ui.button("Use phone number instead").clicked() {
                                    self.phase = Phase::Phone;
                                }
                                if ui.button("Advanced: my own API ID").clicked() {
                                    self.phase = Phase::Setup;
                                }
                            });
                        }
                        Phase::Setup => {
                            ui.label("API ID");
                            ui.text_edit_singleline(&mut self.cfg.api_id);
                            ui.label("API hash");
                            ui.text_edit_singleline(&mut self.cfg.api_hash);
                            ui.add_space(10.0);
                            if ui.button("Connect").clicked() {
                                match self.cfg.api_id.trim().parse::<i32>() {
                                    Ok(id) if !self.cfg.api_hash.trim().is_empty() => {
                                        save_cfg(&self.cfg);
                                        let db = dirs::config_dir()
                                            .unwrap_or_default()
                                            .join("tgrab")
                                            .join("session.db");
                                        if let Some(p) = db.parent() {
                                            let _ = std::fs::create_dir_all(p);
                                        }
                                        let _ = self.tx.send(Cmd::Connect {
                                            api_id: id,
                                            api_hash: self.cfg.api_hash.trim().to_string(),
                                            db,
                                        });
                                        self.status = "connecting...".into();
                                    }
                                    _ => self.status = "invalid API ID".into(),
                                }
                            }
                        }
                        Phase::Phone => {
                            ui.label("Phone number (with country code)");
                            ui.text_edit_singleline(&mut self.cfg.phone);
                            ui.add_space(10.0);
                            if ui.button("Send code").clicked() {
                                save_cfg(&self.cfg);
                                let _ = self.tx.send(Cmd::RequestCode {
                                    phone: self.cfg.phone.trim().to_string(),
                                });
                                self.phase = Phase::Code;
                                self.status = "code requested".into();
                            }
                        }
                        Phase::Code => {
                            ui.label("Login code from Telegram");
                            let mut code = String::new();
                            ui.text_edit_singleline(&mut code);
                            ui.add_space(10.0);
                            if ui.button("Sign in").clicked() {
                                let _ =
                                    self.tx.send(Cmd::SignIn { code: code.trim().to_string() });
                            }
                        }
                        Phase::Password => {
                            ui.label("Two-step verification password");
                            let mut pw = String::new();
                            ui.add(egui::TextEdit::singleline(&mut pw).password(true));
                            ui.add_space(10.0);
                            if ui.button("Unlock").clicked() {
                                let _ = self.tx.send(Cmd::Password { pw });
                            }
                        }
                        Phase::Ready => {}
                    }
                });
            });
            return;
        }

        egui::Panel::bottom("queue")
            .resizable(true)
            .default_size(200.0)
            .show(ui, |ui| {
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    ui.strong(format!("Queue ({})", self.queue.len()));
                    let done = self
                        .queue
                        .iter()
                        .filter(|q| matches!(q.state, QState::Ok))
                        .count();
                    let failed = self
                        .queue
                        .iter()
                        .filter(|q| matches!(q.state, QState::Err(_)))
                        .count();
                    let skipped = self
                        .queue
                        .iter()
                        .filter(|q| matches!(q.state, QState::Skip))
                        .count();
                    if skipped > 0 {
                        ui.label(format!("{done} done, {skipped} skipped, {failed} failed"));
                    } else {
                        ui.label(format!("{done} done, {failed} failed"));
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        // Realtime throughput: sliding ~600 ms window over byte deltas,
                        // recomputed every frame rather than on a 1 s tick.
                        let now = std::time::Instant::now();
                        self.speed.push_back((now, self.dl_bytes));
                        while self.speed.len() > 2
                            && now.duration_since(self.speed[0].0).as_millis() > 600
                        {
                            self.speed.pop_front();
                        }
                        let (t0, b0) = *self.speed.front().unwrap_or(&(now, self.dl_bytes));
                        let dt = now.duration_since(t0).as_secs_f64();
                        let mbps = if dt > 0.10 && self.dl_bytes > b0 {
                            (self.dl_bytes - b0) as f64 / dt / 1_000_000.0
                        } else {
                            0.0
                        };
                        if mbps > 0.0 {
                            ui.label(
                                egui::RichText::new(format!("{mbps:.1} MB/s"))
                                    .strong()
                                    .color(egui::Color32::from_rgb(120, 205, 255)),
                            );
                            ui.label(
                                egui::RichText::new(format!(
                                    "{:.0} MB",
                                    self.dl_bytes as f64 / 1_000_000.0
                                ))
                                .weak(),
                            );
                        } else if self.dl_bytes > 0 {
                            ui.label(
                                egui::RichText::new(format!(
                                    "{:.0} MB",
                                    self.dl_bytes as f64 / 1_000_000.0
                                ))
                                .weak(),
                            );
                        }
                        if ui.button("Open folder").clicked() {
                            let d = match self.media_chat {
                                Some(c) => self.chat_dir(c),
                                None => self.base_dir(),
                            };
                            let _ = std::fs::create_dir_all(&d);
                            let _ = open::that(d);
                        }
                        if ui.button("Clear finished").clicked() {
                            self.queue
                                .retain(|q| !matches!(q.state, QState::Ok | QState::Skip));
                            self.qidx = self
                                .queue
                                .iter()
                                .enumerate()
                                .map(|(i, q)| (q.key.clone(), i))
                                .collect();
                        }
                    });
                });
                ui.separator();
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        for q in &self.queue {
                            ui.horizontal(|ui| {
                                let frac = if q.total > 0 {
                                    (q.done as f32 / q.total as f32).clamp(0.0, 1.0)
                                } else {
                                    0.0
                                };
                                ui.add_sized(
                                    [220.0, 18.0],
                                    egui::ProgressBar::new(frac).show_percentage(),
                                );
                                match &q.state {
                                    QState::Ok => ui.colored_label(
                                        egui::Color32::from_rgb(90, 200, 120),
                                        "ok",
                                    ),
                                    QState::Err(e) => ui.colored_label(
                                        egui::Color32::from_rgb(230, 100, 100),
                                        e,
                                    ),
                                    QState::Run => ui.label("downloading"),
                                    QState::Skip => ui.colored_label(
                                        egui::Color32::from_rgb(150, 150, 160),
                                        "skipped (on disk)",
                                    ),
                                    QState::Wait => ui.label("queued"),
                                };
                                ui.label(&q.name);
                            });
                        }
                    });
            });

        egui::Panel::left("chats")
            .resizable(true)
            .default_size(300.0)
            .show(ui, |ui| {
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    ui.label("Chats");
                    if ui.small_button("reload").clicked() {
                        let _ = self.tx.send(Cmd::LoadChats);
                    }
                });
                if ui.text_edit_singleline(&mut self.search).changed() {
                    self.refilter();
                }
                ui.separator();
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        for k in 0..self.filtered.len() {
                            let pos = self.filtered[k];
                            let c = &self.chats[pos];
                            let label = format!("{}  [{}]", c.name, c.kind);
                            let idx = c.idx;
                            let sel = self.sel_chat == Some(idx);
                            if ui.selectable_label(sel, label).clicked() {
                                self.sel_chat = Some(idx);
                                self.media_chat = Some(idx);
                                self.media.clear();
                                self.media_count = None;
                                self.scan_progress = None;
                                self.scanning_all = false;
                                self.loading_media = true;
                                let _ = self.tx.send(Cmd::CountMedia { chat: idx });
                                let _ = self.tx.send(Cmd::LoadMedia {
                                    chat: idx,
                                    limit: self.limit,
                                });
                            }
                        }
                    });
            });

        egui::CentralPanel::default_margins().show(ui, |ui| {
            ui.add_space(4.0);
            ui.horizontal_wrapped(|ui| {
                ui.label("Media");
                ui.separator();
                ui.label("scan last");
                ui.add(egui::DragValue::new(&mut self.limit).range(50..=20000));
                if ui.button("Load").clicked() {
                    if let Some(c) = self.sel_chat {
                        self.loading_media = true;
                        let _ = self.tx.send(Cmd::LoadMedia {
                            chat: c,
                            limit: self.limit,
                        });
                    }
                }
                if ui
                    .button("Load all")
                    .on_hover_text("scan every message in this chat")
                    .clicked()
                {
                    if let Some(c) = self.sel_chat {
                        self.scanning_all = true;
                        self.loading_media = true;
                        self.scan_progress = Some((0, 0));
                        let _ = self.tx.send(Cmd::LoadMediaAll { chat: c });
                    }
                }
                ui.separator();
                if ui.button("Select all").clicked() {
                    for m in &mut self.media {
                        m.selected = true;
                    }
                }
                if ui.button("None").clicked() {
                    for m in &mut self.media {
                        m.selected = false;
                    }
                }
                if ui.button("Videos only").clicked() {
                    for m in &mut self.media {
                        m.selected = m.kind == Kind::Video;
                    }
                }
                if ui
                    .button("Remove dupes")
                    .on_hover_text("drop rows that repeat a file name (keeps the first)")
                    .clicked()
                {
                    let n = dedup_by_name(&mut self.media);
                    self.status = if n == 0 {
                        "no duplicate file names".to_string()
                    } else {
                        format!("removed {n} duplicate rows")
                    };
                }
                ui.separator();
                let n = self.media.iter().filter(|m| m.selected).count();
                let bytes: u64 = self.media.iter().filter(|m| m.selected).map(|m| m.size).sum();
                if ui
                    .button(format!("Download {n}  ({})", human(bytes)))
                    .clicked()
                {
                    let ids: Vec<i32> = self
                        .media
                        .iter()
                        .filter(|m| m.selected)
                        .map(|m| m.msg_id)
                        .collect();
                    self.start_download(ids);
                }
                if ui
                    .add_enabled(
                        n > 0,
                        egui::Button::new(format!("Forward {n}")),
                    )
                    .on_hover_text("forward the selected messages to one of your channels")
                    .clicked()
                {
                    self.fwd_open = true;
                    self.fwd_target = None;
                    self.fwd_filter.clear();
                }
            });
            ui.horizontal(|ui| {
                match self.media_count {
                    Some((total, files)) => {
                        ui.label(
                            egui::RichText::new(format!(
                                "{} media available  ({} photos/videos · {} files)",
                                total,
                                total - files,
                                files
                            ))
                            .strong(),
                        );
                        ui.separator();
                        ui.label(format!("{} loaded", self.media.len()));
                    }
                    None => {
                        if self.sel_chat.is_some() {
                            ui.label("counting media...");
                        } else {
                            ui.label("pick a chat on the left");
                        }
                    }
                }
                if let Some((scanned, found)) = self.scan_progress {
                    ui.separator();
                    ui.label(format!("scanned {scanned} · found {found}"));
                }
                ui.separator();
                ui.label("sort");
                let mut picked = self.sort;
                egui::ComboBox::from_id_salt("sortmode")
                    .selected_text(picked.label())
                    .show_ui(ui, |ui| {
                        for s in [
                            SortMode::Newest,
                            SortMode::Oldest,
                            SortMode::SizeDesc,
                            SortMode::SizeAsc,
                            SortMode::Name,
                        ] {
                            ui.selectable_value(&mut picked, s, s.label());
                        }
                    });
                if picked != self.sort {
                    self.sort = picked;
                    self.apply_sort();
                }
            });
            ui.separator();

            ui.horizontal_wrapped(|ui| {
                ui.label("save to");
                let base = self.base_dir();
                ui.add(
                    egui::Label::new(egui::RichText::new(base.display().to_string()).monospace())
                        .truncate(),
                );
                if ui
                    .button("Choose…")
                    .on_hover_text("pick a drive or folder — each chat gets its own subfolder")
                    .clicked()
                    && self.pick_rx.is_none()
                {
                    let start = if base.exists() {
                        base.clone()
                    } else {
                        dirs::home_dir().unwrap_or_default()
                    };
                    // Run the native picker off the UI thread: blocking the main
                    // thread makes the compositor mark the window "not responding"
                    // (and on macOS the async API is the supported path).
                    let (tx, rx) = std::sync::mpsc::channel();
                    std::thread::spawn(move || {
                        let picked = pollster::block_on(
                            rfd::AsyncFileDialog::new()
                                .set_title("Choose download folder")
                                .set_directory(start)
                                .pick_folder(),
                        )
                        .map(|h| h.path().to_path_buf());
                        let _ = tx.send(picked);
                    });
                    self.pick_rx = Some(rx);
                    self.status = "waiting for folder selection…".to_string();
                }
                if !self.cfg.out_dir.is_empty()
                    && ui
                        .button("Reset")
                        .on_hover_text("back to the default folder")
                        .clicked()
                {
                    self.cfg.out_dir.clear();
                    save_cfg(&self.cfg);
                    self.status = "downloads go to the default folder".to_string();
                }
                ui.separator();
                ui.label("parallel")
                    .on_hover_text("files at once × parallel parts per file = open streams");
                let mut files = self.cfg.concurrent.clamp(1, 16);
                if ui
                    .add(egui::DragValue::new(&mut files).range(1..=16).suffix(" files"))
                    .changed()
                {
                    self.cfg.concurrent = files;
                    save_cfg(&self.cfg);
                }
                let mut parts = self.cfg.parts.clamp(1, 16);
                if ui
                    .add(egui::DragValue::new(&mut parts).range(1..=16).suffix(" parts"))
                    .changed()
                {
                    self.cfg.parts = parts;
                    save_cfg(&self.cfg);
                }
                ui.label(
                    egui::RichText::new(format!("= {} streams", files * parts)).weak(),
                );
            });
            ui.separator();

            if self.loading_media {
                ui.add_space(20.0);
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label("scanning chat...");
                });
            }

            let row_h = ui.spacing().interact_size.y.max(
                ui.text_style_height(&egui::TextStyle::Body),
            ) + ui.spacing().item_spacing.y;
            let n_media = self.media.len();
            egui::ScrollArea::vertical()
                .auto_shrink([false, false])
                .show_rows(ui, row_h, n_media, |ui, range| {
                    egui::Grid::new("media")
                        .num_columns(5)
                        .striped(true)
                        .min_col_width(0.0)
                        .show(ui, |ui| {
                            for i in range {
                                let m = &mut self.media[i];
                                ui.checkbox(&mut m.selected, "");
                                ui.label(m.kind.label());
                                ui.label(&m.name);
                                ui.label(human(m.size));
                                ui.label(&m.date);
                                ui.end_row();
                            }
                        });
                });
        });
        self.forward_window(&ctx);
    }
}

impl App {
    /// Modal picker: choose one of the user's channels to forward the selection into.
    fn forward_window(&mut self, ctx: &egui::Context) {
        if !self.fwd_open {
            return;
        }
        let ids: Vec<i32> = self
            .media
            .iter()
            .filter(|m| m.selected)
            .map(|m| m.msg_id)
            .collect();
        let n = ids.len();
        let mut open = true;
        let mut close = false;
        let mut go: Option<usize> = None;

        egui::Window::new("Forward to…")
            .collapsible(false)
            .resizable(true)
            .default_width(460.0)
            .default_height(520.0)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .open(&mut open)
            .show(ctx, |ui| {
                ui.label(format!("forward {n} selected message(s) to:"));
                ui.horizontal(|ui| {
                    ui.label("filter");
                    ui.text_edit_singleline(&mut self.fwd_filter);
                });
                ui.checkbox(&mut self.fwd_groups, "include groups");
                ui.separator();

                let q = self.fwd_filter.trim().to_lowercase();
                let mut rows: Vec<(usize, String, bool)> = Vec::new();
                for c in &self.chats {
                    let wanted = c.kind == "channel" || (self.fwd_groups && c.kind == "group");
                    if !wanted {
                        continue;
                    }
                    if !q.is_empty() && !c.name.to_lowercase().contains(&q) {
                        continue;
                    }
                    let private = c.kind == "channel" && c.username.is_none();
                    rows.push((c.idx, c.name.clone(), private));
                }
                // private channels first, then public ones, then groups; A-Z within each
                rows.sort_by(|a, b| {
                    b.2.cmp(&a.2)
                        .then_with(|| a.1.to_lowercase().cmp(&b.1.to_lowercase()))
                });
                if rows.is_empty() && q.is_empty() {
                    ui.weak("no channels yet — create one in Telegram first");
                }
                egui::ScrollArea::vertical()
                    .max_height(360.0)
                    .auto_shrink([false, true])
                    .show(ui, |ui| {
                        // Saved Messages always works and is a safe place to park things
                        if q.is_empty() || "saved messages".contains(&q) {
                            let sel = self.fwd_target == Some(usize::MAX);
                            if ui
                                .selectable_label(sel, "📥 Saved Messages (you)")
                                .clicked()
                            {
                                self.fwd_target = Some(usize::MAX);
                            }
                        }
                        for (idx, name, private) in &rows {
                            let sel = self.fwd_target == Some(*idx);
                            let label = if *private {
                                format!("🔒 {name}")
                            } else {
                                format!("    {name}")
                            };
                            if ui.selectable_label(sel, label).clicked() {
                                self.fwd_target = Some(*idx);
                            }
                        }
                    });
                ui.separator();
                ui.horizontal(|ui| {
                    let can = self.fwd_target.is_some() && n > 0;
                    if ui
                        .add_enabled(can, egui::Button::new(format!("Forward {n}")))
                        .clicked()
                    {
                        go = self.fwd_target;
                    }
                    if ui.button("Cancel").clicked() {
                        close = true;
                    }
                    if let Some(t) = self.fwd_target {
                        if t == usize::MAX {
                            ui.weak("→ Saved Messages");
                        } else if let Some(c) = self.chats.iter().find(|c| c.idx == t) {
                            ui.weak(format!("→ {}", c.name));
                        }
                    }
                });
            });

        if let Some(to) = go {
            if let Some(chat) = self.media_chat {
                let _ = self.tx.send(Cmd::Forward {
                    chat,
                    msg_ids: ids,
                    to,
                });
                self.status = format!("forwarding {n} message(s)…");
            }
            close = true;
        }
        if close || !open {
            self.fwd_open = false;
        }
    }
}

fn dedup_by_name(media: &mut Vec<MediaRow>) -> usize {
    let before = media.len();
    let mut seen: HashSet<String> = HashSet::with_capacity(before);
    media.retain(|m| seen.insert(m.name.to_lowercase()));
    before - media.len()
}

fn safe_dir_name(name: &str, fallback: usize) -> String {
    let mut s: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => '_',
            c if c.is_control() => ' ',
            c => c,
        })
        .collect();
    s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    let s = s.trim_matches(|c: char| c == '.' || c.is_whitespace());
    let out: String = s.chars().take(80).collect();
    if out.trim().is_empty() {
        format!("chat-{fallback}")
    } else {
        out
    }
}

fn setup_fonts(ctx: &egui::Context) {
    // Bundled so Sinhala and emoji render the same on Linux, macOS and Windows.
    // (System font paths differ per OS; shipping the bytes removes that variable.)
    const BUNDLED: [(&str, &[u8]); 3] = [
        ("noto-sans", include_bytes!("../assets/NotoSans-Regular.ttf")),
        (
            "noto-sinhala",
            include_bytes!("../assets/NotoSansSinhala-Regular.ttf"),
        ),
        ("noto-emoji", include_bytes!("../assets/NotoColorEmoji.ttf")),
    ];
    let mut fonts = egui::FontDefinitions::default();
    for (name, bytes) in BUNDLED {
        fonts.font_data.insert(
            name.to_owned(),
            std::sync::Arc::new(egui::FontData::from_static(bytes)),
        );
        for fam in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
            fonts.families.entry(fam).or_default().push(name.to_owned());
        }
    }
    ctx.set_fonts(fonts);
}

fn draw_qr(ui: &mut egui::Ui, url: &str, size: f32) {
    let Ok(code) = qrcode::QrCode::new(url.as_bytes()) else {
        ui.label("QR generation failed");
        return;
    };
    let w = code.width();
    let colors = code.to_colors();
    let quiet = 2usize;
    let total = w + quiet * 2;
    let cell = size / total as f32;
    let (rect, _) = ui.allocate_exact_size(egui::vec2(size, size), egui::Sense::hover());
    let p = ui.painter();
    p.rect_filled(rect, 0.0, egui::Color32::WHITE);
    for y in 0..w {
        for x in 0..w {
            if colors[y * w + x] == qrcode::Color::Dark {
                let min = rect.min
                    + egui::vec2((x + quiet) as f32 * cell, (y + quiet) as f32 * cell);
                p.rect_filled(
                    egui::Rect::from_min_size(min, egui::vec2(cell, cell)),
                    0.0,
                    egui::Color32::BLACK,
                );
            }
        }
    }
}

fn human(b: u64) -> String {
    const U: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{v:.1} {}", U[i])
}

fn cfg_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_default()
        .join("tgrab")
        .join("config.json")
}

fn session_db() -> PathBuf {
    let p = dirs::config_dir()
        .unwrap_or_default()
        .join("tgrab")
        .join("session.db");
    if let Some(d) = p.parent() {
        let _ = std::fs::create_dir_all(d);
    }
    p
}

fn load_cfg() -> Config {
    let mut c = std::fs::read_to_string(cfg_path())
        .ok()
        .and_then(|s| serde_json::from_str::<Config>(&s).ok())
        .unwrap_or_else(|| Config {
            api_id: String::new(),
            api_hash: String::new(),
            phone: String::new(),
            out_dir: dirs::download_dir()
                .unwrap_or_default()
                .join("tgrab")
                .display()
                .to_string(),
            parts: 6,
            concurrent: 6,
        });
    if c.api_id.trim().is_empty() || c.api_hash.trim().is_empty() {
        c.api_id = DEF_API_ID.to_string();
        c.api_hash = DEF_API_HASH.to_string();
    }
    if c.parts == 0 {
        c.parts = 6;
    }
    if c.concurrent == 0 {
        c.concurrent = 6;
    }
    c
}

fn save_cfg(c: &Config) {
    let p = cfg_path();
    if let Some(d) = p.parent() {
        let _ = std::fs::create_dir_all(d);
    }
    if let Ok(s) = serde_json::to_string_pretty(c) {
        let _ = std::fs::write(p, s);
    }
}

#[cfg(test)]
mod tests {
    use super::safe_dir_name;

    #[test]
    fn strips_path_separators() {
        assert_eq!(safe_dir_name("A/B\\C", 3), "A_B_C");
    }

    #[test]
    fn keeps_sinhala_and_emoji() {
        assert_eq!(safe_dir_name("ජිල් ගෙදර VIP ❤️‍🩹🥹", 0), "ජිල් ගෙදර VIP ❤️‍🩹🥹");
    }

    #[test]
    fn falls_back_when_empty() {
        assert_eq!(safe_dir_name("   ", 7), "chat-7");
        assert_eq!(safe_dir_name("..", 1), "chat-1");
        assert_eq!(safe_dir_name("", 9), "chat-9");
    }

    #[test]
    fn collapses_whitespace_and_caps_length() {
        assert_eq!(safe_dir_name("a   b\n c", 0), "a b c");
        assert_eq!(safe_dir_name(&"x".repeat(200), 0).chars().count(), 80);
    }

    fn row(id: i32, name: &str) -> crate::model::MediaRow {
        crate::model::MediaRow {
            msg_id: id,
            name: name.to_string(),
            size: 1,
            date: String::new(),
            kind: crate::model::Kind::Video,
            selected: false,
        }
    }

    #[test]
    fn dedup_keeps_first_occurrence() {
        let mut v = vec![
            row(1, "a.mp4"),
            row(2, "b.mp4"),
            row(3, "a.mp4"),
            row(4, "c.mp4"),
            row(5, "b.mp4"),
        ];
        let removed = super::dedup_by_name(&mut v);
        assert_eq!(removed, 2);
        assert_eq!(v.len(), 3);
        assert_eq!(
            v.iter().map(|m| m.msg_id).collect::<Vec<_>>(),
            vec![1, 2, 4]
        );
    }

    #[test]
    fn dedup_is_case_insensitive() {
        let mut v = vec![row(1, "Clip.MP4"), row(2, "clip.mp4")];
        assert_eq!(super::dedup_by_name(&mut v), 1);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].msg_id, 1);
    }

    #[test]
    fn dedup_no_duplicates_reports_zero() {
        let mut v = vec![row(1, "a.mp4"), row(2, "b.mp4")];
        assert_eq!(super::dedup_by_name(&mut v), 0);
        assert_eq!(v.len(), 2);
    }
}
