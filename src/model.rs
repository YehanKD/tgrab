use std::path::PathBuf;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Video,
    Photo,
    Audio,
    Doc,
    Sticker,
}

impl Kind {
    pub fn label(self) -> &'static str {
        match self {
            Kind::Video => "video",
            Kind::Photo => "photo",
            Kind::Audio => "audio",
            Kind::Doc => "file",
            Kind::Sticker => "sticker",
        }
    }
}

#[derive(Clone)]
pub struct ChatRow {
    pub idx: usize,
    pub name: String,
    pub kind: &'static str,
    pub username: Option<String>,
}

#[derive(Clone)]
pub struct MediaRow {
    pub msg_id: i32,
    pub name: String,
    pub size: u64,
    pub date: String,
    pub kind: Kind,
    pub selected: bool,
}

pub enum Cmd {
    Connect {
        api_id: i32,
        api_hash: String,
        db: PathBuf,
    },
    RequestCode {
        phone: String,
    },
    QrLogin,
    SignIn {
        code: String,
    },
    Password {
        pw: String,
    },
    LoadChats,
    LoadMedia {
        chat: usize,
        limit: usize,
    },
    LoadMediaAll {
        chat: usize,
    },
    CountMedia {
        chat: usize,
    },
    Download {
        chat: usize,
        msg_ids: Vec<i32>,
        dir: PathBuf,
        parts: usize,
        concurrent: usize,
    },
    Forward {
        chat: usize,
        msg_ids: Vec<i32>,
        to: usize,
    },
}

pub enum Ev {
    Log(String),
    NeedCode,
    NeedQr,
    Qr(String),
    Reconnect,
    NeedPassword(Option<String>),
    Authorized {
        me: String,
    },
    Chats(Vec<ChatRow>),
    Media {
        chat: usize,
        items: Vec<MediaRow>,
    },
    MediaCount {
        chat: usize,
        total: i32,
        photos: i32,
        files: i32,
    },
    MediaProgress {
        chat: usize,
        scanned: usize,
        found: usize,
    },
    Skipped {
        key: String,
        path: String,
    },
    Forwarded {
        done: usize,
        total: usize,
    },
    Prog {
        key: String,
        done: u64,
        total: u64,
    },
    Done {
        key: String,
        path: String,
    },
    Fail {
        key: String,
        err: String,
    },
    Finished,
    Err(String),
}
