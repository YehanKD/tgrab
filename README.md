# tgrab

Fast Telegram media archiver for Linux. Native Rust — no `tdl`, no Go binary,
no subprocess. Talks MTProto directly via [`grammers`](https://github.com/LonamiWebs/grammers).

## What it does

- Lists your chats (channels, groups, DMs) in a searchable sidebar
- Scans a chat for media and shows name / type / size / date
- Downloads selected items with **parallel chunk workers per file**
- Live per-file progress in a queue panel
- Works on `noforwards` (content-protected) channels, because it speaks MTProto
  and the media still streams to a client that is allowed to display it

## Build

```bash
cargo build --release
./target/release/tgrab
```

Debug build is fine too (`cargo build`), deps are compiled at opt-level 3 either way.

## First run — just scan the QR

No API ID needed. On launch tgrab connects and shows a QR code:

1. Open Telegram on your phone
2. **Settings → Devices → Link Desktop Device**
3. Point your phone at the QR

That's it. The session is saved to `~/.config/tgrab/session.db`, so you only do
this once — every later launch goes straight to your chat list.

If you'd rather not scan, "Use phone number instead" does the classic
phone → code → (2FA) flow.

### About the built-in API identity

Telegram requires every MTProto client to send an `api_id`. tgrab ships with the
Telegram Desktop identity (`2040` / `b18441a1...`) — the same one `tdl` uses,
which is why `tdl` never asks you for one either.

If you prefer your own: register an app at <https://my.telegram.org> (API
development tools), then **Advanced: my own API ID** on the QR screen. Your
values are saved to `~/.config/tgrab/config.json`.

Do not use an API ID that isn't yours or Telegram Desktop's — accounts running
on an unknown third-party ID can be flagged.

## Downloading

1. Pick a chat in the left sidebar — it scans the last *N* messages (default 500)
2. `Videos only` / `All` / `None` to select, or tick rows individually
3. `Download N (size)` — files land in `~/Downloads/tgrab`

Files are named `<msg_id>__<original name>`. Each file is pre-allocated to its
final size and written at chunk offsets, so a partial file can be resumed later.

## Speed tuning

Two knobs, both in the code today:

| Setting | Default | Effect |
|---|---|---|
| `parts` | 4 | parallel chunk streams **per file** |
| `concurrent` | 4 | files downloading **at once** |

`parts` is the one that matters — a single Telegram stream is slow, four
parallel 512 KB chunk streams typically multiply throughput several times over.
Raise carefully: this is your own account, and aggressive concurrency is how you
earn a `FLOOD_WAIT`.

## Known limitation: CDN-served files

Telegram serves some files from CDN edge datacenters (`upload.fileCdnRedirect`).
`grammers` 0.12 sets `cdn_supported: false` and **panics** when it sees one.
tgrab catches that panic per file and marks it `CDN-served file - grammers cannot
follow the redirect` instead of crashing.

This is the same wall `tdl` hits (`redirect to CDN DC 201`). Affected files need
the official Desktop client or a browser userscript. Most files are not CDN-served.

## Architecture

```
src/main.rs   egui UI, app state, event pump
src/tg.rs     tokio worker: connect, login, dialogs, media index, downloads
src/model.rs  Cmd / Ev message types and row structs
```

The UI thread and the network thread talk over two channels:

- `tokio::sync::mpsc::UnboundedSender<Cmd>` — UI → worker
- `std::sync::mpsc::Sender<Ev>` — worker → UI, polled each frame

The worker calls `egui::Context::request_repaint()` on progress so the UI wakes
up without busy-looping.

## Building for macOS

Pure Rust + egui, so the source compiles on macOS unchanged. Fonts live in
`assets/` and are compiled in with `include_bytes!`, so Sinhala and colour emoji
render the same on every OS (system font paths differ per platform — this was
the one Linux-only assumption in the code).

### On a Mac

```sh
xcode-select --install         # once, provides the linker
./scripts/macos-bundle.sh      # -> dist/tgrab.app + dist/tgrab-0.1.0-macos.dmg
```

The script builds `aarch64-apple-darwin` and `x86_64-apple-darwin`, `lipo`s
them into one universal binary, writes `Info.plist`, ad-hoc signs, and packs a
UDZO dmg.

### Without a Mac

Push the repo to GitHub, then run the `macos` workflow manually or push a tag:

```sh
git tag v0.1.0 && git push --tags
```

`.github/workflows/macos.yml` builds on `macos-14` and uploads the dmg as the
`tgrab-macos` artifact.

### Gatekeeper

The build is ad-hoc signed, not notarized — notarization needs a paid Apple
Developer ID. On another Mac, first launch requires right-click → Open, or:

```sh
xattr -dr com.apple.quarantine /Applications/tgrab.app
```

### Why not cross-compile from Linux

eframe links against the macOS SDK; osxcross can be made to work but it is
fragile and still yields an unsigned binary. A Mac or the CI job is the sane
route.

## Platform notes

| | Linux | macOS |
|---|---|---|
| Renderer | OpenGL (glow) | OpenGL (glow), Metal via wgpu later |
| Session db | `~/.config/tgrab/session.db` | `~/Library/Application Support/tgrab/session.db` |
| Config | `~/.config/tgrab/config.json` | `~/Library/Application Support/tgrab/config.json` |
| Default download dir | `~/Downloads/tgrab` | `~/Downloads/tgrab` |

Both paths come from the `dirs` crate, so they are resolved per platform.
