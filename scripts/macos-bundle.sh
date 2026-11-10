#!/usr/bin/env bash
# Build a universal (arm64 + x86_64) tgrab.app and a .dmg.
# Must run on macOS with Xcode command line tools installed.
set -euo pipefail

cd "$(dirname "$0")/.."

APP_NAME="tgrab"
BUNDLE_ID="lk.tgrab.app"
VERSION="$(grep -m1 '^version' Cargo.toml | cut -d'"' -f2)"
DIST="dist"
APP="$DIST/$APP_NAME.app"

echo "==> tgrab $VERSION"

if ! command -v lipo >/dev/null; then
  echo "error: lipo not found — run this on macOS with Xcode CLT installed" >&2
  exit 1
fi

echo "==> building arm64"
rustup target add aarch64-apple-darwin >/dev/null 2>&1 || true
cargo build --release --target aarch64-apple-darwin

echo "==> building x86_64"
rustup target add x86_64-apple-darwin >/dev/null 2>&1 || true
cargo build --release --target x86_64-apple-darwin

echo "==> assembling $APP"
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"

lipo -create \
  "target/aarch64-apple-darwin/release/$APP_NAME" \
  "target/x86_64-apple-darwin/release/$APP_NAME" \
  -output "$APP/Contents/MacOS/$APP_NAME"
chmod +x "$APP/Contents/MacOS/$APP_NAME"

cp assets/icon.icns "$APP/Contents/Resources/icon.icns"

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>$APP_NAME</string>
  <key>CFBundleDisplayName</key><string>$APP_NAME</string>
  <key>CFBundleExecutable</key><string>$APP_NAME</string>
  <key>CFBundleIdentifier</key><string>$BUNDLE_ID</string>
  <key>CFBundleIconFile</key><string>icon.icns</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>$VERSION</string>
  <key>CFBundleVersion</key><string>$VERSION</string>
  <key>LSMinimumSystemVersion</key><string>11.0</string>
  <key>LSApplicationCategoryType</key><string>public.app-category.utilities</string>
  <key>NSHighResolutionCapable</key><true/>
  <key>NSSupportsAutomaticGraphicsSwitching</key><true/>
</dict>
</plist>
PLIST

# Ad-hoc signature: enough for local use. Gatekeeper still warns on first launch
# until the app is signed with a Developer ID and notarized.
echo "==> ad-hoc codesign"
codesign --force --deep --sign - "$APP" 2>/dev/null || echo "   (codesign skipped)"

echo "==> building dmg"
DMG="$DIST/$APP_NAME-$VERSION-macos.dmg"
rm -f "$DMG"
hdiutil create -volname "$APP_NAME" -srcfolder "$APP" -ov -format UDZO "$DMG" >/dev/null

echo
echo "done:"
echo "  $APP"
echo "  $DMG"
echo
echo "First launch on another Mac: right-click > Open, or"
echo "  xattr -dr com.apple.quarantine /Applications/$APP_NAME.app"
