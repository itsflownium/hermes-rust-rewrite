#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
destination=${1:-"$PWD/dist/Hermes.app"}
# An ad-hoc signature unless CODESIGN_IDENTITY names a Developer ID certificate in the keychain.
identity=${CODESIGN_IDENTITY:--}
sign() {
    if [ "$identity" = - ]; then
        codesign --force --sign - "$@"
    else
        codesign --force --options runtime --timestamp --entitlements packaging/macos/entitlements.plist --sign "$identity" "$@"
    fi
}
# hermes-python-host embeds this exact CPython; it ships in Contents/Resources/python.
if [ -z "${PYO3_CONFIG_FILE:-}" ]; then
    python_settings=$(scripts/python-runtime.sh)
    while IFS= read -r setting; do
        export "$setting"
    done <<EOF
$python_settings
EOF
fi
cargo build --locked --release --features embedded-herdr -p hermes-desktop
cargo build --locked --release -p hermes-python-host
target_dir=$(cargo metadata --no-deps --format-version 1 | plutil -extract target_directory raw -o - -)
mkdir -p "$destination/Contents/MacOS" "$destination/Contents/Resources"
install -m 755 "$target_dir/release/hermes-desktop" "$destination/Contents/MacOS/hermes-desktop"
if [ -f "$destination/Contents/MacOS/hermes-gateway" ]; then
    rm "$destination/Contents/MacOS/hermes-gateway"
fi
install -m 755 "$target_dir/release/hermes-python-host" "$destination/Contents/MacOS/hermes-python-host"
scripts/bundle-python.sh "$PYO3_PYTHON" "$destination/Contents/Resources/python"
"$PYO3_PYTHON" scripts/stage-whatsapp-bridge.py "$destination/Contents/Resources/whatsapp-bridge"
find "$destination/Contents/Resources/whatsapp-bridge" -type f \( -name '*.node' -o -name '*.dylib' \) |
    while IFS= read -r library; do sign "$library"; done
host="$destination/Contents/MacOS/hermes-python-host"
library=$(otool -L "$host" | awk '/libpython3|Python\.framework/ { print $1; exit }')
pyver=$("$PYO3_PYTHON" -c 'import sys; print("%d.%d" % sys.version_info[:2])')
case "$library" in
    @rpath/*) ;;
    *) install_name_tool -change "$library" "@rpath/libpython$pyver.dylib" "$host" ;;
esac
install_name_tool -id "@rpath/libpython$pyver.dylib" "$destination/Contents/Resources/python/lib/libpython$pyver.dylib"
find "$destination/Contents/Resources/python" -type f \( -name '*.dylib' -o -name '*.so' \) | while IFS= read -r library; do sign "$library"; done
sign "$host"
cp packaging/macos/Info.plist "$destination/Contents/Info.plist"
cp assets/icon.icns "$destination/Contents/Resources/Hermes.icns"
mkdir -p "$destination/Contents/Resources/Notices"
for notice in crates/desktop/assets/fonts/*NOTICE.txt crates/desktop/assets/fonts/*LICENSE*.txt; do
    if [ -f "$notice" ]; then cp "$notice" "$destination/Contents/Resources/Notices/"; fi
done
"$PYO3_PYTHON" scripts/reliquary-notices.py stage "$destination/Contents/Resources/Notices" "$destination/Contents/MacOS/hermes-desktop" \
    --save-runtime-notices "$destination/Contents/Resources/Notices/Herdr-THIRD-PARTY-NOTICES"
sign "$destination"
scripts/verify-native-macos.sh "$destination"
# Notarization needs a Developer ID signature and an App Store Connect key.
if [ "$identity" != - ] && [ -n "${NOTARY_KEY:-}" ]; then
    archive=$(mktemp -d)/Hermes.zip
    ditto -c -k --sequesterRsrc --keepParent "$destination" "$archive"
    xcrun notarytool submit "$archive" --key "$NOTARY_KEY" --key-id "$NOTARY_KEY_ID" --issuer "$NOTARY_ISSUER" --wait
    xcrun stapler staple "$destination"
fi
# A disk image with a link to Applications: open it, drag Hermes across.
image="$(dirname "$destination")/Hermes-macOS.dmg"
layout=$(mktemp -d)
ditto "$destination" "$layout/Hermes.app"
ln -s /Applications "$layout/Applications"
# hdiutil can fail with "Resource busy" while the system still holds the folder.
attempt=1
until hdiutil create -ov -volname Hermes -srcfolder "$layout" -format UDZO "$image"; do
    [ "$attempt" -lt 4 ] || exit 1
    attempt=$((attempt + 1))
    sleep 10
done
if [ "$identity" != - ]; then codesign --force --timestamp --sign "$identity" "$image"; fi
printf 'Built %s and %s\n' "$destination" "$image"
