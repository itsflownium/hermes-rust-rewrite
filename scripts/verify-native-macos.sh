#!/bin/sh
# Audit the actual installed payload, including nested helper binaries.
set -eu
app=${1:?Pass the path to the native Hermes.app}
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
desktop="$app/Contents/MacOS/hermes-desktop"
test -x "$desktop"
test ! -e "$app/Contents/MacOS/hermes-gateway"
"$desktop" gateway --help >/dev/null
plutil -lint "$app/Contents/Info.plist" >/dev/null
test "$(plutil -extract CFBundleExecutable raw -o - "$app/Contents/Info.plist")" = hermes-desktop
test "$(plutil -extract CFBundleDisplayName raw -o - "$app/Contents/Info.plist")" = Hermes
cmp "$app/Contents/Resources/Hermes.icns" "$root/assets/icon.icns"
codesign --verify --deep --strict "$app"

host="$app/Contents/MacOS/hermes-python-host"
python=$(cd "$app/Contents/Resources/python" && pwd -P)
test -x "$host"
# Python plugins run in the embedded CPython; it is the only interpreter, and only the host links it.
"$host" --version | grep -q " $python\$" || { echo "hermes-python-host does not run the bundled Python" >&2; exit 1; }
bridge="$app/Contents/Resources/whatsapp-bridge"
modules="$bridge/node_modules"
bridge_source="$root/crates/plugins/whatsapp/bridge"
for asset in bridge.mjs package.json package-lock.json; do
    cmp "$bridge/$asset" "$bridge_source/$asset"
done
test -f "$modules/@whiskeysockets/baileys/package.json" || {
    echo "The bundled WhatsApp bridge is missing Baileys" >&2
    exit 1
}

# The pinned bridge runs with an external Node.js installation.
foreign=$(find "$app/Contents" \
    -path "$app/Contents/Resources/python" -prune -o ! -path "$host" \
    \( -name '*.asar' -o -iname '*electron*' -o -iname '*chromium*' \
       -o -iname '*tauri*' -o -iname '*webkit*' -o -iname '*python*' \
       -o \( -iname node \( ! -type d -o ! -path "$modules/*" \) \) \
       -o -iname node.exe -o -iname 'libnode.*' -o -name '*.py' -o -name '*.pyc' \
       -o \( ! -path "$bridge/bridge.mjs" ! -path "$modules" ! -path "$modules/*" \
              \( -iname node_modules -o -name '*.node' -o -name '*.html' -o -name '*.js' \
                 -o -name '*.mjs' -o -name '*.cjs' -o -name '*.wasm' \) \) \
    \) -print)
if [ -n "$foreign" ]; then
    echo "Unexpected interpreter, web runtime, or script in application bundle" >&2
    echo "$foreign" >&2
    exit 1
fi
find "$modules" -type f \( -name '*.node' -o -name '*.dylib' \) |
    while IFS= read -r library; do codesign --verify --strict "$library"; done
for executable in "$app/Contents/MacOS/"*; do
    file "$executable" | grep -q 'Mach-O'
    # The HTML artifact preview uses the operating system's own WKWebView
    # (/System/Library/Frameworks/WebKit.framework). Nothing web-related is bundled.
    links=$(otool -L "$executable" | tail -n +2)
    if [ "$executable" = "$host" ]; then
        echo "$links" | grep -q '@rpath/libpython3' || { echo "hermes-python-host does not link the bundled libpython" >&2; exit 1; }
        links=$(echo "$links" | grep -v '@rpath/libpython3')
        if otool -l "$executable" | grep -A2 LC_RPATH | grep ' path ' | grep -v '@executable_path'; then
            echo "hermes-python-host carries a build-machine rpath" >&2
            exit 1
        fi
    fi
    if echo "$links" | grep -v '^\s*/System/Library/Frameworks/WebKit.framework/' | grep -Ei 'Electron|Chromium|WebKit|JavaScriptCore|Python|libnode|libjvm'; then
        echo "Unexpected runtime dependency in $executable" >&2
        exit 1
    fi
done
printf 'Verified native executables, signing, official icon, bundled Python, the WhatsApp bridge, and runtime dependencies in %s\n' "$app"
