#!/bin/sh
# Build a relocatable Linux native distribution. Its bin and share directories
# also follow the XDG layout when installed under the same prefix.
set -eu
cd "$(dirname "$0")/.."
destination=${1:-"$PWD/dist"}
mkdir -p "$destination"
destination=$(cd "$destination" && pwd)
build=$(mktemp -d)
trap 'rm -rf "$build"' EXIT
# hermes-python-host embeds this exact CPython; it ships in lib/hermes/python.
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
target=${CARGO_TARGET_DIR:-"$PWD/target"}
stage="$build/Hermes"
install -d "$stage/bin" "$stage/share/applications" "$stage/share/icons/hicolor/512x512/apps" "$stage/share/licenses/hermes"
install -m755 "$target/release/hermes-desktop" "$stage/bin/hermes"
install -m755 "$target/release/hermes-python-host" "$stage/bin/"
scripts/bundle-python.sh "$PYO3_PYTHON" "$stage/lib/hermes/python"
"$PYO3_PYTHON" scripts/stage-whatsapp-bridge.py "$stage/share/hermes/whatsapp-bridge"
"$stage/bin/hermes-python-host" --version | grep -q " $stage/lib/hermes/python\$" || { echo "hermes-python-host does not run the bundled Python" >&2; exit 1; }
install -m644 assets/icon.png "$stage/share/icons/hicolor/512x512/apps/com.nousresearch.hermes.rust-development.png"
install -m644 packaging/linux/com.nousresearch.hermes.rust-development.desktop "$stage/share/applications/"
"$PYO3_PYTHON" scripts/reliquary-notices.py stage "$stage/share/licenses/hermes" "$stage/bin/hermes" \
    --save-runtime-notices "$stage/share/licenses/hermes/Herdr-THIRD-PARTY-NOTICES"
for notice in crates/desktop/assets/fonts/*NOTICE.txt crates/desktop/assets/fonts/*LICENSE*.txt; do
  if [ -f "$notice" ]; then install -m644 "$notice" "$stage/share/licenses/hermes/"; fi
done
tar -C "$build" -czf "$destination/Hermes-linux-$(uname -m).tar.gz" Hermes
# The same files as a Debian package, under /opt with a launcher on the path.
if command -v dpkg-deb >/dev/null; then
    version=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\(.*\)"/\1/p' Cargo.toml)
    deb="$build/deb"
    install -d "$deb/DEBIAN" "$deb/opt" "$deb/usr/bin" "$deb/usr/share/applications" "$deb/usr/share/icons/hicolor/512x512/apps"
    cp -a "$stage" "$deb/opt/hermes-rust"
    ln -s /opt/hermes-rust/bin/hermes "$deb/usr/bin/hermes-rust"
    cp "$stage"/share/icons/hicolor/512x512/apps/*.png "$deb/usr/share/icons/hicolor/512x512/apps/"
    for entry in "$stage"/share/applications/*.desktop; do
        sed 's|^Exec=[^ ]*|Exec=/opt/hermes-rust/bin/hermes|' "$entry" > "$deb/usr/share/applications/$(basename "$entry")"
    done
    cat > "$deb/DEBIAN/control" <<CONTROL
Package: hermes-rust
Version: $version
Architecture: $(dpkg --print-architecture)
Maintainer: itsflownium <157689911+itsflownium@users.noreply.github.com>
Depends: libwebkit2gtk-4.1-0, libgtk-3-0t64 | libgtk-3-0, libasound2t64 | libasound2, libxdo3, libdbus-1-3, libxkbcommon-x11-0
Section: utils
Priority: optional
Description: Hermes desktop, the community Rust rewrite
CONTROL
    chmod -R u+rwX,go+rX,go-w "$deb"
    dpkg-deb --root-owner-group --build "$deb" "$destination/Hermes-linux-$(dpkg --print-architecture).deb" >/dev/null
fi
