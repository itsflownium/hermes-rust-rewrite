"""Ship the pinned linked-device bridge alongside the native owner."""

import os
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> None:
    repository = Path(__file__).resolve().parents[1]
    source = repository / "crates/plugins/whatsapp/bridge"
    destination = Path(sys.argv[1]).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    for asset in ("bridge.mjs", "package.json", "package-lock.json"):
        shutil.copyfile(source / asset, destination / asset)
    npm = shutil.which("npm.cmd" if os.name == "nt" else "npm")
    node = shutil.which("node")
    if not npm or not node:
        raise SystemExit("Node.js 20 or newer and npm are required to package the WhatsApp bridge")
    subprocess.run(
        [npm, "ci", "--omit=dev", "--ignore-scripts", "--no-audit", "--no-fund"],
        cwd=destination,
        check=True,
    )
    subprocess.run(
        [node, "--input-type=module", "-e", "await import('@whiskeysockets/baileys')"],
        cwd=destination,
        check=True,
    )


if __name__ == "__main__":
    main()
