#!/usr/bin/env python3
"""Install connector source into a local OsEngine checkout (no automatic git commits).

Usage:
  python tools/install_into_osengine.py --osengine C:/src/OsEngine/project/OsEngine
  python tools/install_into_osengine.py --osengine ... --check

The command updates only ServerMaster.cs and Market/Servers/AlfaPro/.
Run from this connector repository. Requires a checkout with the current layout.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import re
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
ENGINE_FILES = ROOT / "src" / "OsEngine"
CORE_FILES = ROOT / "src" / "AlfaPro.Core"

def require_replace(text: str, before: str, after: str, expected_count: int) -> str:
    if after in text:
        return text
    count = text.count(before)
    if count != expected_count:
        raise RuntimeError(f"Expected {expected_count} instances of {before!r}, found {count}")
    return text.replace(before, after)

def prepare_server_master(original: str) -> str:
    result = original
    result = require_replace(result,
        "using OsEngine.Market.Servers.FinamGrpc;",
        "using OsEngine.Market.Servers.FinamGrpc;\nusing OsEngine.Market.Servers.AlfaPro;", 1)
    result = require_replace(result,
        "serverTypes.Add(ServerType.FinamGrpc);",
        "serverTypes.Add(ServerType.FinamGrpc);\n                serverTypes.Add(ServerType.AlfaPro);", 2)
    # Append to the enum instead of inserting in the middle: keep existing numeric ServerType IDs stable.
    result = require_replace(result,
        "        BCS\\n    }\\n}",
        "        BCS,\\n\\n        /// <summary>Alfa-Investments PRO Terminal WebSocket</summary>\\n        AlfaPro\\n    }\\n}", 1)

    creation = re.compile(
        r"(else if \(type == ServerType\.FinamGrpc\)\s*\{\s*"
        r"newServer = new FinamGrpcServer\(uniqueNum\);\s*\})")
    if "newServer = new AlfaProServer();" not in result:
        result, n = creation.subn(
            r"\1\n                     else if (type == ServerType.AlfaPro)\n"
            r"                     {\n                         newServer = new AlfaProServer();\n                     }", result)
        if n != 1:
            raise RuntimeError(f"Cannot find FinamGrpc server factory anchor (found {n})")

    permissions = re.compile(
        r"(else if \(type == ServerType\.FinamGrpc\)\s*\{\s*"
        r"serverPermission = new FinamGrpcServerPermission\(\);\s*\})")
    if "serverPermission = new AlfaProServerPermission();" not in result:
        result, n = permissions.subn(
            r"\1\n                 else if (type == ServerType.AlfaPro)\n"
            r"                 {\n                     serverPermission = new AlfaProServerPermission();\n                 }", result)
        if n != 1:
            raise RuntimeError(f"Cannot find FinamGrpc permission factory anchor (found {n})")
    return result

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--osengine", required=True,
                        help="Directory containing OsEngine.csproj and Market/ServerMaster.cs")
    parser.add_argument("--check", action="store_true", help="Validate anchors without writing")
    args = parser.parse_args()
    root = Path(args.osengine).expanduser().resolve()
    master = root / "Market" / "ServerMaster.cs"
    if not (root / "OsEngine.csproj").is_file() or not master.is_file():
        parser.error("Point --osengine to the directory containing OsEngine.csproj")
    original = master.read_text(encoding="utf-8-sig")
    modified = prepare_server_master(original)
    sources = list(ENGINE_FILES.glob("*.cs")) + list(CORE_FILES.glob("*.cs"))
    if not sources:
        raise RuntimeError("Connector sources are missing")
    dest = root / "Market" / "Servers" / "AlfaPro"
    print("Base:", root)
    print("Would copy", len(sources), "C# files to", dest)
    print("ServerMaster requires change:", original != modified)
    if args.check:
        return 0
    # Stage all source reads and master edits before any disk writes.
    staged = [(src.name, src.read_bytes()) for src in sources]
    dest.mkdir(parents=True, exist_ok=True)
    if original != modified:
        backup = master.with_suffix(".cs.alfapro-backup")
        if not backup.exists(): shutil.copy2(master, backup)
        master.write_text(modified, encoding="utf-8")
    for name, content in staged:
        (dest / name).write_bytes(content)
    print("Installed source only. Review git diff, build, test and keep live trading OFF.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
