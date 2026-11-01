#!/usr/bin/env python3
"""Change only BEpusdt RPC settings, with a consistent backup and guarded rollback.

Run as the operator that owns the database. Restart BEpusdt separately after a
successful apply or rollback. Neither action restores or rewrites order tables.
"""
import argparse
import json
import os
import pathlib
import sqlite3
import urllib.parse
import urllib.request

CHAINS = ("ethereum", "bsc", "polygon", "arbitrum", "base", "xlayer", "plasma", "solana", "aptos", "ton", "tron")


def private_json(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as file:
        json.dump(value, file, indent=2)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())


def connect(path):
    # mode=rw prevents a path typo from creating an empty database.
    return sqlite3.connect(path.resolve().as_uri() + "?mode=rw", uri=True, timeout=15)


def apply(args):
    if args.token_file.stat().st_mode & 0o077:
        raise ValueError("credential file must have mode 0600")
    token = args.token_file.read_text().strip()
    if not 32 <= len(token) <= 1024:
        raise ValueError("invalid credential length")
    base = args.http_base.rstrip("/")
    url = urllib.parse.urlsplit(base)
    if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.query or url.fragment or url.path:
        raise ValueError("http-base must be an HTTP(S) origin without credentials")
    if ":" not in args.tron_target or "/" in args.tron_target or "@" in args.tron_target:
        raise ValueError("tron-target must be host:port")
    request = urllib.request.Request(base + "/readyz", headers={"Authorization": "Bearer " + token})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=10) as response:
        status = json.load(response)
    healthy = {chain["name"]: chain["healthy_upstreams"] for chain in status["chains"]}
    if status.get("service") != "rpc-failover-rs" or not status.get("ready") or any(healthy.get(name, 0) < 2 for name in CHAINS):
        raise ValueError("cutover requires two healthy upstreams for all eleven chains")
    credential = urllib.parse.quote(token, safe="")
    authenticated_base = urllib.parse.urlunsplit((url.scheme, "bepusdt:" + credential + "@" + url.netloc, "", "", ""))
    replacements = {"rpc_endpoint_" + name: authenticated_base + "/" + name for name in CHAINS if name not in ("ton", "tron")}
    replacements["rpc_endpoint_aptos"] += "/"
    replacements["rpc_global_config_url_ton"] = authenticated_base + "/ton"
    replacements["rpc_endpoint_tron"] = args.tron_target
    replacements["rpc_endpoint_tron_grid_api_key"] = token
    if any(len(value) > 512 for value in replacements.values()):
        raise ValueError("configured endpoint exceeds BEpusdt's column limit")
    args.backup_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
    db = connect(args.db)
    backup_path = args.backup_dir / "sqlite.db"
    fd = os.open(backup_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    with sqlite3.connect(backup_path) as backup:
        db.backup(backup)
        if backup.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise ValueError("database backup integrity check failed")
    try:
        db.execute("BEGIN IMMEDIATE")
        before = {}
        for key in replacements:
            row = db.execute("SELECT v FROM bep_conf WHERE k=?", (key,)).fetchone()
            before[key] = row[0] if row else None
        record = {"format": 1, "db": str(args.db.resolve()), "before": before, "after": replacements}
        private_json(args.backup_dir / "rpc-settings.json", record)
        for key, value in replacements.items():
            db.execute("INSERT INTO bep_conf(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, value))
        for key, value in replacements.items():
            if db.execute("SELECT v FROM bep_conf WHERE k=?", (key,)).fetchone() != (value,):
                raise ValueError("RPC setting verification failed")
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()
    print("Updated 12 RPC settings. Backup and guarded rollback record:", args.backup_dir)
    print("Restart only the BEpusdt container, then verify its normal scan logs.")


def rollback(args):
    if args.record.stat().st_mode & 0o077:
        raise ValueError("rollback record must have mode 0600")
    record = json.loads(args.record.read_text())
    if record.get("format") != 1 or record.get("db") != str(args.db.resolve()):
        raise ValueError("rollback record does not match the database")
    before, after = record["before"], record["after"]
    allowed = {"rpc_endpoint_" + name for name in CHAINS if name != "ton"} | {"rpc_global_config_url_ton", "rpc_endpoint_tron_grid_api_key"}
    if set(before) != allowed or set(after) != allowed:
        raise ValueError("rollback must contain exactly the twelve RPC settings")
    db = connect(args.db)
    try:
        db.execute("BEGIN IMMEDIATE")
        for key, value in after.items():
            if db.execute("SELECT v FROM bep_conf WHERE k=?", (key,)).fetchone() != (value,):
                raise ValueError("RPC settings changed after cutover; refusing to overwrite operator changes")
        for key, value in before.items():
            if value is None:
                db.execute("DELETE FROM bep_conf WHERE k=? AND v=?", (key, after[key]))
            else:
                db.execute("UPDATE bep_conf SET v=? WHERE k=? AND v=?", (value, key, after[key]))
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()
    print("Restored only the twelve RPC settings. Restart only the BEpusdt container.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    change = commands.add_parser("apply")
    change.add_argument("--db", type=pathlib.Path, required=True)
    change.add_argument("--token-file", type=pathlib.Path, required=True)
    change.add_argument("--http-base", required=True)
    change.add_argument("--tron-target", required=True)
    change.add_argument("--backup-dir", type=pathlib.Path, required=True)
    undo = commands.add_parser("rollback")
    undo.add_argument("--db", type=pathlib.Path, required=True)
    undo.add_argument("--record", type=pathlib.Path, required=True)
    args = parser.parse_args()
    try:
        (apply if args.command == "apply" else rollback)(args)
    except Exception:
        # Raw HTTP/SQLite exceptions can contain a credential-bearing endpoint.
        parser.exit(1, "RPC configuration operation failed; check local files, readiness, permissions, and operator changes. No credentials printed.\n")


if __name__ == "__main__":
    main()
