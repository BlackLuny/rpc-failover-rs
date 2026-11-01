"""Exercise cutover and guarded rollback without a production database."""
import http.server
import importlib.util
import json
import pathlib
import sqlite3
import tempfile
import threading
import types
import unittest

SPEC = importlib.util.spec_from_file_location("bepusdt_config", pathlib.Path(__file__).parents[1] / "tools/bepusdt_config.py")
OPERATOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OPERATOR)
TOKEN = "isolated-test-credential-with-at-least-32-bytes"


class Ready(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.headers.get("Authorization") != "Bearer " + TOKEN:
            self.send_error(401)
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps({"service": "rpc-failover-rs", "ready": True, "chains": [
            {"name": name, "healthy_upstreams": 2} for name in OPERATOR.CHAINS
        ]}).encode())

    def log_message(self, *args):
        pass


class Cutover(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.folder.name)
        self.db = root / "sqlite.db"
        self.token = root / "token"
        self.token.write_text(TOKEN)
        self.token.chmod(0o600)
        with sqlite3.connect(self.db) as db:
            db.executescript("CREATE TABLE bep_conf(k TEXT PRIMARY KEY,v TEXT NOT NULL); CREATE TABLE orders(id INTEGER PRIMARY KEY,status TEXT);")
            db.execute("INSERT INTO bep_conf VALUES(?,?)", ("rpc_endpoint_polygon", "https://old.invalid"))
            db.execute("INSERT INTO bep_conf VALUES(?,?)", ("unrelated_setting", "keep"))
            db.execute("INSERT INTO orders VALUES(1,'paid')")
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Ready)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.args = types.SimpleNamespace(db=self.db, token_file=self.token,
            http_base="http://127.0.0.1:" + str(self.server.server_port), tron_target="127.0.0.1:8547", backup_dir=root / "backup")

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.folder.cleanup()

    def test_cutover_and_rollback_preserve_new_orders_and_unrelated_configuration(self):
        OPERATOR.apply(self.args)
        with sqlite3.connect(self.db) as db:
            values = dict(db.execute("SELECT k,v FROM bep_conf"))
            self.assertTrue(values["rpc_endpoint_aptos"].endswith("/aptos/"))
            self.assertEqual(values["rpc_endpoint_tron_grid_api_key"], TOKEN)
            db.execute("INSERT INTO orders VALUES(2,'paid-after-cutover')")
        record = self.args.backup_dir / "rpc-settings.json"
        self.assertEqual(record.stat().st_mode & 0o777, 0o600)
        with sqlite3.connect(self.args.backup_dir / "sqlite.db") as backup:
            self.assertEqual(backup.execute("PRAGMA integrity_check").fetchone(), ("ok",))
        OPERATOR.rollback(types.SimpleNamespace(db=self.db, record=record))
        with sqlite3.connect(self.db) as db:
            self.assertEqual(dict(db.execute("SELECT k,v FROM bep_conf")), {"rpc_endpoint_polygon": "https://old.invalid", "unrelated_setting": "keep"})
            self.assertEqual(db.execute("SELECT count(*) FROM orders").fetchone()[0], 2)

    def test_rollback_refuses_to_overwrite_later_operator_change(self):
        OPERATOR.apply(self.args)
        with sqlite3.connect(self.db) as db:
            db.execute("UPDATE bep_conf SET v=? WHERE k=?", ("https://operator.invalid", "rpc_endpoint_polygon"))
        with self.assertRaisesRegex(ValueError, "changed after cutover"):
            OPERATOR.rollback(types.SimpleNamespace(db=self.db, record=self.args.backup_dir / "rpc-settings.json"))
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM bep_conf").fetchone()[0], 13)
            self.assertEqual(db.execute("SELECT v FROM bep_conf WHERE k='rpc_endpoint_polygon'").fetchone()[0], "https://operator.invalid")

    def test_readiness_auth_failure_does_not_touch_database(self):
        self.token.write_text("wrong-but-long-enough-test-credential-for-gateway")
        with self.assertRaises(Exception):
            OPERATOR.apply(self.args)
        self.assertFalse(self.args.backup_dir.exists())
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM bep_conf").fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
