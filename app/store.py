import json
import sqlite3

from app.config import DATA_DIR
from app.errors import DomainError


def connect():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DATA_DIR / "records.db", timeout=20)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY,payload TEXT NOT NULL,version INTEGER NOT NULL DEFAULT 1,decision TEXT NOT NULL DEFAULT '待复核')")
    db.execute("CREATE TABLE IF NOT EXISTS snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT,business_date TEXT,payload TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')))")
    db.execute("CREATE TABLE IF NOT EXISTS batch_files (id INTEGER PRIMARY KEY AUTOINCREMENT,snapshot_id INTEGER NOT NULL,sha256 TEXT NOT NULL,seq INTEGER NOT NULL,mode TEXT,mode_basis TEXT,replayed INTEGER NOT NULL DEFAULT 0,FOREIGN KEY(snapshot_id) REFERENCES snapshots(id))")
    return db


def save(run_id, payload):
    with connect() as db:
        db.execute("INSERT OR IGNORE INTO runs(id,payload) VALUES (?,?)", (run_id, json.dumps(payload, ensure_ascii=False)))
    return get(run_id)


def get(run_id):
    with connect() as db:
        row = db.execute("SELECT payload,version,decision FROM runs WHERE id=?", (run_id,)).fetchone()
    if not row:
        raise DomainError("NOT_FOUND", "批次不存在", 404)
    return {**json.loads(row[0]), "version": row[1], "decision": row[2]}


def latest():
    with connect() as db:
        row = db.execute("SELECT id FROM runs ORDER BY rowid DESC LIMIT 1").fetchone()
    return get(row[0]) if row else None


def history():
    with connect() as db:
        return [{"run_id": i, "version": v, "decision": d} for i, v, d in db.execute("SELECT id,version,decision FROM runs ORDER BY rowid DESC")]


def review(run_id, version, decision):
    if decision not in {"已复核", "退回复核"}:
        raise DomainError("BAD_DECISION", "复核状态无效")
    with connect() as db:
        cursor = db.execute("UPDATE runs SET version=version+1,decision=? WHERE id=? AND version=?", (decision, run_id, version))
        if cursor.rowcount != 1:
            if not db.execute("SELECT 1 FROM runs WHERE id=?", (run_id,)).fetchone():
                raise DomainError("NOT_FOUND", "批次不存在", 404)
            raise DomainError("STALE_VERSION", "版本已更新，请刷新", 409)
    return get(run_id)


def latest_snapshot():
    """返回当前可用扫描全集；从未采信过时为 None。"""
    with connect() as db:
        row = db.execute("SELECT payload FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()
    return json.loads(row[0]) if row else None


def save_snapshot(snapshot):
    """单事务写入全集快照与文件台账；失败整体回滚，旧快照保持可用。

    返回快照序号。同一文件集合（按 sha256 序列）重复提交时复用既有快照，
    保证同一补片重试不产生新批次。
    """
    files = snapshot.get("per_file", [])
    fingerprint = "|".join(f["sha256"] for f in files)
    with connect() as db:
        prior = db.execute(
            "SELECT s.id,s.payload FROM snapshots s JOIN batch_files f ON f.snapshot_id=s.id "
            "GROUP BY s.id HAVING GROUP_CONCAT(f.sha256,'|')=? ORDER BY s.id DESC LIMIT 1",
            (fingerprint,)).fetchone()
        if prior:
            # 同一文件集合重传：复用既有快照序号与行内容，但本次提交的重放标记
            # （replayed/sequence）以新构建对象为准，保证同一补片重试可见且不新增。
            return prior[0], True, snapshot
        cur = db.execute("INSERT INTO snapshots(business_date,payload) VALUES (?,?)",
                         (snapshot.get("business_date"), json.dumps(snapshot, ensure_ascii=False)))
        snapshot_id = cur.lastrowid
        db.executemany(
            "INSERT INTO batch_files(snapshot_id,sha256,seq,mode,mode_basis,replayed) VALUES (?,?,?,?,?,?)",
            [(snapshot_id, f["sha256"], f["sequence"], f.get("mode"),
              f.get("mode_basis"), 1 if f.get("replayed") else 0) for f in files])
    return snapshot_id, False, snapshot


def snapshot_ledger():
    with connect() as db:
        rows = db.execute(
            "SELECT s.id,s.business_date,s.created_at,COUNT(f.id),"
            "SUM(f.replayed),GROUP_CONCAT(f.mode,',') "
            "FROM snapshots s LEFT JOIN batch_files f ON f.snapshot_id=s.id "
            "GROUP BY s.id ORDER BY s.id DESC").fetchall()
    return [{"snapshot_id": r[0], "business_date": r[1], "created_at": r[2],
             "files": r[3], "replayed": r[4] or 0, "modes": (r[5] or "").split(",")}
            for r in rows]
