import pytest

from app import batch, store
from app.config import COLUMNS, ROOT
from app.errors import DomainError
from app.pipeline import ingest_files

HEADER = ",".join(COLUMNS)
DIR = ROOT / "sample_data"


def sample(name):
    return (DIR / name).read_bytes()


def test_declared_full_then_patch_builds_unique_universe():
    full = sample("batch_full_a.csv")
    patch = sample("batch_patch_b.csv")
    result = ingest_files([full, patch])
    # 全集：0100/0101/0102/0103 四条，0110 被补片撤回，0101 重复不新增
    ids = {r["scan_id"] for r in result["stages"][0]["rows"]}
    assert ids == {"0100", "0101", "0102", "0103"}
    m = result["stages"][0]["metrics"]
    assert m["withdrawn"] == 1 and m["new"] == 5 and m["duplicate"] == 1
    patch_counts = result["stages"][0]["file_ledger"][1]["counts"]
    assert patch_counts == {"new": 2, "duplicate": 1, "conflict": 0,
                            "withdrawn": 1, "withdraw_unknown": 0}
    ledger = result["stages"][0]["file_ledger"]
    assert [f["mode"] for f in ledger] == ["full", "patch"]
    assert all(f["mode_basis"] == "declared" for f in ledger)


def test_patch_retry_is_idempotent_and_adds_nothing():
    full = sample("batch_full_a.csv")
    patch = sample("batch_patch_b.csv")
    first = ingest_files([full, patch])
    first_ids = {r["scan_id"] for r in first["stages"][0]["rows"]}
    # 同一补片文件重传：文件级 sha256 命中，整体快照复用，不产生新批次
    second = ingest_files([full, patch])
    assert second["run_id"] == first["run_id"]
    assert second["reused_snapshot"] is True
    assert {r["scan_id"] for r in second["stages"][0]["rows"]} == first_ids
    assert len(store.snapshot_ledger()) == 1


def test_full_retransmission_replaces_whole_batch():
    # 先采信首批，再传一份新的整批：先前扫描整体替换，不做累加
    ingest_files([sample("batch_full_a.csv")])
    new_full = ("#mode=full\n" + HEADER + "\n"
                "0300,0009,收件,2026-11-18T00:00:00Z,2026-11-18T01:00:00Z,JP,24,200,0,新整批\n"
                "0301,0009,妥投,2026-11-18T10:00:00Z,2026-11-18T11:00:00Z,JP,24,200,0,新整批\n").encode()
    result = ingest_files([new_full])
    ids = {r["scan_id"] for r in result["stages"][0]["rows"]}
    assert ids == {"0300", "0301"}
    counts = result["stages"][0]["file_ledger"][0]["counts"]
    assert counts["withdrawn"] == 3  # 0100/0101/0110 全部退出全集


def test_parse_failure_keeps_previous_snapshot(client):
    first = ingest_files([sample("batch_full_a.csv")])
    before = store.latest_snapshot()
    # 坏表头文件必须整体失败，旧全集原样可用
    bad = ("#mode=patch\nwrong,header\nx,y\n").encode()
    with pytest.raises(DomainError) as e:
        ingest_files([bad])
    assert e.value.code == "BAD_HEADER"
    after = store.latest_snapshot()
    assert after["canonical_scan_count"] == before["canonical_scan_count"] == 3
    assert len(store.snapshot_ledger()) == 1
    # 页面与详情接口仍返回上一可用批次
    resp = client.get("/api/batches")
    assert resp.status_code == 200
    assert resp.get_json()["current"]["canonical_scan_count"] == 3


def test_mode_inference_rules():
    # 无指令但含撤回 -> 推断补片
    parsed = batch.parse_file(("#withdraw=0100\n" + HEADER +
                               "\n0102,0001,清关,2026-11-18T12:00:00Z,2026-11-18T13:00:00Z,JP,24,200,0,x\n").encode())
    mode, basis = batch.infer_mode(parsed, {"0100"}, {"0102"})
    assert (mode, basis) == ("patch", "inferred")
    # 无指令且覆盖既有全部扫描 -> 推断整批
    parsed2 = batch.parse_file((HEADER +
                                "\n0100,0001,收件,2026-11-18T00:00:00Z,2026-11-18T01:00:00Z,JP,24,200,0,x\n"
                                "0101,0001,起运,2026-11-18T06:00:00Z,2026-11-18T07:00:00Z,JP,24,200,0,x\n").encode())
    mode2, basis2 = batch.infer_mode(parsed2, {"0100", "0101"}, {"0100", "0101"})
    assert (mode2, basis2) == ("full", "inferred")
    # 线索不足 -> 拒绝猜测
    parsed3 = batch.parse_file((HEADER +
                                "\n0999,0009,收件,2026-11-18T00:00:00Z,2026-11-18T01:00:00Z,JP,24,200,0,x\n").encode())
    with pytest.raises(DomainError) as e:
        batch.infer_mode(parsed3, {"0100"}, {"0999"})
    assert e.value.code == "AMBIGUOUS_MODE"


def test_withdraw_unknown_is_quarantined_not_silent():
    patch = ("#mode=patch\n#withdraw=missing-id\n" + HEADER +
             "\n0102,0001,清关,2026-11-18T12:00:00Z,2026-11-18T13:00:00Z,JP,24,200,0,x\n").encode()
    result = ingest_files([sample("batch_full_a.csv"), patch])
    reasons = {q["reason"] for q in result["stages"][0]["quarantine"]}
    assert "WITHDRAW_UNKNOWN" in reasons
    m = result["stages"][0]["metrics"]
    assert m["withdraw_unknown"] == 1


def test_cross_file_conflict_keeps_first_and_quarantines():
    # 同 scan_id 不同内容：保留全集原值，冲突行进隔离，不覆盖
    conflicting = ("#mode=patch\n" + HEADER +
                   "\n0101,0001,起运,2026-11-18T09:00:00Z,2026-11-18T10:00:00Z,JP,24,200,0,改值\n").encode()
    result = ingest_files([sample("batch_full_a.csv"), conflicting])
    kept = next(r for r in result["stages"][0]["rows"] if r["scan_id"] == "0101")
    assert kept["event_time"] == "2026-11-18T06:00:00Z"
    assert any(q["reason"] == "CANONICAL_CONFLICT" for q in result["stages"][0]["quarantine"])


def test_full_mode_conflict_is_quarantined_and_keeps_old_value():
    ingest_files([sample("batch_full_a.csv")])
    full2 = ("#mode=full\n" + HEADER +
             "\n0100,0001,收件,2026-11-18T00:00:00Z,2026-11-18T01:00:00Z,JP,24,200,0,首批"
             "\n0101,0001,起运,2026-11-18T09:00:00Z,2026-11-18T10:00:00Z,JP,24,200,0,改值"
             "\n0300,0009,收件,2026-11-18T00:00:00Z,2026-11-18T01:00:00Z,JP,24,200,0,新\n").encode()
    r = ingest_files([full2])
    counts = r["stages"][0]["file_ledger"][0]["counts"]
    assert counts == {"new": 1, "duplicate": 1, "conflict": 1,
                      "withdrawn": 1, "withdraw_unknown": 0}
    assert any(q["reason"] == "CANONICAL_CONFLICT" for q in r["stages"][0]["quarantine"])
    kept = next(x for x in r["stages"][0]["rows"] if x["scan_id"] == "0101")
    assert kept["event_time"] == "2026-11-18T06:00:00Z"  # 旧全集原值保留


def test_bad_directive_rejected():
    with pytest.raises(DomainError) as e:
        batch.parse_file(("#weird\n" + HEADER + "\n").encode())
    assert e.value.code == "BAD_DIRECTIVE"
    with pytest.raises(DomainError) as e:
        batch.parse_file(("#mode=replace\n" + HEADER + "\n").encode())
    assert e.value.code == "BAD_DIRECTIVE"


def test_duplicate_file_in_same_submission_rejected():
    full = sample("batch_full_a.csv")
    with pytest.raises(DomainError) as e:
        ingest_files([full, full])
    assert e.value.code == "DUP_FILE"


def test_paths_and_timeliness_only_read_canonical_universe():
    # 撤回的 0110（parcel 0002）不得在路径/时效阶段复活
    result = ingest_files([sample("batch_full_a.csv"), sample("batch_patch_b.csv")])
    parcels = {r["parcel"] for r in result["stages"][3]["rows"]}
    assert "0002" not in parcels and "0001" in parcels
    # m6 全集口径行数平衡
    m6 = result["stages"][5]["metrics"]
    assert m6["balanced"] is True
    assert m6["canonical_scans"] == 4
    # m7 归档可封存
    assert result["stages"][6]["metrics"]["ready"] is True


def test_batch_http_endpoint_and_ledger(client):
    data = {"csv_files": [sample("batch_full_a.csv").decode(),
                          sample("batch_patch_b.csv").decode()]}
    resp = client.post("/api/batches", json=data)
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["stages"][0]["canonical"] is True
    ledger = client.get("/api/batches").get_json()
    assert len(ledger["snapshots"]) == 1
    assert ledger["current"]["business_date"] == "2026-11-18"


def test_multipart_batch_upload(client):
    from io import BytesIO
    data = {"files": [(BytesIO(sample("batch_full_a.csv")), "a.csv"),
                      (BytesIO(sample("batch_patch_b.csv")), "b.csv")]}
    resp = client.post("/api/batches", data=data, content_type="multipart/form-data")
    assert resp.status_code == 200
    assert resp.get_json()["stages"][0]["metrics"]["withdrawn"] == 1


def test_patch_only_retransmission_does_not_duplicate_or_lose_history():
    # 首批只传整批；之后单独重传同一个补片文件
    first = ingest_files([sample("batch_full_a.csv")])
    assert {r["scan_id"] for r in first["stages"][0]["rows"]} == {"0100", "0101", "0110"}
    again = ingest_files([sample("batch_patch_b.csv")])
    ids = {r["scan_id"] for r in again["stages"][0]["rows"]}
    # 先前扫描保留，撤回生效，重复不新增
    assert ids == {"0100", "0101", "0102", "0103"}
    ledger = again["stages"][0]["file_ledger"]
    assert ledger[-1]["mode"] == "patch"
    # 同一补片再传一次：sha256 命中，标记重放且全集不变
    retry = ingest_files([sample("batch_patch_b.csv")])
    assert retry["reused_snapshot"] is True
    last = retry["stages"][0]["file_ledger"][-1]
    assert last["replayed"] is True and last["counts"]["withdrawn"] == 1
    assert {r["scan_id"] for r in retry["stages"][0]["rows"]} == ids


def test_inferred_patch_without_mode_directive():
    # batch_patch_c 无任何指令：含新扫描但既不覆盖全集也无撤回 -> 歧义拒绝
    with pytest.raises(DomainError) as e:
        ingest_files([sample("batch_full_a.csv"), sample("batch_patch_c.csv")])
    assert e.value.code == "AMBIGUOUS_MODE"
    # 显式声明为补片后即可采信
    declared = b"#mode=patch\n" + sample("batch_patch_c.csv")
    result = ingest_files([sample("batch_full_a.csv"), declared])
    assert result["stages"][0]["file_ledger"][1]["mode_basis"] == "declared"
    assert "0200" in {r["scan_id"] for r in result["stages"][0]["rows"]}
