import hashlib
import json
import time

from app.config import ROOT, MODULES, RULE_VERSION
from app.errors import DomainError
from app.values import scalar
from app.manifest import digest, manifest
from app import store
from app import batch as batch_rules
from app.modules.m1_scans.rules import evaluate as stage1
from app.modules.m2_nodes.rules import evaluate as stage2
from app.modules.m3_timezones.rules import evaluate as stage3
from app.modules.m4_paths.rules import evaluate as stage4
from app.modules.m5_timeliness.rules import evaluate as stage5
from app.modules.m6_compensation.rules import evaluate as stage6
from app.modules.m7_archives.rules import evaluate as stage7
STAGES = [stage1, stage2, stage3, stage4, stage5, stage6, stage7]
def load_sample(name):
    if name not in {"dirty.csv","clean.csv","legacy.csv"}: raise DomainError("BAD_SAMPLE","样例不存在",404)
    return (ROOT/"sample_data"/name).read_bytes()
def calculate(raw,params=None,stop=6):
    params=params or {};started=time.monotonic();context={"parameters":params,"raw":raw,"quarantine":[]};stages=[];rows=[]
    for index,stage in enumerate(STAGES[:stop+1]):
        result=stage(raw if index==0 else rows,context)
        result=scalar(result);rows=result["rows"]
        context["quarantine"].extend(result.get("quarantine",[]))
        if index==0: context["intake"]=result["metrics"]
        result["cumulative_quarantine"]=list(context["quarantine"])
        context[MODULES[index][0]]=result
        stages.append(result)
    return {"run_id":digest(raw,params)[:20],"stages":stages,"manifest":manifest(raw,params,started),"parameters":params}
def execute(raw,params=None):
    result=calculate(raw,params);return store.save(result["run_id"],result)
def stage_preview(index,payload):
    if "rows" in payload and index>0:
        if not isinstance(payload["rows"],list) or any(not isinstance(row,dict) for row in payload["rows"]): raise DomainError("BAD_ROWS","记录需要对象列表")
        if not isinstance(payload.get("context",{}),dict): raise DomainError("BAD_CONTEXT","上下文需要对象")
        return scalar(STAGES[index](payload["rows"],payload.get("context",{"parameters":{},"quarantine":[]})))
    raw=payload.get("csv")
    raw=raw.encode() if isinstance(raw,str) else load_sample(payload.get("sample","dirty.csv"))
    return calculate(raw,payload.get("parameters",{}),index)["stages"][-1]


def _canonical_source_label(source):
    # 全集物理溯源形如 [文件序号, 物理行号]，压平为 "F1:L12"；重放时幂等。
    if isinstance(source, str):
        return source
    if isinstance(source, list) and len(source) == 2 and isinstance(source[0], int):
        return f"F{source[0]}:L{source[1]}"
    return source


def run_canonical_pipeline(snapshot, params=None):
    """在唯一扫描全集上重跑 m2-m7；路径与时效只读取该快照。"""
    params = params or {}
    started = time.monotonic()
    rows = list(snapshot["rows"].values())
    for row in rows:
        origin = [_canonical_source_label(s) for s in row.get("_sources", [])]
        row["_origin"] = origin
        # 血缘以稳定的 scan_id 为准：重传后文件序号可能变化，scan_id 不变，
        # m6 血缘去重与 m7 封存因此可跨批次复算。
        row["_sources"] = [row["scan_id"]]
    quarantine = list(snapshot.get("quarantine", []))
    for row in quarantine:
        row["_origin"] = [_canonical_source_label(s) for s in row.get("_sources", [])]
        # 隔离行（冲突/撤回未命中）可能与采纳行共享 scan_id，加 Q: 前缀区分血缘，
        # 避免 m6 的同一来源行重复裁决检查误判。
        if row.get("scan_id"):
            row["_sources"] = ["Q:" + row["scan_id"]]
        else:
            row["_sources"] = ["Q:" + ":".join(row["_origin"])]

    files = snapshot.get("per_file", [])
    def _counts(f):
        return f.get("counts") or {}
    intake = {
        "mode": "canonical",
        "canonical_scans": len(rows),
        "files": len(files),
        "new": sum(_counts(f).get("new", 0) for f in files),
        "duplicate": sum(_counts(f).get("duplicate", 0)
                         + (f.get("metrics") or {}).get("duplicate", 0) for f in files),
        "withdrawn": sum(_counts(f).get("withdrawn", 0) for f in files),
        "withdraw_unknown": sum(_counts(f).get("withdraw_unknown", 0) for f in files),
        "conflicts": sum(_counts(f).get("conflict", 0) for f in files),
        "replayed": sum(1 for f in files if f.get("replayed")),
        "business_date": snapshot.get("business_date"),
        "input": len(rows) + len(quarantine),
    }
    context = {"parameters": params, "raw": b"", "quarantine": quarantine,
               "intake": intake, "canonical": True,
               "snapshot_files": files}
    stages = []
    for index, stage in enumerate(STAGES[1:], start=1):
        result = scalar(stage(rows, context))
        rows = result["rows"]
        context["quarantine"].extend(result.get("quarantine", []))
        result["cumulative_quarantine"] = list(context["quarantine"])
        context[MODULES[index][0]] = result
        stages.append(result)

    first_stage = {"rows": list(snapshot["rows"].values()), "quarantine": quarantine,
                   "metrics": intake, "canonical": True,
                   "file_ledger": files}
    stages = [first_stage] + stages
    canonical_hash = hashlib.sha256(
        str(sorted(snapshot["rows"].keys())).encode()
        + hashlib.sha256(json.dumps(params, sort_keys=True, ensure_ascii=False).encode()).digest()
        + ("canonical:" + RULE_VERSION).encode()
    ).hexdigest()[:20]
    run_id = "C" + canonical_hash
    return {"run_id": run_id, "stages": stages,
            "manifest": {"input_sha256": hashlib.sha256(
                "|".join(f["sha256"] for f in files).encode()).hexdigest(),
                "rule_version": RULE_VERSION,
                "parameters": params, "generated_at": manifest(b"", params, started)["generated_at"],
                "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
                "canonical": True, "business_date": snapshot.get("business_date")},
            "parameters": params}


def ingest_files(raw_files, params=None):
    """完整采信入口：解析失败保留上一可用快照，成功后单事务一次性切换。"""
    previous = store.latest_snapshot()
    # 全部解析与裁决在内存完成；build_snapshot 抛错时不会触碰数据库。
    snapshot = batch_rules.build_snapshot(raw_files, previous)
    snapshot_id, reused, persisted = store.save_snapshot(snapshot)
    result = run_canonical_pipeline(persisted, params or {})
    result["snapshot_id"] = snapshot_id
    saved = store.save(result["run_id"], result)
    saved["reused_snapshot"] = reused  # INSERT OR IGNORE 命中旧行时从本次事务补齐标记
    if reused:
        # run 行复用旧 payload，但本次提交的重放台账（replayed/文件序号）以新快照为准。
        saved["stages"][0]["file_ledger"] = result["stages"][0]["file_ledger"]
        saved["stages"][0]["metrics"] = result["stages"][0]["metrics"]
    return saved
