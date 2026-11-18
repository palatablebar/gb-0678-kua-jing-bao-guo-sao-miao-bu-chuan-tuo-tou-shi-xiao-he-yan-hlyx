"""同日扫描批次采信：识别重传性质，维护唯一扫描全集（canonical snapshot）。

协议（见 docs/BATCH-INGEST.md）：
- 指令行以 ``#`` 开头，只允许出现在文件首部数据行之前：
  ``#mode=full|patch`` 声明整批覆盖或补片；``#date=YYYY-MM-DD`` 声明业务日；
  ``#withdraw=<scan_id>`` 可重复，声明撤回既有扫描。
- 无 mode 指令时按线索推断，并把推断依据写回 mode_basis；线索冲突无法判定时
  抛 AMBIGUOUS_MODE，拒绝静默猜测。
- 解析（编码/表头/列数/空态）与全集裁决全部成功后，由 store 在单事务内切换；
  任一阶段失败都保留上一可用批次。
- 同一文件按 sha256 去重：重传同一补片命中既有批次，不新增任何扫描。
"""
import csv
import hashlib
import io

from app.config import COLUMNS
from app.errors import DomainError
from app.modules.m1_scans.rules import decode
from app.reasons import reject

FULL = "full"
PATCH = "patch"


def _parse_directive(line, line_no):
    body = line[1:].strip()
    if not body or "=" not in body:
        raise DomainError("BAD_DIRECTIVE", f"第{line_no}行指令格式应为 #key=value")
    key, value = body.split("=", 1)
    return key.strip().lower(), value.strip()


def parse_file(raw, sequence=1):
    """解析单个扫描文件，保留与 m1 相同的编码/表头/空态语义。

    返回指令、撤回清单与行级裁决；不做跨文件全集裁决。
    """
    text, encoding = decode(raw)
    if not text:
        raise DomainError("EMPTY_FILE", "文件为空")
    if not text.strip():
        raise DomainError("BLANK_ONLY", "全部为空行")

    directives, withdrawals = {}, []
    physical_lines = text.splitlines()
    cursor = 0
    for line in physical_lines:
        stripped = line.strip()
        if not stripped:
            cursor += 1
            continue
        if not stripped.startswith("#"):
            break
        cursor += 1
        key, value = _parse_directive(stripped, cursor)
        if key == "mode":
            if value not in (FULL, PATCH):
                raise DomainError("BAD_DIRECTIVE", "mode 只允许 full 或 patch")
            directives["mode"] = value
        elif key == "date":
            if len(value) != 10 or value[4] != "-" or value[7] != "-":
                raise DomainError("BAD_DIRECTIVE", "date 格式应为 YYYY-MM-DD")
            directives["date"] = value
        elif key == "withdraw":
            if not value:
                raise DomainError("BAD_DIRECTIVE", "withdraw 需要扫描号")
            withdrawals.append(value)
        else:
            raise DomainError("BAD_DIRECTIVE", f"未知指令 #{key}")

    payload = "\n".join(physical_lines[cursor:])
    if not payload.strip():
        raise DomainError("HEADER_ONLY", "仅有指令或表头")
    stream = csv.reader(io.StringIO(payload, newline=""))
    header = next(stream, None)
    if header != COLUMNS:
        raise DomainError("BAD_HEADER", "表头不匹配")

    rows, bad = [], []
    seen = {}
    duplicate = blank = repeated = count = 0
    offset = cursor + 1  # 物理行号 = 指令/空行数 + csv 逻辑行号
    try:
        for cells in stream:
            count += 1
            line = stream.line_num + offset - 1
            if not cells or not any(c.strip() for c in cells):
                blank += 1
                continue
            if cells == header:
                repeated += 1
                continue
            row = {"_line": line, "_sources": [[sequence, line]], "_raw": cells,
                   "_file": sequence}
            if len(cells) != len(header):
                bad.append(reject(row, "BAD_COLUMNS", "列数不匹配"))
                continue
            row.update(dict(zip(header, [x.strip() for x in cells])))
            if not row[COLUMNS[0]]:
                bad.append(reject(row, "EMPTY_VALUE", "业务键为空"))
                continue
            key = row[header[0]]
            if key in seen:
                if cells == seen[key]["_raw"]:
                    duplicate += 1
                else:
                    bad.append(reject(row, "DUPLICATE_CONFLICT", "同键不同值"))
                continue
            seen[key] = row
            rows.append(row)
    except csv.Error:
        raise DomainError("BAD_CSV", "引号未闭合或文件损坏")
    if count == 0:
        raise DomainError("HEADER_ONLY", "仅有表头")
    if not rows and not bad:
        raise DomainError("BLANK_ONLY", "全部为空行")

    metrics = {"input": count, "accepted": len(rows), "quarantine": len(bad),
               "duplicate": duplicate, "blank": blank,
               "repeated_headers": repeated, "encoding": encoding,
               "withdrawals": len(withdrawals)}
    return {"directives": directives, "withdrawals": withdrawals, "rows": rows,
            "quarantine": bad, "metrics": metrics,
            "sha256": hashlib.sha256(raw).hexdigest()}


def infer_mode(parsed, snapshot_ids, current_ids):
    """无显式 mode 时的推断规则，返回 (mode, basis)。"""
    if parsed["directives"].get("mode"):
        return parsed["directives"]["mode"], "declared"
    if parsed["withdrawals"]:
        return PATCH, "inferred"
    if snapshot_ids and current_ids and snapshot_ids.issubset(current_ids):
        return FULL, "inferred"
    if not snapshot_ids:
        return FULL, "inferred"
    raise DomainError(
        "AMBIGUOUS_MODE",
        "无法判定重传性质：文件既无 #mode 指令，也不覆盖既有全集且不含撤回指令")


def _merge_row(existing, incoming):
    """同 scan_id 跨文件裁决：完全相同计重复，值不同隔离并保留原值。"""
    if incoming["_raw"] == existing["_raw"]:
        return existing, "duplicate"
    return existing, "conflict"


def apply_file(snapshot, parsed, sequence):
    """把单个已解析文件裁决到内存快照上（不写库）。

    snapshot: {"rows": {scan_id: row}, "files": [文件级元数据], ...}
    返回本次文件的裁决计数。
    """
    current = {r[COLUMNS[0]] for r in parsed["rows"]}
    mode, basis = infer_mode(parsed, set(snapshot["rows"]), current)

    counts = {"new": 0, "duplicate": 0, "conflict": 0, "withdrawn": 0,
              "withdraw_unknown": 0}
    new_quarantine = list(parsed["quarantine"])

    if mode == FULL:
        # 整批：以本文件为新全集；旧全集中未出现的扫描记为撤回。
        incoming = {}
        for row in parsed["rows"]:
            key = row[COLUMNS[0]]
            old = snapshot["rows"].get(key)
            if old is not None:
                _, verdict = _merge_row(old, row)
                counts[verdict] += 1
                if verdict == "conflict":
                    # 整批内同键异值同样保留旧全集原值，新行隔离不切换。
                    new_quarantine.append(reject(row, "CANONICAL_CONFLICT",
                                                 "整批重传与既有扫描同键不同值，保留原值"))
                    incoming[key] = old
                    continue
            else:
                counts["new"] += 1
            incoming[key] = row
        counts["withdrawn"] += len(set(snapshot["rows"]) - set(incoming))
        snapshot["rows"] = incoming
    else:
        # 补片：只做差量增删，先前扫描原样保留。
        for scan_id in parsed["withdrawals"]:
            if scan_id in snapshot["rows"]:
                del snapshot["rows"][scan_id]
                counts["withdrawn"] += 1
            else:
                counts["withdraw_unknown"] += 1
                new_quarantine.append(reject(
                    {"_line": 0, "_sources": [[sequence, 0]], "_file": sequence,
                     COLUMNS[0]: scan_id},
                    "WITHDRAW_UNKNOWN", "撤回目标在当前全集中不存在"))
        for row in parsed["rows"]:
            key = row[COLUMNS[0]]
            old = snapshot["rows"].get(key)
            if old is None:
                snapshot["rows"][key] = row
                counts["new"] += 1
                continue
            kept, verdict = _merge_row(old, row)
            counts[verdict] += 1
            if verdict == "conflict":
                new_quarantine.append(reject(row, "CANONICAL_CONFLICT",
                                             "与全集既有扫描同键不同值，保留原值"))

    snapshot.setdefault("files", []).append({
        "sequence": sequence,
        "sha256": parsed["sha256"],
        "mode": mode,
        "mode_basis": basis,
        "date": parsed["directives"].get("date"),
        "metrics": parsed["metrics"],
        "counts": counts,
    })
    snapshot.setdefault("quarantine", []).extend(new_quarantine)
    return mode, basis, counts


def build_snapshot(raw_files, previous=None):
    """对一批同日文件做完整采信，返回待持久化的快照结构。

    raw_files: list[bytes]。previous: 上一可用快照（dict）或 None。
    任一文件解析失败直接抛错，调用方据此保留 previous。
    """
    if not raw_files:
        raise DomainError("EMPTY_FILE", "没有可采信的扫描文件")
    seen_hashes = set()
    previous = previous or {}
    # 历史台账同时存在 files（裁决元数据）与 per_file（持久化视图），两处都要检索。
    history_files = list(previous.get("files", [])) + [
        f for f in previous.get("per_file", []) if "counts" in f]
    snapshot = {"rows": dict(previous.get("rows", {})),
                "files": [], "quarantine": list(previous.get("quarantine", [])),
                "business_date": previous.get("business_date")}
    per_file = []
    for sequence, raw in enumerate(raw_files, 1):
        digest = hashlib.sha256(raw).hexdigest()
        if digest in seen_hashes:
            raise DomainError("DUP_FILE", f"第{sequence}个文件与同批前文完全相同")
        seen_hashes.add(digest)
        # 同一补片重试：命中任一历史已采信文件，幂等跳过、不新增扫描。
        prior = next((f for f in history_files if f["sha256"] == digest), None)
        if prior is not None:
            entry = dict(prior, replayed=True, sequence=sequence)
            snapshot["files"].append(entry)
            per_file.append(entry)
            continue
        parsed = parse_file(raw, sequence)
        if parsed["directives"].get("date"):
            snapshot["business_date"] = parsed["directives"]["date"]
        mode, basis, counts = apply_file(snapshot, parsed, sequence)
        per_file.append({"sequence": sequence, "sha256": digest, "replayed": False,
                         "mode": mode, "mode_basis": basis, "counts": counts,
                         "metrics": parsed["metrics"]})
    snapshot["per_file"] = per_file
    snapshot["canonical_scan_count"] = len(snapshot["rows"])
    return snapshot
