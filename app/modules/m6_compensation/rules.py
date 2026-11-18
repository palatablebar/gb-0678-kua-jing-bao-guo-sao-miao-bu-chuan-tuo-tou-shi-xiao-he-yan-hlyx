from decimal import Decimal
from app.reasons import sources
from app.errors import DomainError
def evaluate(rows,context):
    canonical=context.get("canonical")
    intake=context.get("intake")
    bad=context.get("quarantine",[])
    accepted=set(x for row in rows for x in sources(row))
    rejected=set(x for row in bad for x in sources(row))
    if accepted & rejected: raise DomainError("LINEAGE_OVERLAP","同一来源行被重复裁决")
    m={"accepted_inputs":len(accepted),"quarantine_inputs":len(rejected),"output_records":len(rows)}
    if canonical:
        # 全集模式：输入=当前唯一全集扫描数；重复/重放为同一扫描的多次出现，
        # 撤回与撤回未命中不进入全集，单独披露。
        # 平衡口径：全集每条 scan_id 要么最终采纳、要么在 M2-M4 被隔离；
        # 采信阶段的冲突/撤回未命中行带 Q: 前缀，属于全集之外的额外拒绝，不计入平衡。
        excluded=intake["duplicate"]+intake["replayed"]
        canonical_lineage={x for x in (accepted | rejected) if not str(x).startswith("Q:")}
        m.update(input=intake["canonical_scans"]+excluded,excluded=excluded,
                 balanced=intake["canonical_scans"]==len(canonical_lineage),
                 canonical_scans=intake["canonical_scans"],new_scans=intake["new"],
                 duplicate_scans=intake["duplicate"],withdrawn_scans=intake["withdrawn"],
                 withdraw_unknown=intake["withdraw_unknown"],replayed_files=intake["replayed"])
    elif intake:
        excluded=intake["duplicate"]+intake["blank"]+intake["repeated_headers"]
        m.update(input=intake["input"],excluded=excluded,balanced=intake["input"]==len(accepted)+len(rejected)+excluded)
    else:
        m.update(input=len(accepted)+len(rejected),excluded=0,balanced=True)
    output=[]
    for row in rows:
        expected=Decimal(str(row.get("expected","0")));reported=Decimal(str(row.get("reported","0")))
        output.append({**row,"difference":str(expected-reported),"status":"通过" if expected==reported else "差异"})
    m.update(expected_total=str(sum((Decimal(str(r.get("expected","0"))) for r in rows),Decimal(0))),reported_total=str(sum((Decimal(str(r.get("reported","0"))) for r in rows),Decimal(0))),differences=sum(r["status"]=="差异" for r in output))
    return {"rows":output,"quarantine":[],"metrics":m}
