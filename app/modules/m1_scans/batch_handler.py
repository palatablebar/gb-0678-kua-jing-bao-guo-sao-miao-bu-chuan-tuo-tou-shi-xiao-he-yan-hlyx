from flask import Blueprint, request, jsonify

from app.errors import DomainError
from app import pipeline, store

bp = Blueprint("batches", __name__)


@bp.post("/api/batches")
def ingest():
    """同日多文件采信：multipart 字段 files 可重复，或 JSON {"csv_files": [...]}。

    解析失败返回 422 且保留上一可用批次；成功后一次性切换到新全集并返回完整流水线。
    """
    params = {}
    if request.content_type and request.content_type.startswith("multipart/"):
        files = [f.read() for f in request.files.getlist("files")]
    else:
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            raise DomainError("BAD_BODY", "请求需要对象")
        params = payload.get("parameters", {})
        if not isinstance(params, dict):
            raise DomainError("BAD_PARAMETERS", "参数需要对象")
        texts = payload.get("csv_files")
        if not isinstance(texts, list) or not texts or not all(isinstance(x, str) for x in texts):
            raise DomainError("BAD_BODY", "csv_files 需要非空字符串数组")
        files = [x.encode() for x in texts]
    return jsonify(pipeline.ingest_files(files, params))


@bp.get("/api/batches")
def ledger():
    return jsonify({"snapshots": store.snapshot_ledger(),
                    "current": store.latest_snapshot()})
