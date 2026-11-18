# 接口

模块GET使用仓内样例，模块POST可传csv或rows与context，直接计算当前规则。/api/runs使用完整流水线并原子保存。每个阶段返回rows、quarantine、metrics；后续阶段还返回累计隔离。/api/upload接收multipart文件file。/api/runs/<id>/archive只导出一份带清单与证据的CSV。

同日多文件重传走/api/batches：POST以multipart重复字段files或JSON {"csv_files":[...]}提交，按docs/BATCH-INGEST.md识别整批/补片并原子切换唯一扫描全集，随后在全集上重跑M2–M7；解析失败返回422且保留上一可用全集。GET /api/batches返回快照台账与当前全集。
