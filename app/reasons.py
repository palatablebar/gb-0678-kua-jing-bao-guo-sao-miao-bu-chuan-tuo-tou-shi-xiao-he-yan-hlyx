REASONS={"BAD_COLUMNS":"列数不匹配","EMPTY_VALUE":"必填值为空","INVALID_NUMBER":"数字格式错误","INVALID_TIME":"时间缺少偏移或格式错误","DUPLICATE_CONFLICT":"同键取值冲突","UNKNOWN_REFERENCE":"引用不存在","STALE_VERSION":"版本已更新","WITHDRAWN_SCAN":"扫描已撤回","CANONICAL_CONFLICT":"与全集既有扫描冲突","WITHDRAW_UNKNOWN":"撤回目标不存在","BAD_DIRECTIVE":"批次指令格式错误","AMBIGUOUS_MODE":"重传性质无法判定","EMPTY_FILE":"文件为空","HEADER_ONLY":"仅有表头","BLANK_ONLY":"全部为空行"}
def reject(row, code, detail):
    return {**row,"reason":code,"detail":detail}
def sources(row):
    return row.get("_sources", [row.get("_line",0)])
