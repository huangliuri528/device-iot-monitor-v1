# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-25（第 5 天 / 共 15 天）
# 模块功能：本文件仅含该软件第 5 天新增与修改的源代码，不含第 1~4 天已有代码
#           本次迭代主题：设备参数阈值告警模块 + 告警中心页面
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）
# 接入说明：以下内容为当日增量，需与前面各天的代码合并后运行——
#           新增模块按文件名放入工程目录；建表脚本增量追加进对应 .sql；
#           主程序增量片段插入 app.py 中 Flask 实例之后；前端资源按文末清单
#           放入 templates/ 与 static/ 目录


# --------------------------------------------------------------------------
# 第 5 天新增业务模块 1：iot_alarm.py（完整源码）
# --------------------------------------------------------------------------

# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-25（第 5 天 / 共 15 天）
# 模块功能：设备运行参数阈值告警模块。负责按设备与指标维护上下限阈值规则、
#           对最新采集值做二级阈值（警告/严重）判定、生成并去重累计告警
#           记录、完成告警确认与处置闭环，并输出告警概览、等级分布、
#           指标告警分布、设备告警排行与近 12 个时间片告警趋势，同时为
#           大屏底部滚动告警条提供实时告警文本
# 依赖包：sqlite3（Python 内置）、datetime（Python 内置）

from datetime import datetime
from iot_config import METRIC_FIELD_DICT
from iot_store import (connect_db, dict_list, now_text, fetch_device_nodes,
                       save_telemetry)

# 告警等级字典：警告为一级预警，严重为需立即处置的越限
ALARM_LEVEL_DICT = {
    "NORMAL": "正常",
    "WARN": "警告",
    "CRITICAL": "严重",
}

# 告警处置状态字典：覆盖从触发到处置完成的闭环
HANDLE_STATE_DICT = {
    "OPEN": "未处理",
    "ACK": "已确认",
    "DONE": "已处置",
}

# 默认阈值模板：设备未单独配置规则时使用，键为指标、值为
# （警告下限, 警告上限, 严重下限, 严重上限）
DEFAULT_RULE_MAP = {
    "temp": (10.0, 40.0, 5.0, 45.0),
    "volt": (198.0, 242.0, 180.0, 260.0),
    "curr": (0.5, 12.0, 0.2, 16.0),
    "power": (10.0, 1200.0, 5.0, 1500.0),
    "press": (10.0, 320.0, 5.0, 380.0),
    "humid": (30.0, 70.0, 20.0, 80.0),
}

# 参与告警扫描的指标（按重要性排序，便于生成规则时保持顺序稳定）
ALARM_METRIC_KEYS = ["temp", "volt", "curr", "power", "press", "humid"]


def fetch_alarm_rules(device_code=""):
    """读取阈值告警规则，可按设备编号过滤。"""
    conn = connect_db()
    try:
        if device_code:
            rows = conn.execute(
                "SELECT rule_id, device_code, metric_key, warn_low, warn_high, "
                "critical_low, critical_high, enabled_flag, updated_at "
                "FROM iot_alarm_rule WHERE device_code=? ORDER BY metric_key",
                (device_code,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT rule_id, device_code, metric_key, warn_low, warn_high, "
                "critical_low, critical_high, enabled_flag, updated_at "
                "FROM iot_alarm_rule ORDER BY device_code, metric_key"
            ).fetchall()
    finally:
        conn.close()
    result = []
    for item in dict_list(rows):
        item["metric_label"] = METRIC_FIELD_DICT.get(item["metric_key"], item["metric_key"])
        item["enabled_text"] = "启用" if item["enabled_flag"] else "停用"
        result.append(item)
    return result


def save_alarm_rule(device_code, metric_key, warn_low, warn_high,
                    critical_low, critical_high, enabled_flag=1):
    """写入或更新一条阈值规则，规则按「设备 + 指标」唯一。"""
    if metric_key not in METRIC_FIELD_DICT:
        return {"success": False, "message": "指标未登记：%s" % metric_key}
    try:
        wl, wh = float(warn_low), float(warn_high)
        cl, ch = float(critical_low), float(critical_high)
    except (TypeError, ValueError):
        return {"success": False, "message": "阈值必须为数字"}
    if wl >= wh:
        return {"success": False, "message": "警告下限必须小于警告上限"}
    if cl >= ch:
        return {"success": False, "message": "严重下限必须小于严重上限"}
    if cl > wl or ch < wh:
        return {"success": False, "message": "严重区间必须包含警告区间"}

    conn = connect_db()
    try:
        exist = conn.execute(
            "SELECT rule_id FROM iot_alarm_rule WHERE device_code=? AND metric_key=?",
            (device_code, metric_key),
        ).fetchone()
        if exist is None:
            conn.execute(
                "INSERT INTO iot_alarm_rule (device_code, metric_key, warn_low, warn_high, "
                "critical_low, critical_high, enabled_flag) VALUES (?,?,?,?,?,?,?)",
                (device_code, metric_key, wl, wh, cl, ch, int(enabled_flag)),
            )
            action = "新增"
        else:
            conn.execute(
                "UPDATE iot_alarm_rule SET warn_low=?, warn_high=?, critical_low=?, "
                "critical_high=?, enabled_flag=?, updated_at=? WHERE rule_id=?",
                (wl, wh, cl, ch, int(enabled_flag), now_text(), exist["rule_id"]),
            )
            action = "更新"
        conn.commit()
    finally:
        conn.close()
    return {
        "success": True,
        "message": "阈值规则已%s：%s · %s"
                   % (action, device_code or "全局",
                      METRIC_FIELD_DICT.get(metric_key, metric_key)),
    }


def fetch_latest_metric_value(device_code, metric_key):
    """读取某设备某指标的最新采集值与采集时间。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT metric_value, metric_unit, collect_time FROM iot_telemetry_log "
            "WHERE device_code=? AND metric_key=? "
            "ORDER BY telemetry_id DESC LIMIT 1",
            (device_code, metric_key),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    item = dict(row)
    item["metric_value"] = round(float(item["metric_value"]), 2)
    return item


def judge_alarm_level(metric_value, rule):
    """按规则判定采集值所处告警等级。

    判定顺序：先判严重区间（超出即 CRITICAL），再判警告区间（超出即 WARN），
    落在警告区间内为 NORMAL。返回 (等级编码, 越限说明文本)。
    """
    value = float(metric_value)
    label = METRIC_FIELD_DICT.get(rule["metric_key"], rule["metric_key"])
    if value < float(rule["critical_low"]):
        return "CRITICAL", "%s %.2f 低于严重下限 %.2f" % (
            label, value, float(rule["critical_low"]))
    if value > float(rule["critical_high"]):
        return "CRITICAL", "%s %.2f 超出严重上限 %.2f" % (
            label, value, float(rule["critical_high"]))
    if value < float(rule["warn_low"]):
        return "WARN", "%s %.2f 低于警告下限 %.2f" % (
            label, value, float(rule["warn_low"]))
    if value > float(rule["warn_high"]):
        return "WARN", "%s %.2f 超出警告上限 %.2f" % (
            label, value, float(rule["warn_high"]))
    return "NORMAL", "%s %.2f 处于正常区间" % (label, value)


def build_rule_for_device(device_code, metric_key):
    """取某设备某指标生效的规则：优先设备专属规则，其次默认模板。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT rule_id, device_code, metric_key, warn_low, warn_high, "
            "critical_low, critical_high, enabled_flag FROM iot_alarm_rule "
            "WHERE device_code=? AND metric_key=?",
            (device_code, metric_key),
        ).fetchone()
    finally:
        conn.close()
    if row is not None:
        return dict(row)
    wl, wh, cl, ch = DEFAULT_RULE_MAP.get(metric_key, (0.0, 9999.0, -1.0, 99999.0))
    return {
        "rule_id": 0,
        "device_code": device_code,
        "metric_key": metric_key,
        "warn_low": wl,
        "warn_high": wh,
        "critical_low": cl,
        "critical_high": ch,
        "enabled_flag": 1,
    }


def write_alarm_record(device_code, device_name, metric_key, level_code,
                       alarm_value, alarm_text, threshold_text):
    """写入一条告警记录。

    去重策略：同设备同指标已存在未处理或已确认的同级告警时，只累加触发
    次数并刷新最新值与时间，避免告警列表被同一故障反复刷屏。
    """
    conn = connect_db()
    try:
        exist = conn.execute(
            "SELECT record_id, trigger_amount FROM iot_alarm_record "
            "WHERE device_code=? AND metric_key=? AND alarm_level=? "
            "AND handle_state IN ('OPEN','ACK') ORDER BY record_id DESC LIMIT 1",
            (device_code, metric_key, level_code),
        ).fetchone()
        if exist is not None:
            conn.execute(
                "UPDATE iot_alarm_record SET alarm_value=?, alarm_text=?, "
                "threshold_text=?, trigger_amount=?, alarm_time=? WHERE record_id=?",
                (alarm_value, alarm_text, threshold_text,
                 int(exist["trigger_amount"]) + 1, now_text(), exist["record_id"]),
            )
            record_id = exist["record_id"]
            repeated = True
        else:
            cur = conn.execute(
                "INSERT INTO iot_alarm_record (device_code, device_name, metric_key, "
                "alarm_level, alarm_value, alarm_text, threshold_text, handle_state) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (device_code, device_name, metric_key, level_code, alarm_value,
                 alarm_text, threshold_text, "OPEN"),
            )
            record_id = cur.lastrowid
            repeated = False
        conn.commit()
    finally:
        conn.close()
    return {"record_id": record_id, "repeated": repeated}


def scan_device_alarms(device_code, device_name="", ward_name=""):
    """对单台设备的全部登记指标做阈值扫描，返回本次触发的告警列表。"""
    triggered = []
    for metric_key in ALARM_METRIC_KEYS:
        latest = fetch_latest_metric_value(device_code, metric_key)
        if latest is None:
            continue
        rule = build_rule_for_device(device_code, metric_key)
        if not int(rule.get("enabled_flag") or 0):
            continue
        level_code, text = judge_alarm_level(latest["metric_value"], rule)
        if level_code == "NORMAL":
            continue
        threshold_text = "警告[%.1f~%.1f] 严重[%.1f~%.1f]" % (
            float(rule["warn_low"]), float(rule["warn_high"]),
            float(rule["critical_low"]), float(rule["critical_high"]))
        outcome = write_alarm_record(device_code, device_name, metric_key,
                                     level_code, latest["metric_value"], text,
                                     threshold_text)
        triggered.append({
            "device_code": device_code,
            "device_name": device_name,
            "metric_key": metric_key,
            "metric_label": METRIC_FIELD_DICT.get(metric_key, metric_key),
            "alarm_level": level_code,
            "level_label": ALARM_LEVEL_DICT[level_code],
            "alarm_value": latest["metric_value"],
            "alarm_text": text,
            "record_id": outcome["record_id"],
            "repeated": outcome["repeated"],
        })
    return triggered


def scan_all_alarms():
    """对全部在册设备执行阈值扫描，返回本次新触发与累计重复的告警统计。"""
    nodes = fetch_device_nodes()
    triggered = []
    for node in nodes:
        if node.get("run_state") == "OFFLINE":
            continue
        triggered.extend(scan_device_alarms(
            node.get("device_code", ""),
            node.get("device_name", ""),
            node.get("ward_name", ""),
        ))
    new_amount = sum(1 for t in triggered if not t["repeated"])
    return {
        "scan_amount": len(nodes),
        "trigger_amount": len(triggered),
        "new_amount": new_amount,
        "repeat_amount": len(triggered) - new_amount,
        "rows": triggered,
    }


def fetch_alarm_records(device_code="", level_code="", state_code="", limit=50):
    """查询告警记录，支持按设备、等级与处置状态组合过滤。"""
    where_parts = []
    params = []
    if device_code:
        where_parts.append("device_code=?")
        params.append(device_code)
    if level_code:
        where_parts.append("alarm_level=?")
        params.append(level_code)
    if state_code:
        where_parts.append("handle_state=?")
        params.append(state_code)
    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT record_id, device_code, device_name, metric_key, alarm_level, "
            "alarm_value, alarm_text, threshold_text, handle_state, handler_name, "
            "handle_note, trigger_amount, alarm_time, handle_time "
            "FROM iot_alarm_record" + where_sql +
            " ORDER BY record_id DESC LIMIT ?", params + [int(limit)]
        ).fetchall()
    finally:
        conn.close()
    result = []
    for item in dict_list(rows):
        item["metric_label"] = METRIC_FIELD_DICT.get(item["metric_key"], item["metric_key"])
        item["level_label"] = ALARM_LEVEL_DICT.get(item["alarm_level"], "未知")
        item["state_label"] = HANDLE_STATE_DICT.get(item["handle_state"], "未知")
        result.append(item)
    return result


def handle_alarm_record(record_id, handle_state, handler_name="admin", handle_note=""):
    """告警处置：把告警置为已确认或已处置，并记录处置人与说明。"""
    if handle_state not in HANDLE_STATE_DICT:
        return {"success": False, "message": "处置状态不合法：%s" % handle_state}
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT record_id, device_code, handle_state FROM iot_alarm_record "
            "WHERE record_id=?", (int(record_id),)
        ).fetchone()
        if row is None:
            return {"success": False, "message": "告警记录不存在：%s" % record_id}
        conn.execute(
            "UPDATE iot_alarm_record SET handle_state=?, handler_name=?, "
            "handle_note=?, handle_time=? WHERE record_id=?",
            (handle_state, handler_name, (handle_note or "").strip(),
             now_text(), int(record_id)),
        )
        conn.commit()
    finally:
        conn.close()
    return {
        "success": True,
        "message": "告警已标记为%s" % HANDLE_STATE_DICT[handle_state],
        "device_code": row["device_code"],
    }


def summarize_alarms():
    """汇总告警情况：各等级与处置状态的告警数量。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT COUNT(1) AS total, "
            "SUM(CASE WHEN alarm_level='CRITICAL' THEN 1 ELSE 0 END) AS critical_amount, "
            "SUM(CASE WHEN alarm_level='WARN' THEN 1 ELSE 0 END) AS warn_amount, "
            "SUM(CASE WHEN handle_state='OPEN' THEN 1 ELSE 0 END) AS open_amount, "
            "SUM(CASE WHEN handle_state='ACK' THEN 1 ELSE 0 END) AS ack_amount, "
            "SUM(CASE WHEN handle_state='DONE' THEN 1 ELSE 0 END) AS done_amount, "
            "COALESCE(SUM(trigger_amount), 0) AS trigger_total "
            "FROM iot_alarm_record"
        ).fetchone()
    finally:
        conn.close()
    stat = dict(row) if row else {}
    total = int(stat.get("total") or 0)
    done = int(stat.get("done_amount") or 0)
    return {
        "total": total,
        "critical_amount": int(stat.get("critical_amount") or 0),
        "warn_amount": int(stat.get("warn_amount") or 0),
        "open_amount": int(stat.get("open_amount") or 0),
        "ack_amount": int(stat.get("ack_amount") or 0),
        "done_amount": done,
        "trigger_total": int(stat.get("trigger_total") or 0),
        "handle_rate": round(done / total * 100, 2) if total else 0.0,
    }


def build_alarm_charts():
    """组织告警分析图表：等级分布、指标分布、设备排行与告警趋势。"""
    conn = connect_db()
    try:
        level_rows = conn.execute(
            "SELECT alarm_level, COUNT(1) AS amount FROM iot_alarm_record "
            "GROUP BY alarm_level"
        ).fetchall()
        metric_rows = conn.execute(
            "SELECT metric_key, COUNT(1) AS amount FROM iot_alarm_record "
            "GROUP BY metric_key ORDER BY amount DESC"
        ).fetchall()
        device_rows = conn.execute(
            "SELECT device_code, device_name, COUNT(1) AS amount FROM iot_alarm_record "
            "GROUP BY device_code ORDER BY amount DESC LIMIT 6"
        ).fetchall()
        trend_rows = conn.execute(
            "SELECT substr(alarm_time, 12, 2) AS hour_tag, COUNT(1) AS amount "
            "FROM iot_alarm_record GROUP BY hour_tag ORDER BY hour_tag DESC LIMIT 12"
        ).fetchall()
    finally:
        conn.close()

    hours = dict_list(trend_rows)
    hours.reverse()
    return {
        "level_distribution": [{
            "name": ALARM_LEVEL_DICT.get(r["alarm_level"], r["alarm_level"]),
            "code": r["alarm_level"],
            "value": r["amount"],
        } for r in level_rows],
        "metric_distribution": [{
            "name": METRIC_FIELD_DICT.get(r["metric_key"], r["metric_key"]),
            "value": r["amount"],
        } for r in metric_rows],
        "device_top": [{
            "device_code": r["device_code"],
            "device_name": r["device_name"],
            "amount": r["amount"],
        } for r in device_rows],
        "hour_trend": {
            "hours": [h["hour_tag"] for h in hours],
            "amounts": [h["amount"] for h in hours],
        },
    }


def build_alarm_payload(device_code="", level_code="", state_code="", limit=50):
    """组装告警中心页面数据：汇总、图表、告警记录与规则清单。"""
    return {
        "summary": summarize_alarms(),
        "charts": build_alarm_charts(),
        "records": fetch_alarm_records(device_code, level_code, state_code, limit),
        "rules": fetch_alarm_rules(device_code),
        "device_options": [{"code": n["device_code"], "label": n["device_name"]}
                           for n in fetch_device_nodes()],
        "metric_options": [{"code": k, "label": v} for k, v in METRIC_FIELD_DICT.items()],
        "level_options": [{"code": k, "label": v} for k, v in ALARM_LEVEL_DICT.items()
                          if k != "NORMAL"],
        "state_options": [{"code": k, "label": v} for k, v in HANDLE_STATE_DICT.items()],
        "generated_at": now_text(),
    }


def build_alarm_ticker():
    """为监控大屏底部滚动告警条提供最近未处置的告警文本。"""
    rows = fetch_alarm_records(state_code="OPEN", limit=8)
    if not rows:
        return {"rows": [], "text": "当前无未处置告警，设备运行参数均在阈值范围内"}
    return {
        "rows": rows,
        "text": " | ".join("%s %s：%s" % (r["device_name"], r["metric_label"], r["alarm_text"])
                           for r in rows),
    }


def seed_demo_alarm_samples():
    """灌入演示用越限采集样本并立即扫描，保证第 5 天启动即有告警可看。

    样本覆盖警告与严重两个等级：温度略超上限触发警告，温度大幅越限、
    功率越限与电压跌落触发严重告警。
    """
    conn = connect_db()
    try:
        exist = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_alarm_record").fetchone()["c"]
    finally:
        conn.close()
    if exist > 0:
        return 0

    nodes = [n for n in fetch_device_nodes() if n.get("run_state") != "OFFLINE"]
    if not nodes:
        return 0
    # （设备序号, 指标, 采集值, 单位）按演示设备数量取模，避免越界
    demo_samples = [
        (0, "temp", 39.8, "℃"),
        (1, "temp", 48.0, "℃"),
        (2, "power", 1600.0, "W"),
        (3, "volt", 175.0, "V"),
    ]
    for index, metric_key, value, unit in demo_samples:
        node = nodes[index % len(nodes)]
        save_telemetry(node.get("device_code", ""), node.get("gateway_code", ""),
                       metric_key, value, unit)
    outcome = scan_all_alarms()
    return outcome["new_amount"]


def seed_demo_alarm_rules():
    """灌入演示用阈值规则：为每台在线设备建立温度与功率两级阈值。

    温度阈值按设备类型差异化设置，使演示扫描时既有警告也有严重告警。
    """
    conn = connect_db()
    try:
        exist = conn.execute("SELECT COUNT(1) AS c FROM iot_alarm_rule").fetchone()["c"]
        if exist > 0:
            return 0
        nodes = fetch_device_nodes()
        demo_rows = []
        for index, node in enumerate(nodes):
            code = node.get("device_code", "")
            # 每隔两台收紧温度上限，制造可触发的告警样本
            temp_high = 37.5 if index % 2 == 0 else 41.0
            demo_rows.append((code, "temp", 10.0, temp_high, 5.0, temp_high + 5.0, 1))
            demo_rows.append((code, "power", 10.0, 1200.0, 5.0, 1500.0, 1))
        conn.executemany(
            "INSERT INTO iot_alarm_rule (device_code, metric_key, warn_low, warn_high, "
            "critical_low, critical_high, enabled_flag) VALUES (?,?,?,?,?,?,?)",
            demo_rows)
        conn.commit()
        amount = len(demo_rows)
    finally:
        conn.close()
    return amount

# --------------------------------------------------------------------------
# 第 5 天数据表脚本增量：iot_schema.sql（M，已内联为字符串常量 DAY5_SCHEMA_SQL）
# --------------------------------------------------------------------------

DAY5_SCHEMA_SQL = r"""
-- ==================================================================
-- 第 5 天新增：设备参数阈值告警规则表
-- ==================================================================
-- 告警规则表：按「设备 + 指标」配置两级阈值区间（警告区间与严重区间），
-- 采集值越过警告区间即产生警告告警，越过严重区间即产生严重告警
CREATE TABLE IF NOT EXISTS iot_alarm_rule (
    rule_id       INTEGER PRIMARY KEY AUTOINCREMENT,   -- 规则主键
    device_code   TEXT    NOT NULL,                    -- 设备编号
    metric_key    TEXT    NOT NULL,                    -- 监测指标编码
    warn_low      REAL    DEFAULT 0,                   -- 警告下限
    warn_high     REAL    DEFAULT 0,                   -- 警告上限
    critical_low  REAL    DEFAULT 0,                   -- 严重下限
    critical_high REAL    DEFAULT 0,                   -- 严重上限
    enabled_flag  INTEGER DEFAULT 1,                   -- 是否启用：1 启用 / 0 停用
    updated_at    TEXT    DEFAULT (datetime('now', 'localtime')),
    UNIQUE (device_code, metric_key)
);

-- ==================================================================
-- 第 5 天新增：设备参数告警记录表
-- ==================================================================
-- 告警记录表：保存每一次阈值越限产生的告警，含等级、触发值、阈值区间、
-- 触发次数与处置状态，支撑告警确认、处置闭环与历史追溯
CREATE TABLE IF NOT EXISTS iot_alarm_record (
    record_id      INTEGER PRIMARY KEY AUTOINCREMENT,  -- 告警主键
    device_code    TEXT    NOT NULL,                   -- 设备编号
    device_name    TEXT    DEFAULT '',                 -- 设备名称快照
    metric_key     TEXT    NOT NULL,                   -- 监测指标编码
    alarm_level    TEXT    DEFAULT 'WARN',             -- 告警等级（见 ALARM_LEVEL_DICT）
    alarm_value    REAL    DEFAULT 0,                  -- 触发时的采集值
    alarm_text     TEXT    DEFAULT '',                 -- 告警描述文本
    threshold_text TEXT    DEFAULT '',                 -- 触发时套用的阈值区间说明
    handle_state   TEXT    DEFAULT 'OPEN',             -- 处置状态（见 HANDLE_STATE_DICT）
    handler_name   TEXT    DEFAULT '',                 -- 处置人
    handle_note    TEXT    DEFAULT '',                 -- 处置说明
    trigger_amount INTEGER DEFAULT 1,                  -- 累计触发次数
    alarm_time     TEXT    DEFAULT (datetime('now', 'localtime')),
    handle_time    TEXT    DEFAULT ''                  -- 处置完成时间
);

CREATE INDEX IF NOT EXISTS idx_alarmrule_device ON iot_alarm_rule (device_code);
CREATE INDEX IF NOT EXISTS idx_alarmrec_device  ON iot_alarm_record (device_code);
CREATE INDEX IF NOT EXISTS idx_alarmrec_level   ON iot_alarm_record (alarm_level);
CREATE INDEX IF NOT EXISTS idx_alarmrec_state   ON iot_alarm_record (handle_state);
CREATE INDEX IF NOT EXISTS idx_alarmrec_time    ON iot_alarm_record (alarm_time);
"""


def apply_day5_schema(conn):
    """把第 5 天新增的数据表脚本应用到已有数据库。"""
    conn.executescript(DAY5_SCHEMA_SQL)
    conn.commit()
    return True

# --------------------------------------------------------------------------
# 第 5 天主程序新增路由与接口：app.py（仅增量片段）
# --------------------------------------------------------------------------

# 开发日期：2026-10-25（第 5 天 / 共 15 天）
# 模块功能：Flask 主程序。启动时完成建表、演示拓扑初始化、遥测预置、
#           接入、在线状态、实时参数采集与阈值告警中心五类页面路由，提供
#           设备注册增删改查、心跳上报、离线扫描识别、实时参数采集存储、
#           动态曲线、阈值扫描与告警处置 JSON 接口

from iot_alarm import (
    build_alarm_payload,
    build_alarm_ticker,
    scan_all_alarms,
    scan_device_alarms,
    handle_alarm_record,
    save_alarm_rule,
    fetch_alarm_rules,
    seed_demo_alarm_rules,
    seed_demo_alarm_samples,
    ALARM_LEVEL_DICT,
    HANDLE_STATE_DICT,
)

# ==================================================================
# 以下为第 5 天新增：设备参数阈值告警接口
# ==================================================================


@app.route("/iot/alarm")
def page_alarm():
    """参数阈值告警中心页面：告警概览、四图表、告警处置与规则维护。"""
    return render_template(
        "page_alarm.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        refresh_interval=REFRESH_INTERVAL_SEC,
    )


@app.route("/api/iot/alarm/panel")
def api_alarm_panel():
    """告警中心数据接口：汇总、四图表、告警记录与阈值规则。"""
    payload = build_alarm_payload(
        device_code=(request.args.get("device_code") or "").strip(),
        level_code=(request.args.get("level_code") or "").strip(),
        state_code=(request.args.get("state_code") or "").strip(),
        limit=int(request.args.get("limit", 50)),
    )
    payload["result"] = "OK"
    return jsonify(payload)


@app.route("/api/iot/alarm/scan", methods=["POST"])
def api_alarm_scan():
    """阈值扫描接口：对全部在线设备的最新采集值套用阈值规则。"""
    body = request.get_json(silent=True) or {}
    device_code = (body.get("device_code") or "").strip()
    if device_code:
        node = fetch_device_by_code(device_code)
        if node is None:
            return jsonify({"result": "FAIL", "message": "设备未注册：%s" % device_code})
        rows = scan_device_alarms(device_code, node.get("device_name", ""),
                                  node.get("ward_name", ""))
        outcome = {
            "scan_amount": 1,
            "trigger_amount": len(rows),
            "new_amount": sum(1 for r in rows if not r["repeated"]),
            "repeat_amount": sum(1 for r in rows if r["repeated"]),
            "rows": rows,
        }
    else:
        outcome = scan_all_alarms()
    outcome["result"] = "OK"
    return jsonify(outcome)


@app.route("/api/iot/alarm/handle", methods=["POST"])
def api_alarm_handle():
    """告警处置接口：把告警置为已确认或已处置。"""
    body = request.get_json(silent=True) or {}
    outcome = handle_alarm_record(
        int(body.get("record_id") or 0),
        (body.get("handle_state") or "").strip(),
        (body.get("handler_name") or "admin").strip(),
        body.get("handle_note") or "",
    )
    outcome["result"] = "OK" if outcome.get("success") else "FAIL"
    return jsonify(outcome)


@app.route("/api/iot/alarm/rule", methods=["GET", "POST"])
def api_alarm_rule():
    """阈值规则接口：GET 查询规则，POST 新增或更新规则。"""
    if request.method == "GET":
        return jsonify({
            "result": "OK",
            "rules": fetch_alarm_rules((request.args.get("device_code") or "").strip()),
        })
    body = request.get_json(silent=True) or {}
    outcome = save_alarm_rule(
        (body.get("device_code") or "").strip(),
        (body.get("metric_key") or "").strip(),
        body.get("warn_low") or 0,
        body.get("warn_high") or 0,
        body.get("critical_low") or 0,
        body.get("critical_high") or 0,
        int(body.get("enabled_flag") or 1),
    )
    outcome["result"] = "OK" if outcome.get("success") else "FAIL"
    return jsonify(outcome)


@app.route("/api/iot/alarm/ticker")
def api_alarm_ticker():
    """滚动告警条接口：返回最近未处置告警的滚动文本。"""
    payload = build_alarm_ticker()
    payload["result"] = "OK"
    return jsonify(payload)


@app.route("/api/iot/alarm/options")
def api_alarm_options():
    """告警页下拉选项接口：告警等级与处置状态字典。"""
    return jsonify({
        "result": "OK",
        "level_options": [{"code": k, "label": v} for k, v in ALARM_LEVEL_DICT.items()
                          if k != "NORMAL"],
        "state_options": [{"code": k, "label": v} for k, v in HANDLE_STATE_DICT.items()],
    })



# 【函数内插入】def bootstrap():
#    seed_demo_alarm_rules()
#    seed_demo_alarm_samples()

# 【函数内插入】if __name__ == "__main__":
#    print("告警中心： http://127.0.0.1:%d/iot/alarm" % PORT)

# --------------------------------------------------------------------------
# 第 5 天新增/修改的前端资源清单（文件本体见工程 templates/、static/ 目录）
# --------------------------------------------------------------------------

#   [新增] static/iot_alarm.js              新增 323 行
#   [修改] templates/iot_base.html          新增 1 行
#   [新增] templates/page_alarm.html        新增 142 行
