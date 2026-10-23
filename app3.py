# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-23（第 3 天 / 共 15 天）
# 模块功能：本文件为该软件的完整源代码单文件版，按依赖顺序依次内联配置模块、建表脚本、数据仓储层、各业务模块与 Flask 主程序入口，可独立阅读与归档，用于计算机软件著作权申报
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python 医疗设备物联网实时管控平台V1.0.py （默认 http://127.0.0.1:5003/iot/monitor）
# 说明：运行需与本工程 templates/、static/ 目录同级放置

import os
import sqlite3
import time
import json
import threading
import random
from flask import Flask, render_template, jsonify, redirect, url_for, request
from datetime import datetime, timedelta

# --------------------------------------------------------------------------
# 模块 1：iot_config.py
# 模块功能：物联网平台参数配置。定义 MQTT  broker 连接参数、订阅主题规则、 在线判定超时阈值、采集指标字典与运行状态字典
# --------------------------------------------------------------------------


# ------------------------------------------------------------------
# 作者信息预留位（软著申报时可按需修改）
# ------------------------------------------------------------------
AUTHOR_NAME = "【作者姓名预留】"
AUTHOR_ORG = "【作者单位预留】"
SOFTWARE_FULL_NAME = "医疗设备物联网实时管控平台"
SOFTWARE_VERSION = "V1.0"
SOFTWARE_CODE = "MED-IOT-MONITOR"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "iot_monitor.db")
SCHEMA_FILE = os.path.join(BASE_DIR, "iot_schema.sql")

HOST = "0.0.0.0"
PORT = 5003
DEBUG = True

# ------------------------------------------------------------------
# MQTT 连接参数（第 10 天网关管理模块将支持界面维护）
# ------------------------------------------------------------------
MQTT_BROKER_HOST = "127.0.0.1"
MQTT_BROKER_PORT = 1883
MQTT_KEEPALIVE = 60
MQTT_USERNAME = ""
MQTT_PASSWORD = ""
MQTT_CLIENT_ID = "med-iot-platform-v1"

# 订阅主题规则：{gateway} 为网关编号，{device} 为设备编号
MQTT_TOPIC_TELEMETRY = "med/iot/{gateway}/{device}/telemetry"
MQTT_TOPIC_HEARTBEAT = "med/iot/{gateway}/{device}/heartbeat"
MQTT_TOPIC_WILDCARD = "med/iot/#"

# ------------------------------------------------------------------
# 在线判定：超过该秒数未收到心跳即判定离线（第 3 天离线识别使用）
# ------------------------------------------------------------------
OFFLINE_TIMEOUT_SEC = 90

# 采集指标字典：网关上报报文中的字段名与中文名映射
METRIC_FIELD_DICT = {
    "temp": "运行温度",
    "volt": "工作电压",
    "curr": "工作电流",
    "power": "实时功率",
    "press": "管路压力",
    "humid": "环境湿度",
}

# 设备运行状态字典
DEVICE_RUN_STATE = {
    "ONLINE": "在线",
    "OFFLINE": "离线",
    "FAULT": "故障",
    "SLEEP": "待机",
}

# 遥测数据保留条数上限，防止数据库无限膨胀
TELEMETRY_KEEP_LIMIT = 5000


def ensure_data_dir():
    """确保数据目录存在。"""
    if not os.path.isdir(DATA_DIR):
        os.makedirs(DATA_DIR)
    return DATA_DIR


# --------------------------------------------------------------------------
# 内联建表脚本：iot_schema.sql（原 .sql 文件，此处转为字符串常量 IOT_SCHEMA_SQL）
# --------------------------------------------------------------------------
IOT_SCHEMA_SQL = r"""
-- ==================================================================
-- 软件全称：医疗设备物联网实时管控平台 V1.0
-- 版本：V1.0
-- 开发日期：2026-10-21（第 1 天 / 共 15 天）
-- 模块功能：物联网平台建表脚本，定义物联网关表、设备节点表、遥测数据表
--           三张基础表，为第 2 天设备接入、第 3 天在线心跳、第 4 天
--           实时参数采集提供数据底座
-- 依赖包：SQLite3（Python 内置）
-- ==================================================================

PRAGMA foreign_keys = ON;

-- 物联网关表：登记接入平台的边缘网关设备
CREATE TABLE IF NOT EXISTS iot_gateway_node (
    gateway_id     INTEGER PRIMARY KEY AUTOINCREMENT, -- 网关主键
    gateway_code   TEXT    NOT NULL UNIQUE,           -- 网关编号（MQTT 主题中的 gateway 段）
    gateway_name   TEXT    DEFAULT '',                -- 网关名称
    ward_name      TEXT    DEFAULT '',                -- 部署病区 / 位置
    ip_address     TEXT    DEFAULT '',                -- 网关 IP 地址
    protocol_type  TEXT    DEFAULT 'MQTT',            -- 接入协议
    bind_amount    INTEGER DEFAULT 0,                 -- 已绑定设备数量
    register_time  TEXT    DEFAULT (datetime('now', 'localtime')),
    last_seen_time TEXT    DEFAULT ''                 -- 最近一次通信时间
);

-- 设备节点表：登记挂接到网关下的每一台医疗设备
CREATE TABLE IF NOT EXISTS iot_device_node (
    node_id        INTEGER PRIMARY KEY AUTOINCREMENT, -- 节点主键
    device_code    TEXT    NOT NULL UNIQUE,           -- 设备编号（MQTT 主题中的 device 段）
    device_name    TEXT    NOT NULL,                  -- 设备名称
    gateway_code   TEXT    NOT NULL,                  -- 所属网关编号
    device_type    TEXT    DEFAULT '',                -- 设备类型
    ward_name      TEXT    DEFAULT '',                -- 所在病区
    run_state      TEXT    DEFAULT 'OFFLINE',         -- 运行状态 ONLINE/OFFLINE/FAULT/SLEEP
    heartbeat_time TEXT    DEFAULT '',                -- 最近心跳时间
    register_time  TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- 遥测数据表：存放网关上报的实时参数报文
CREATE TABLE IF NOT EXISTS iot_telemetry_log (
    telemetry_id  INTEGER PRIMARY KEY AUTOINCREMENT,  -- 遥测记录主键
    device_code   TEXT    NOT NULL,                   -- 设备编号
    gateway_code  TEXT    DEFAULT '',                 -- 网关编号
    metric_key    TEXT    NOT NULL,                   -- 指标字段名（temp/volt/curr/power 等）
    metric_value  REAL    DEFAULT 0,                  -- 指标数值
    metric_unit   TEXT    DEFAULT '',                 -- 指标单位
    collect_time  TEXT    DEFAULT (datetime('now', 'localtime'))  -- 采集时间
);

CREATE INDEX IF NOT EXISTS idx_tele_device ON iot_telemetry_log (device_code);
CREATE INDEX IF NOT EXISTS idx_tele_time   ON iot_telemetry_log (collect_time);
CREATE INDEX IF NOT EXISTS idx_node_state  ON iot_device_node (run_state);

-- ==================================================================
-- 第 3 天新增：设备心跳日志表
-- ==================================================================
-- 设备心跳日志表：记录每一次心跳报文的到达时间、信号强度与电量，
-- 供在线状态看板的在线率统计、信号质量评估与心跳趋势分析使用
CREATE TABLE IF NOT EXISTS iot_heartbeat_log (
    beat_id       INTEGER PRIMARY KEY AUTOINCREMENT, -- 心跳记录主键
    device_code   TEXT    NOT NULL,                 -- 设备编号
    gateway_code  TEXT    DEFAULT '',               -- 所属网关编号
    rssi_value    INTEGER DEFAULT 0,                -- 信号强度（dBm，负值越大越强）
    battery_value REAL    DEFAULT 0,                -- 设备电量百分比
    uptime_sec    INTEGER DEFAULT 0,                -- 连续运行时长（秒）
    source_ip     TEXT    DEFAULT '',               -- 上报来源地址
    beat_time     TEXT    DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_beat_device ON iot_heartbeat_log (device_code);
CREATE INDEX IF NOT EXISTS idx_beat_time   ON iot_heartbeat_log (beat_time);
"""


# --------------------------------------------------------------------------
# 模块 3：iot_store.py
# 模块功能：物联网数据仓储层。负责建表、网关/节点初始化、遥测报文落库、 在线数量统计、实时曲线取数、历史遥测清理与监控大屏数据组装
# --------------------------------------------------------------------------



def connect_db():
    """建立数据库连接。"""
    ensure_data_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def setup_storage():
    """执行建表脚本完成数据库初始化。"""
    ensure_data_dir()
    # 单文件版：建表脚本已内联为 IOT_SCHEMA_SQL 常量，无需外部 .sql 文件
    script = IOT_SCHEMA_SQL
    conn = connect_db()
    try:
        conn.executescript(script)
        conn.commit()
    finally:
        conn.close()
    return True


def dict_list(rows):
    """结果集转字典列表。"""
    return [dict(row) for row in rows]


def now_text():
    """返回统一的本地时间字符串。"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def save_telemetry(device_code, gateway_code, metric_key, metric_value, metric_unit=""):
    """写入一条遥测数据记录。"""
    conn = connect_db()
    try:
        conn.execute(
            "INSERT INTO iot_telemetry_log "
            "(device_code, gateway_code, metric_key, metric_value, metric_unit, collect_time) "
            "VALUES (?,?,?,?,?,?)",
            (device_code, gateway_code, metric_key, float(metric_value), metric_unit, now_text()),
        )
        conn.commit()
    finally:
        conn.close()
    return True


def refresh_heartbeat(device_code, run_state="ONLINE"):
    """更新设备心跳时间与运行状态。"""
    conn = connect_db()
    try:
        conn.execute(
            "UPDATE iot_device_node SET heartbeat_time=?, run_state=? WHERE device_code=?",
            (now_text(), run_state, device_code),
        )
        conn.commit()
    finally:
        conn.close()
    return True


def count_device_by_state():
    """按运行状态统计设备数量。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT run_state, COUNT(1) AS amount FROM iot_device_node GROUP BY run_state"
        ).fetchall()
    finally:
        conn.close()
    result = []
    for item in dict_list(rows):
        item["state_label"] = DEVICE_RUN_STATE.get(item["run_state"], item["run_state"])
        result.append(item)
    return result


def fetch_device_nodes():
    """读取全部设备节点及其最近心跳时间。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT node_id, device_code, device_name, gateway_code, device_type, ward_name, "
            "run_state, heartbeat_time FROM iot_device_node ORDER BY node_id"
        ).fetchall()
    finally:
        conn.close()
    return dict_list(rows)


def fetch_telemetry_curve(device_code, metric_key, limit=30):
    """读取指定设备指定指标的最近若干条数据，用于实时动态曲线。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT collect_time, metric_value FROM iot_telemetry_log "
            "WHERE device_code=? AND metric_key=? ORDER BY telemetry_id DESC LIMIT ?",
            (device_code, metric_key, limit),
        ).fetchall()
    finally:
        conn.close()
    data = dict_list(rows)
    data.reverse()
    return data


def clear_expired_telemetry():
    """清理超出保留上限的历史遥测数据，避免数据库膨胀。"""
    conn = connect_db()
    try:
        conn.execute(
            "DELETE FROM iot_telemetry_log WHERE telemetry_id NOT IN "
            "(SELECT telemetry_id FROM iot_telemetry_log ORDER BY telemetry_id DESC LIMIT ?)",
            (TELEMETRY_KEEP_LIMIT,),
        )
        conn.commit()
    finally:
        conn.close()


def build_monitor_payload():
    """组装实时监控大屏数据。

    第 1 天输出：设备总数、在线数量、离线数量、状态分布、设备清单、
    首台设备的温度曲线。第 3~8 天将补充告警、能耗统计与趋势接口。
    """
    state_rows = count_device_by_state()
    state_map = {item["run_state"]: item["amount"] for item in state_rows}
    nodes = fetch_device_nodes()
    curve = []
    focus_device = ""
    if nodes:
        focus_device = nodes[0]["device_code"]
        curve = fetch_telemetry_curve(focus_device, "temp", 30)
    gateway_amount = count_gateway()
    return {
        "device_total": len(nodes),
        "online_amount": state_map.get("ONLINE", 0),
        "offline_amount": state_map.get("OFFLINE", 0),
        "fault_amount": state_map.get("FAULT", 0),
        "gateway_amount": gateway_amount,
        "state_distribution": state_rows,
        "offline_timeout": OFFLINE_TIMEOUT_SEC,
        "metric_dict": METRIC_FIELD_DICT,
        "device_nodes": nodes,
        "focus_device": focus_device,
        "temp_curve": curve,
        "update_time": now_text(),
    }


def count_gateway():
    """统计已接入网关数量。"""
    conn = connect_db()
    try:
        row = conn.execute("SELECT COUNT(1) AS cnt FROM iot_gateway_node").fetchone()
        return row["cnt"] if row else 0
    finally:
        conn.close()


def seed_demo_topology():
    """初始化演示用网关与设备节点拓扑。"""
    conn = connect_db()
    try:
        exist = conn.execute("SELECT COUNT(1) AS cnt FROM iot_gateway_node").fetchone()
        if exist["cnt"] == 0:
            gateways = [
                ("GW-ICU-01", "重症监护网关", "ICU 病区", "192.168.10.21"),
                ("GW-OPR-02", "手术中心网关", "手术中心", "192.168.10.35"),
                ("GW-LAB-03", "检验中心网关", "检验中心", "192.168.10.52"),
            ]
            conn.executemany(
                "INSERT INTO iot_gateway_node (gateway_code, gateway_name, ward_name, ip_address, last_seen_time) "
                "VALUES (?,?,?,?,?)",
                [(g[0], g[1], g[2], g[3], now_text()) for g in gateways],
            )
        exist_dev = conn.execute("SELECT COUNT(1) AS cnt FROM iot_device_node").fetchone()
        if exist_dev["cnt"] == 0:
            devices = [
                ("IOT-DEV-001", "呼吸机 A1", "GW-ICU-01", "治疗救护类", "ICU 病区", "ONLINE"),
                ("IOT-DEV-002", "监护仪 B2", "GW-ICU-01", "监护类", "ICU 病区", "ONLINE"),
                ("IOT-DEV-003", "麻醉机 C3", "GW-OPR-02", "治疗救护类", "手术中心", "ONLINE"),
                ("IOT-DEV-004", "高频电刀 D4", "GW-OPR-02", "治疗救护类", "手术中心", "OFFLINE"),
                ("IOT-DEV-005", "生化分析仪 E5", "GW-LAB-03", "检验分析类", "检验中心", "ONLINE"),
                ("IOT-DEV-006", "离心机 F6", "GW-LAB-03", "检验分析类", "检验中心", "FAULT"),
            ]
            conn.executemany(
                "INSERT INTO iot_device_node "
                "(device_code, device_name, gateway_code, device_type, ward_name, run_state, heartbeat_time) "
                "VALUES (?,?,?,?,?,?,?)",
                [(d[0], d[1], d[2], d[3], d[4], d[5], now_text()) for d in devices],
            )
        conn.commit()
    finally:
        conn.close()


def seed_demo_telemetry():
    """为演示设备预置一段遥测数据，保证首日启动曲线不为空。"""
    conn = connect_db()
    try:
        row = conn.execute("SELECT COUNT(1) AS cnt FROM iot_telemetry_log").fetchone()
        if row["cnt"] > 0:
            return 0
    finally:
        conn.close()
    base_time = time.time() - 600
    rows = []
    for idx in range(30):
        stamp = datetime.fromtimestamp(base_time + idx * 20).strftime("%Y-%m-%d %H:%M:%S")
        rows.append(("IOT-DEV-001", "GW-ICU-01", "temp", round(36.5 + (idx % 7) * 0.35, 2), "℃", stamp))
        rows.append(("IOT-DEV-001", "GW-ICU-01", "power", round(120 + (idx % 9) * 6.5, 2), "W", stamp))
    conn = connect_db()
    try:
        conn.executemany(
            "INSERT INTO iot_telemetry_log "
            "(device_code, gateway_code, metric_key, metric_value, metric_unit, collect_time) VALUES (?,?,?,?,?,?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()
    return len(rows)


# ==================================================================
# 以下为第 2 天新增：设备接入注册管理底层支撑
# ==================================================================

def fetch_gateway_rows():
    """读取全部网关记录，供设备绑定下拉列表使用。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT gateway_code, gateway_name, ward_name, ip_address, bind_amount "
            "FROM iot_gateway_node ORDER BY gateway_id"
        ).fetchall()
    finally:
        conn.close()
    return dict_list(rows)


def fetch_device_by_code(device_code):
    """按设备编号读取节点记录。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_device_node WHERE device_code=?", (device_code,)
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def insert_device_node(device_code, device_name, gateway_code, device_type, ward_name, run_state):
    """写入一条新的设备节点记录，返回新主键。"""
    conn = connect_db()
    try:
        cur = conn.execute(
            "INSERT INTO iot_device_node "
            "(device_code, device_name, gateway_code, device_type, ward_name, run_state, heartbeat_time) "
            "VALUES (?,?,?,?,?,?,?)",
            (device_code, device_name, gateway_code, device_type, ward_name, run_state, now_text()),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_device_fields(device_code, field_map):
    """按设备编号更新节点字段（字段名白名单由调用方保证）。"""
    if not field_map:
        return False
    setter = ",".join(["%s=?" % key for key in field_map.keys()])
    params = list(field_map.values()) + [device_code]
    conn = connect_db()
    try:
        conn.execute("UPDATE iot_device_node SET %s WHERE device_code=?" % setter, params)
        conn.commit()
    finally:
        conn.close()
    return True


def delete_device_node(device_code):
    """注销设备节点，同时清理其历史遥测数据。"""
    conn = connect_db()
    try:
        conn.execute("DELETE FROM iot_device_node WHERE device_code=?", (device_code,))
        conn.execute("DELETE FROM iot_telemetry_log WHERE device_code=?", (device_code,))
        conn.commit()
    finally:
        conn.close()
    return True


def refresh_gateway_bind_amount(gateway_code):
    """重新统计某网关下绑定的设备数量并回写网关表。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT COUNT(1) AS cnt FROM iot_device_node WHERE gateway_code=?", (gateway_code,)
        ).fetchone()
        conn.execute(
            "UPDATE iot_gateway_node SET bind_amount=? WHERE gateway_code=?",
            (row["cnt"], gateway_code),
        )
        conn.commit()
        return row["cnt"]
    finally:
        conn.close()


def query_device_nodes_filtered(keyword="", gateway_code="", run_state=""):
    """按关键字、网关、运行状态过滤设备节点列表。"""
    where_parts = []
    params = []
    if keyword:
        where_parts.append("(device_code LIKE ? OR device_name LIKE ? OR ward_name LIKE ?)")
        like = "%%%s%%" % keyword
        params.extend([like, like, like])
    if gateway_code:
        where_parts.append("gateway_code=?")
        params.append(gateway_code)
    if run_state:
        where_parts.append("run_state=?")
        params.append(run_state)
    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT node_id, device_code, device_name, gateway_code, device_type, ward_name, "
            "run_state, heartbeat_time FROM iot_device_node" + where_sql +
            " ORDER BY node_id DESC",
            params,
        ).fetchall()
    finally:
        conn.close()
    return dict_list(rows)


def count_telemetry_by_device(device_code):
    """统计某台设备已采集的遥测数据条数。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT COUNT(1) AS cnt FROM iot_telemetry_log WHERE device_code=?", (device_code,)
        ).fetchone()
    finally:
        conn.close()
    return row["cnt"] if row else 0


# --------------------------------------------------------------------------
# 模块 4：iot_device.py
# 模块功能：物联网设备接入与注册管理业务层。实现设备编号自动生成、注册信息 合法性校验、网关绑定关系维护、运行状态切换、设备注销清理， 并在注册成功后按标准主题格式返回该设备的 MQTT 上报主题
# --------------------------------------------------------------------------



def generate_device_code():
    """自动生成设备编号：IOT-DEV-三位流水号（在现有最大编号上递增）。"""
    nodes = fetch_device_nodes()
    max_seq = 0
    for node in nodes:
        tail = node["device_code"].split("-")[-1]
        if tail.isdigit():
            max_seq = max(max_seq, int(tail))
    return "IOT-DEV-%03d" % (max_seq + 1)


def validate_device_payload(payload):
    """校验设备注册表单。

    校验项：设备名称必填、运行状态编码合法、网关编号必须已存在。
    返回 (是否通过, 错误提示, 规范化字段字典)。
    """
    errors = []
    name = (payload.get("device_name") or "").strip()
    if not name:
        errors.append("设备名称不能为空")

    run_state = (payload.get("run_state") or "OFFLINE").strip()
    if run_state not in DEVICE_RUN_STATE:
        errors.append("运行状态编码不合法：%s" % run_state)

    gateway_code = (payload.get("gateway_code") or "").strip()
    gateway_codes = [g["gateway_code"] for g in fetch_gateway_rows()]
    if gateway_code and gateway_code not in gateway_codes:
        errors.append("网关编号不存在：%s" % gateway_code)
    if not gateway_code:
        errors.append("必须选择所属网关")

    fields = {
        "device_name": name,
        "gateway_code": gateway_code,
        "device_type": (payload.get("device_type") or "").strip(),
        "ward_name": (payload.get("ward_name") or "").strip(),
        "run_state": run_state,
    }
    return (len(errors) == 0), ("；".join(errors) if errors else ""), fields


def build_device_topics(device_code, gateway_code):
    """按平台主题规则生成该设备的遥测上报主题与心跳上报主题。"""
    return {
        "telemetry_topic": MQTT_TOPIC_TELEMETRY.format(gateway=gateway_code, device=device_code),
        "heartbeat_topic": MQTT_TOPIC_HEARTBEAT.format(gateway=gateway_code, device=device_code),
    }


def register_device_node(payload, operator="admin"):
    """注册一台新的物联网设备节点。

    成功后返回设备编号与对应的 MQTT 上报主题，便于现场网关配置。
    """
    ok, message, fields = validate_device_payload(payload)
    if not ok:
        return {"success": False, "message": message}

    device_code = (payload.get("device_code") or "").strip()
    if not device_code:
        device_code = generate_device_code()
    elif fetch_device_by_code(device_code):
        return {"success": False, "message": "设备编号已存在：%s" % device_code}

    insert_device_node(
        device_code, fields["device_name"], fields["gateway_code"],
        fields["device_type"], fields["ward_name"], fields["run_state"],
    )
    refresh_gateway_bind_amount(fields["gateway_code"])
    topics = build_device_topics(device_code, fields["gateway_code"])
    return {
        "success": True,
        "device_code": device_code,
        "topics": topics,
        "operator": operator,
        "message": "设备注册成功，请按以下主题配置网关上报",
    }


def modify_device_node(device_code, payload, operator="admin"):
    """修改设备节点信息；若更换网关，同步刷新新旧网关的绑定数量。"""
    origin = fetch_device_by_code(device_code)
    if not origin:
        return {"success": False, "message": "设备不存在：%s" % device_code}

    ok, message, fields = validate_device_payload(payload)
    if not ok:
        return {"success": False, "message": message}

    old_gateway = origin["gateway_code"]
    update_device_fields(device_code, fields)
    refresh_gateway_bind_amount(fields["gateway_code"])
    if old_gateway != fields["gateway_code"]:
        refresh_gateway_bind_amount(old_gateway)
    return {"success": True, "message": "设备信息修改成功", "operator": operator}


def unregister_device_node(device_code, operator="admin"):
    """注销设备节点，同时清理遥测数据并刷新网关绑定数量。"""
    origin = fetch_device_by_code(device_code)
    if not origin:
        return {"success": False, "message": "设备不存在：%s" % device_code}
    delete_device_node(device_code)
    refresh_gateway_bind_amount(origin["gateway_code"])
    return {"success": True, "message": "设备已注销", "operator": operator}


def switch_device_state(device_code, run_state, operator="admin"):
    """切换设备运行状态（在线 / 离线 / 故障 / 待机）。"""
    if run_state not in DEVICE_RUN_STATE:
        return {"success": False, "message": "运行状态编码不合法：%s" % run_state}
    if not fetch_device_by_code(device_code):
        return {"success": False, "message": "设备不存在：%s" % device_code}
    update_device_fields(device_code, {"run_state": run_state})
    return {"success": True, "message": "状态已切换为「%s」" % DEVICE_RUN_STATE[run_state]}


def list_device_registry(keyword="", gateway_code="", run_state=""):
    """设备接入台账查询，附带每台设备的遥测条数与离线判定阈值。"""
    rows = query_device_nodes_filtered(keyword, gateway_code, run_state)
    for item in rows:
        item["state_label"] = DEVICE_RUN_STATE.get(item["run_state"], item["run_state"])
        item["telemetry_amount"] = count_telemetry_by_device(item["device_code"])
    return {
        "rows": rows,
        "total": len(rows),
        "offline_timeout": OFFLINE_TIMEOUT_SEC,
    }


def build_registry_options():
    """返回设备接入页需要的下拉选项：网关清单与运行状态字典。"""
    return {
        "gateway_options": fetch_gateway_rows(),
        "state_options": [{"code": k, "label": v} for k, v in DEVICE_RUN_STATE.items()],
    }


def summarize_registry():
    """汇总接入概况：设备总数、网关总数、在线数、遥测总数。"""
    nodes = fetch_device_nodes()
    gateways = fetch_gateway_rows()
    online = len([n for n in nodes if n["run_state"] == "ONLINE"])
    telemetry = sum(count_telemetry_by_device(n["device_code"]) for n in nodes)
    return {
        "device_total": len(nodes),
        "gateway_total": len(gateways),
        "online_amount": online,
        "telemetry_total": telemetry,
    }


# --------------------------------------------------------------------------
# 模块 5：iot_heartbeat.py
# 模块功能：设备在线心跳检测与离线识别业务层。实现心跳报文落库、设备在线 状态自动恢复、按超时阈值批量识别离线设备、在线率与信号质量统计、 按网关分组的在线情况汇总、近 24 小时心跳趋势统计，并输出在线 状态看板所需的全部图表数据
# --------------------------------------------------------------------------


# 心跳超时后允许自动恢复为在线的运行状态（故障设备需人工复位）
AUTO_RECOVER_STATES = ["OFFLINE", "SLEEP"]


def record_heartbeat(device_code, rssi_value=0, battery_value=0, uptime_sec=0, source_ip=""):
    """记录一次设备心跳。

    处理流程：校验设备是否存在 → 写入心跳日志 → 刷新设备最近心跳时间；
    若设备此前处于离线或休眠状态，则自动恢复为在线。
    """
    node = fetch_device_by_code(device_code)
    if not node:
        return {"success": False, "message": "设备编号不存在：%s" % device_code}

    conn = connect_db()
    try:
        conn.execute(
            "INSERT INTO iot_heartbeat_log "
            "(device_code, gateway_code, rssi_value, battery_value, uptime_sec, source_ip, beat_time) "
            "VALUES (?,?,?,?,?,?,?)",
            (device_code, node["gateway_code"], int(rssi_value or 0),
             float(battery_value or 0), int(uptime_sec or 0), source_ip, now_text()),
        )
        recovered = 0
        if node["run_state"] in AUTO_RECOVER_STATES:
            conn.execute(
                "UPDATE iot_device_node SET run_state='ONLINE', heartbeat_time=? WHERE device_code=?",
                (now_text(), device_code),
            )
            recovered = 1
        else:
            conn.execute(
                "UPDATE iot_device_node SET heartbeat_time=? WHERE device_code=?",
                (now_text(), device_code),
            )
        conn.commit()
    finally:
        conn.close()
    return {
        "success": True,
        "device_code": device_code,
        "recovered": recovered,
        "beat_time": now_text(),
        "message": "心跳已记录" + ("，设备恢复在线" if recovered else ""),
    }


def fetch_timeout_devices(timeout_sec=OFFLINE_TIMEOUT_SEC):
    """查询心跳已超时但仍标记为在线/休眠的设备。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT device_code, device_name, gateway_code, run_state, heartbeat_time "
            "FROM iot_device_node "
            "WHERE run_state IN ('ONLINE','SLEEP') AND "
            "(heartbeat_time IS NULL OR heartbeat_time='' OR "
            " heartbeat_time < datetime('now','localtime','-%d seconds')) "
            "ORDER BY gateway_code, device_code" % int(timeout_sec)
        ).fetchall()
    finally:
        conn.close()
    return dict_list(rows)


def scan_offline_devices(timeout_sec=OFFLINE_TIMEOUT_SEC, operator="system"):
    """批量识别离线设备：把心跳超时的在线/休眠设备标记为离线。

    返回本次扫描的离线设备清单与数量。
    """
    rows = fetch_timeout_devices(timeout_sec)
    conn = connect_db()
    try:
        for item in rows:
            conn.execute(
                "UPDATE iot_device_node SET run_state='OFFLINE' WHERE device_code=?",
                (item["device_code"],),
            )
        conn.commit()
    finally:
        conn.close()
    result = []
    for item in rows:
        result.append({
            "device_code": item["device_code"],
            "device_name": item["device_name"],
            "gateway_code": item["gateway_code"],
            "last_beat": item["heartbeat_time"] or "",
            "offline_seconds": int(timeout_sec),
        })
    return {
        "scan_time": now_text(),
        "timeout_sec": int(timeout_sec),
        "offline_amount": len(result),
        "rows": result,
    }


def fetch_heartbeat_history(device_code="", limit=20):
    """读取心跳日志：指定设备则取该设备最近 N 条，否则取全库最近 N 条。"""
    conn = connect_db()
    try:
        if device_code:
            rows = conn.execute(
                "SELECT device_code, gateway_code, rssi_value, battery_value, uptime_sec, "
                "source_ip, beat_time FROM iot_heartbeat_log WHERE device_code=? "
                "ORDER BY beat_id DESC LIMIT ?",
                (device_code, int(limit)),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT device_code, gateway_code, rssi_value, battery_value, uptime_sec, "
                "source_ip, beat_time FROM iot_heartbeat_log "
                "ORDER BY beat_id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
    finally:
        conn.close()
    return dict_list(rows)


def fetch_beat_trend(hours=24):
    """统计近 N 小时的心跳次数趋势（按小时分组），用于在线趋势折线图。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT substr(beat_time,1,13) AS hour_text, COUNT(1) AS beat_count "
            "FROM iot_heartbeat_log "
            "WHERE beat_time >= datetime('now','localtime','-%d hours') "
            "GROUP BY hour_text ORDER BY hour_text" % int(hours)
        ).fetchall()
        stat = {r["hour_text"]: r["beat_count"] for r in dict_list(rows)}
    finally:
        conn.close()
    # 补齐缺失小时，保证折线图时间轴连续
    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    series = []
    for offset in range(int(hours) - 1, -1, -1):
        point = now - timedelta(hours=offset)
        key = point.strftime("%Y-%m-%d %H")
        series.append({
            "hour_text": point.strftime("%m-%d %H:00"),
            "beat_count": stat.get(key, 0),
        })
    return series


def fetch_gateway_online_stat():
    """按网关统计设备总数与在线数量，用于网关在线情况堆叠柱状图。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT gateway_code, COUNT(1) AS total_amount, "
            "SUM(CASE WHEN run_state='ONLINE' THEN 1 ELSE 0 END) AS online_amount, "
            "SUM(CASE WHEN run_state='FAULT' THEN 1 ELSE 0 END) AS fault_amount "
            "FROM iot_device_node GROUP BY gateway_code ORDER BY gateway_code"
        ).fetchall()
    finally:
        conn.close()
    result = []
    for item in dict_list(rows):
        item["offline_amount"] = item["total_amount"] - item["online_amount"] - item["fault_amount"]
        result.append(item)
    return result


def build_online_statistics(timeout_sec=OFFLINE_TIMEOUT_SEC):
    """组装在线状态看板所需的统计与图表数据。"""
    state_stat = count_device_by_state()
    total = sum(item["amount"] for item in state_stat) if state_stat else 0
    state_map = {item["run_state"]: item["amount"] for item in state_stat}
    online_amount = state_map.get("ONLINE", 0)

    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT COUNT(1) AS beat_total, ROUND(AVG(rssi_value),1) AS avg_rssi "
            "FROM iot_heartbeat_log WHERE beat_time >= datetime('now','localtime','-1 hours')"
        ).fetchone()
        avg_rssi = row["avg_rssi"] if row and row["avg_rssi"] is not None else 0.0
        beat_total = row["beat_total"] if row else 0
    finally:
        conn.close()

    offline_rows = []
    for item in fetch_device_nodes():
        if item["run_state"] == "OFFLINE":
            offline_rows.append({
                "device_code": item["device_code"],
                "device_name": item["device_name"],
                "gateway_code": item["gateway_code"],
                "ward_name": item["ward_name"],
                "last_beat": item["heartbeat_time"] or "",
            })

    return {
        "scan_time": now_text(),
        "timeout_sec": int(timeout_sec),
        "device_total": total,
        "online_amount": online_amount,
        "offline_amount": state_map.get("OFFLINE", 0),
        "fault_amount": state_map.get("FAULT", 0),
        "sleep_amount": state_map.get("SLEEP", 0),
        "online_rate": round(online_amount * 100.0 / total, 1) if total else 0.0,
        "beat_total_1h": beat_total,
        "avg_rssi": avg_rssi,
        "state_distribution": state_stat,
        "gateway_online": fetch_gateway_online_stat(),
        "beat_trend": fetch_beat_trend(24),
        "offline_list": offline_rows,
        "recent_beats": fetch_heartbeat_history("", 12),
    }


# --------------------------------------------------------------------------
# 模块 6：iot_mqtt.py
# 模块功能：MQTT 物联网数据接入框架。实现订阅客户端封装、主题解析、 报文落地回调；当运行环境未安装 paho-mqtt 时自动降级为内置 模拟采集器，保证平台在无 broker 环境下依然完整可运行
# --------------------------------------------------------------------------



try:
    import paho.mqtt.client as mqtt
    MQTT_AVAILABLE = True
except ImportError:
    mqtt = None
    MQTT_AVAILABLE = False


def now_text():
    """返回本地时间字符串。"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_topic(topic):
    """解析主题字符串，提取网关编号与设备编号。

    主题格式：med/iot/{gateway}/{device}/telemetry
    返回：(gateway_code, device_code, message_type)
    """
    parts = topic.split("/")
    gateway_code = parts[2] if len(parts) > 2 else ""
    device_code = parts[3] if len(parts) > 3 else ""
    msg_type = parts[4] if len(parts) > 4 else "unknown"
    return gateway_code, device_code, msg_type


def handle_telemetry_message(gateway_code, device_code, payload_text):
    """处理遥测报文：解析 JSON 后逐字段落库，并刷新设备心跳。

    payload 示例：{"temp": 36.8, "volt": 220.5, "curr": 1.2, "power": 130}
    """
    try:
        payload = json.loads(payload_text)
    except (ValueError, TypeError):
        return False
    unit_map = {"temp": "℃", "volt": "V", "curr": "A", "power": "W", "press": "kPa", "humid": "%"}
    for key, value in payload.items():
        save_telemetry(device_code, gateway_code, key, value, unit_map.get(key, ""))
    refresh_heartbeat(device_code, "ONLINE")
    return True


class MqttIngestClient:
    """MQTT 订阅客户端封装类。

    第 1 天实现连接、订阅与消息分发基础框架；
    第 9 天将在此类中扩展远程指令下发的发布方法。
    """

    def __init__(self):
        self.client = None
        self.connected = False
        self.received_count = 0

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """连接成功回调：订阅通配主题。"""
        if rc == 0:
            self.connected = True
            client.subscribe(MQTT_TOPIC_WILDCARD, qos=1)
            print("[MQTT] 已连接 broker，订阅主题：%s" % MQTT_TOPIC_WILDCARD)
        else:
            self.connected = False
            print("[MQTT] 连接失败，返回码：%s" % rc)

    def _on_message(self, client, userdata, msg):
        """消息到达回调：按消息类型分发处理。"""
        self.received_count += 1
        gateway, device, msg_type = parse_topic(msg.topic)
        if msg_type == "telemetry":
            handle_telemetry_message(gateway, device, msg.payload.decode("utf-8"))
        elif msg_type == "heartbeat":
            # 第 3 天改为落库到心跳日志，并自动恢复离线设备为在线
            body = {}
            try:
                body = json.loads(msg.payload.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                body = {}
            record_heartbeat(
                device,
                rssi_value=body.get("rssi", 0),
                battery_value=body.get("battery", 0),
                uptime_sec=body.get("uptime", 0),
                source_ip=body.get("ip", ""),
            )

    def start(self):
        """启动订阅客户端（阻塞式 loop_start 改为后台线程，避免阻塞 Flask）。"""
        if not MQTT_AVAILABLE:
            print("[MQTT] 未检测到 paho-mqtt，已自动切换为内置模拟采集器")
            return False
        try:
            try:
                self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
            except AttributeError:
                self.client = mqtt.Client(client_id=MQTT_CLIENT_ID)
            self.client.on_connect = self._on_connect
            self.client.on_message = self._on_message
            if MQTT_USERNAME:
                self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
            self.client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_KEEPALIVE)
            self.client.loop_start()
            return True
        except Exception as exc:
            print("[MQTT] 启动异常，切换模拟采集器：%s" % exc)
            return False

    def stop(self):
        """停止订阅客户端。"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()


class LocalSimulator:
    """内置模拟采集器。

    在无 MQTT broker 环境下，按 5 秒周期为一台演示设备生成遥测数据，
    用于验证大屏实时曲线与在线状态展示效果。
    """

    def __init__(self, interval=5):
        self.interval = interval
        self.running = False
        self.thread = None

    def _loop(self):
        while self.running:
            nodes = fetch_device_nodes()
            for node in nodes:
                if node["run_state"] == "OFFLINE":
                    continue
                save_telemetry(node["device_code"], node["gateway_code"], "temp",
                               round(random.uniform(36.2, 38.4), 2), "℃")
                save_telemetry(node["device_code"], node["gateway_code"], "power",
                               round(random.uniform(90, 260), 2), "W")
                # 第 3 天：模拟采集同时上报心跳，供在线状态看板统计
                record_heartbeat(
                    node["device_code"],
                    rssi_value=random.randint(-85, -45),
                    battery_value=round(random.uniform(35, 100), 1),
                    uptime_sec=random.randint(600, 86400),
                    source_ip="127.0.0.1",
                )
            if random.randint(1, 20) == 1:
                clear_expired_telemetry()
            time.sleep(self.interval)

    def start(self):
        """以后台线程方式启动模拟采集。"""
        if self.running:
            return False
        self.running = True
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        print("[SIM] 内置模拟采集器已启动，采集周期 %d 秒" % self.interval)
        return True

    def stop(self):
        """停止模拟采集。"""
        self.running = False


def launch_ingest_service():
    """统一入口：优先启动真实 MQTT 订阅，失败则启动模拟采集器。

    返回：(worker 对象, 运行模式字符串)
    """
    client = MqttIngestClient()
    if client.start():
        return client, "MQTT"
    simulator = LocalSimulator(interval=5)
    simulator.start()
    return simulator, "SIMULATOR"


# --------------------------------------------------------------------------
# 模块 7：app.py
# 模块功能：Flask 主程序。启动时完成建表、演示拓扑初始化、遥测预置， 拉起 MQTT 订阅服务（或模拟采集器），并注册实时监控大屏、设备 接入与在线状态三类页面路由，提供设备注册增删改查、心跳上报、 离线扫描识别与实时数据 JSON 接口
# --------------------------------------------------------------------------


app = Flask(__name__)
app.json.ensure_ascii = False

INGEST_WORKER = None
INGEST_MODE = "UNKNOWN"


@app.route("/")
def root_entry():
    """根路径跳转到实时监控大屏。"""
    return redirect(url_for("screen_monitor"))


@app.route("/iot/monitor")
def screen_monitor():
    """物联网实时监控大屏页面（第 12 天补充动态刷新与图表联动优化）。"""
    return render_template(
        "screen_monitor.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        offline_timeout=OFFLINE_TIMEOUT_SEC,
    )


@app.route("/iot/devices")
def page_devices():
    """物联网设备接入管理页面：设备注册、网关绑定、状态切换、注销（第 2 天实现）。"""
    return render_template(
        "page_devices.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        offline_timeout=OFFLINE_TIMEOUT_SEC,
    )


@app.route("/iot/online")
def page_online():
    """设备在线状态看板页面（第 3 天新增）。

    展示在线率、离线设备清单、近 24 小时心跳趋势、网关在线情况与运行
    状态分布，支持手动触发离线扫描与查看心跳日志。
    """
    return render_template(
        "page_online.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        offline_timeout=OFFLINE_TIMEOUT_SEC,
    )


# ==================================================================
# 以下为第 2 天新增：设备接入注册管理接口
# ==================================================================

@app.route("/api/iot/devices", methods=["GET", "POST"])
def api_device_collection():
    """设备集合接口：GET 查询接入台账，POST 注册新设备。"""
    if request.method == "GET":
        result = list_device_registry(
            keyword=request.args.get("keyword", ""),
            gateway_code=request.args.get("gateway_code", ""),
            run_state=request.args.get("run_state", ""),
        )
        result["result"] = "OK"
        result["summary"] = summarize_registry()
        return jsonify(result)

    payload = request.get_json(silent=True) or request.form.to_dict()
    outcome = register_device_node(payload, operator="admin")
    outcome["result"] = "OK" if outcome.get("success") else "FAIL"
    return jsonify(outcome)


@app.route("/api/iot/devices/<device_code>", methods=["GET", "PUT", "DELETE"])
def api_device_item(device_code):
    """设备单条接口：GET 详情，PUT 修改，DELETE 注销。"""
    if request.method == "GET":
        detail = fetch_device_by_code(device_code)
        if not detail:
            return jsonify({"result": "FAIL", "message": "设备不存在"})
        return jsonify({
            "result": "OK",
            "detail": detail,
            "topics": build_device_topics(device_code, detail["gateway_code"]),
            "telemetry_amount": count_telemetry_by_device(device_code),
        })

    if request.method == "PUT":
        payload = request.get_json(silent=True) or request.form.to_dict()
        outcome = modify_device_node(device_code, payload, operator="admin")
        outcome["result"] = "OK" if outcome.get("success") else "FAIL"
        return jsonify(outcome)

    outcome = unregister_device_node(device_code, operator="admin")
    outcome["result"] = "OK" if outcome.get("success") else "FAIL"
    return jsonify(outcome)


@app.route("/api/iot/devices/<device_code>/state", methods=["POST"])
def api_device_state(device_code):
    """设备运行状态切换接口。"""
    payload = request.get_json(silent=True) or request.form.to_dict()
    outcome = switch_device_state(device_code, payload.get("run_state", ""), operator="admin")
    outcome["result"] = "OK" if outcome.get("success") else "FAIL"
    return jsonify(outcome)


@app.route("/api/iot/registry/options")
def api_registry_options():
    """设备接入下拉选项接口：网关清单与运行状态字典。"""
    return jsonify({"result": "OK", "options": build_registry_options()})


# ==================================================================
# 以下为第 3 天新增：心跳检测与离线识别接口
# ==================================================================

@app.route("/api/iot/heartbeat", methods=["POST"])
def api_heartbeat_report():
    """心跳上报接口：接收设备心跳报文并落库，离线设备自动恢复在线。"""
    body = request.get_json(silent=True) or request.form.to_dict()
    outcome = record_heartbeat(
        (body.get("device_code") or "").strip(),
        rssi_value=body.get("rssi", 0),
        battery_value=body.get("battery", 0),
        uptime_sec=body.get("uptime", 0),
        source_ip=(body.get("ip") or request.remote_addr or ""),
    )
    outcome["result"] = "OK" if outcome.get("success") else "FAIL"
    return jsonify(outcome)


@app.route("/api/iot/online/stat")
def api_online_statistics():
    """在线状态统计接口：在线率、状态分布、网关在线情况、心跳趋势与离线清单。"""
    payload = build_online_statistics()
    payload["result"] = "OK"
    return jsonify(payload)


@app.route("/api/iot/online/scan", methods=["POST"])
def api_online_scan():
    """离线扫描接口：按超时阈值批量识别并标记离线设备。"""
    body = request.get_json(silent=True) or {}
    result = scan_offline_devices(
        int(body.get("timeout_sec") or OFFLINE_TIMEOUT_SEC), operator="admin"
    )
    result["result"] = "OK"
    return jsonify(result)


@app.route("/api/iot/heartbeat/log")
def api_heartbeat_log():
    """心跳日志查询接口：支持按设备编号过滤。"""
    return jsonify({
        "result": "OK",
        "device_code": request.args.get("device_code", ""),
        "rows": fetch_heartbeat_history(
            request.args.get("device_code", ""),
            int(request.args.get("limit", 20)),
        ),
    })


@app.route("/api/iot/monitor")
def api_monitor():
    """监控大屏数据接口：在线/离线统计、状态分布、设备清单、温度曲线。"""
    payload = build_monitor_payload()
    payload["ingest_mode"] = INGEST_MODE
    payload["result"] = "OK"
    return jsonify(payload)


@app.route("/api/iot/curve")
def api_curve():
    """实时曲线接口：按设备编号与指标名返回最近 N 条遥测数据。"""
    from flask import request
    device_code = request.args.get("device_code", "")
    metric_key = request.args.get("metric_key", "temp")
    limit = int(request.args.get("limit", 30))
    if not device_code:
        nodes = fetch_device_nodes()
        device_code = nodes[0]["device_code"] if nodes else ""
    data = fetch_telemetry_curve(device_code, metric_key, limit)
    return jsonify({
        "result": "OK",
        "device_code": device_code,
        "metric_key": metric_key,
        "points": data,
    })


@app.route("/api/iot/health")
def api_health():
    """健康检查接口。"""
    return jsonify({
        "result": "OK",
        "software": SOFTWARE_FULL_NAME,
        "version": SOFTWARE_VERSION,
        "ingest_mode": INGEST_MODE,
    })


def bootstrap():
    """启动引导：建表 → 初始化拓扑 → 预置遥测 → 拉起采集服务。"""
    global INGEST_WORKER, INGEST_MODE
    setup_storage()
    seed_demo_topology()
    seed_demo_telemetry()
    INGEST_WORKER, INGEST_MODE = launch_ingest_service()


if __name__ == "__main__":
    bootstrap()
    print("=" * 62)
    print("%s %s 启动中..." % (SOFTWARE_FULL_NAME, SOFTWARE_VERSION))
    print("数据接入模式：%s" % INGEST_MODE)
    print("访问地址： http://127.0.0.1:%d/iot/monitor" % PORT)
    print("在线状态： http://127.0.0.1:%d/iot/online" % PORT)
    print("=" * 62)
    app.run(host=HOST, port=PORT, debug=DEBUG, use_reloader=False)
