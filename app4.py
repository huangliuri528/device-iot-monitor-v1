# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-24（第 4 天 / 共 15 天）
# 模块功能：本文件仅含该软件第 4 天新增与修改的源代码，不含第 1~3 天已有代码
#           本次迭代主题：设备运行实时参数采集存储模块 + 实时动态曲线
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）
# 接入说明：以下内容为当日增量，需与前面各天的代码合并后运行——
#           新增模块按文件名放入工程目录；建表脚本增量追加进对应 .sql；
#           主程序增量片段插入 app.py 中 Flask 实例之后；前端资源按文末清单
#           放入 templates/ 与 static/ 目录


# --------------------------------------------------------------------------
# 第 4 天新增业务模块 1：iot_telemetry.py（完整源码）
# --------------------------------------------------------------------------

# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-24（第 4 天 / 共 15 天）
# 模块功能：设备运行实时参数采集存储模块。负责一次采集轮次内多指标批量
#           落库、采集报文合法性校验、指标窗口聚合（分钟级最大/最小/均值）、
#           实时曲线取数、多指标同屏曲线组装与指标卡汇总，并对接 MQTT 报文
#           回调与内置模拟采集器，为实时监控大屏提供动态曲线数据源
# 依赖包：sqlite3（Python 内置）、datetime（Python 内置）

from datetime import datetime
from iot_config import (
    METRIC_FIELD_DICT,
    TELEMETRY_KEEP_LIMIT,
)
from iot_store import connect_db, dict_list, now_text, save_telemetry

# 指标单位字典：与 iot_config 的指标字典配套，落库时一并写入单位
METRIC_UNIT_MAP = {
    "temp": "℃",
    "volt": "V",
    "curr": "A",
    "power": "W",
    "press": "kPa",
    "humid": "%",
}

# 各指标的合理量程，用于采集报文异常值过滤（第 11 天将扩展为可配置阈值）
METRIC_RANGE_MAP = {
    "temp": (0.0, 120.0),
    "volt": (0.0, 400.0),
    "curr": (0.0, 60.0),
    "power": (0.0, 5000.0),
    "press": (0.0, 1000.0),
    "humid": (0.0, 100.0),
}

# 实时曲线默认窗口长度（秒）
DEFAULT_WINDOW_SEC = 600


def valid_metric_value(metric_key, metric_value):
    """校验采集值是否落在合理量程内，剔除跳变异常数据。"""
    try:
        value = float(metric_value)
    except (TypeError, ValueError):
        return False
    if metric_key not in METRIC_FIELD_DICT:
        return False
    low, high = METRIC_RANGE_MAP.get(metric_key, (-999999.0, 999999.0))
    return low <= value <= high


def collect_metric_batch(device_code, gateway_code, metric_map, source_tag="SIM"):
    """批量采集落库：一次采集轮次内的多个指标一次性写入遥测表。

    返回本次成功写入的指标数量与剔除的异常项，便于采集日志排查。
    """
    accepted = []
    rejected = []
    for metric_key, raw_value in (metric_map or {}).items():
        if metric_key not in METRIC_FIELD_DICT:
            rejected.append({"metric_key": metric_key, "reason": "未登记指标"})
            continue
        if not valid_metric_value(metric_key, raw_value):
            rejected.append({"metric_key": metric_key, "reason": "超出合理量程"})
            continue
        save_telemetry(device_code, gateway_code, metric_key,
                       round(float(raw_value), 2),
                       METRIC_UNIT_MAP.get(metric_key, ""))
        accepted.append(metric_key)

    log_collect_round(device_code, gateway_code, len(accepted),
                      len(rejected), source_tag)
    return {
        "device_code": device_code,
        "accepted": accepted,
        "accepted_amount": len(accepted),
        "rejected": rejected,
        "rejected_amount": len(rejected),
    }


def log_collect_round(device_code, gateway_code, ok_amount, bad_amount, source_tag):
    """记录一次采集轮次的执行情况到聚合表，用于采集质量追溯。"""
    conn = connect_db()
    try:
        conn.execute(
            "INSERT INTO iot_collect_round (device_code, gateway_code, "
            "metric_amount, reject_amount, source_tag) VALUES (?,?,?,?,?)",
            (device_code, gateway_code, ok_amount, bad_amount, source_tag),
        )
        conn.commit()
    finally:
        conn.close()
    return True


def run_collect_round(node, metric_map, source_tag="SIM"):
    """对单台设备执行一次完整采集：批量落库 + 心跳刷新由调用方负责。"""
    return collect_metric_batch(node.get("device_code"),
                                node.get("gateway_code"),
                                metric_map, source_tag)


def fetch_realtime_curve(device_code, metric_key, window_sec=DEFAULT_WINDOW_SEC,
                         limit=60):
    """按时间窗口取单指标实时曲线，返回时间与数值两个等长数组。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT collect_time, metric_value FROM iot_telemetry_log "
            "WHERE device_code=? AND metric_key=? "
            "ORDER BY telemetry_id DESC LIMIT ?",
            (device_code, metric_key, limit)
        ).fetchall()
    finally:
        conn.close()
    points = list(reversed(dict_list(rows)))
    return {
        "metric_key": metric_key,
        "metric_label": METRIC_FIELD_DICT.get(metric_key, metric_key),
        "metric_unit": METRIC_UNIT_MAP.get(metric_key, ""),
        "times": [p["collect_time"] for p in points],
        "values": [p["metric_value"] for p in points],
        "point_amount": len(points),
        "window_sec": window_sec,
    }


def fetch_multi_metric_curve(device_code, metric_keys, limit=30):
    """多指标同屏曲线：一次返回多个指标的时间轴与序列。"""
    result = []
    for metric_key in metric_keys:
        if metric_key not in METRIC_FIELD_DICT:
            continue
        result.append(fetch_realtime_curve(device_code, metric_key,
                                           DEFAULT_WINDOW_SEC, limit))
    return result


def fetch_metric_summary(device_code, metric_key):
    """单指标统计汇总：最新值、均值、最大值、最小值与样本数。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT COUNT(1) AS amount, AVG(metric_value) AS avg_value, "
            "MAX(metric_value) AS max_value, MIN(metric_value) AS min_value "
            "FROM iot_telemetry_log WHERE device_code=? AND metric_key=?",
            (device_code, metric_key)
        ).fetchone()
        last = conn.execute(
            "SELECT metric_value, collect_time FROM iot_telemetry_log "
            "WHERE device_code=? AND metric_key=? "
            "ORDER BY telemetry_id DESC LIMIT 1",
            (device_code, metric_key)
        ).fetchone()
    finally:
        conn.close()
    stat = dict(row) if row else {}
    latest = dict(last) if last else {}
    return {
        "metric_key": metric_key,
        "metric_label": METRIC_FIELD_DICT.get(metric_key, metric_key),
        "metric_unit": METRIC_UNIT_MAP.get(metric_key, ""),
        "last_value": latest.get("metric_value", 0),
        "last_time": latest.get("collect_time", ""),
        "avg_value": round(float(stat.get("avg_value") or 0), 2),
        "max_value": round(float(stat.get("max_value") or 0), 2),
        "min_value": round(float(stat.get("min_value") or 0), 2),
        "sample_amount": int(stat.get("amount") or 0),
    }


def build_metric_cards(device_code):
    """组装设备全部已登记指标的实时指标卡。"""
    return [fetch_metric_summary(device_code, key)
            for key in METRIC_FIELD_DICT.keys()]


def aggregate_metrics(slice_minutes=5):
    """窗口聚合：把最近一个时间片内的遥测数据聚合成分钟级统计。

    聚合结果写入 iot_metric_aggregate，供趋势图与能耗统计复用，避免
    大屏每次刷新都扫描全量明细。
    """
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT device_code, metric_key, "
            "COUNT(1) AS amount, AVG(metric_value) AS avg_value, "
            "MAX(metric_value) AS max_value, MIN(metric_value) AS min_value "
            "FROM iot_telemetry_log "
            "WHERE collect_time >= datetime('now','localtime',? ) "
            "GROUP BY device_code, metric_key",
            ("-%d minutes" % slice_minutes,)
        ).fetchall()
        stat_rows = dict_list(rows)
        slice_tag = datetime.now().strftime("%Y-%m-%d %H:%M")
        for item in stat_rows:
            conn.execute(
                "INSERT INTO iot_metric_aggregate (device_code, metric_key, "
                "slice_tag, sample_amount, avg_value, max_value, min_value) "
                "VALUES (?,?,?,?,?,?,?)",
                (item["device_code"], item["metric_key"], slice_tag,
                 item["amount"], round(float(item["avg_value"] or 0), 2),
                 round(float(item["max_value"] or 0), 2),
                 round(float(item["min_value"] or 0), 2)),
            )
        conn.commit()
    finally:
        conn.close()
    return len(stat_rows)


def fetch_aggregate_rows(device_code, metric_key, limit=30):
    """读取指标聚合结果，供趋势图展示长时间跨度的变化。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT slice_tag, avg_value, max_value, min_value "
            "FROM iot_metric_aggregate WHERE device_code=? AND metric_key=? "
            "ORDER BY aggregate_id DESC LIMIT ?",
            (device_code, metric_key, limit)
        ).fetchall()
    finally:
        conn.close()
    return list(reversed(dict_list(rows)))


def fetch_collect_round_log(limit=10):
    """最近采集轮次日志，用于采集质量监控。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT device_code, source_tag, metric_amount, reject_amount, "
            "round_time FROM iot_collect_round "
            "ORDER BY round_id DESC LIMIT ?", (limit,)
        ).fetchall()
    finally:
        conn.close()
    return dict_list(rows)


def build_telemetry_payload(device_code, metric_key="", limit=60):
    """组装实时参数页数据：指标卡、主曲线、多指标曲线、采集日志。"""
    if not metric_key:
        metric_key = "temp"
    multi_keys = ["temp", "power", "volt"]
    return {
        "device_code": device_code,
        "metric_options": [{"key": k, "label": v}
                           for k, v in METRIC_FIELD_DICT.items()],
        "metric_cards": build_metric_cards(device_code),
        "main_curve": fetch_realtime_curve(device_code, metric_key,
                                           DEFAULT_WINDOW_SEC, limit),
        "multi_curves": fetch_multi_metric_curve(device_code, multi_keys),
        "aggregate_rows": fetch_aggregate_rows(device_code, metric_key),
        "collect_log": fetch_collect_round_log(10),
        "keep_limit": TELEMETRY_KEEP_LIMIT,
        "generated_at": now_text(),
    }

# --------------------------------------------------------------------------
# 第 4 天数据表脚本增量：iot_schema.sql（M，已内联为字符串常量 DAY4_SCHEMA_SQL）
# --------------------------------------------------------------------------

DAY4_SCHEMA_SQL = r"""
-- ==================================================================
-- 第 4 天新增：采集轮次日志表 + 指标窗口聚合表
-- ==================================================================
-- 采集轮次日志表：记录每一次采集的执行情况（成功指标数、剔除异常数、
-- 数据来源），用于采集链路质量追溯
CREATE TABLE IF NOT EXISTS iot_collect_round (
    round_id       INTEGER PRIMARY KEY AUTOINCREMENT,  -- 轮次主键
    device_code    TEXT    NOT NULL,                   -- 设备编号
    gateway_code   TEXT    DEFAULT '',                 -- 所属网关编号
    metric_amount  INTEGER DEFAULT 0,                  -- 本轮成功入库指标数
    reject_amount  INTEGER DEFAULT 0,                  -- 本轮剔除异常指标数
    source_tag     TEXT    DEFAULT 'SIM',              -- 数据来源（MQTT/SIM/API）
    round_time     TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- 指标窗口聚合表：按设备 + 指标 + 时间片聚合最值均值，供趋势图复用，
-- 避免大屏每次刷新扫描全量遥测明细
CREATE TABLE IF NOT EXISTS iot_metric_aggregate (
    aggregate_id   INTEGER PRIMARY KEY AUTOINCREMENT,  -- 聚合主键
    device_code    TEXT    NOT NULL,                   -- 设备编号
    metric_key     TEXT    NOT NULL,                   -- 指标字段名
    slice_tag      TEXT    DEFAULT '',                 -- 聚合时间片标签
    sample_amount  INTEGER DEFAULT 0,                  -- 样本条数
    avg_value      REAL    DEFAULT 0,                  -- 均值
    max_value      REAL    DEFAULT 0,                  -- 最大值
    min_value      REAL    DEFAULT 0,                  -- 最小值
    created_at     TEXT    DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_round_device ON iot_collect_round (device_code);
CREATE INDEX IF NOT EXISTS idx_agg_device   ON iot_metric_aggregate (device_code, metric_key);
"""


def apply_day4_schema(conn):
    """把第 4 天新增的数据表脚本应用到已有数据库。"""
    conn.executescript(DAY4_SCHEMA_SQL)
    conn.commit()
    return True

# --------------------------------------------------------------------------
# 第 4 天修改的既有模块 1：iot_config.py（仅增量片段）
# --------------------------------------------------------------------------

# ------------------------------------------------------------------
# 大屏刷新参数（第 4 天实时参数页与第 12 天动态刷新优化共用）
# ------------------------------------------------------------------
# 实时监控大屏前端轮询周期（秒），物联网场景要求秒级刷新
REFRESH_INTERVAL_SEC = 3

# --------------------------------------------------------------------------
# 第 4 天修改的既有模块 2：iot_mqtt.py（仅增量片段）
# --------------------------------------------------------------------------

from iot_telemetry import collect_metric_batch, aggregate_metrics

# 【函数内插入】def handle_telemetry_message(gateway_code, device_code, payload_text):
#    # 第 4 天：改走统一采集存储模块，报文按指标批量落库并剔除异常值
#    outcome = collect_metric_batch(device_code, gateway_code, payload, "MQTT")
#    return outcome["accepted_amount"] > 0

# 【函数内插入】class LocalSimulator:
#        # 第 4 天：累计采集轮次，用于按固定间隔触发窗口聚合
#        self.round_count = 0

# 【函数内插入】class LocalSimulator:
#                # 第 4 天：模拟一轮完整采集，温度/功率/电压/湿度四个指标批量入库
#                collect_metric_batch(
#                    node["device_code"], node["gateway_code"],
#                    {
#                        "temp": round(random.uniform(36.2, 38.4), 2),
#                        "power": round(random.uniform(90, 260), 2),
#                        "volt": round(random.uniform(210, 235), 2),
#                        "humid": round(random.uniform(40, 65), 2),
#                    },
#                    "SIM",
#                )

# 【函数内插入】class LocalSimulator:
#            # 每 12 轮做一次窗口聚合，供趋势图与后续能耗统计复用
#            self.round_count += 1
#            if self.round_count % 12 == 0:
#                aggregate_metrics(5)

# --------------------------------------------------------------------------
# 第 4 天主程序新增路由与接口：app.py（仅增量片段）
# --------------------------------------------------------------------------

# 开发日期：2026-10-24（第 4 天 / 共 15 天）
#           接入、在线状态与实时参数采集四类页面路由，提供设备注册增删改查、
#           心跳上报、离线扫描识别、实时参数采集存储与动态曲线 JSON 接口
from iot_config import (HOST, PORT, DEBUG, SOFTWARE_FULL_NAME, SOFTWARE_VERSION,
                        OFFLINE_TIMEOUT_SEC, REFRESH_INTERVAL_SEC)

from iot_telemetry import (
    collect_metric_batch,
    build_telemetry_payload,
    fetch_realtime_curve,
    aggregate_metrics,
    fetch_collect_round_log,
)

# ==================================================================
# 以下为第 4 天新增：设备运行实时参数采集存储接口
# ==================================================================


@app.route("/iot/telemetry")
def page_telemetry():
    """实时参数采集页面：指标卡、动态曲线、多指标同屏与采集日志。"""
    return render_template(
        "page_telemetry.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        refresh_interval=REFRESH_INTERVAL_SEC,
    )


@app.route("/api/iot/telemetry/panel")
def api_telemetry_panel():
    """实时参数页数据接口：指标卡 + 主曲线 + 多指标曲线 + 采集日志。"""
    device_code = (request.args.get("device_code") or "").strip()
    if not device_code:
        nodes = fetch_device_nodes()
        device_code = nodes[0]["device_code"] if nodes else ""
    payload = build_telemetry_payload(
        device_code,
        (request.args.get("metric_key") or "").strip(),
        int(request.args.get("limit", 60)),
    )
    payload["device_options"] = [
        {"device_code": n["device_code"], "device_name": n["device_name"]}
        for n in fetch_device_nodes()
    ]
    payload["result"] = "OK"
    return jsonify(payload)


@app.route("/api/iot/telemetry/collect", methods=["POST"])
def api_telemetry_collect():
    """采集上报接口：接收一个采集轮次的多指标报文并批量落库。"""
    body = request.get_json(silent=True) or {}
    device_code = (body.get("device_code") or "").strip()
    if not device_code:
        return jsonify({"result": "FAIL", "message": "设备编号不能为空"})
    node = fetch_device_by_code(device_code)
    if node is None:
        return jsonify({"result": "FAIL",
                        "message": "设备未注册：%s" % device_code})
    outcome = collect_metric_batch(
        device_code,
        node.get("gateway_code", ""),
        body.get("metrics") or {},
        (body.get("source_tag") or "API").strip(),
    )
    outcome["result"] = "OK"
    return jsonify(outcome)


@app.route("/api/iot/telemetry/curve")
def api_telemetry_curve():
    """实时曲线接口：按设备与指标返回窗口内的时间序列。"""
    device_code = (request.args.get("device_code") or "").strip()
    metric_key = (request.args.get("metric_key") or "temp").strip()
    if not device_code:
        nodes = fetch_device_nodes()
        device_code = nodes[0]["device_code"] if nodes else ""
    curve = fetch_realtime_curve(
        device_code, metric_key,
        int(request.args.get("window_sec", 600)),
        int(request.args.get("limit", 60)),
    )
    curve["result"] = "OK"
    return jsonify(curve)


@app.route("/api/iot/telemetry/aggregate", methods=["POST"])
def api_telemetry_aggregate():
    """窗口聚合接口：对最近时间片内的遥测数据做分钟级聚合。"""
    slice_minutes = int(request.args.get("slice_minutes", 5))
    amount = aggregate_metrics(slice_minutes)
    return jsonify({
        "result": "OK",
        "message": "已完成窗口聚合，覆盖 %d 个设备指标" % amount,
        "slice_minutes": slice_minutes,
    })


@app.route("/api/iot/telemetry/rounds")
def api_collect_rounds():
    """采集轮次日志接口，用于采集链路质量监控。"""
    return jsonify({
        "result": "OK",
        "rows": fetch_collect_round_log(int(request.args.get("limit", 10))),
    })



# 【函数内插入】if __name__ == "__main__":
#    print("实时参数： http://127.0.0.1:%d/iot/telemetry" % PORT)

# --------------------------------------------------------------------------
# 第 4 天新增/修改的前端资源清单（文件本体见工程 templates/、static/ 目录）
# --------------------------------------------------------------------------

#   [新增] static/iot_telemetry.js          新增 222 行
#   [修改] templates/iot_base.html          新增 1 行
#   [新增] templates/page_telemetry.html    新增 66 行
