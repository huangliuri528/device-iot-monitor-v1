# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-30（第 10 天 / 共 15 天）
# 模块功能：本文件仅含该软件第 10 天新增与修改的源代码，不含第 1~9 天已有代码
#           本次迭代主题：数据采集点位与转发规则模块 + 采集点位页面
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）
# 接入说明：以下内容为当日增量，需与前面各天的代码合并后运行——
#           新增模块按文件名放入工程目录；建表脚本增量追加进对应 .sql；
#           主程序增量片段插入 app.py 中 Flask 实例之后；前端资源按文末清单
#           放入 templates/ 与 static/ 目录


# --------------------------------------------------------------------------
# 第 10 天新增业务模块 1：iot_collect.py（完整源码）
# --------------------------------------------------------------------------

# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-30（第 10 天 / 共 15 天）
# 模块功能：数据采集点位与转发规则模块。实现采集点位的档案维护、量程系数
#           换算与采样周期管理、按网关批量轮询采集并登记采集批次留痕、采集
#           数值的越限判定，以及转发规则的目标主题配置、周期转发与变化转发
#           两种触发方式执行、转发计数与状态跟踪；输出点位状态分布、指标
#           采集量分布、网关采集成功率对比、近十批采集趋势与转发目标分布
#           五类统计图表，形成「建点位 → 批量采集 → 越限判定 → 配规则 →
#           转发外送」的数据采集与转发闭环
# 依赖包：sqlite3（Python 内置）、datetime（Python 内置）、random（Python 内置）

import datetime
import random

from iot_store import connect_db, dict_list, now_text, fetch_gateway_rows
from iot_store import fetch_device_nodes, save_telemetry
from iot_config import METRIC_FIELD_DICT, MQTT_TOPIC_TELEMETRY
from iot_telemetry import METRIC_UNIT_MAP, METRIC_RANGE_MAP

# ------------------------------------------------------------------
# 采集指标字典：键为指标键，值为（指标名称，默认单位，默认量程系数）
# 与实时参数采集模块共用同一套指标定义，保证采集口径一致
# ------------------------------------------------------------------
COLLECT_METRIC_DICT = {
    "temp": ("设备温度", "℃", 1.0),
    "volt": ("工作电压", "V", 1.0),
    "curr": ("工作电流", "A", 1.0),
    "power": ("实时功率", "W", 1.0),
    "humid": ("环境湿度", "%RH", 1.0),
    "press": ("管路压力", "kPa", 1.0),
    "flow": ("介质流量", "L/min", 1.0),
    "shake": ("振动幅度", "mm/s", 1.0),
}

# 数值类型字典：决定采集原始值如何换算为工程值
POINT_VALUE_DICT = {
    "FLOAT": "浮点型",
    "INT": "整型",
    "BOOL": "开关量",
}

# 采集点位状态字典：由最近一次采集结果与越限判定共同决定
POINT_STATE_DICT = {
    "IDLE": "待采集",
    "NORMAL": "采集正常",
    "OVER": "越限告警",
    "FAIL": "采集失败",
    "STOP": "已停用",
}

# 采集批次状态字典
COLLECT_TASK_DICT = {
    "DONE": "采集完成",
    "PARTIAL": "部分失败",
    "FAILED": "采集失败",
}

# 转发目标系统字典：区分院内不同业务系统
FORWARD_TARGET_DICT = {
    "EMR": "电子病历系统",
    "HIS": "医院信息系统",
    "MDMS": "医疗器械管理系统",
    "BIGDATA": "院级数据中心",
    "CLOUD": "区域健康云",
}

# 转发方式字典：周期转发按固定间隔外送，变化转发按阈值触发
FORWARD_MODE_DICT = {
    "PERIOD": "周期转发",
    "CHANGE": "变化转发",
    "BOTH": "周期+变化",
}

# 转发规则状态字典
FORWARD_RULE_DICT = {
    "IDLE": "待触发",
    "RUNNING": "转发正常",
    "PAUSED": "已暂停",
    "ERROR": "转发异常",
}

# 主题前缀：转发目标主题统一前缀，便于消费端按前缀订阅
FORWARD_TOPIC_PREFIX = "med/iot/forward"

# 采集点位编号前缀，编号形如 CP-0007
POINT_CODE_PREFIX = "CP"

# 采集批次编号前缀，编号形如 CT-20261030221503
TASK_CODE_PREFIX = "CT"

# 转发规则编号前缀，编号形如 FR-0003
RULE_CODE_PREFIX = "FR"

# 单次批量采集的耗时基数与每点位耗时，用于模拟采集耗时（毫秒）
COLLECT_BASE_MS = 40
COLLECT_PER_POINT_MS = 6

# 采集失败概率基准：网关离线时该点位按此概率采集失败
COLLECT_FAIL_RATE = 0.35

# 越限判定容差：工程值超出上限或下限即判定越限
OVER_LIMIT_TOLERATE = 0.0

# 默认采样周期（秒）
DEFAULT_SAMPLE_RATE = 3

# 默认转发间隔（秒）
DEFAULT_INTERVAL_SEC = 30

# 演示数据随机种子，保证每次启动生成的采集数据一致
DEMO_RANDOM_SEED = 20261030


# ------------------------------------------------------------------
# 通用工具
# ------------------------------------------------------------------
def round2(value):
    """数值保留两位小数，空值与非数值按 0 处理。"""
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return 0.0


def today_text():
    """返回今天日期文本 YYYY-MM-DD。"""
    return datetime.date.today().strftime("%Y-%m-%d")


def next_serial(table_name, code_column, prefix):
    """查询某编号前缀下的下一个流水序号，无历史记录时从 1 开始。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT %s FROM %s WHERE %s LIKE ? ORDER BY %s DESC LIMIT 1"
            % (code_column, table_name, code_column, code_column),
            (prefix + "%",)).fetchone()
    finally:
        conn.close()
    if not row:
        return 1
    try:
        return int(row[code_column].rsplit("-", 1)[-1]) + 1
    except (ValueError, IndexError, AttributeError, TypeError):
        return 1


def build_point_code():
    """生成采集点位编号，形如 CP-0007。"""
    return "%s-%04d" % (POINT_CODE_PREFIX,
                        next_serial("iot_collect_point", "point_code",
                                    POINT_CODE_PREFIX + "-"))


def build_rule_code():
    """生成转发规则编号，形如 FR-0003。"""
    return "%s-%04d" % (RULE_CODE_PREFIX,
                        next_serial("iot_forward_rule", "rule_code",
                                    RULE_CODE_PREFIX + "-"))


def build_task_code():
    """生成采集批次编号，形如 CT-20261030221503123。

    末三位为毫秒序号，保证同一秒内连续发起的多批采集编号不冲突。
    """
    stamp = datetime.datetime.now()
    return "%s-%s%03d" % (TASK_CODE_PREFIX, stamp.strftime("%Y%m%d%H%M%S"),
                          stamp.microsecond // 1000)


def metric_profile(metric_key):
    """读取指标的默认名称、单位与量程系数，未登记指标按原样返回。"""
    name, unit, factor = COLLECT_METRIC_DICT.get(
        metric_key, (METRIC_FIELD_DICT.get(metric_key, metric_key),
                     METRIC_UNIT_MAP.get(metric_key, ""), 1.0))
    return name, unit, factor


def convert_engineering_value(raw_value, scale_factor):
    """把采集原始值按量程系数换算为工程值。"""
    factor = scale_factor if scale_factor else 1.0
    try:
        return round2(float(raw_value) * float(factor))
    except (TypeError, ValueError):
        return 0.0


# ==================================================================
# 采集点位维护
# ==================================================================
def save_collect_point(payload):
    """登记或修改一条采集点位档案，返回点位编号。"""
    device_code = (payload.get("device_code") or "").strip()
    metric_key = (payload.get("metric_key") or "").strip()
    if not device_code:
        return {"success": False, "message": "设备编号不能为空"}
    if not metric_key:
        return {"success": False, "message": "采集指标不能为空"}

    name, unit, factor = metric_profile(metric_key)
    point_code = (payload.get("point_code") or "").strip()
    scale_factor = payload.get("scale_factor")
    scale_factor = factor if scale_factor in (None, "") else round2(scale_factor)
    if not scale_factor:
        scale_factor = factor
    low, high = METRIC_RANGE_MAP.get(metric_key, (0.0, 0.0))
    upper_limit = payload.get("upper_limit")
    upper_limit = high if upper_limit in (None, "") else round2(upper_limit)
    lower_limit = payload.get("lower_limit")
    lower_limit = low if lower_limit in (None, "") else round2(lower_limit)

    node_map = {}
    for node in fetch_device_nodes():
        node_map[node["device_code"]] = node
    node = node_map.get(device_code, {})
    point_addr = (payload.get("point_addr") or "").strip() or (
        "4%04d" % (next_serial("iot_collect_point", "point_code",
                               POINT_CODE_PREFIX + "-") + 1))

    conn = connect_db()
    try:
        if point_code:
            row = conn.execute(
                "SELECT point_id FROM iot_collect_point WHERE point_code=?",
                (point_code,)).fetchone()
            if row is None:
                return {"success": False, "message": "采集点位不存在"}
            conn.execute(
                "UPDATE iot_collect_point SET device_code=?, device_name=?, "
                "metric_key=?, metric_name=?, metric_unit=?, point_addr=?, "
                "value_type=?, scale_factor=?, sample_rate=?, upper_limit=?, "
                "lower_limit=?, enable_flag=?, operator_name=? WHERE point_code=?",
                (device_code,
                 (payload.get("device_name") or "").strip() or node.get("device_name", ""),
                 metric_key, name, unit, point_addr,
                 (payload.get("value_type") or "FLOAT").strip(), scale_factor,
                 int(payload.get("sample_rate") or DEFAULT_SAMPLE_RATE),
                 upper_limit, lower_limit,
                 int(payload.get("enable_flag", 1)),
                 (payload.get("operator_name") or "").strip() or "admin",
                 point_code))
        else:
            point_code = build_point_code()
            conn.execute(
                "INSERT INTO iot_collect_point "
                "(point_code, device_code, device_name, metric_key, metric_name, "
                " metric_unit, point_addr, value_type, scale_factor, sample_rate, "
                " upper_limit, lower_limit, enable_flag, point_state, "
                " operator_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (point_code, device_code,
                 (payload.get("device_name") or "").strip() or node.get("device_name", ""),
                 metric_key, name, unit, point_addr,
                 (payload.get("value_type") or "FLOAT").strip(), scale_factor,
                 int(payload.get("sample_rate") or DEFAULT_SAMPLE_RATE),
                 upper_limit, lower_limit, int(payload.get("enable_flag", 1)),
                 "STOP" if not int(payload.get("enable_flag", 1)) else "IDLE",
                 (payload.get("operator_name") or "").strip() or "admin"))
        conn.commit()
    except Exception as exc:
        return {"success": False, "message": "点位登记失败：%s" % exc}
    finally:
        conn.close()
    return {"success": True, "point_code": point_code,
            "message": "采集点位「%s」已保存" % point_code}


def toggle_collect_point(point_code, enable_flag):
    """启停采集点位：停用后不再参与批量采集，状态置为已停用。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_collect_point WHERE point_code=?",
            (point_code,)).fetchone()
        if row is None:
            return {"success": False, "message": "采集点位不存在"}
        flag = 1 if int(enable_flag) else 0
        conn.execute(
            "UPDATE iot_collect_point SET enable_flag=?, point_state=? "
            "WHERE point_code=?",
            (flag, "IDLE" if flag else "STOP", point_code))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "采集点位「%s」已%s" % (point_code, "启用" if flag else "停用")}


def remove_collect_point(point_code):
    """删除采集点位及其关联的转发规则。"""
    conn = connect_db()
    try:
        conn.execute(
            "DELETE FROM iot_collect_point WHERE point_code=?", (point_code,))
        conn.execute(
            "DELETE FROM iot_forward_rule WHERE source_point=?", (point_code,))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "采集点位「%s」已删除" % point_code}


# ==================================================================
# 批量采集
# ==================================================================
def simulate_raw_value(point_row):
    """按指标量程模拟一次采集原始值，供无真实设备时的采集演示使用。"""
    randomizer = random.Random(
        hash((point_row["point_code"], now_text())) & 0x7FFFFFFF)
    low, high = METRIC_RANGE_MAP.get(point_row["metric_key"], (0.0, 100.0))
    span = high - low
    value = low + span * randomizer.uniform(0.18, 0.82)
    return round2(value)


def judge_point_state(point_row, engineering_value, collect_ok):
    """按采集结果与工程值越限情况判定点位状态。"""
    if not collect_ok:
        return "FAIL"
    upper = round2(point_row.get("upper_limit"))
    lower = round2(point_row.get("lower_limit"))
    if upper and engineering_value > upper + OVER_LIMIT_TOLERATE:
        return "OVER"
    if lower and engineering_value < lower - OVER_LIMIT_TOLERATE:
        return "OVER"
    return "NORMAL"


def run_collect_batch(gateway_code="", trigger_mode="MANUAL",
                      operator_name="采集调度器"):
    """对指定网关（或全部）下的启用点位执行一次批量采集。

    采集结果写入遥测记录表并回写点位最近值与状态，同时登记采集批次留痕。
    """
    conn = connect_db()
    try:
        clauses = ["enable_flag=1"]
        params = []
        gateway_code = (gateway_code or "").strip()
        if gateway_code:
            clauses.append(
                "device_code IN (SELECT device_code FROM iot_device_node "
                "WHERE gateway_code=?)")
            params.append(gateway_code)
        rows = conn.execute(
            "SELECT * FROM iot_collect_point WHERE " + " AND ".join(clauses) +
            " ORDER BY point_id", params).fetchall()
        points = dict_list(rows)
        if not points:
            return {"success": False, "message": "未找到可采集的启用点位"}

        online_map = {}
        for node in conn.execute(
                "SELECT device_code, run_state FROM iot_device_node").fetchall():
            online_map[node["device_code"]] = node["run_state"]
        gateway_map = {}
        for node in conn.execute(
                "SELECT device_code, gateway_code FROM iot_device_node").fetchall():
            gateway_map[node["device_code"]] = node["gateway_code"]

        randomizer = random.Random(DEMO_RANDOM_SEED + len(points))
        success_count = 0
        fail_count = 0
        for point in points:
            device_code = point["device_code"]
            run_state = online_map.get(device_code, "OFFLINE")
            collect_ok = run_state != "OFFLINE"
            if collect_ok and run_state == "FAULT":
                collect_ok = randomizer.random() > COLLECT_FAIL_RATE
            raw_value = simulate_raw_value(point)
            value = convert_engineering_value(raw_value, point["scale_factor"])
            state = judge_point_state(point, value, collect_ok)
            if collect_ok:
                success_count += 1
                # 采集批次在同一事务内完成，直接写遥测表避免跨连接锁冲突
                conn.execute(
                    "INSERT INTO iot_telemetry_log "
                    "(device_code, gateway_code, metric_key, metric_value, "
                    " metric_unit, collect_time) VALUES (?,?,?,?,?,?)",
                    (device_code, gateway_map.get(device_code, ""),
                     point["metric_key"], value, point["metric_unit"], now_text()))
            else:
                fail_count += 1
                value = 0.0
            conn.execute(
                "UPDATE iot_collect_point SET point_state=?, collect_time=?, "
                "latest_value=? WHERE point_code=?",
                (state, now_text(), value, point["point_code"]))

        elapsed = COLLECT_BASE_MS + len(points) * COLLECT_PER_POINT_MS
        task_state = "DONE" if not fail_count else (
            "FAILED" if not success_count else "PARTIAL")
        task_code = build_task_code()
        conn.execute(
            "INSERT INTO iot_collect_task "
            "(task_code, gateway_code, point_amount, success_count, fail_count, "
            " elapsed_ms, task_state, trigger_mode, operator_name) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (task_code, gateway_code or "全部网关", len(points), success_count,
             fail_count, elapsed, task_state,
             (trigger_mode or "MANUAL").strip(),
             (operator_name or "").strip() or "采集调度器"))
        conn.commit()
    finally:
        conn.close()
    return {
        "success": True,
        "task_code": task_code,
        "message": "采集批次「%s」完成：成功 %d 条 / 失败 %d 条，耗时 %d 毫秒"
                   % (task_code, success_count, fail_count, elapsed),
    }


# ==================================================================
# 转发规则
# ==================================================================
def save_forward_rule(payload):
    """登记或修改一条数据转发规则，返回规则编号。"""
    source_point = (payload.get("source_point") or "").strip()
    if not source_point:
        return {"success": False, "message": "源采集点位不能为空"}
    target_system = (payload.get("target_system") or "EMR").strip()
    if target_system not in FORWARD_TARGET_DICT:
        return {"success": False, "message": "目标系统编码不合法"}
    forward_mode = (payload.get("forward_mode") or "PERIOD").strip()
    if forward_mode not in FORWARD_MODE_DICT:
        return {"success": False, "message": "转发方式编码不合法"}

    conn = connect_db()
    try:
        point_row = conn.execute(
            "SELECT * FROM iot_collect_point WHERE point_code=?",
            (source_point,)).fetchone()
        if point_row is None:
            return {"success": False, "message": "源采集点位不存在"}
        rule_code = (payload.get("rule_code") or "").strip()
        rule_name = (payload.get("rule_name") or "").strip() or (
            "%s→%s数据转发" % (point_row["device_name"] or point_row["device_code"],
                             FORWARD_TARGET_DICT[target_system]))
        topic = (payload.get("target_topic") or "").strip() or (
            "%s/%s/%s" % (FORWARD_TOPIC_PREFIX, target_system.lower(),
                          point_row["device_code"]))
        interval_sec = int(payload.get("interval_sec") or DEFAULT_INTERVAL_SEC)
        change_limit = round2(payload.get("change_limit"))
        enable_flag = int(payload.get("enable_flag", 1))
        if rule_code:
            row = conn.execute(
                "SELECT rule_id FROM iot_forward_rule WHERE rule_code=?",
                (rule_code,)).fetchone()
            if row is None:
                return {"success": False, "message": "转发规则不存在"}
            conn.execute(
                "UPDATE iot_forward_rule SET rule_name=?, source_point=?, "
                "device_code=?, metric_key=?, target_topic=?, target_system=?, "
                "forward_mode=?, interval_sec=?, change_limit=?, enable_flag=?, "
                "rule_state=?, operator_name=? WHERE rule_code=?",
                (rule_name, source_point, point_row["device_code"],
                 point_row["metric_key"], topic, target_system, forward_mode,
                 interval_sec, change_limit, enable_flag,
                 "IDLE" if enable_flag else "PAUSED",
                 (payload.get("operator_name") or "").strip() or "admin",
                 rule_code))
        else:
            rule_code = build_rule_code()
            conn.execute(
                "INSERT INTO iot_forward_rule "
                "(rule_code, rule_name, source_point, device_code, metric_key, "
                " target_topic, target_system, forward_mode, interval_sec, "
                " change_limit, enable_flag, rule_state, operator_name) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (rule_code, rule_name, source_point, point_row["device_code"],
                 point_row["metric_key"], topic, target_system, forward_mode,
                 interval_sec, change_limit, enable_flag,
                 "IDLE" if enable_flag else "PAUSED",
                 (payload.get("operator_name") or "").strip() or "admin"))
        conn.commit()
    except Exception as exc:
        return {"success": False, "message": "转发规则保存失败：%s" % exc}
    finally:
        conn.close()
    return {"success": True, "rule_code": rule_code,
            "message": "转发规则「%s」已保存" % rule_code}


def toggle_forward_rule(rule_code, enable_flag):
    """启停转发规则：停用后不再参与转发调度，状态置为已暂停。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT rule_id FROM iot_forward_rule WHERE rule_code=?",
            (rule_code,)).fetchone()
        if row is None:
            return {"success": False, "message": "转发规则不存在"}
        flag = 1 if int(enable_flag) else 0
        conn.execute(
            "UPDATE iot_forward_rule SET enable_flag=?, rule_state=? "
            "WHERE rule_code=?",
            (flag, "IDLE" if flag else "PAUSED", rule_code))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "转发规则「%s」已%s" % (rule_code, "启用" if flag else "停用")}


def remove_forward_rule(rule_code):
    """删除一条数据转发规则。"""
    conn = connect_db()
    try:
        conn.execute(
            "DELETE FROM iot_forward_rule WHERE rule_code=?", (rule_code,))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "转发规则「%s」已删除" % rule_code}


def execute_forward_rules():
    """执行一次转发调度：遍历启用规则，按转发方式判定是否外送。

    周期转发按间隔判定到达时间即外送；变化转发在数值变化超过阈值时外送。
    """
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT * FROM iot_forward_rule WHERE enable_flag=1 "
            "ORDER BY rule_id").fetchall()
        rules = dict_list(rows)
        if not rules:
            return {"success": True, "forward_amount": 0,
                    "message": "暂无启用的转发规则"}
        now_stamp = datetime.datetime.now()
        sent_amount = 0
        for rule in rules:
            point_row = conn.execute(
                "SELECT * FROM iot_collect_point WHERE point_code=?",
                (rule["source_point"],)).fetchone()
            if point_row is None:
                conn.execute(
                    "UPDATE iot_forward_rule SET rule_state='ERROR' "
                    "WHERE rule_code=?", (rule["rule_code"],))
                continue
            value = round2(point_row["latest_value"])
            need_send = False
            if rule["forward_mode"] in ("PERIOD", "BOTH"):
                last_text = rule.get("last_forward") or ""
                try:
                    last_stamp = datetime.datetime.strptime(
                        last_text, "%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError):
                    last_stamp = now_stamp - datetime.timedelta(days=1)
                gap_sec = (now_stamp - last_stamp).total_seconds()
                if gap_sec >= max(1, int(
                        rule["interval_sec"] or DEFAULT_INTERVAL_SEC)):
                    need_send = True
            if not need_send and rule["forward_mode"] in ("CHANGE", "BOTH"):
                limit = round2(rule["change_limit"])
                if limit and abs(value - round2(rule["last_value"])) >= limit:
                    need_send = True
            if not need_send:
                continue
            conn.execute(
                "UPDATE iot_forward_rule SET forward_count=forward_count+1, "
                "last_forward=?, last_value=?, rule_state='RUNNING' "
                "WHERE rule_code=?", (now_text(), value, rule["rule_code"]))
            sent_amount += 1
        conn.commit()
    finally:
        conn.close()
    return {
        "success": True,
        "forward_amount": sent_amount,
        "message": "本次转发调度完成，外送 %d 条数据" % sent_amount,
    }


# ==================================================================
# 查询与统计
# ==================================================================
def fetch_point_rows(keyword="", device_code="", metric_key="", point_state=""):
    """按条件查询采集点位清单，停用点位排在最后。"""
    clauses = []
    params = []
    keyword = (keyword or "").strip()
    if keyword:
        clauses.append("(point_code LIKE ? OR device_code LIKE ? "
                       "OR device_name LIKE ?)")
        params.extend(["%%%s%%" % keyword] * 3)
    device_code = (device_code or "").strip()
    if device_code:
        clauses.append("device_code=?")
        params.append(device_code)
    metric_key = (metric_key or "").strip()
    if metric_key:
        clauses.append("metric_key=?")
        params.append(metric_key)
    point_state = (point_state or "").strip()
    if point_state:
        clauses.append("point_state=?")
        params.append(point_state)
    where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT * FROM iot_collect_point" + where_sql +
            " ORDER BY enable_flag DESC, point_id", params).fetchall()
        rules = conn.execute(
            "SELECT source_point, COUNT(1) AS rule_amount, "
            "SUM(forward_count) AS total_forward FROM iot_forward_rule "
            "GROUP BY source_point").fetchall()
    finally:
        conn.close()
    rule_map = {row["source_point"]: row for row in rules}
    result = []
    for row in dict_list(rows):
        item = dict(row)
        item["state_label"] = POINT_STATE_DICT.get(
            row["point_state"], row["point_state"])
        item["value_label"] = POINT_VALUE_DICT.get(
            row["value_type"], row["value_type"])
        bind = rule_map.get(row["point_code"])
        item["rule_amount"] = bind["rule_amount"] if bind else 0
        item["total_forward"] = bind["total_forward"] if bind else 0
        result.append(item)
    return result


def fetch_rule_rows():
    """读取转发规则清单，并补上源点位的最近采集值。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT * FROM iot_forward_rule ORDER BY enable_flag DESC, rule_id"
        ).fetchall()
    finally:
        conn.close()
    result = []
    for row in dict_list(rows):
        item = dict(row)
        item["mode_label"] = FORWARD_MODE_DICT.get(
            row["forward_mode"], row["forward_mode"])
        item["target_label"] = FORWARD_TARGET_DICT.get(
            row["target_system"], row["target_system"])
        item["state_label"] = FORWARD_RULE_DICT.get(
            row["rule_state"], row["rule_state"])
        item["metric_label"] = COLLECT_METRIC_DICT.get(
            row["metric_key"], (row["metric_key"], "", 1))[0]
        result.append(item)
    return result


def fetch_task_rows(limit=10):
    """读取最近若干条采集批次留痕。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT * FROM iot_collect_task ORDER BY task_id DESC LIMIT ?",
            (limit,)).fetchall()
    finally:
        conn.close()
    result = []
    for row in dict_list(rows):
        item = dict(row)
        item["state_label"] = COLLECT_TASK_DICT.get(
            row["task_state"], row["task_state"])
        item["success_rate"] = round2(
            row["success_count"] * 100.0 / row["point_amount"]
            if row["point_amount"] else 0)
        result.append(item)
    return result


def build_collect_summary():
    """统计采集点位总量、启用量、越限与失败量、转发规则量与累计转发条数。"""
    conn = connect_db()
    try:
        total = conn.execute(
            "SELECT COUNT(1) FROM iot_collect_point").fetchone()[0]
        enabled = conn.execute(
            "SELECT COUNT(1) FROM iot_collect_point WHERE enable_flag=1"
        ).fetchone()[0]
        normal = conn.execute(
            "SELECT COUNT(1) FROM iot_collect_point WHERE point_state='NORMAL'"
        ).fetchone()[0]
        over = conn.execute(
            "SELECT COUNT(1) FROM iot_collect_point WHERE point_state='OVER'"
        ).fetchone()[0]
        fail = conn.execute(
            "SELECT COUNT(1) FROM iot_collect_point WHERE point_state='FAIL'"
        ).fetchone()[0]
        idle = conn.execute(
            "SELECT COUNT(1) FROM iot_collect_point WHERE point_state='IDLE'"
        ).fetchone()[0]
        rule_total = conn.execute(
            "SELECT COUNT(1) FROM iot_forward_rule").fetchone()[0]
        rule_enabled = conn.execute(
            "SELECT COUNT(1) FROM iot_forward_rule WHERE enable_flag=1"
        ).fetchone()[0]
        forward_total = conn.execute(
            "SELECT COALESCE(SUM(forward_count),0) FROM iot_forward_rule"
        ).fetchone()[0]
        batch_total = conn.execute(
            "SELECT COUNT(1) FROM iot_collect_task").fetchone()[0]
        batch_row = conn.execute(
            "SELECT AVG(success_count * 1.0 / point_amount) AS rate "
            "FROM iot_collect_task WHERE point_amount>0").fetchone()
    finally:
        conn.close()
    return {
        "point_total": total,
        "point_enabled": enabled,
        "normal_amount": normal,
        "over_amount": over,
        "fail_amount": fail,
        "idle_amount": idle,
        "rule_total": rule_total,
        "rule_enabled": rule_enabled,
        "forward_total": forward_total,
        "batch_total": batch_total,
        "avg_success_rate": round2(
            (batch_row["rate"] or 0) * 100 if batch_row else 0),
    }


def build_collect_charts():
    """构建点位状态、指标采集量、网关采集成功率、批次趋势与转发目标五类图。"""
    conn = connect_db()
    try:
        state_rows = conn.execute(
            "SELECT point_state, COUNT(1) AS amount FROM iot_collect_point "
            "GROUP BY point_state").fetchall()
        metric_rows = conn.execute(
            "SELECT metric_key, COUNT(1) AS amount FROM iot_collect_point "
            "GROUP BY metric_key ORDER BY amount DESC").fetchall()
        gateway_rows = conn.execute(
            "SELECT gateway_code, COUNT(1) AS point_amount, "
            "SUM(CASE WHEN point_state='NORMAL' THEN 1 ELSE 0 END) AS ok_amount, "
            "SUM(CASE WHEN point_state='FAIL' THEN 1 ELSE 0 END) AS fail_amount "
            "FROM iot_collect_point p JOIN iot_device_node d "
            "ON p.device_code=d.device_code GROUP BY gateway_code "
            "ORDER BY point_amount DESC").fetchall()
        task_rows = conn.execute(
            "SELECT task_code, success_count, fail_count, elapsed_ms, created_at "
            "FROM iot_collect_task ORDER BY task_id DESC LIMIT 10").fetchall()
        target_rows = conn.execute(
            "SELECT target_system, COUNT(1) AS rule_amount, "
            "COALESCE(SUM(forward_count),0) AS forward_amount "
            "FROM iot_forward_rule GROUP BY target_system "
            "ORDER BY forward_amount DESC").fetchall()
        device_rows = conn.execute(
            "SELECT device_code, device_name, COUNT(1) AS point_amount "
            "FROM iot_collect_point GROUP BY device_code, device_name "
            "ORDER BY point_amount DESC LIMIT 8").fetchall()
    finally:
        conn.close()

    state_distribution = [
        {"name": POINT_STATE_DICT.get(row["point_state"], row["point_state"]),
         "value": row["amount"]}
        for row in state_rows
    ]
    metric_distribution = [
        {"name": COLLECT_METRIC_DICT.get(
            row["metric_key"], (row["metric_key"], "", 1))[0],
         "value": row["amount"]}
        for row in metric_rows
    ]
    gateway_compare = [
        {"name": row["gateway_code"], "point_amount": row["point_amount"],
         "ok_amount": row["ok_amount"], "fail_amount": row["fail_amount"],
         "success_rate": round2(row["ok_amount"] * 100.0 / row["point_amount"])
         if row["point_amount"] else 0.0}
        for row in gateway_rows
    ]
    task_trend = [
        {"name": (row["created_at"] or "")[-8:] or row["task_code"][-6:],
         "success": row["success_count"],
         "fail": row["fail_count"], "elapsed": row["elapsed_ms"]}
        for row in reversed(task_rows)
    ]
    target_distribution = [
        {"name": FORWARD_TARGET_DICT.get(row["target_system"], row["target_system"]),
         "rule_amount": row["rule_amount"], "forward_amount": row["forward_amount"]}
        for row in target_rows
    ]
    device_ranking = [
        {"device_code": row["device_code"], "device_name": row["device_name"],
         "point_amount": row["point_amount"]}
        for row in device_rows
    ]
    return {
        "state_distribution": state_distribution,
        "metric_distribution": metric_distribution,
        "gateway_compare": gateway_compare,
        "task_trend": task_trend,
        "target_distribution": target_distribution,
        "device_ranking": device_ranking,
    }


def build_collect_payload(keyword="", device_code="", metric_key="",
                          point_state=""):
    """组装采集点位页面所需的全部数据：汇总、图表、点位清单、规则与批次。"""
    return {
        "result": "OK",
        "summary": build_collect_summary(),
        "charts": build_collect_charts(),
        "points": fetch_point_rows(keyword, device_code, metric_key, point_state),
        "rules": fetch_rule_rows(),
        "tasks": fetch_task_rows(10),
        "options": {
            "metric_dict": {key: value[0]
                            for key, value in COLLECT_METRIC_DICT.items()},
            "metric_unit": {key: value[1]
                            for key, value in COLLECT_METRIC_DICT.items()},
            "value_dict": POINT_VALUE_DICT,
            "state_dict": POINT_STATE_DICT,
            "task_dict": COLLECT_TASK_DICT,
            "target_dict": FORWARD_TARGET_DICT,
            "mode_dict": FORWARD_MODE_DICT,
            "rule_dict": FORWARD_RULE_DICT,
            "device_list": [{"code": node["device_code"],
                             "name": node["device_name"]}
                            for node in fetch_device_nodes()],
            "gateway_list": [row["gateway_code"] for row in fetch_gateway_rows()],
        },
    }


def build_collect_brief():
    """数据采集大屏摘要，供实时监控大屏嵌入。"""
    summary = build_collect_summary()
    return {
        "point_total": summary["point_total"],
        "over_amount": summary["over_amount"],
        "fail_amount": summary["fail_amount"],
        "forward_total": summary["forward_total"],
        "text": "越限 %d 点 / 失败 %d 点" % (
            summary["over_amount"], summary["fail_amount"]),
    }


# ==================================================================
# 演示数据
# ==================================================================
def seed_demo_collect():
    """灌入第 10 天演示数据：按设备自动建点位、执行采集与转发调度。"""
    nodes = fetch_device_nodes()
    if not nodes:
        return 0

    metric_cycle = ["temp", "volt", "curr", "power"]
    randomizer = random.Random(DEMO_RANDOM_SEED)
    amount = 0
    for index, node in enumerate(nodes):
        # 每台设备建 2 至 3 个采集点位，指标在四类常用指标中轮换
        metric_count = 2 + (index % 2)
        for step in range(metric_count):
            metric_key = metric_cycle[(index + step) % len(metric_cycle)]
            outcome = save_collect_point({
                "device_code": node["device_code"],
                "metric_key": metric_key,
                "scale_factor": 1.0,
                "sample_rate": DEFAULT_SAMPLE_RATE + step * 2,
                "value_type": "FLOAT",
                "enable_flag": 1,
                "operator_name": "物联网运维助手",
            })
            if outcome.get("success"):
                amount += 1
    if not amount:
        return 0

    # 停用两个点位，展示停用状态与采集跳过逻辑
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT point_code FROM iot_collect_point ORDER BY point_id "
            "LIMIT 2").fetchall()
    finally:
        conn.close()
    for row in rows:
        toggle_collect_point(row["point_code"], 0)

    # 收紧两个点位的工程值上限，用于演示采集越限告警状态
    conn = connect_db()
    try:
        narrow_rows = conn.execute(
            "SELECT point_code, metric_key, lower_limit, upper_limit "
            "FROM iot_collect_point WHERE enable_flag=1 "
            "ORDER BY point_id LIMIT 2").fetchall()
    finally:
        conn.close()
    for row in narrow_rows:
        low, high = METRIC_RANGE_MAP.get(row["metric_key"], (0.0, 100.0))
        conn = connect_db()
        try:
            conn.execute(
                "UPDATE iot_collect_point SET upper_limit=? WHERE point_code=?",
                (round2(low + (high - low) * 0.55), row["point_code"]))
            conn.commit()
        finally:
            conn.close()

    # 连续执行三轮采集，形成批次趋势
    gateways = [row["gateway_code"] for row in fetch_gateway_rows()]
    for round_index in range(3):
        run_collect_batch(
            gateway_code=gateways[round_index % len(gateways)]
            if gateways else "",
            trigger_mode="AUTO",
            operator_name="采集调度器")

    # 为前若干个点位配置转发规则，覆盖不同目标系统与转发方式
    conn = connect_db()
    try:
        point_rows = conn.execute(
            "SELECT point_code FROM iot_collect_point WHERE enable_flag=1 "
            "ORDER BY point_id LIMIT 5").fetchall()
    finally:
        conn.close()
    target_plan = ["MDMS", "EMR", "BIGDATA", "HIS", "CLOUD"]
    mode_plan = ["PERIOD", "CHANGE", "BOTH", "PERIOD", "CHANGE"]
    for index, row in enumerate(point_rows):
        save_forward_rule({
            "source_point": row["point_code"],
            "target_system": target_plan[index % len(target_plan)],
            "forward_mode": mode_plan[index % len(mode_plan)],
            "interval_sec": DEFAULT_INTERVAL_SEC,
            "change_limit": round2(randomizer.uniform(0.5, 3.0)),
            "enable_flag": 1,
            "operator_name": "物联网运维助手",
        })
    # 停用最后一条规则，展示暂停状态
    if len(point_rows) >= 5:
        conn = connect_db()
        try:
            rule_row = conn.execute(
                "SELECT rule_code FROM iot_forward_rule ORDER BY rule_id DESC "
                "LIMIT 1").fetchone()
        finally:
            conn.close()
        if rule_row:
            toggle_forward_rule(rule_row["rule_code"], 0)
    # 执行两轮转发调度，累计转发计数
    execute_forward_rules()
    execute_forward_rules()
    return amount

# --------------------------------------------------------------------------
# 第 10 天数据表脚本增量：iot_schema.sql（M，已内联为字符串常量 DAY10_SCHEMA_SQL）
# --------------------------------------------------------------------------

DAY10_SCHEMA_SQL = r"""
-- ==================================================================
-- 第 10 天新增：数据采集点位与转发规则
-- ==================================================================
CREATE TABLE IF NOT EXISTS iot_collect_point (
    point_id      INTEGER PRIMARY KEY AUTOINCREMENT,  -- 采集点位主键
    point_code    TEXT    NOT NULL UNIQUE,            -- 点位编号（CP-序号）
    device_code   TEXT    NOT NULL,                   -- 关联设备编号
    device_name   TEXT    DEFAULT '',                 -- 设备名称快照
    metric_key    TEXT    NOT NULL,                   -- 采集指标键（temp/volt/curr/power）
    metric_name   TEXT    DEFAULT '',                 -- 指标名称
    metric_unit   TEXT    DEFAULT '',                 -- 指标单位
    point_addr    TEXT    DEFAULT '',                 -- 寄存器或点位地址
    value_type    TEXT    DEFAULT 'FLOAT',            -- 数值类型（见 POINT_VALUE_DICT）
    scale_factor  REAL    DEFAULT 1.0,                -- 量程系数：原始值乘系数得工程值
    sample_rate   INTEGER DEFAULT 3,                  -- 采样周期（秒）
    upper_limit   REAL    DEFAULT 0,                  -- 工程值上限
    lower_limit   REAL    DEFAULT 0,                  -- 工程值下限
    enable_flag   INTEGER DEFAULT 1,                  -- 启用标志（1=启用 0=停用）
    point_state   TEXT    DEFAULT 'IDLE',             -- 点位状态（见 POINT_STATE_DICT）
    collect_time  TEXT    DEFAULT '',                 -- 最近采集时间
    latest_value  REAL    DEFAULT 0,                  -- 最近采集工程值
    operator_name TEXT    DEFAULT 'admin',            -- 点位维护人
    created_at    TEXT    DEFAULT (datetime('now', 'localtime'))
);

CREATE TABLE IF NOT EXISTS iot_collect_task (
    task_id       INTEGER PRIMARY KEY AUTOINCREMENT,  -- 采集批次主键
    task_code     TEXT    NOT NULL UNIQUE,            -- 批次编号（CT-年月日时分秒）
    gateway_code  TEXT    DEFAULT '',                 -- 采集网关编号
    point_amount  INTEGER DEFAULT 0,                  -- 本批采集点位数量
    success_count INTEGER DEFAULT 0,                  -- 成功采集条数
    fail_count    INTEGER DEFAULT 0,                  -- 采集失败条数
    elapsed_ms    INTEGER DEFAULT 0,                  -- 采集耗时（毫秒）
    task_state    TEXT    DEFAULT 'DONE',             -- 批次状态（见 COLLECT_TASK_DICT）
    trigger_mode  TEXT    DEFAULT 'AUTO',             -- 触发方式（AUTO=周期 MANUAL=手动）
    operator_name TEXT    DEFAULT '采集调度器',        -- 触发人
    created_at    TEXT    DEFAULT (datetime('now', 'localtime'))
);

CREATE TABLE IF NOT EXISTS iot_forward_rule (
    rule_id       INTEGER PRIMARY KEY AUTOINCREMENT,  -- 转发规则主键
    rule_code     TEXT    NOT NULL UNIQUE,            -- 规则编号（FR-序号）
    rule_name     TEXT    DEFAULT '',                 -- 规则名称
    source_point  TEXT    DEFAULT '',                 -- 源采集点位编号
    device_code   TEXT    DEFAULT '',                 -- 关联设备编号
    metric_key    TEXT    DEFAULT '',                 -- 关联采集指标键
    target_topic  TEXT    DEFAULT '',                 -- 转发目标主题
    target_system TEXT    DEFAULT 'EMR',              -- 目标系统（见 FORWARD_TARGET_DICT）
    forward_mode  TEXT    DEFAULT 'PERIOD',           -- 转发方式（见 FORWARD_MODE_DICT）
    interval_sec  INTEGER DEFAULT 30,                 -- 转发间隔（秒，周期转发生效）
    change_limit  REAL    DEFAULT 0,                  -- 变化阈值（变化转发生效）
    enable_flag   INTEGER DEFAULT 1,                  -- 启用标志（1=启用 0=停用）
    forward_count INTEGER DEFAULT 0,                  -- 累计转发条数
    last_forward  TEXT    DEFAULT '',                 -- 最近转发时间
    last_value    REAL    DEFAULT 0,                  -- 最近转发数值
    rule_state    TEXT    DEFAULT 'IDLE',             -- 规则状态（见 FORWARD_RULE_DICT）
    operator_name TEXT    DEFAULT 'admin',            -- 规则维护人
    created_at    TEXT    DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_cpoint_code    ON iot_collect_point (point_code);
CREATE INDEX IF NOT EXISTS idx_cpoint_device  ON iot_collect_point (device_code);
CREATE INDEX IF NOT EXISTS idx_cpoint_metric  ON iot_collect_point (metric_key);
CREATE INDEX IF NOT EXISTS idx_cpoint_state   ON iot_collect_point (point_state);
CREATE INDEX IF NOT EXISTS idx_ctask_code     ON iot_collect_task (task_code);
CREATE INDEX IF NOT EXISTS idx_ctask_gateway  ON iot_collect_task (gateway_code);
CREATE INDEX IF NOT EXISTS idx_frule_code     ON iot_forward_rule (rule_code);
CREATE INDEX IF NOT EXISTS idx_frule_point    ON iot_forward_rule (source_point);
CREATE INDEX IF NOT EXISTS idx_frule_target   ON iot_forward_rule (target_system);
"""


def apply_day10_schema(conn):
    """把第 10 天新增的数据表脚本应用到已有数据库。"""
    conn.executescript(DAY10_SCHEMA_SQL)
    conn.commit()
    return True

# --------------------------------------------------------------------------
# 第 10 天主程序新增路由与接口：app.py（仅增量片段）
# --------------------------------------------------------------------------

# 开发日期：2026-10-30（第 10 天 / 共 15 天）
#           阈值、能耗、远程控制、分组标签、网关管理与采集点位演示数据灌入、
#           拉起 MQTT 订阅服务（或模拟采集器），并注册实时监控大屏、设备
#           接入、在线状态、实时参数采集、阈值告警中心、能耗统计、远程控制、
#           分组标签管理、物联网网关管理与采集点位转发十类页面路由，提供
#           设备注册增删改查、心跳上报、离线扫描识别、实时参数采集存储、
#           动态曲线、阈值扫描与告警处置、能耗分时采集、峰谷电费测算与节能
#           建议、远程指令模板维护、指令下发回执与重发、设备分组维护、入组
#           退组、标签维护与批量打标、网关档案维护、链路探测、远程重启、
#           固件升级与运行事件处置，以及采集点位档案维护启停、按网关批量
#           轮询采集、转发规则配置与转发调度执行 JSON 接口

from iot_collect import (
    build_collect_payload,
    build_collect_brief,
    save_collect_point,
    toggle_collect_point,
    remove_collect_point,
    run_collect_batch,
    save_forward_rule,
    toggle_forward_rule,
    remove_forward_rule,
    execute_forward_rules,
    seed_demo_collect,
    COLLECT_METRIC_DICT,
    POINT_VALUE_DICT,
    POINT_STATE_DICT,
    COLLECT_TASK_DICT,
    FORWARD_TARGET_DICT,
    FORWARD_MODE_DICT,
    FORWARD_RULE_DICT,
)

# 【函数内插入】def api_monitor():
#    # 第 10 天新增：大屏叠加采集点位摘要，供采集概况卡渲染
#    payload["collect_brief"] = build_collect_brief()

# ==================================================================
# 以下为第 10 天新增：数据采集点位与转发规则页面与接口
# ==================================================================


@app.route("/iot/collect")
def page_collect():
    """采集点位与转发规则页面：采集指标、五类图表、点位档案与转发规则。"""
    return render_template(
        "page_collect.html",
        software_name=SOFTWARE_FULL_NAME,
        refresh_interval=REFRESH_INTERVAL_SEC,
    )


@app.route("/api/iot/collect/panel")
def api_collect_panel():
    """采集点位数据接口：汇总指标、五类图表、点位清单、转发规则与批次留痕。"""
    return jsonify(build_collect_payload(
        keyword=(request.args.get("keyword") or "").strip(),
        device_code=(request.args.get("device_code") or "").strip(),
        metric_key=(request.args.get("metric_key") or "").strip(),
        point_state=(request.args.get("point_state") or "").strip(),
    ))


@app.route("/api/iot/collect/point", methods=["POST"])
def api_collect_point_save():
    """采集点位接口：登记或修改一条采集点位档案。"""
    body = request.get_json(silent=True) or {}
    result = save_collect_point(body)
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/collect/point/toggle", methods=["POST"])
def api_collect_point_toggle():
    """采集点位启停接口：停用后不再参与批量采集。"""
    body = request.get_json(silent=True) or {}
    result = toggle_collect_point(
        (body.get("point_code") or "").strip(),
        body.get("enable_flag", 1),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/collect/point/remove", methods=["POST"])
def api_collect_point_remove():
    """删除采集点位及其关联的转发规则。"""
    body = request.get_json(silent=True) or {}
    result = remove_collect_point((body.get("point_code") or "").strip())
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/collect/run", methods=["POST"])
def api_collect_run():
    """执行一次批量采集：按网关轮询启用点位并登记采集批次留痕。"""
    body = request.get_json(silent=True) or {}
    result = run_collect_batch(
        (body.get("gateway_code") or "").strip(),
        (body.get("trigger_mode") or "MANUAL").strip(),
        (body.get("operator_name") or "").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/collect/rule", methods=["POST"])
def api_forward_rule_save():
    """转发规则接口：登记或修改一条数据转发规则。"""
    body = request.get_json(silent=True) or {}
    result = save_forward_rule(body)
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/collect/rule/toggle", methods=["POST"])
def api_forward_rule_toggle():
    """转发规则启停接口：暂停后不再参与转发调度。"""
    body = request.get_json(silent=True) or {}
    result = toggle_forward_rule(
        (body.get("rule_code") or "").strip(),
        body.get("enable_flag", 1),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/collect/rule/remove", methods=["POST"])
def api_forward_rule_remove():
    """删除一条数据转发规则。"""
    body = request.get_json(silent=True) or {}
    result = remove_forward_rule((body.get("rule_code") or "").strip())
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/collect/forward", methods=["POST"])
def api_forward_execute():
    """执行一次转发调度：按周期与变化阈值判定并外送采集数据。"""
    result = execute_forward_rules()
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/collect/options")
def api_collect_options():
    """采集点位下拉选项接口：采集指标、数值类型、点位状态与目标系统字典。"""
    return jsonify({
        "result": "OK",
        "metric_options": [
            {"code": k, "label": v[0], "unit": v[1], "factor": v[2]}
            for k, v in COLLECT_METRIC_DICT.items()
        ],
        "value_options": [{"code": k, "label": v}
                          for k, v in POINT_VALUE_DICT.items()],
        "state_options": [{"code": k, "label": v}
                          for k, v in POINT_STATE_DICT.items()],
        "task_options": [{"code": k, "label": v}
                         for k, v in COLLECT_TASK_DICT.items()],
        "target_options": [{"code": k, "label": v}
                           for k, v in FORWARD_TARGET_DICT.items()],
        "mode_options": [{"code": k, "label": v}
                         for k, v in FORWARD_MODE_DICT.items()],
        "rule_options": [{"code": k, "label": v}
                         for k, v in FORWARD_RULE_DICT.items()],
    })



# 【函数内插入】def bootstrap():
#    seed_demo_collect()

# 【函数内插入】if __name__ == "__main__":
#    print("采集点位： http://127.0.0.1:%d/iot/collect" % PORT)

# --------------------------------------------------------------------------
# 第 10 天新增/修改的前端资源清单（文件本体见工程 templates/、static/ 目录）
# --------------------------------------------------------------------------

#   [新增] static/iot_collect.js            新增 785 行
#   [修改] templates/iot_base.html          新增 1 行
#   [新增] templates/page_collect.html      新增 321 行
