# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-11-04（第 15 天 / 共 15 天）
# 模块功能：本文件仅含该软件第 15 天新增与修改的源代码，不含第 1~14 天已有代码
#           本次迭代主题：新增边缘计算任务管理模块（任务编排、排队下发、五类就地计算、超时重试取消、运行记录追溯）
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）
# 接入说明：以下内容为当日增量，需与前面各天的代码合并后运行——
#           新增模块按文件名放入工程目录；建表脚本增量追加进对应 .sql；
#           主程序增量片段插入 app.py 中 Flask 实例之后；前端资源按文末清单
#           放入 templates/ 与 static/ 目录


# --------------------------------------------------------------------------
# 第 15 天新增业务模块 1：iot_edge.py（完整源码）
# --------------------------------------------------------------------------

# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-11-04（第 15 天 / 共 15 天）
# 模块功能：边缘计算任务管理模块。实现边缘任务建档（目标网关、任务类型、触发
#           方式、点位范围、聚合窗口、批量条数、优先级、超时与重试上限）、任务
#           排队与下发、按任务类型在真实遥测数据上就地计算（数据清洗剔除越界
#           点、窗口聚合压缩原始点位、断点补传回填缺失窗口、本地缓存生成快照、
#           告警预处理筛选历史告警）、执行耗时与超时判定、失败重试与取消编排、
#           运行记录逐次留存；形成「任务编排 → 排队下发 → 边缘就地计算 → 结果
#           回传 → 失败重试 / 取消」的边缘算力任务闭环；输出任务类型分布、任务
#           状态分布、网关任务量排行、近 7 日执行趋势与输入输出对比五类统计图表
# 依赖包：sqlite3（Python 内置）、datetime（Python 内置）、time（Python 内置）

import datetime
import time

from iot_store import connect_db, dict_list, now_text, fetch_gateway_rows

# ------------------------------------------------------------------
# 任务类型字典：边缘侧五类就地计算任务
# ------------------------------------------------------------------
TASK_TYPE_DICT = {
    "CLEAN": "数据清洗",
    "AGGREGATE": "窗口聚合",
    "BACKFILL": "断点补传",
    "CACHE": "本地缓存",
    "PRECHECK": "告警预处理",
}

# 触发方式字典
TRIGGER_MODE_DICT = {
    "SCHEDULE": "周期调度",
    "EVENT": "事件触发",
    "MANUAL": "人工下发",
}

# 优先级字典
PRIORITY_LEVEL_DICT = {
    "HIGH": "高",
    "MID": "中",
    "LOW": "低",
}

# 任务状态字典：覆盖编排、排队、执行与三类终态
TASK_STATE_DICT = {
    "DRAFT": "待编排",
    "QUEUED": "已排队",
    "RUNNING": "执行中",
    "SUCCESS": "执行成功",
    "FAIL": "执行失败",
    "TIMEOUT": "执行超时",
    "CANCEL": "已取消",
}

# 运行状态字典
RUN_STATE_DICT = {
    "SUCCESS": "成功",
    "FAIL": "失败",
    "TIMEOUT": "超时",
}

# 编排动作字典
ACTION_TYPE_DICT = {
    "CREATE": "建档",
    "DISPATCH": "下发",
    "RETRY": "重试",
    "CANCEL": "取消",
}

# 可再次下发的任务状态（排队 / 待编排 / 失败 / 超时）
DISPATCH_ALLOW_STATES = ("DRAFT", "QUEUED", "FAIL", "TIMEOUT")

# 遥测指标正常区间，用于清洗任务判定越界点
TELEMETRY_RANGE_MAP = {
    "temp": (18.0, 42.0),
    "volt": (200.0, 250.0),
    "power": (50.0, 1600.0),
    "humid": (35.0, 70.0),
    "curr": (5.0, 25.0),
}

# 边缘计算取数窗口（小时）
EDGE_WINDOW_HOURS = 24

# 断点补传的基准采集节拍（秒）
BACKFILL_TICK_SEC = 60

# 运行记录分页尺寸
RUN_PAGE_SIZE = 8

# 最近日志展示条数
LOG_SHOW_LIMIT = 12

# 演示任务清单
# （任务名称，网关序号，任务类型，触发方式，点位范围，窗口秒数，批量条数，
#   优先级，超时秒数，重试上限，目标状态）
DEMO_TASK_LIST = (
    ("重症监护遥测数据清洗", 0, "CLEAN", "SCHEDULE", "", 300, 800, "HIGH", 60, 2,
     "SUCCESS"),
    ("检验设备功率窗口聚合", 2, "AGGREGATE", "SCHEDULE", "power", 600, 500, "HIGH",
     60, 2, "SUCCESS"),
    ("手术中心断点数据补传", 1, "BACKFILL", "EVENT", "", 60, 1000, "MID", 90, 3,
     "SUCCESS"),
    ("重症告警边缘预处理", 0, "PRECHECK", "EVENT", "", 60, 300, "HIGH", 30, 2,
     "SUCCESS"),
    ("检验中心边缘缓存刷新", 2, "CACHE", "MANUAL", "", 120, 400, "LOW", 45, 1,
     "QUEUED"),
    ("急救设备清洗策略调优", 1, "CLEAN", "SCHEDULE", "temp,volt", 300, 600, "MID",
     60, 2, "CANCEL"),
    ("检验中心温湿度聚合（超时）", 2, "AGGREGATE", "SCHEDULE", "humid", 900, 1200,
     "MID", 5, 3, "TIMEOUT"),
    ("手术中心电流点位聚合（待编排）", 1, "AGGREGATE", "MANUAL", "curr", 300, 500,
     "LOW", 60, 2, "DRAFT"),
    ("手术中心湿度清洗（取数失败）", 1, "CLEAN", "SCHEDULE", "humid", 300, 400,
     "MID", 30, 2, "FAIL"),
)

# 演示遥测补齐参数：（指标键，基准值，波动步长，单位）
DEMO_METRIC_PROFILE = (
    ("temp", 33.0, 0.9, "℃"),
    ("power", 320.0, 42.0, "W"),
    ("volt", 224.0, 2.4, "V"),
)


# ------------------------------------------------------------------
# 通用小工具
# ------------------------------------------------------------------
def parse_time(text):
    """把 'YYYY-MM-DD HH:MM:SS' 文本解析为 datetime，异常时返回当前时间。"""
    try:
        return datetime.datetime.strptime(str(text)[:19], "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return datetime.datetime.now()


def shift_days_text(days):
    """返回相对今天偏移若干天的日期文本。"""
    return (datetime.date.today() + datetime.timedelta(days=days)).strftime(
        "%Y-%m-%d")


def shift_hours_text(hours):
    """返回相对当前时间偏移若干小时的时间文本。"""
    return (datetime.datetime.now() + datetime.timedelta(hours=hours)).strftime(
        "%Y-%m-%d %H:%M:%S")


def round1(value):
    """数值保留一位小数，非数值按 0 处理。"""
    try:
        return round(float(value or 0), 1)
    except (TypeError, ValueError):
        return 0.0


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


def next_serial_in_conn(conn, table_name, code_column, prefix):
    """在当前连接（含未提交插入）内取下一个流水序号。"""
    row = conn.execute(
        "SELECT %s FROM %s WHERE %s LIKE ? ORDER BY %s DESC LIMIT 1"
        % (code_column, table_name, code_column, code_column),
        (prefix + "%",)).fetchone()
    if not row:
        return 1
    try:
        return int(row[code_column].rsplit("-", 1)[-1]) + 1
    except (ValueError, IndexError, AttributeError, TypeError):
        return 1


def build_task_code():
    """生成边缘任务编号，形如 ET-000001。"""
    return "ET-%06d" % next_serial("iot_edge_task", "task_code", "ET-")


def build_run_code_in_conn(conn):
    """在当前连接内生成运行记录编号，形如 ER-000001。"""
    return "ER-%06d" % next_serial_in_conn(
        conn, "iot_edge_run", "run_code", "ER-")


def build_log_code_in_conn(conn):
    """在当前连接内生成任务日志编号，形如 EL-000001。"""
    return "EL-%06d" % next_serial_in_conn(
        conn, "iot_edge_log", "log_code", "EL-")


def fetch_task_row(task_code):
    """按任务编号读取一条边缘任务，不存在返回 None。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_edge_task WHERE task_code=?",
            ((task_code or "").strip(),)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def fetch_gateway_name(gateway_code):
    """按网关编号取网关名称，查不到返回空串。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT gateway_name FROM iot_gateway_node WHERE gateway_code=?",
            (gateway_code or "",)).fetchone()
    finally:
        conn.close()
    return row["gateway_name"] if row else ""


def fetch_gateway_devices(conn, gateway_code):
    """读取网关下挂接的设备清单。"""
    rows = conn.execute(
        "SELECT device_code, device_name FROM iot_device_node "
        "WHERE gateway_code=? ORDER BY device_code", (gateway_code or "",)
    ).fetchall()
    return dict_list(rows)


def fetch_metric_keys():
    """读取遥测库中出现过的指标键，供点位范围下拉使用。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT DISTINCT metric_key FROM iot_telemetry_log "
            "ORDER BY metric_key").fetchall()
    finally:
        conn.close()
    return [row["metric_key"] for row in rows]


# ------------------------------------------------------------------
# 任务建档
# ------------------------------------------------------------------
def save_edge_task(task_name, gateway_code, task_type="CLEAN",
                   trigger_mode="SCHEDULE", point_scope="", window_sec=300,
                   batch_size=500, priority_level="MID", plan_time="",
                   timeout_sec=60, max_retry=2, remark_text="", operator="admin"):
    """新建一条边缘计算任务并按参数决定初始状态。

    任务名与目标网关必填；网关须已登记；类型 / 触发方式 / 优先级须在字典内；
    窗口秒数 30~3600、批量条数 10~5000、超时 5~600 秒、重试上限 0~5。
    计划时间为空则记录为立即执行，任务直接进入排队状态，否则落为待编排。

    :return: 含 success、message 与 task_code 的结果字典
    """
    name = (task_name or "").strip()
    if not name:
        return {"success": False, "message": "任务名称不能为空"}
    gateway = (gateway_code or "").strip()
    if not gateway:
        return {"success": False, "message": "请选择目标网关"}
    gateway_name = fetch_gateway_name(gateway)
    if not gateway_name:
        return {"success": False, "message": "网关 %s 未登记，请先在拓扑中接入" % gateway}
    if task_type not in TASK_TYPE_DICT:
        return {"success": False, "message": "任务类型不合法"}
    if trigger_mode not in TRIGGER_MODE_DICT:
        return {"success": False, "message": "触发方式不合法"}
    if priority_level not in PRIORITY_LEVEL_DICT:
        return {"success": False, "message": "优先级不合法"}
    try:
        window_value = int(window_sec)
        batch_value = int(batch_size)
        timeout_value = int(timeout_sec)
        retry_value = int(max_retry)
    except (TypeError, ValueError):
        return {"success": False, "message": "窗口、批量、超时与重试须为整数"}
    if window_value < 30 or window_value > 3600:
        return {"success": False, "message": "聚合窗口须在 30~3600 秒之间"}
    if batch_value < 10 or batch_value > 5000:
        return {"success": False, "message": "批量条数须在 10~5000 之间"}
    if timeout_value < 5 or timeout_value > 600:
        return {"success": False, "message": "执行超时须在 5~600 秒之间"}
    if retry_value < 0 or retry_value > 5:
        return {"success": False, "message": "重试上限须在 0~5 之间"}
    scope = ",".join([item.strip() for item in (point_scope or "").split(",")
                      if item.strip()])
    unknown = [key for key in scope.split(",") if key and
               key not in TELEMETRY_RANGE_MAP]
    if unknown:
        return {"success": False,
                "message": "点位 %s 不在指标字典中" % "、".join(unknown)}

    state = "QUEUED" if not (plan_time or "").strip() else "DRAFT"
    code = build_task_code()
    conn = connect_db()
    try:
        conn.execute(
            "INSERT INTO iot_edge_task (task_code, task_name, gateway_code, "
            "gateway_name, task_type, trigger_mode, point_scope, window_sec, "
            "batch_size, priority_level, plan_time, timeout_sec, max_retry, "
            "retry_times, task_state, remark_text, operator_name) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?)",
            (code, name, gateway, gateway_name, task_type, trigger_mode, scope,
             window_value, batch_value, priority_level, (plan_time or "").strip(),
             timeout_value, retry_value, state, (remark_text or "").strip(),
             (operator or "admin").strip()))
        write_log(conn, code, name, "CREATE",
                  "建档边缘任务：%s / %s / %s" % (
                      TASK_TYPE_DICT[task_type], TRIGGER_MODE_DICT[trigger_mode],
                      gateway_name), operator)
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "边缘任务 %s 已建档（%s）" % (code, TASK_STATE_DICT[state]),
            "task_code": code}


def write_log(conn, task_code, task_name, action_type, detail, operator):
    """在当前连接内写一条边缘编排日志。"""
    conn.execute(
        "INSERT INTO iot_edge_log (log_code, task_code, task_name, action_type, "
        "action_detail, operator_name, log_time) VALUES (?,?,?,?,?,?,?)",
        (build_log_code_in_conn(conn), task_code, task_name or "", action_type,
         detail or "", (operator or "admin").strip(), now_text()))


# ------------------------------------------------------------------
# 边缘就地计算引擎
# ------------------------------------------------------------------
def fetch_edge_rows(conn, device_codes, point_scope, batch_size):
    """按窗口与点位范围读取待处理的遥测数据（按时间倒序截断到批量上限）。"""
    if not device_codes:
        return []
    holders = ",".join(["?"] * len(device_codes))
    sql = ("SELECT telemetry_id, device_code, metric_key, metric_value, "
           "collect_time FROM iot_telemetry_log WHERE device_code IN (%s) "
           "AND collect_time >= datetime('now','localtime','-%d hours')"
           % (holders, EDGE_WINDOW_HOURS))
    args = [item["device_code"] for item in device_codes]
    keys = [key for key in (point_scope or "").split(",") if key]
    if keys:
        sql += " AND metric_key IN (%s)" % ",".join(["?"] * len(keys))
        args.extend(keys)
    sql += " ORDER BY telemetry_id DESC LIMIT ?"
    args.append(int(batch_size))
    return dict_list(conn.execute(sql, args).fetchall())


def count_alarm_rows(conn, device_codes):
    """统计网关下属设备近 24 小时的告警条数，供告警预处理任务取数。"""
    if not device_codes:
        return 0
    holders = ",".join(["?"] * len(device_codes))
    row = conn.execute(
        "SELECT COUNT(1) AS cnt FROM iot_alarm_record WHERE device_code IN (%s) "
        "AND alarm_time >= datetime('now','localtime','-%d hours')"
        % (holders, EDGE_WINDOW_HOURS),
        [item["device_code"] for item in device_codes]).fetchone()
    return int(row["cnt"] or 0) if row else 0


def compute_clean(rows):
    """数据清洗：按指标正常区间剔除越界点，输出合规点位条数。"""
    outliers = 0
    for row in rows:
        limit = TELEMETRY_RANGE_MAP.get(row["metric_key"])
        if not limit:
            continue
        try:
            value = float(row["metric_value"])
        except (TypeError, ValueError):
            outliers += 1
            continue
        if value < limit[0] or value > limit[1]:
            outliers += 1
    total = len(rows)
    return {"input": total, "output": total - outliers, "backfill": 0,
            "drop": outliers,
            "detail": "剔除越界点 %d 条，保留合规点位 %d 条" % (
                outliers, total - outliers)}


def compute_aggregate(rows, window_sec):
    """窗口聚合：按窗口秒数把原始点位压缩为窗口均值快照。"""
    step = max(30, int(window_sec or 300))
    buckets = set()
    for row in rows:
        stamp = parse_time(row["collect_time"])
        buckets.add((row["metric_key"], int(stamp.timestamp()) // step))
    total = len(rows)
    output = len(buckets)
    return {"input": total, "output": output, "backfill": 0,
            "drop": max(0, total - output),
            "detail": "按 %d 秒窗口聚合，原始 %d 条压缩为 %d 个窗口快照" % (
                step, total, output)}


def compute_backfill(rows):
    """断点补传：以 60 秒为节拍统计缺失窗口并回填。"""
    observed = set()
    moments = []
    for row in rows:
        stamp = parse_time(row["collect_time"])
        moments.append(stamp)
        observed.add((row["device_code"], row["metric_key"],
                      stamp.strftime("%Y-%m-%d %H:%M")))
    if not moments:
        return {"input": 0, "output": 0, "backfill": 0, "drop": 0,
                "detail": "无可用遥测点位"}
    span_minutes = max(1, int((max(moments) - min(moments)).total_seconds() / 60) + 1)
    devices = set(row["device_code"] for row in rows)
    metrics = set(row["metric_key"] for row in rows)
    expected = len(devices) * len(metrics) * span_minutes
    missing = max(0, expected - len(observed))
    total = len(rows)
    return {"input": total, "output": total + missing, "backfill": missing,
            "drop": 0,
            "detail": "覆盖 %d 分钟节拍，应到 %d 点、实到 %d 点，补传 %d 点" % (
                span_minutes, expected, len(observed), missing)}


def compute_cache(rows):
    """本地缓存：按设备与指标生成缓存快照条目。"""
    pairs = set((row["device_code"], row["metric_key"]) for row in rows)
    total = len(rows)
    return {"input": total, "output": len(pairs), "backfill": 0, "drop": 0,
            "detail": "生成 %d 个设备指标缓存快照（原始 %d 条）" % (
                len(pairs), total)}


def run_edge_compute(task, rows, device_codes, conn):
    """按任务类型调度对应的边缘计算算子，返回输入输出统计。"""
    task_type = task["task_type"]
    if task_type == "CLEAN":
        return compute_clean(rows)
    if task_type == "AGGREGATE":
        return compute_aggregate(rows, task["window_sec"])
    if task_type == "BACKFILL":
        return compute_backfill(rows)
    if task_type == "CACHE":
        return compute_cache(rows)
    alarms = count_alarm_rows(conn, device_codes)
    return {"input": len(rows), "output": alarms, "backfill": 0, "drop": 0,
            "detail": "预处理近 24 小时告警 %d 条，筛出待复核项" % alarms}


# ------------------------------------------------------------------
# 任务下发 / 重试 / 取消
# ------------------------------------------------------------------
def execute_edge_task(task_code, action_type="DISPATCH", operator="admin"):
    """下发一次边缘任务：取真实遥测数据就地计算并留存运行记录。

    网关下须有绑定设备，且近 24 小时有可处理遥测数据；执行耗时超过任务超时
    门限则判为超时，计算异常或无数据判为失败。

    :return: 含 success、message 与运行结果的结果字典
    """
    task = fetch_task_row(task_code)
    if task is None:
        return {"success": False, "message": "边缘任务不存在"}
    state = task["task_state"]
    if action_type == "DISPATCH" and state not in DISPATCH_ALLOW_STATES:
        return {"success": False,
                "message": "任务当前状态为「%s」，不可再度下发" % TASK_STATE_DICT.get(
                    state, state)}
    if action_type == "RETRY":
        if state not in ("FAIL", "TIMEOUT"):
            return {"success": False, "message": "仅执行失败或超时的任务可重试"}
        if int(task["retry_times"] or 0) >= int(task["max_retry"] or 0):
            return {"success": False,
                    "message": "已达重试上限 %d 次，请调整参数后重新建档" % int(
                        task["max_retry"] or 0)}

    conn = connect_db()
    try:
        devices = fetch_gateway_devices(conn, task["gateway_code"])
        if not devices:
            return {"success": False,
                    "message": "网关 %s 下无绑定设备，无法执行边缘任务" % task["gateway_code"]}
        rows = fetch_edge_rows(conn, devices, task["point_scope"],
                               task["batch_size"])
        start_text = now_text()
        begin = time.time()
        if not rows:
            cost_ms = max(1, int((time.time() - begin) * 1000))
            result = {"input": 0, "output": 0, "backfill": 0, "drop": 0,
                      "detail": "近 %d 小时无可处理遥测数据" % EDGE_WINDOW_HOURS}
            run_state = "FAIL"
        else:
            try:
                result = run_edge_compute(task, rows, devices, conn)
                run_state = "SUCCESS"
            except Exception as error:  # noqa: BLE001 - 边缘算子异常统一落失败
                cost_ms = max(1, int((time.time() - begin) * 1000))
                result = {"input": len(rows), "output": 0, "backfill": 0,
                          "drop": 0, "detail": "算子执行异常：%s" % error}
                run_state = "FAIL"
            else:
                cost_ms = max(1, int((time.time() - begin) * 1000))
                if cost_ms > int(task["timeout_sec"] or 0) * 1000:
                    run_state = "TIMEOUT"
                    result["detail"] = "%s；耗时 %d ms 超过 %d 秒门限" % (
                        result["detail"], cost_ms, int(task["timeout_sec"] or 0))

        run_code = build_run_code_in_conn(conn)
        conn.execute(
            "INSERT INTO iot_edge_run (run_code, task_code, task_name, "
            "gateway_code, task_type, run_state, input_rows, output_rows, "
            "backfill_rows, drop_rows, cost_ms, start_time, end_time, exit_text) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_code, task["task_code"], task["task_name"], task["gateway_code"],
             task["task_type"], run_state, int(result["input"]),
             int(result["output"]), int(result["backfill"]), int(result["drop"]),
             cost_ms, start_text, now_text(), result["detail"]))
        retry_times = int(task["retry_times"] or 0)
        if action_type == "RETRY":
            retry_times += 1
        conn.execute(
            "UPDATE iot_edge_task SET task_state=?, input_rows=?, output_rows=?, "
            "cost_ms=?, last_run_at=?, result_text=?, retry_times=? "
            "WHERE task_code=?",
            (run_state, int(result["input"]), int(result["output"]), cost_ms,
             now_text(), result["detail"], retry_times, task["task_code"]))
        write_log(conn, task["task_code"], task["task_name"], action_type,
                  "%s %s：%s（%s，耗时 %d ms）" % (
                      ACTION_TYPE_DICT.get(action_type, action_type),
                      run_code, result["detail"],
                      RUN_STATE_DICT.get(run_state, run_state), cost_ms), operator)
        conn.commit()
    finally:
        conn.close()

    label = RUN_STATE_DICT.get(run_state, run_state)
    if run_state == "SUCCESS":
        message = "任务 %s 执行成功，%s，耗时 %d ms" % (
            task["task_code"], result["detail"], cost_ms)
    else:
        message = "任务 %s 执行%s：%s" % (
            task["task_code"], label, result["detail"])
    return {"success": run_state == "SUCCESS", "message": message,
            "run_code": run_code, "run_state": run_state, "cost_ms": cost_ms,
            "input_rows": int(result["input"]), "output_rows": int(result["output"])}


def cancel_edge_task(task_code, operator="admin"):
    """取消一条尚未成功归档的边缘任务，并写入编排日志。"""
    task = fetch_task_row(task_code)
    if task is None:
        return {"success": False, "message": "边缘任务不存在"}
    if task["task_state"] == "CANCEL":
        return {"success": False, "message": "任务已处于取消状态"}
    if task["task_state"] == "SUCCESS":
        return {"success": False, "message": "已成功执行的任务不可取消"}
    conn = connect_db()
    try:
        conn.execute(
            "UPDATE iot_edge_task SET task_state='CANCEL' WHERE task_code=?",
            (task["task_code"],))
        write_log(conn, task["task_code"], task["task_name"], "CANCEL",
                  "取消任务，原状态 %s" % TASK_STATE_DICT.get(
                      task["task_state"], task["task_state"]), operator)
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "任务 %s 已取消" % task["task_code"]}


# ------------------------------------------------------------------
# 查询与统计
# ------------------------------------------------------------------
def fetch_edge_tasks(gateway_code="", task_type="", task_state="",
                     priority_level="", keyword=""):
    """按网关、类型、状态、优先级与关键字筛选边缘任务。"""
    sql = "SELECT * FROM iot_edge_task WHERE 1=1"
    args = []
    if gateway_code:
        sql += " AND gateway_code=?"
        args.append(gateway_code)
    if task_type:
        sql += " AND task_type=?"
        args.append(task_type)
    if task_state:
        sql += " AND task_state=?"
        args.append(task_state)
    if priority_level:
        sql += " AND priority_level=?"
        args.append(priority_level)
    if keyword:
        sql += (" AND (task_code LIKE ? OR task_name LIKE ? "
                "OR gateway_name LIKE ?)")
        like = "%" + keyword.strip() + "%"
        args.extend([like, like, like])
    sql += " ORDER BY task_id DESC"
    conn = connect_db()
    try:
        rows = conn.execute(sql, args).fetchall()
    finally:
        conn.close()
    result = []
    for row in dict_list(rows):
        item = dict(row)
        item["type_label"] = TASK_TYPE_DICT.get(row["task_type"], row["task_type"])
        item["trigger_label"] = TRIGGER_MODE_DICT.get(row["trigger_mode"],
                                                      row["trigger_mode"])
        item["priority_label"] = PRIORITY_LEVEL_DICT.get(row["priority_level"],
                                                         row["priority_level"])
        item["state_label"] = TASK_STATE_DICT.get(row["task_state"], row["task_state"])
        item["scope_label"] = row["point_scope"] or "全部点位"
        result.append(item)
    return result


def fetch_edge_runs(task_code="", run_state="", keyword="", page=1):
    """分页读取边缘任务运行记录，返回记录列表与分页信息。"""
    where = " WHERE 1=1"
    args = []
    if task_code:
        where += " AND task_code=?"
        args.append(task_code)
    if run_state:
        where += " AND run_state=?"
        args.append(run_state)
    if keyword:
        where += " AND (run_code LIKE ? OR task_name LIKE ? OR exit_text LIKE ?)"
        like = "%" + keyword.strip() + "%"
        args.extend([like, like, like])
    conn = connect_db()
    try:
        total = conn.execute(
            "SELECT COUNT(1) AS cnt FROM iot_edge_run" + where, args).fetchone()["cnt"]
        try:
            page_no = max(1, int(page))
        except (TypeError, ValueError):
            page_no = 1
        page_amount = max(1, (int(total) + RUN_PAGE_SIZE - 1) // RUN_PAGE_SIZE)
        page_no = min(page_no, page_amount)
        rows = conn.execute(
            "SELECT * FROM iot_edge_run" + where +
            " ORDER BY run_id DESC LIMIT ? OFFSET ?",
            args + [RUN_PAGE_SIZE, (page_no - 1) * RUN_PAGE_SIZE]).fetchall()
    finally:
        conn.close()
    records = []
    for row in dict_list(rows):
        item = dict(row)
        item["state_label"] = RUN_STATE_DICT.get(row["run_state"], row["run_state"])
        item["type_label"] = TASK_TYPE_DICT.get(row["task_type"], row["task_type"])
        records.append(item)
    return {"records": records, "page": page_no, "page_amount": page_amount,
            "total": int(total)}


def fetch_edge_logs(task_code=""):
    """读取最近若干条边缘编排日志。"""
    sql = "SELECT * FROM iot_edge_log"
    args = []
    if task_code:
        sql += " WHERE task_code=?"
        args.append(task_code)
    sql += " ORDER BY elog_id DESC LIMIT ?"
    args.append(LOG_SHOW_LIMIT)
    conn = connect_db()
    try:
        rows = conn.execute(sql, args).fetchall()
    finally:
        conn.close()
    records = []
    for row in dict_list(rows):
        item = dict(row)
        item["action_label"] = ACTION_TYPE_DICT.get(row["action_type"],
                                                    row["action_type"])
        records.append(item)
    return records


def build_edge_summary():
    """统计边缘任务概览指标：任务量、状态分布、运行量、耗时与数据压缩比。"""
    conn = connect_db()
    try:
        task_rows = conn.execute(
            "SELECT task_state, COUNT(1) AS cnt, SUM(input_rows) AS input_sum, "
            "SUM(output_rows) AS output_sum, AVG(cost_ms) AS avg_cost "
            "FROM iot_edge_task GROUP BY task_state").fetchall()
        run_row = conn.execute(
            "SELECT COUNT(1) AS cnt, SUM(input_rows) AS input_sum, "
            "SUM(output_rows) AS output_sum, SUM(backfill_rows) AS backfill_sum, "
            "SUM(drop_rows) AS drop_sum, AVG(cost_ms) AS avg_cost "
            "FROM iot_edge_run").fetchone()
        gateway_row = conn.execute(
            "SELECT COUNT(DISTINCT gateway_code) AS gw FROM iot_edge_task").fetchone()
    finally:
        conn.close()

    state_map = {}
    for row in task_rows:
        state_map[row["task_state"]] = int(row["cnt"] or 0)
    task_total = sum(state_map.values())
    input_total = int(run_row["input_sum"] or 0)
    output_total = int(run_row["output_sum"] or 0)
    run_total = int(run_row["cnt"] or 0)
    success_run = 0
    conn = connect_db()
    try:
        success_run = int(conn.execute(
            "SELECT COUNT(1) AS cnt FROM iot_edge_run WHERE run_state='SUCCESS'"
        ).fetchone()["cnt"] or 0)
    finally:
        conn.close()
    compress = round1(output_total * 100.0 / input_total) if input_total else 0.0
    return {
        "task_total": task_total,
        "draft_task": state_map.get("DRAFT", 0),
        "queued_task": state_map.get("QUEUED", 0),
        "running_task": state_map.get("RUNNING", 0),
        "success_task": state_map.get("SUCCESS", 0),
        "fail_task": state_map.get("FAIL", 0),
        "timeout_task": state_map.get("TIMEOUT", 0),
        "cancel_task": state_map.get("CANCEL", 0),
        "run_total": run_total,
        "success_run": success_run,
        "fail_run": run_total - success_run,
        "avg_cost": round1(run_row["avg_cost"] or 0),
        "input_total": input_total,
        "output_total": output_total,
        "backfill_total": int(run_row["backfill_sum"] or 0),
        "drop_total": int(run_row["drop_sum"] or 0),
        "compress_rate": compress,
        "success_rate": round1(success_run * 100.0 / run_total) if run_total else 0.0,
        "gateway_total": int(gateway_row["gw"] or 0),
    }


def build_edge_charts():
    """构建边缘任务五张统计图表数据。"""
    conn = connect_db()
    try:
        type_rows = conn.execute(
            "SELECT task_type, COUNT(1) AS cnt FROM iot_edge_task "
            "GROUP BY task_type ORDER BY cnt DESC").fetchall()
        state_rows = conn.execute(
            "SELECT task_state, COUNT(1) AS cnt FROM iot_edge_task "
            "GROUP BY task_state").fetchall()
        gateway_rows = conn.execute(
            "SELECT gateway_code, COUNT(1) AS cnt FROM iot_edge_task "
            "GROUP BY gateway_code ORDER BY cnt DESC LIMIT 6").fetchall()
        task_rows = conn.execute(
            "SELECT task_code, task_name, input_rows, output_rows FROM iot_edge_task "
            "WHERE input_rows > 0 ORDER BY input_rows DESC LIMIT 6").fetchall()
        run_rows = conn.execute(
            "SELECT substr(start_time, 1, 10) AS day, COUNT(1) AS cnt, "
            "SUM(input_rows) AS input_sum FROM iot_edge_run "
            "GROUP BY day ORDER BY day DESC LIMIT 7").fetchall()
    finally:
        conn.close()

    type_distribution = [
        {"name": TASK_TYPE_DICT.get(row["task_type"], row["task_type"]),
         "value": int(row["cnt"] or 0)} for row in type_rows]
    state_distribution = [
        {"name": TASK_STATE_DICT.get(row["task_state"], row["task_state"]),
         "value": int(row["cnt"] or 0)} for row in state_rows]
    gateway_rank = [
        {"name": row["gateway_code"], "value": int(row["cnt"] or 0)}
        for row in gateway_rows]
    io_compare = [
        {"name": row["task_code"], "task_name": row["task_name"],
         "input": int(row["input_rows"] or 0),
         "output": int(row["output_rows"] or 0)} for row in task_rows]
    trend_rows = list(reversed(run_rows))
    day_trend = {
        "days": [row["day"][5:] if row["day"] else "" for row in trend_rows],
        "run_values": [int(row["cnt"] or 0) for row in trend_rows],
        "input_values": [int(row["input_sum"] or 0) for row in trend_rows],
    }
    return {"type_distribution": type_distribution,
            "state_distribution": state_distribution,
            "gateway_rank": gateway_rank,
            "io_compare": io_compare,
            "day_trend": day_trend}


def fetch_edge_options():
    """构建页面下拉选项：网关、任务类型、触发方式、状态、优先级与点位。"""
    gateways = [{"code": row["gateway_code"],
                 "label": row["gateway_code"] + " · " + (row["gateway_name"] or "")}
                for row in fetch_gateway_rows()]
    metric_list = [{"code": key, "label": key} for key in fetch_metric_keys()]
    return {
        "gateway_list": gateways,
        "type_options": [{"code": k, "label": v} for k, v in TASK_TYPE_DICT.items()],
        "trigger_options": [{"code": k, "label": v}
                            for k, v in TRIGGER_MODE_DICT.items()],
        "state_options": [{"code": k, "label": v} for k, v in TASK_STATE_DICT.items()],
        "priority_options": [{"code": k, "label": v}
                             for k, v in PRIORITY_LEVEL_DICT.items()],
        "run_options": [{"code": k, "label": v} for k, v in RUN_STATE_DICT.items()],
        "action_options": [{"code": k, "label": v}
                           for k, v in ACTION_TYPE_DICT.items()],
        "dispatch_actions": [
            {"code": "DISPATCH", "label": "下发执行"},
            {"code": "RETRY", "label": "失败重试"},
            {"code": "CANCEL", "label": "取消任务"},
        ],
        "metric_list": metric_list,
    }


def build_edge_payload(gateway_code="", task_type="", task_state="",
                       priority_level="", keyword="", run_task="",
                       run_state="", page="1"):
    """组装边缘任务面板数据：概览、图表、任务表、运行记录、日志与选项。"""
    summary = build_edge_summary()
    charts = build_edge_charts()
    tasks = fetch_edge_tasks(gateway_code, task_type, task_state, priority_level,
                             keyword)
    run_page = fetch_edge_runs(run_task, run_state, "", page)
    logs = fetch_edge_logs(run_task)
    return {
        "result": "OK",
        "summary": summary,
        "charts": charts,
        "tasks": tasks,
        "runs": run_page["records"],
        "run_total": run_page["total"],
        "page": run_page["page"],
        "page_amount": run_page["page_amount"],
        "logs": logs,
        "options": fetch_edge_options(),
    }


def build_edge_brief():
    """边缘计算任务摘要，供实时监控大屏嵌入。"""
    summary = build_edge_summary()
    return {
        "task_total": summary["task_total"],
        "run_total": summary["run_total"],
        "success_rate": summary["success_rate"],
        "avg_cost": summary["avg_cost"],
        "input_total": summary["input_total"],
        "compress_rate": summary["compress_rate"],
        "backfill_total": summary["backfill_total"],
    }


# ------------------------------------------------------------------
# 演示数据
# ------------------------------------------------------------------
def ensure_edge_demo_telemetry():
    """为缺少遥测样本的演示设备补齐近 3 小时点位，保证边缘任务可计算。"""
    conn = connect_db()
    try:
        devices = dict_list(conn.execute(
            "SELECT device_code, gateway_code FROM iot_device_node "
            "ORDER BY node_id").fetchall())
        added = 0
        for device in devices:
            count = conn.execute(
                "SELECT COUNT(1) AS cnt FROM iot_telemetry_log WHERE device_code=?",
                (device["device_code"],)).fetchone()["cnt"]
            if int(count or 0) >= 12:
                continue
            base = datetime.datetime.now() - datetime.timedelta(minutes=170)
            rows = []
            for step in range(12):
                stamp = (base + datetime.timedelta(minutes=step * 15)).strftime(
                    "%Y-%m-%d %H:%M:%S")
                for index, (key, base_value, wave, unit) in enumerate(
                        DEMO_METRIC_PROFILE):
                    value = round(base_value + ((step + index) % 5) * wave, 2)
                    rows.append((device["device_code"], device["gateway_code"],
                                 key, value, unit, stamp))
            conn.executemany(
                "INSERT INTO iot_telemetry_log (device_code, gateway_code, "
                "metric_key, metric_value, metric_unit, collect_time) "
                "VALUES (?,?,?,?,?,?)", rows)
            added += len(rows)
        conn.commit()
    finally:
        conn.close()
    return added


def seed_edge_history(task_code, task_name, gateway_code, task_type):
    """为演示任务回填近 6 日运行记录，支撑趋势图与运行量统计。"""
    conn = connect_db()
    try:
        start_no = next_serial_in_conn(conn, "iot_edge_run", "run_code", "ER-")
        for offset in range(6, 0, -1):
            day = shift_days_text(-offset)
            runs = 1 + offset % 3
            for seq in range(runs):
                input_rows = 120 + offset * 24 + seq * 15
                output_rows = max(1, int(input_rows * (0.72 + (seq % 3) * 0.04)))
                conn.execute(
                    "INSERT INTO iot_edge_run (run_code, task_code, task_name, "
                    "gateway_code, task_type, run_state, input_rows, output_rows, "
                    "backfill_rows, drop_rows, cost_ms, start_time, end_time, "
                    "exit_text) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("ER-%06d" % start_no, task_code, task_name, gateway_code,
                     task_type, "SUCCESS", input_rows, output_rows,
                     input_rows - output_rows if task_type == "BACKFILL" else 0,
                     max(0, input_rows - output_rows) if task_type != "BACKFILL"
                     else 0,
                     30 + offset * 6 + seq * 4,
                     day + " 02:%02d:00" % (10 + seq * 5),
                     day + " 02:%02d:12" % (10 + seq * 5),
                     "历史批次：处理点位 %d 条" % input_rows))
                start_no += 1
        conn.commit()
    finally:
        conn.close()


def seed_demo_edge():
    """灌入边缘计算任务演示数据：补齐遥测、建八类任务、真实执行下发、
    回填历史运行记录，并保留排队 / 取消 / 超时 / 待编排四种状态用于演示。

    :return: 生成的边缘任务条数
    """
    conn = connect_db()
    try:
        already = conn.execute(
            "SELECT COUNT(1) AS cnt FROM iot_edge_task").fetchone()["cnt"]
        if int(already or 0) > 0:
            return 0
    finally:
        conn.close()

    ensure_edge_demo_telemetry()
    gateways = fetch_gateway_rows()
    if not gateways:
        return 0

    created = 0
    timed_out_code = ""
    for (name, gw_index, task_type, trigger, scope, window, batch, priority,
         timeout, retry, target) in DEMO_TASK_LIST:
        gateway = gateways[gw_index % len(gateways)]
        plan_time = "" if target in ("SUCCESS", "FAIL", "TIMEOUT", "QUEUED") else \
            shift_days_text(1) + " 08:00"
        saved = save_edge_task(name, gateway["gateway_code"], task_type, trigger,
                               scope, window, batch, priority, plan_time, timeout,
                               retry, "演示边缘算力任务", "物联中心-尹工")
        if not saved.get("success"):
            continue
        created += 1
        code = saved["task_code"]
        if target in ("SUCCESS", "FAIL", "TIMEOUT"):
            execute_edge_task(code, "DISPATCH", "物联中心-尹工")
            seed_edge_history(code, name, gateway["gateway_code"], task_type)
        if target == "CANCEL":
            cancel_edge_task(code, "物联中心-尹工")
        if target == "TIMEOUT":
            timed_out_code = code

    # 超时任务：改写最近一次运行与任务状态，演示算力不足导致的门限超时
    if timed_out_code:
        conn = connect_db()
        try:
            row = conn.execute(
                "SELECT run_id FROM iot_edge_run WHERE task_code=? "
                "ORDER BY run_id DESC LIMIT 1", (timed_out_code,)).fetchone()
            if row:
                conn.execute(
                    "UPDATE iot_edge_run SET run_state='TIMEOUT', cost_ms=5200, "
                    "exit_text='边缘节点算力不足，任务在 5 秒门限内未完成' "
                    "WHERE run_id=?", (row["run_id"],))
            conn.execute(
                "UPDATE iot_edge_task SET task_state='TIMEOUT', cost_ms=5200, "
                "result_text='边缘节点算力不足，任务在 5 秒门限内未完成', "
                "last_run_at=? WHERE task_code=?",
                (now_text(), timed_out_code))
            conn.commit()
        finally:
            conn.close()
    return created

# --------------------------------------------------------------------------
# 第 15 天数据表脚本增量：iot_schema.sql（M，已内联为字符串常量 DAY15_SCHEMA_SQL）
# --------------------------------------------------------------------------

DAY15_SCHEMA_SQL = r"""
-- ==================================================================
-- 第 15 天新增：边缘计算任务三表
-- ==================================================================
-- 边缘任务表：按「一个任务一条」登记任务名称、目标网关、任务类型（数据清洗、
-- 窗口聚合、断点补传、本地缓存、告警预处理）、触发方式、点位范围、窗口秒数、
-- 批量条数、优先级、计划时间、超时秒数与重试上限，记录最近一次执行的输入输出
-- 条数、耗时与结果，支撑边缘侧算力任务的编排与下发
CREATE TABLE IF NOT EXISTS iot_edge_task (
    task_id        INTEGER PRIMARY KEY AUTOINCREMENT, -- 任务主键
    task_code      TEXT    NOT NULL UNIQUE,           -- 任务编号（ET-序号）
    task_name      TEXT    DEFAULT '',                -- 任务名称
    gateway_code   TEXT    DEFAULT '',                -- 目标网关编号
    gateway_name   TEXT    DEFAULT '',                -- 网关名称快照
    task_type      TEXT    DEFAULT 'CLEAN',           -- 任务类型（见 TASK_TYPE_DICT）
    trigger_mode   TEXT    DEFAULT 'SCHEDULE',        -- 触发方式（见 TRIGGER_MODE_DICT）
    point_scope    TEXT    DEFAULT '',                -- 点位范围（指标键，逗号分隔，空为全部）
    window_sec     INTEGER DEFAULT 300,               -- 聚合窗口（秒）
    batch_size     INTEGER DEFAULT 500,               -- 单批处理条数上限
    priority_level TEXT    DEFAULT 'MID',             -- 优先级（见 PRIORITY_LEVEL_DICT）
    plan_time      TEXT    DEFAULT '',                -- 计划执行时间
    timeout_sec    INTEGER DEFAULT 60,                -- 执行超时（秒）
    max_retry      INTEGER DEFAULT 2,                 -- 重试上限
    retry_times    INTEGER DEFAULT 0,                 -- 已重试次数
    task_state     TEXT    DEFAULT 'DRAFT',           -- 任务状态（见 TASK_STATE_DICT）
    input_rows     INTEGER DEFAULT 0,                 -- 最近一次输入条数
    output_rows    INTEGER DEFAULT 0,                 -- 最近一次输出条数
    cost_ms        INTEGER DEFAULT 0,                 -- 最近一次耗时（毫秒）
    last_run_at    TEXT    DEFAULT '',                -- 最近执行时间
    result_text    TEXT    DEFAULT '',                -- 最近执行结果说明
    remark_text    TEXT    DEFAULT '',                -- 任务说明
    operator_name  TEXT    DEFAULT '',                -- 创建人
    created_at     TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- 边缘任务运行记录表：任务每下发一次生成一条运行记录，逐次留存输入条数、
-- 输出条数、补传条数、剔除条数、耗时与退出信息，支撑执行质量追溯
CREATE TABLE IF NOT EXISTS iot_edge_run (
    run_id        INTEGER PRIMARY KEY AUTOINCREMENT,  -- 运行记录主键
    run_code      TEXT    NOT NULL UNIQUE,            -- 运行编号（ER-序号）
    task_code     TEXT    DEFAULT '',                 -- 所属任务编号
    task_name     TEXT    DEFAULT '',                 -- 任务名称快照
    gateway_code  TEXT    DEFAULT '',                 -- 执行网关编号
    task_type     TEXT    DEFAULT '',                 -- 任务类型快照
    run_state     TEXT    DEFAULT 'SUCCESS',          -- 运行状态（见 RUN_STATE_DICT）
    input_rows    INTEGER DEFAULT 0,                  -- 输入条数
    output_rows   INTEGER DEFAULT 0,                  -- 输出条数
    backfill_rows INTEGER DEFAULT 0,                  -- 补传条数
    drop_rows     INTEGER DEFAULT 0,                  -- 剔除条数
    cost_ms       INTEGER DEFAULT 0,                  -- 执行耗时（毫秒）
    start_time    TEXT    DEFAULT '',                 -- 开始时间
    end_time      TEXT    DEFAULT '',                 -- 结束时间
    exit_text     TEXT    DEFAULT '',                 -- 退出信息
    created_at    TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- 边缘任务日志表：记录任务建档、下发、重试、取消等动作，形成编排留痕
CREATE TABLE IF NOT EXISTS iot_edge_log (
    elog_id       INTEGER PRIMARY KEY AUTOINCREMENT,  -- 日志主键
    log_code      TEXT    NOT NULL UNIQUE,            -- 日志编号（EL-序号）
    task_code     TEXT    DEFAULT '',                 -- 所属任务编号
    task_name     TEXT    DEFAULT '',                 -- 任务名称快照
    action_type   TEXT    DEFAULT 'CREATE',           -- 动作类型（CREATE/DISPATCH/RETRY/CANCEL）
    action_detail TEXT    DEFAULT '',                 -- 动作说明
    operator_name TEXT    DEFAULT '',                 -- 操作人
    log_time      TEXT    DEFAULT '',                 -- 动作时间
    created_at    TEXT    DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_etask_code      ON iot_edge_task (task_code);
CREATE INDEX IF NOT EXISTS idx_etask_gateway   ON iot_edge_task (gateway_code);
CREATE INDEX IF NOT EXISTS idx_etask_type      ON iot_edge_task (task_type);
CREATE INDEX IF NOT EXISTS idx_etask_state     ON iot_edge_task (task_state);
CREATE INDEX IF NOT EXISTS idx_etask_priority  ON iot_edge_task (priority_level);
CREATE INDEX IF NOT EXISTS idx_erun_code       ON iot_edge_run (run_code);
CREATE INDEX IF NOT EXISTS idx_erun_task       ON iot_edge_run (task_code);
CREATE INDEX IF NOT EXISTS idx_erun_state      ON iot_edge_run (run_state);
CREATE INDEX IF NOT EXISTS idx_erun_time       ON iot_edge_run (start_time);
CREATE INDEX IF NOT EXISTS idx_elog_code       ON iot_edge_log (log_code);
CREATE INDEX IF NOT EXISTS idx_elog_task       ON iot_edge_log (task_code);
CREATE INDEX IF NOT EXISTS idx_elog_action     ON iot_edge_log (action_type);
"""


def apply_day15_schema(conn):
    """把第 15 天新增的数据表脚本应用到已有数据库。"""
    conn.executescript(DAY15_SCHEMA_SQL)
    conn.commit()
    return True

# --------------------------------------------------------------------------
# 第 15 天主程序新增路由与接口：app.py（仅增量片段）
# --------------------------------------------------------------------------

from iot_edge import (
    build_edge_payload,
    build_edge_brief,
    save_edge_task,
    execute_edge_task,
    cancel_edge_task,
    seed_demo_edge,
    TASK_TYPE_DICT,
    TRIGGER_MODE_DICT,
    PRIORITY_LEVEL_DICT,
    TASK_STATE_DICT,
    RUN_STATE_DICT,
    ACTION_TYPE_DICT,
)

# 【函数内插入】def api_monitor():
#    # 第 15 天新增：大屏叠加边缘计算任务摘要，供边缘算力概况卡渲染
#    payload["edge_brief"] = build_edge_brief()

# ==================================================================
# 以下为第 15 天新增：边缘计算任务管理页面与接口
# ==================================================================


@app.route("/iot/edge")
def page_edge():
    """边缘计算任务页面：任务编排、下发执行与运行记录追溯（第 15 天）。"""
    return render_template(
        "page_edge.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        refresh_interval=REFRESH_INTERVAL_SEC,
    )


@app.route("/api/iot/edge/panel")
def api_edge_panel():
    """边缘任务面板数据接口：概览、图表、任务表、运行记录与编排日志。"""
    payload = build_edge_payload(
        gateway_code=request.args.get("gateway_code", ""),
        task_type=request.args.get("task_type", ""),
        task_state=request.args.get("task_state", ""),
        priority_level=request.args.get("priority_level", ""),
        keyword=request.args.get("keyword", ""),
        run_task=request.args.get("run_task", ""),
        run_state=request.args.get("run_state", ""),
        page=request.args.get("page", "1"),
    )
    return jsonify(payload)


@app.route("/api/iot/edge/task/save", methods=["POST"])
def api_edge_task_save():
    """边缘任务建档接口：校验网关、类型与参数区间后落库排队。"""
    body = request.get_json(silent=True) or {}
    result = save_edge_task(
        body.get("task_name") or "",
        body.get("gateway_code") or "",
        body.get("task_type") or "CLEAN",
        body.get("trigger_mode") or "SCHEDULE",
        body.get("point_scope") or "",
        body.get("window_sec") if body.get("window_sec") is not None else 300,
        body.get("batch_size") if body.get("batch_size") is not None else 500,
        body.get("priority_level") or "MID",
        body.get("plan_time") or "",
        body.get("timeout_sec") if body.get("timeout_sec") is not None else 60,
        body.get("max_retry") if body.get("max_retry") is not None else 2,
        body.get("remark_text") or "",
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/edge/task/action", methods=["POST"])
def api_edge_task_action():
    """任务编排接口：下发执行 / 失败重试 / 取消任务三选一。"""
    body = request.get_json(silent=True) or {}
    action = (body.get("action_type") or "DISPATCH").strip().upper()
    operator = (body.get("operator") or "admin").strip()
    task_code = body.get("task_code") or ""
    if action == "CANCEL":
        result = cancel_edge_task(task_code, operator)
    elif action in ("DISPATCH", "RETRY"):
        result = execute_edge_task(task_code, action, operator)
    else:
        result = {"success": False, "message": "编排动作不合法"}
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/edge/options")
def api_edge_options():
    """边缘任务下拉选项接口：网关、类型、触发方式、状态、优先级与动作。"""
    from iot_edge import fetch_edge_options
    payload = fetch_edge_options()
    payload["result"] = "OK"
    return jsonify(payload)



# 【函数内插入】def bootstrap():
#    seed_demo_edge()

# 【函数内插入】if __name__ == "__main__":
#    print("健康评估： http://127.0.0.1:%d/iot/health" % PORT)
#    print("边缘计算： http://127.0.0.1:%d/iot/edge" % PORT)

# --------------------------------------------------------------------------
# 第 15 天新增/修改的前端资源清单（文件本体见工程 templates/、static/ 目录）
# --------------------------------------------------------------------------

#   [新增] static/iot_edge.js               新增 515 行
#   [修改] templates/iot_base.html          新增 1 行
#   [新增] templates/page_edge.html         新增 289 行
