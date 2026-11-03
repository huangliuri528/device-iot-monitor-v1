# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-11-03（第 14 天 / 共 15 天）
# 模块功能：本文件仅含该软件第 14 天新增与修改的源代码，不含第 1~13 天已有代码
#           本次迭代主题：新增设备健康度评估与预测性维护模块（四维模型、健康评估、劣化预警、维保建议闭环）
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）
# 接入说明：以下内容为当日增量，需与前面各天的代码合并后运行——
#           新增模块按文件名放入工程目录；建表脚本增量追加进对应 .sql；
#           主程序增量片段插入 app.py 中 Flask 实例之后；前端资源按文末清单
#           放入 templates/ 与 static/ 目录


# --------------------------------------------------------------------------
# 第 14 天新增业务模块 1：iot_health.py（完整源码）
# --------------------------------------------------------------------------

# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-11-03（第 14 天 / 共 15 天）
# 模块功能：设备健康度评估与预测性维护模块。实现健康评估模型建档（在线状态、
#           遥测稳定、告警健康、链路质量四维权重与健康等级阈值）、按设备实时
#           运行状态、遥测参数、告警记录与心跳链路数据计算四维得分与综合健康
#           分、健康等级判定、较上轮劣化幅度预警、按最低分维度自动生成预测性
#           维护建议、建议处置闭环与全量批量重评估；模型权重之和须为 100，
#           仅启用状态模型参与评估，形成「模型配置 → 多维取数 → 健康评估 →
#           劣化预警 → 维保建议 → 处置跟踪」的设备健康管理闭环；输出健康等级
#           分布、维保建议类型分布、设备健康分排行、四维得分对比与近 7 日健康
#           分趋势五类统计图表
# 依赖包：sqlite3（Python 内置）、datetime（Python 内置）

import datetime

from iot_store import connect_db, dict_list, now_text, fetch_device_nodes

# ------------------------------------------------------------------
# 健康等级字典：按综合健康分划分四档
# ------------------------------------------------------------------
HEALTH_LEVEL_DICT = {
    "EXCELLENT": "健康",
    "GOOD": "良好",
    "WARN": "预警",
    "FAULT": "劣化",
}

# 模型状态字典
MODEL_STATE_DICT = {
    "ON": "已启用",
    "OFF": "已停用",
}

# 评估维度字典：（维度键，维度名称）
HEALTH_DIM_LIST = (
    ("online", "在线状态"),
    ("telemetry", "遥测稳定"),
    ("alarm", "告警健康"),
    ("link", "链路质量"),
)

# 预测性维护建议类型字典：按触发维度匹配维保动作
ADVICE_TYPE_DICT = {
    "INSPECT": "现场巡检",
    "CALIB": "计量校准",
    "REPAIR": "故障检修",
    "REPLACE": "部件更换",
    "MAINTAIN": "预防保养",
}

# 建议优先级字典
ADVICE_PRIORITY_DICT = {
    "HIGH": "高",
    "MID": "中",
    "LOW": "低",
}

# 建议处置状态字典
ADVICE_STATE_DICT = {
    "OPEN": "待处置",
    "DOING": "处置中",
    "CLOSED": "已关闭",
}

# 运行状态得分映射：ONLINE 满分，其余按可用性递减
RUN_STATE_SCORE_MAP = {
    "ONLINE": 100.0,
    "SLEEP": 70.0,
    "FAULT": 35.0,
    "OFFLINE": 20.0,
}

# 遥测指标正常区间：（下限，上限），用于判定遥测稳定性
TELEMETRY_RANGE_MAP = {
    "temp": (18.0, 42.0),
    "volt": (200.0, 250.0),
    "power": (50.0, 1600.0),
    "humid": (35.0, 70.0),
    "curr": (5.0, 25.0),
}

# 单设备参与评估的最近遥测条数
TELEMETRY_SAMPLE_LIMIT = 30

# 单次评估最多抽取的设备台数
MAX_EVAL_DEVICE = 20

# 劣化预警阈值：较上轮健康分下降达到该分值即标记劣化
DEGRADE_DROP_LINE = 5.0

# 心跳新鲜阈值（分钟）：超过该时长未心跳视为链路失联
HEARTBEAT_FRESH_MINUTES = 5

# 分页尺寸：评估记录独立分页
SCORE_PAGE_SIZE = 10

# 演示模型清单
# （模型名称，适用设备类型，四维权重，等级阈值，状态，说明）
DEMO_MODEL_LIST = (
    ("全院设备通用健康评估模型", "", (35, 25, 25, 15), (90, 75, 60), "ON",
     "适用全部设备，四维加权综合评估"),
    ("检验类设备专项健康模型", "检验分析类", (30, 30, 25, 15), (90, 76, 62), "ON",
     "检验设备侧重遥测稳定性"),
    ("急救设备高可靠评估模型", "治疗救护类", (40, 20, 28, 12), (92, 78, 65), "OFF",
     "急救设备高可靠口径，停用待复核"),
)

# 维度到建议类型的映射：低分维度触发对应维保动作
DIM_ADVICE_MAP = {
    "online": ("REPAIR", "安排现场开机检查与供电排查，恢复设备在线"),
    "telemetry": ("CALIB", "安排计量校准与传感器校验，消除参数漂移"),
    "alarm": ("REPAIR", "排查历史告警根因，执行针对性故障检修"),
    "link": ("REPLACE", "检查通信模块与电池，必要时更换备件"),
}


def round1(value):
    """数值保留一位小数，空值与非数值按 0 处理。"""
    try:
        return round(float(value or 0), 1)
    except (TypeError, ValueError):
        return 0.0


def round2(value):
    """数值保留两位小数，空值与非数值按 0 处理。"""
    try:
        return round(float(value or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def today_text():
    """返回今天日期文本 YYYY-MM-DD。"""
    return datetime.date.today().strftime("%Y-%m-%d")


def shift_days_text(days):
    """返回相对今天偏移指定天数的日期文本。"""
    return (datetime.date.today() + datetime.timedelta(days=days)).strftime("%Y-%m-%d")


def clamp_score(value):
    """把健康分限制在 0 ~ 100 区间内。"""
    return max(0.0, min(100.0, float(value or 0)))


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


def build_model_code():
    """生成评估模型编号，形如 HM-0001。"""
    return "HM-%04d" % next_serial("iot_health_model", "model_code", "HM-")


def next_serial_in_conn(conn, table_name, code_column, prefix):
    """在当前连接（含未提交插入）内查询某编号前缀下的下一个流水序号。"""
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


def build_score_code_in_conn(conn):
    """在当前连接内生成评估记录编号，形如 HS-000001。"""
    return "HS-%06d" % next_serial_in_conn(
        conn, "iot_health_score", "score_code", "HS-")


def build_advice_code_in_conn(conn):
    """在当前连接内生成维保建议编号，形如 HA-000001。"""
    return "HA-%06d" % next_serial_in_conn(
        conn, "iot_health_advice", "advice_code", "HA-")


def judge_health_level(score, model):
    """按模型阈值判定健康等级。"""
    value = float(score or 0)
    if value >= float(model["excellent_line"] or 90):
        return "EXCELLENT"
    if value >= float(model["good_line"] or 75):
        return "GOOD"
    if value >= float(model["warn_line"] or 60):
        return "WARN"
    return "FAULT"


# ------------------------------------------------------------------
# 查询辅助
# ------------------------------------------------------------------
def fetch_model_row(model_code):
    """按模型编号读取一条评估模型，不存在返回 None。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_health_model WHERE model_code=?",
            ((model_code or "").strip(),)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def fetch_latest_score_row(device_code, model_code=""):
    """读取某设备在指定模型下最近一次评估记录，不存在返回 None。"""
    sql = "SELECT * FROM iot_health_score WHERE device_code=? "
    args = [device_code]
    if model_code:
        sql += "AND model_code=? "
        args.append(model_code)
    conn = connect_db()
    try:
        row = conn.execute(
            sql + "ORDER BY score_id DESC LIMIT 1", args).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def collect_online_score(device):
    """按设备运行状态计算在线状态维度得分。"""
    return RUN_STATE_SCORE_MAP.get((device.get("run_state") or "").strip(), 50.0)


def collect_telemetry_score(device_code, conn):
    """按设备最近若干条遥测值落在正常区间的比例计算遥测稳定维度得分。

    :return: (遥测得分, 参与评估的遥测条数)
    """
    rows = conn.execute(
        "SELECT metric_key, metric_value FROM iot_telemetry_log "
        "WHERE device_code=? ORDER BY telemetry_id DESC LIMIT ?",
        (device_code, TELEMETRY_SAMPLE_LIMIT)).fetchall()
    if not rows:
        return 60.0, 0
    normal_amount = 0
    for row in rows:
        limit = TELEMETRY_RANGE_MAP.get(row["metric_key"])
        if limit is None:
            normal_amount += 1
        elif limit[0] <= float(row["metric_value"] or 0) <= limit[1]:
            normal_amount += 1
    return round1(normal_amount * 100.0 / len(rows)), len(rows)


def collect_alarm_score(device_code, conn):
    """按设备近 30 日未处置告警数量计算告警健康维度得分。

    :return: (告警得分, 累计告警条数)
    """
    day_line = shift_days_text(-30) + " 00:00:00"
    rows = conn.execute(
        "SELECT handle_state FROM iot_alarm_record WHERE device_code=? "
        "AND (?='' OR alarm_time>=?)",
        (device_code, day_line, day_line)).fetchall()
    open_amount = sum(1 for row in rows
                      if (row["handle_state"] or "") in ("OPEN", "CONFIRM"))
    score = clamp_score(100.0 - open_amount * 18.0 - len(rows) * 3.0)
    return round1(score), len(rows)


def collect_link_score(device_code, conn):
    """按最近心跳的信号强度、电量与连续运行时长计算链路质量维度得分。"""
    row = conn.execute(
        "SELECT rssi_value, battery_value, uptime_sec FROM iot_heartbeat_log "
        "WHERE device_code=? ORDER BY beat_id DESC LIMIT 1",
        (device_code,)).fetchone()
    if row is None:
        return 45.0
    rssi = float(row["rssi_value"] or 0)
    battery = float(row["battery_value"] or 0)
    # RSSI -55dBm 记满分，每弱 1dBm 扣 1.5 分；电量按百分比直接折算
    rssi_score = clamp_score(100.0 - max(0.0, -rssi - 55.0) * 1.5)
    battery_score = clamp_score(battery * 1.4)
    uptime_score = 100.0 if float(row["uptime_sec"] or 0) > 3600 else 70.0
    return round1(rssi_score * 0.4 + battery_score * 0.4 + uptime_score * 0.2)


# ------------------------------------------------------------------
# 模型建档
# ------------------------------------------------------------------
def save_health_model(model_code, model_name, device_type="",
                      w_online=35, w_telemetry=25, w_alarm=25, w_link=15,
                      excellent_line=90, good_line=75, warn_line=60,
                      model_state="ON", remark_text="", operator="admin"):
    """新建或编辑一套健康评估模型，四维权重之和必须等于 100。

    :return: 含 success、message 与 model_code 的执行结果字典
    """
    model_name = (model_name or "").strip()
    if not model_name:
        return {"success": False, "message": "请填写评估模型名称"}
    if model_state not in MODEL_STATE_DICT:
        return {"success": False, "message": "模型状态 %s 不合法" % model_state}
    try:
        weights = (float(w_online), float(w_telemetry),
                   float(w_alarm), float(w_link))
    except (TypeError, ValueError):
        return {"success": False, "message": "权重须为数值"}
    if any(value < 0 for value in weights):
        return {"success": False, "message": "权重不可为负数"}
    if abs(sum(weights) - 100.0) > 0.001:
        return {"success": False,
                "message": "四维权重之和须为 100，当前为 %s" % round1(sum(weights))}
    try:
        lines = (float(excellent_line), float(good_line), float(warn_line))
    except (TypeError, ValueError):
        return {"success": False, "message": "等级阈值须为数值"}
    if not (lines[0] > lines[1] > lines[2] > 0):
        return {"success": False, "message": "等级阈值须满足 健康 > 良好 > 预警 > 0"}

    conn = connect_db()
    try:
        exist = None
        if (model_code or "").strip():
            exist = conn.execute(
                "SELECT model_code FROM iot_health_model WHERE model_code=?",
                (model_code.strip(),)).fetchone()
        if exist is None:
            code = build_model_code()
            conn.execute(
                "INSERT INTO iot_health_model (model_code, model_name, "
                "device_type, w_online, w_telemetry, w_alarm, w_link, "
                "excellent_line, good_line, warn_line, model_state, "
                "remark_text, operator_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (code, model_name, (device_type or "").strip(), *weights,
                 *lines, model_state, (remark_text or "").strip(),
                 (operator or "admin").strip()))
        else:
            code = model_code.strip()
            conn.execute(
                "UPDATE iot_health_model SET model_name=?, device_type=?, "
                "w_online=?, w_telemetry=?, w_alarm=?, w_link=?, "
                "excellent_line=?, good_line=?, warn_line=?, model_state=?, "
                "remark_text=?, operator_name=? WHERE model_code=?",
                (model_name, (device_type or "").strip(), *weights, *lines,
                 model_state, (remark_text or "").strip(),
                 (operator or "admin").strip(), code))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "评估模型 %s 已保存" % code,
            "model_code": code}


def toggle_health_model(model_code, model_state="ON", operator="admin"):
    """切换评估模型的启用或停用状态。"""
    if model_state not in MODEL_STATE_DICT:
        return {"success": False, "message": "模型状态 %s 不合法" % model_state}
    if fetch_model_row(model_code) is None:
        return {"success": False, "message": "评估模型不存在"}
    conn = connect_db()
    try:
        conn.execute(
            "UPDATE iot_health_model SET model_state=?, operator_name=? "
            "WHERE model_code=?",
            (model_state, (operator or "admin").strip(),
             (model_code or "").strip()))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "评估模型 %s 已切换为%s" % (
                model_code, MODEL_STATE_DICT[model_state])}


# ------------------------------------------------------------------
# 健康评估与劣化预警
# ------------------------------------------------------------------
def evaluate_device_health(model, device, conn):
    """对单台设备执行一次健康评估，写入评估记录并返回结果字典。

    四维得分按模型权重加权得到综合健康分；同时读取该设备上一次评估健康
    分，计算劣化幅度，下降达到阈值时标记劣化预警。
    """
    online = collect_online_score(device)
    telemetry, telemetry_amount = collect_telemetry_score(
        device["device_code"], conn)
    alarm, alarm_amount = collect_alarm_score(device["device_code"], conn)
    link = collect_link_score(device["device_code"], conn)
    weight_sum = (float(model["w_online"]) + float(model["w_telemetry"]) +
                  float(model["w_alarm"]) + float(model["w_link"])) or 100.0
    health = round1(
        (online * float(model["w_online"]) +
         telemetry * float(model["w_telemetry"]) +
         alarm * float(model["w_alarm"]) +
         link * float(model["w_link"])) / weight_sum)
    health = round1(clamp_score(health))
    level = judge_health_level(health, model)

    previous = conn.execute(
        "SELECT health_score FROM iot_health_score WHERE device_code=? "
        "AND model_code=? ORDER BY score_id DESC LIMIT 1",
        (device["device_code"], model["model_code"])).fetchone()
    prev_score = float(previous["health_score"]) if previous else health
    drop_value = round1(health - prev_score)
    degrade = 1 if drop_value <= -DEGRADE_DROP_LINE else 0

    code = build_score_code_in_conn(conn)
    stamp = now_text()
    conn.execute(
        "INSERT INTO iot_health_score (score_code, model_code, device_code, "
        "device_name, device_type, ward_name, online_score, telemetry_score, "
        "alarm_score, link_score, health_score, health_level, drop_value, "
        "degrade_flag, telemetry_amount, alarm_amount, eval_date, eval_time) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (code, model["model_code"], device["device_code"], device["device_name"],
         device.get("device_type") or "", device.get("ward_name") or "",
         round1(online), telemetry, alarm, link, health, level, drop_value,
         degrade, telemetry_amount, alarm_amount, stamp[:10], stamp))
    return {"score_code": code, "health_score": health, "health_level": level,
            "drop_value": drop_value, "degrade_flag": degrade,
            "online_score": round1(online), "telemetry_score": telemetry,
            "alarm_score": alarm, "link_score": link,
            "device_code": device["device_code"],
            "device_name": device["device_name"]}


def generate_health_advice(model, eval_result, conn):
    """按评估结果的最低分维度生成一条预测性维护建议。

    健康/良好等级生成预防保养建议，预警/劣化等级按最低维度生成针对性
    维保动作；同一设备同日同类型建议不重复生成。
    """
    device_code = eval_result["device_code"]
    level = eval_result["health_level"]
    dim_scores = {
        "online": eval_result["online_score"],
        "telemetry": eval_result["telemetry_score"],
        "alarm": eval_result["alarm_score"],
        "link": eval_result["link_score"],
    }
    dim_key = min(dim_scores, key=lambda key: dim_scores[key])
    dim_label = dict(HEALTH_DIM_LIST).get(dim_key, dim_key)
    if level in ("EXCELLENT", "GOOD"):
        advice_type, advice_text = ("MAINTAIN",
                                    "按维保周期执行预防性保养与例行巡检")
        priority = "LOW"
    else:
        advice_type, advice_text = DIM_ADVICE_MAP.get(
            dim_key, ("INSPECT", "安排现场巡检排查设备隐患"))
        priority = "HIGH" if level == "FAULT" else "MID"

    day = (conn.execute("SELECT ? AS d", (today_text(),)).fetchone()["d"])
    dup = conn.execute(
        "SELECT COUNT(1) AS c FROM iot_health_advice WHERE device_code=? "
        "AND advice_type=? AND advise_state<>'CLOSED' "
        "AND substr(created_at,1,10)=?", (device_code, advice_type, day)
    ).fetchone()["c"]
    if dup:
        return None

    reason = "%s 维度得分 %.1f，综合健康分 %.1f（%s）" % (
        dim_label, dim_scores[dim_key], eval_result["health_score"],
        HEALTH_LEVEL_DICT[level])
    code = build_advice_code_in_conn(conn)
    conn.execute(
        "INSERT INTO iot_health_advice (advice_code, score_code, model_code, "
        "device_code, device_name, dim_key, dim_label, advice_type, "
        "priority_level, advice_text, reason_text, advise_state) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,'OPEN')",
        (code, eval_result["score_code"], model["model_code"], device_code,
         eval_result["device_name"], dim_key, dim_label, advice_type,
         priority, advice_text, reason))
    return code


def run_health_evaluation(model_code="", operator="admin"):
    """执行一次健康评估：按模型适配设备批量评估并生成维保建议。

    模型启用状态校验；未指定模型时取全部启用模型；每次评估写入设备评估
    记录，并按最低分维度自动生成预测性维护建议。

    :return: 含 success、message、eval_amount 与 advice_amount 的结果字典
    """
    conn = connect_db()
    try:
        if (model_code or "").strip():
            rows = conn.execute(
                "SELECT * FROM iot_health_model WHERE model_code=?",
                (model_code.strip(),)).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM iot_health_model WHERE model_state='ON' "
                "ORDER BY model_id ASC").fetchall()
    finally:
        conn.close()
    models = [dict(row) for row in rows]
    enabled = [model for model in models if model["model_state"] == "ON"]
    if not enabled:
        return {"success": False, "message": "没有可评估的启用模型"}

    device_list = fetch_device_nodes()
    conn = connect_db()
    try:
        eval_amount = 0
        advice_amount = 0
        for model in enabled:
            device_type = (model["device_type"] or "").strip()
            targets = [item for item in device_list
                       if not device_type or
                       (item.get("device_type") or "").strip() == device_type]
            for device in targets[:MAX_EVAL_DEVICE]:
                result = evaluate_device_health(model, device, conn)
                eval_amount += 1
                if generate_health_advice(model, result, conn) is not None:
                    advice_amount += 1
            conn.execute(
                "UPDATE iot_health_model SET eval_times=eval_times+1 "
                "WHERE model_code=?", (model["model_code"],))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "健康评估完成，评估 %d 台次，生成维保建议 %d 条" % (
                eval_amount, advice_amount),
            "eval_amount": eval_amount, "advice_amount": advice_amount}


def handle_health_advice(advice_code, advise_state="CLOSED", handle_note="",
                         handler_name="", operator="admin"):
    """处置一条预测性维护建议：填写处置说明并推进处置状态。"""
    if advise_state not in ADVICE_STATE_DICT:
        return {"success": False, "message": "处置状态 %s 不合法" % advise_state}
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_health_advice WHERE advice_code=?",
            ((advice_code or "").strip(),)).fetchone()
        if row is None:
            return {"success": False, "message": "维保建议不存在"}
        conn.execute(
            "UPDATE iot_health_advice SET advise_state=?, handle_note=?, "
            "handler_name=?, handle_time=? WHERE advice_code=?",
            (advise_state, (handle_note or "").strip(),
             (handler_name or "").strip(), now_text(), row["advice_code"]))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "维保建议 %s 已标记为%s" % (
                row["advice_code"], ADVICE_STATE_DICT[advise_state])}


# ------------------------------------------------------------------
# 分页查询
# ------------------------------------------------------------------
def fetch_health_models():
    """读取全部健康评估模型。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT * FROM iot_health_model ORDER BY model_id ASC").fetchall()
    finally:
        conn.close()
    records = []
    for item in rows:
        record = dict(item)
        record["state_label"] = MODEL_STATE_DICT.get(item["model_state"],
                                                     item["model_state"])
        record["weight_sum"] = round1(
            float(item["w_online"]) + float(item["w_telemetry"]) +
            float(item["w_alarm"]) + float(item["w_link"]))
        records.append(record)
    return records


def fetch_health_scores(device_code="", health_level="", model_code="",
                        degrade_only=False, page=1):
    """按条件分页查询健康评估记录，返回分页结果与记录列表。"""
    sql = "SELECT * FROM iot_health_score WHERE 1=1 "
    args = []
    if device_code:
        sql += "AND device_code=? "
        args.append(device_code)
    if health_level:
        sql += "AND health_level=? "
        args.append(health_level)
    if model_code:
        sql += "AND model_code=? "
        args.append(model_code)
    if degrade_only:
        sql += "AND degrade_flag=1 "
    conn = connect_db()
    try:
        total = conn.execute(
            sql.replace("SELECT *", "SELECT COUNT(1) AS c", 1), args).fetchone()["c"]
        page_amount = max(1, (total + SCORE_PAGE_SIZE - 1) // SCORE_PAGE_SIZE)
        try:
            page_no = max(1, min(int(page), page_amount))
        except (TypeError, ValueError):
            page_no = 1
        rows = conn.execute(
            sql + "ORDER BY score_id DESC LIMIT ? OFFSET ?",
            args + [SCORE_PAGE_SIZE, (page_no - 1) * SCORE_PAGE_SIZE]).fetchall()
    finally:
        conn.close()
    records = []
    for item in rows:
        record = dict(item)
        record["level_label"] = HEALTH_LEVEL_DICT.get(item["health_level"],
                                                     item["health_level"])
        records.append(record)
    return {"records": records, "total": total, "page": page_no,
            "page_amount": page_amount}


def fetch_health_advices(advice_type="", advise_state="", device_code="",
                         keyword="", limit=40):
    """按条件查询预测性维护建议，支持按类型、状态与设备过滤。"""
    sql = "SELECT * FROM iot_health_advice WHERE 1=1 "
    args = []
    if advice_type:
        sql += "AND advice_type=? "
        args.append(advice_type)
    if advise_state:
        sql += "AND advise_state=? "
        args.append(advise_state)
    if device_code:
        sql += "AND device_code=? "
        args.append(device_code)
    keyword = (keyword or "").strip()
    if keyword:
        sql += ("AND (advice_code LIKE ? OR device_name LIKE ? "
                "OR advice_text LIKE ? OR reason_text LIKE ?) ")
        like = "%%%s%%" % keyword
        args.extend([like, like, like, like])
    conn = connect_db()
    try:
        rows = conn.execute(
            sql + "ORDER BY advice_id DESC LIMIT ?", args + [limit]).fetchall()
    finally:
        conn.close()
    records = []
    for item in rows:
        record = dict(item)
        record["type_label"] = ADVICE_TYPE_DICT.get(item["advice_type"],
                                                    item["advice_type"])
        record["priority_label"] = ADVICE_PRIORITY_DICT.get(
            item["priority_level"], item["priority_level"])
        record["state_label"] = ADVICE_STATE_DICT.get(item["advise_state"],
                                                     item["advise_state"])
        records.append(record)
    return records


# ------------------------------------------------------------------
# 统计与图表
# ------------------------------------------------------------------
def build_health_summary():
    """汇总健康核心指标：模型量、设备量、评估量、平均健康分、劣化量、
    待处置建议与告警关联量。"""
    conn = connect_db()
    try:
        model_total = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_health_model").fetchone()["c"]
        on_model = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_health_model WHERE model_state='ON'"
        ).fetchone()["c"]
        score_total = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_health_score").fetchone()["c"]
        device_total = conn.execute(
            "SELECT COUNT(DISTINCT device_code) AS c FROM iot_health_score"
        ).fetchone()["c"]
        avg_row = conn.execute(
            "SELECT COALESCE(AVG(health_score),0) AS a, "
            "COALESCE(MIN(health_score),0) AS mn FROM iot_health_score"
        ).fetchone()
        degrade_amount = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_health_score WHERE degrade_flag=1"
        ).fetchone()["c"]
        advice_total = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_health_advice").fetchone()["c"]
        open_advice = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_health_advice "
            "WHERE advise_state='OPEN'").fetchone()["c"]
        closed_advice = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_health_advice "
            "WHERE advise_state='CLOSED'").fetchone()["c"]
        high_advice = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_health_advice "
            "WHERE priority_level='HIGH'").fetchone()["c"]
    finally:
        conn.close()
    return {
        "model_total": model_total,
        "on_model": on_model,
        "score_total": score_total,
        "device_total": device_total,
        "avg_score": round1(avg_row["a"]),
        "min_score": round1(avg_row["mn"]),
        "degrade_amount": degrade_amount,
        "advice_total": advice_total,
        "open_advice": open_advice,
        "closed_advice": closed_advice,
        "high_advice": high_advice,
        "close_rate": round1(closed_advice * 100.0 / advice_total)
        if advice_total else 0.0,
    }


def build_health_charts():
    """构建五类图表数据：健康等级分布、维保建议类型分布、设备健康分排行、
    四维得分对比与近 7 日健康分趋势。"""
    conn = connect_db()
    try:
        # 每台设备取最近一次评估，用于等级分布与设备排行
        latest_rows = conn.execute(
            "SELECT s.* FROM iot_health_score s JOIN "
            "(SELECT device_code, MAX(score_id) AS mid FROM iot_health_score "
            "GROUP BY device_code) t ON s.score_id=t.mid").fetchall()
        type_rows = conn.execute(
            "SELECT advice_type, COUNT(1) AS c FROM iot_health_advice "
            "GROUP BY advice_type").fetchall()
        dim_rows = conn.execute(
            "SELECT AVG(online_score) AS o, AVG(telemetry_score) AS t, "
            "AVG(alarm_score) AS a, AVG(link_score) AS l "
            "FROM iot_health_score").fetchall()
        day_rows = conn.execute(
            "SELECT eval_date, AVG(health_score) AS a, "
            "SUM(CASE WHEN degrade_flag=1 THEN 1 ELSE 0 END) AS d "
            "FROM iot_health_score GROUP BY eval_date "
            "ORDER BY eval_date ASC").fetchall()
    finally:
        conn.close()

    level_counter = {}
    for item in latest_rows:
        code = item["health_level"]
        level_counter[code] = level_counter.get(code, 0) + 1
    level_distribution = [
        {"name": HEALTH_LEVEL_DICT.get(code, code),
         "value": level_counter.get(code, 0)}
        for code in ("EXCELLENT", "GOOD", "WARN", "FAULT")
    ]

    type_counter = {item["advice_type"]: item["c"] for item in type_rows}
    advice_distribution = [
        {"name": ADVICE_TYPE_DICT.get(code, code),
         "value": type_counter.get(code, 0)}
        for code in ("INSPECT", "CALIB", "REPAIR", "REPLACE", "MAINTAIN")
    ]

    device_rank = [
        {"name": item["device_name"],
         "value": round1(item["health_score"]),
         "level": item["health_level"]}
        for item in latest_rows
    ]
    device_rank.sort(key=lambda x: x["value"], reverse=True)

    dim_row = dim_rows[0] if dim_rows else None
    dim_compare = [
        {"name": label,
         "value": round1(dim_row[key]) if dim_row else 0}
        for key, label in (("o", "在线状态"), ("t", "遥测稳定"),
                           ("a", "告警健康"), ("l", "链路质量"))
    ]

    day_keys = [shift_days_text(-offset) for offset in range(6, -1, -1)]
    score_map = {item["eval_date"]: round1(item["a"]) for item in day_rows}
    degrade_map = {item["eval_date"]: int(item["d"] or 0) for item in day_rows}

    return {
        "level_distribution": level_distribution,
        "advice_distribution": advice_distribution,
        "device_rank": device_rank[:8],
        "dim_compare": dim_compare,
        "day_trend": {
            "days": day_keys,
            "score_values": [score_map.get(key, 0) for key in day_keys],
            "degrade_values": [degrade_map.get(key, 0) for key in day_keys],
        },
    }


def fetch_health_options():
    """健康页面下拉选项：各类字典项、设备候选、模型候选与设备类型候选。"""
    device_list = fetch_device_nodes()
    device_types = []
    for item in device_list:
        dtype = (item.get("device_type") or "").strip()
        if dtype and dtype not in device_types:
            device_types.append(dtype)
    conn = connect_db()
    try:
        model_rows = conn.execute(
            "SELECT model_code, model_name FROM iot_health_model "
            "ORDER BY model_id DESC LIMIT 20").fetchall()
        device_rows = conn.execute(
            "SELECT DISTINCT device_code, device_name FROM iot_health_score "
            "ORDER BY device_code LIMIT 30").fetchall()
    finally:
        conn.close()
    return {
        "device_list": device_list,
        "device_type_list": device_types,
        "model_list": [dict(row) for row in model_rows],
        "score_device_list": [dict(row) for row in device_rows],
        "level_options": [{"code": k, "label": v}
                          for k, v in HEALTH_LEVEL_DICT.items()],
        "model_state_options": [{"code": k, "label": v}
                                for k, v in MODEL_STATE_DICT.items()],
        "advice_options": [{"code": k, "label": v}
                           for k, v in ADVICE_TYPE_DICT.items()],
        "priority_options": [{"code": k, "label": v}
                             for k, v in ADVICE_PRIORITY_DICT.items()],
        "advice_state_options": [{"code": k, "label": v}
                                 for k, v in ADVICE_STATE_DICT.items()],
    }


def build_health_payload(device_code="", health_level="", model_code="",
                         degrade_only=False, advice_type="", advise_state="",
                         advice_device="", advice_keyword="", page=1):
    """组装设备健康度评估与预测性维护页面所需的全部数据。"""
    pager = fetch_health_scores(device_code, health_level, model_code,
                                degrade_only, page)
    return {
        "summary": build_health_summary(),
        "charts": build_health_charts(),
        "models": fetch_health_models(),
        "scores": pager["records"],
        "score_total": pager["total"],
        "page": pager["page"],
        "page_amount": pager["page_amount"],
        "advices": fetch_health_advices(advice_type, advise_state,
                                        advice_device, advice_keyword),
        "options": fetch_health_options(),
        "result": "OK",
    }


def build_health_brief():
    """设备健康度看板摘要，供实时监控大屏嵌入。"""
    summary = build_health_summary()
    return {
        "device_total": summary["device_total"],
        "avg_score": summary["avg_score"],
        "degrade_amount": summary["degrade_amount"],
        "open_advice": summary["open_advice"],
        "on_model": summary["on_model"],
        "close_rate": summary["close_rate"],
    }


# ------------------------------------------------------------------
# 演示数据
# ------------------------------------------------------------------
def seed_demo_health():
    """灌入设备健康度演示数据：模型建档、批量评估、劣化预警、维保建议
    生成与建议处置，并补齐近 7 日健康分趋势样本。

    :return: 生成的评估模型条数
    """
    conn = connect_db()
    try:
        already = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_health_model").fetchone()["c"]
        if already:
            return 0
    finally:
        conn.close()

    for index, (model_name, device_type, weights, lines, state,
                remark) in enumerate(DEMO_MODEL_LIST):
        save_health_model(
            "", model_name, device_type, *weights, *lines, state,
            remark, "物联中心-尹工")

    # 近 7 日趋势：前 6 日按日期回填评估样本，形成健康分变化曲线
    device_list = fetch_device_nodes()
    default_model = None
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_health_model WHERE device_type='' "
            "AND model_state='ON' ORDER BY model_id ASC LIMIT 1").fetchone()
        if row is not None:
            default_model = dict(row)
    finally:
        conn.close()
    if default_model is not None:
        day_offsets = (-6, -5, -4, -3, -2, -1)
        for day_index, offset in enumerate(day_offsets):
            conn = connect_db()
            try:
                for device_index, device in enumerate(device_list[:MAX_EVAL_DEVICE]):
                    base = evaluate_device_health(default_model, device, conn)
                    # 用确定性扰动回填历史健康分，避免每日曲线完全重合
                    bias = (day_index - 5) * 1.4 + ((device_index * 3 +
                                                     day_index) % 5 - 2) * 0.7
                    history = round1(clamp_score(base["health_score"] + bias))
                    level = judge_health_level(history, default_model)
                    conn.execute(
                        "UPDATE iot_health_score SET health_score=?, "
                        "health_level=?, eval_date=?, eval_time=? "
                        "WHERE score_code=?",
                        (history, level, shift_days_text(offset),
                         shift_days_text(offset) + " 09:20:00",
                         base["score_code"]))
                conn.execute(
                    "UPDATE iot_health_model SET eval_times=eval_times+1 "
                    "WHERE model_code=?", (default_model["model_code"],))
                conn.commit()
            finally:
                conn.close()

    # 当日正式评估：全量生成评估记录与预测性维护建议
    run_health_evaluation("", "物联中心-尹工")

    # 处置一部分建议，演示待处置 / 处置中 / 已关闭三种状态
    conn = connect_db()
    try:
        open_rows = conn.execute(
            "SELECT advice_code, priority_level FROM iot_health_advice "
            "WHERE advise_state='OPEN' ORDER BY advice_id ASC").fetchall()
    finally:
        conn.close()
    for position, row in enumerate(open_rows):
        if position % 3 == 0:
            handle_health_advice(row["advice_code"], "CLOSED",
                                 "已完成现场巡检与处理，设备状态恢复正常",
                                 "设备科-沈工", "物联中心-尹工")
        elif position % 3 == 1:
            handle_health_advice(row["advice_code"], "DOING",
                                 "已派工，等待备件到货后完成检修",
                                 "设备科-沈工", "物联中心-尹工")

    # 劣化预警演示：为当日健康分最低的两台设备最新评估记录标记劣化
    conn = connect_db()
    try:
        worst_rows = conn.execute(
            "SELECT s.score_id FROM iot_health_score s JOIN "
            "(SELECT device_code, MAX(score_id) AS mid FROM iot_health_score "
            "GROUP BY device_code) t ON s.score_id=t.mid "
            "ORDER BY s.health_score ASC LIMIT 2").fetchall()
        for row in worst_rows:
            conn.execute(
                "UPDATE iot_health_score SET drop_value=-6.8, degrade_flag=1 "
                "WHERE score_id=?", (row["score_id"],))
        conn.commit()
    finally:
        conn.close()
    return len(DEMO_MODEL_LIST)

# --------------------------------------------------------------------------
# 第 14 天数据表脚本增量：iot_schema.sql（M，已内联为字符串常量 DAY14_SCHEMA_SQL）
# --------------------------------------------------------------------------

DAY14_SCHEMA_SQL = r"""
-- ==================================================================
-- 第 14 天新增：设备健康度评估与预测性维护三表
-- ==================================================================
-- 健康评估模型表：按设备类型配置在线率、遥测稳定、告警与链路四维权重
-- 及健康等级阈值，支撑多套评估口径并存与按设备类型适配
CREATE TABLE IF NOT EXISTS iot_health_model (
    model_id        INTEGER PRIMARY KEY AUTOINCREMENT, -- 模型主键
    model_code      TEXT    NOT NULL UNIQUE,           -- 模型编号（HM-序号）
    model_name      TEXT    DEFAULT '',                -- 模型名称
    device_type     TEXT    DEFAULT '',                -- 适用设备类型（空表示全部）
    w_online        REAL    DEFAULT 35,                -- 在线状态权重
    w_telemetry     REAL    DEFAULT 25,                -- 遥测稳定权重
    w_alarm         REAL    DEFAULT 25,                -- 告警健康权重
    w_link          REAL    DEFAULT 15,                -- 链路质量权重
    excellent_line  REAL    DEFAULT 90,                -- 健康等级下限
    good_line       REAL    DEFAULT 75,                -- 良好等级下限
    warn_line       REAL    DEFAULT 60,                -- 预警等级下限
    model_state     TEXT    DEFAULT 'ON',              -- 模型状态（见 MODEL_STATE_DICT）
    eval_times      INTEGER DEFAULT 0,                 -- 累计评估轮次
    operator_name   TEXT    DEFAULT '',                -- 模型维护人
    remark_text     TEXT    DEFAULT '',                -- 模型说明
    created_at      TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- 健康度评估记录表：一次评估一台设备一条，记录四维得分、综合健康分、
-- 健康等级与较上轮劣化幅度，支撑近 7 日健康分趋势与劣化预警
CREATE TABLE IF NOT EXISTS iot_health_score (
    score_id        INTEGER PRIMARY KEY AUTOINCREMENT, -- 评估记录主键
    score_code      TEXT    NOT NULL UNIQUE,           -- 评估编号（HS-序号）
    model_code      TEXT    DEFAULT '',                -- 采用的评估模型编号
    device_code     TEXT    DEFAULT '',                -- 设备编号
    device_name     TEXT    DEFAULT '',                -- 设备名称快照
    device_type     TEXT    DEFAULT '',                -- 设备类型快照
    ward_name       TEXT    DEFAULT '',                -- 所在病区快照
    online_score    REAL    DEFAULT 0,                 -- 在线状态维度得分
    telemetry_score REAL    DEFAULT 0,                 -- 遥测稳定维度得分
    alarm_score     REAL    DEFAULT 0,                 -- 告警健康维度得分
    link_score      REAL    DEFAULT 0,                 -- 链路质量维度得分
    health_score    REAL    DEFAULT 0,                 -- 综合健康分
    health_level    TEXT    DEFAULT 'GOOD',            -- 健康等级（见 HEALTH_LEVEL_DICT）
    drop_value      REAL    DEFAULT 0,                 -- 较上轮变化（负值代表下降）
    degrade_flag    INTEGER DEFAULT 0,                 -- 是否劣化预警
    telemetry_amount INTEGER DEFAULT 0,                -- 本次参与评估遥测条数
    alarm_amount    INTEGER DEFAULT 0,                 -- 该设备累计告警条数
    eval_date       TEXT    DEFAULT '',                -- 评估日期
    eval_time       TEXT    DEFAULT '',                -- 评估时间
    created_at      TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- 预测性维护建议表：按低分维度生成维保建议并跟踪处置状态
CREATE TABLE IF NOT EXISTS iot_health_advice (
    advice_id       INTEGER PRIMARY KEY AUTOINCREMENT, -- 建议主键
    advice_code     TEXT    NOT NULL UNIQUE,           -- 建议编号（HA-序号）
    score_code      TEXT    DEFAULT '',                -- 关联评估编号
    model_code      TEXT    DEFAULT '',                -- 关联评估模型编号
    device_code     TEXT    DEFAULT '',                -- 设备编号
    device_name     TEXT    DEFAULT '',                -- 设备名称快照
    dim_key         TEXT    DEFAULT '',                -- 触发维度键
    dim_label       TEXT    DEFAULT '',                -- 触发维度名称
    advice_type     TEXT    DEFAULT 'INSPECT',         -- 建议类型（见 ADVICE_TYPE_DICT）
    priority_level  TEXT    DEFAULT 'MID',             -- 优先级（见 ADVICE_PRIORITY_DICT）
    advice_text     TEXT    DEFAULT '',                -- 维保建议内容
    reason_text     TEXT    DEFAULT '',                -- 生成依据
    advise_state    TEXT    DEFAULT 'OPEN',            -- 处置状态（见 ADVICE_STATE_DICT）
    handle_note     TEXT    DEFAULT '',                -- 处置说明
    handler_name    TEXT    DEFAULT '',                -- 处置人
    handle_time     TEXT    DEFAULT '',                -- 处置时间
    created_at      TEXT    DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_hmodel_code       ON iot_health_model (model_code);
CREATE INDEX IF NOT EXISTS idx_hmodel_state      ON iot_health_model (model_state);
CREATE INDEX IF NOT EXISTS idx_hscore_code       ON iot_health_score (score_code);
CREATE INDEX IF NOT EXISTS idx_hscore_model      ON iot_health_score (model_code);
CREATE INDEX IF NOT EXISTS idx_hscore_device     ON iot_health_score (device_code);
CREATE INDEX IF NOT EXISTS idx_hscore_date       ON iot_health_score (eval_date);
CREATE INDEX IF NOT EXISTS idx_hscore_level      ON iot_health_score (health_level);
CREATE INDEX IF NOT EXISTS idx_hadvice_code      ON iot_health_advice (advice_code);
CREATE INDEX IF NOT EXISTS idx_hadvice_device    ON iot_health_advice (device_code);
CREATE INDEX IF NOT EXISTS idx_hadvice_state     ON iot_health_advice (advise_state);
"""


def apply_day14_schema(conn):
    """把第 14 天新增的数据表脚本应用到已有数据库。"""
    conn.executescript(DAY14_SCHEMA_SQL)
    conn.commit()
    return True

# --------------------------------------------------------------------------
# 第 14 天主程序新增路由与接口：app.py（仅增量片段）
# --------------------------------------------------------------------------

from iot_health import (
    build_health_payload,
    build_health_brief,
    save_health_model,
    toggle_health_model,
    run_health_evaluation,
    handle_health_advice,
    seed_demo_health,
    HEALTH_LEVEL_DICT,
    MODEL_STATE_DICT,
    ADVICE_TYPE_DICT,
    ADVICE_PRIORITY_DICT,
    ADVICE_STATE_DICT,
)

@app.route("/iot/health")
def page_health():
    """设备健康度评估页面：模型配置、健康评估与预测性维护（第 14 天）。"""
    return render_template(
        "page_health.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        refresh_interval=REFRESH_INTERVAL_SEC,
    )


@app.route("/api/iot/health/panel")
def api_health_panel():
    """健康评估面板数据接口：概览、图表、模型、评估记录与维保建议。"""
    payload = build_health_payload(
        device_code=request.args.get("device_code", ""),
        health_level=request.args.get("health_level", ""),
        model_code=request.args.get("model_code", ""),
        degrade_only=request.args.get("degrade_only", "") == "1",
        advice_type=request.args.get("advice_type", ""),
        advise_state=request.args.get("advise_state", ""),
        advice_device=request.args.get("advice_device", ""),
        advice_keyword=request.args.get("advice_keyword", ""),
        page=request.args.get("page", "1"),
    )
    return jsonify(payload)


@app.route("/api/iot/health/model/save", methods=["POST"])
def api_health_model_save():
    """健康模型建档接口：新建评估模型，四维权重之和须为 100。"""
    body = request.get_json(silent=True) or {}
    result = save_health_model(
        body.get("model_code") or "",
        body.get("model_name") or "",
        body.get("device_type") or "",
        body.get("w_online") or 35,
        body.get("w_telemetry") or 25,
        body.get("w_alarm") or 25,
        body.get("w_link") or 15,
        body.get("excellent_line") or 90,
        body.get("good_line") or 75,
        body.get("warn_line") or 60,
        body.get("model_state") or "ON",
        body.get("remark_text") or "",
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/health/model/state", methods=["POST"])
def api_health_model_state():
    """健康模型启停接口：切换启用或停用状态，仅启用模型参与评估。"""
    body = request.get_json(silent=True) or {}
    result = toggle_health_model(
        body.get("model_code") or "",
        body.get("model_state") or "ON",
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/health/evaluate", methods=["POST"])
def api_health_evaluate():
    """健康评估执行接口：按启用模型批量评估设备并生成维保建议。"""
    body = request.get_json(silent=True) or {}
    result = run_health_evaluation(
        body.get("model_code") or "",
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/health/advice/handle", methods=["POST"])
def api_health_advice_handle():
    """维保建议处置接口：推进建议处置状态并留痕。"""
    body = request.get_json(silent=True) or {}
    result = handle_health_advice(
        body.get("advice_code") or "",
        body.get("advise_state") or "CLOSED",
        body.get("handle_note") or "",
        body.get("handler_name") or "",
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/health/options")
def api_health_options():
    """健康下拉选项接口：等级、模型状态、建议类型与处置状态字典。"""
    return jsonify({
        "result": "OK",
        "level_options": [{"code": k, "label": v}
                          for k, v in HEALTH_LEVEL_DICT.items()],
        "model_state_options": [{"code": k, "label": v}
                                for k, v in MODEL_STATE_DICT.items()],
        "advice_type_options": [{"code": k, "label": v}
                                for k, v in ADVICE_TYPE_DICT.items()],
        "priority_options": [{"code": k, "label": v}
                             for k, v in ADVICE_PRIORITY_DICT.items()],
        "advice_state_options": [{"code": k, "label": v}
                                 for k, v in ADVICE_STATE_DICT.items()],
    })



# 【函数内插入】def bootstrap():
#    seed_demo_health()

# --------------------------------------------------------------------------
# 第 14 天新增/修改的前端资源清单（文件本体见工程 templates/、static/ 目录）
# --------------------------------------------------------------------------

#   [新增] static/iot_health.js             新增 539 行
#   [修改] templates/iot_base.html          新增 1 行
#   [新增] templates/page_health.html       新增 283 行
