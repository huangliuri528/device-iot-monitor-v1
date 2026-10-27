# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-27（第 7 天 / 共 15 天）
# 模块功能：本文件仅含该软件第 7 天新增与修改的源代码，不含第 1~6 天已有代码
#           本次迭代主题：设备远程控制指令模块 + 远程控制页面
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）
# 接入说明：以下内容为当日增量，需与前面各天的代码合并后运行——
#           新增模块按文件名放入工程目录；建表脚本增量追加进对应 .sql；
#           主程序增量片段插入 app.py 中 Flask 实例之后；前端资源按文末清单
#           放入 templates/ 与 static/ 目录


# --------------------------------------------------------------------------
# 第 7 天新增业务模块 1：iot_control.py（完整源码）
# --------------------------------------------------------------------------

# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-27（第 7 天 / 共 15 天）
# 模块功能：设备远程控制指令模块。实现网关侧远程指令的字典维护、下发、
#           执行回执、失败重发与操作审计，支持按指令类型配置参数模板与
#           风险等级，下发时校验设备在线状态与指令启用状态，依据网关回执
#           判定执行成功或失败并记录失败原因，输出指令状态分布、指令类型
#           分布、近 7 日下发趋势、设备指令排行与指令成功率五类统计图表，
#           形成「指令配置 → 下发 → 回执 → 审计」的远程管控闭环
# 依赖包：sqlite3（Python 内置）、datetime（Python 内置）

import datetime

from iot_config import DEVICE_RUN_STATE
from iot_store import connect_db, dict_list, now_text, fetch_device_nodes, fetch_device_by_code

# 指令类型字典：按可远程下发的常见操作划分
COMMAND_TYPE_DICT = {
    "REBOOT": "设备重启",
    "PARAM": "参数下发",
    "SWITCH": "开关机控制",
    "SAMPLE": "采样周期调整",
    "CALIB": "远程校准",
    "UPGRADE": "固件升级",
}

# 指令风险等级字典：高危指令下发时需携带二次确认标记
RISK_LEVEL_DICT = {
    "LOW": "低风险",
    "MIDDLE": "中风险",
    "HIGH": "高风险",
}

# 指令执行状态字典：下发后按网关回执推进
EXEC_STATE_DICT = {
    "PENDING": "待下发",
    "SENT": "已下发待回执",
    "SUCCESS": "执行成功",
    "FAILED": "执行失败",
    "REJECT": "已驳回",
}

# 默认指令模板：（指令编码、指令类型、指令名称、默认参数、风险等级）
DEFAULT_COMMAND_LIST = [
    ("CMD-REBOOT", "REBOOT", "设备软重启", "{\"delay_sec\": 10}", "MIDDLE"),
    ("CMD-PARAM", "PARAM", "运行参数下发", "{\"temp_target\": 37.0}", "MIDDLE"),
    ("CMD-SWITCH", "SWITCH", "远程开关机", "{\"power\": \"on\"}", "HIGH"),
    ("CMD-SAMPLE", "SAMPLE", "采样周期调整", "{\"interval_sec\": 15}", "LOW"),
    ("CMD-CALIB", "CALIB", "远程零点校准", "{\"channel\": 1}", "MIDDLE"),
    ("CMD-UPGRADE", "UPGRADE", "固件版本升级", "{\"version\": \"v1.2.0\"}", "HIGH"),
]

# 指令下发编号前缀，编号形如 DPS-20261027-0001
DISPATCH_CODE_PREFIX = "DPS"

# 指令单页条数
CONTROL_PAGE_SIZE = 10

# 指令回执超时秒数：超过该时长仍未收到回执即判定为执行失败
ACK_TIMEOUT_SEC = 30


def round2(value):
    """保留两位小数，空值与非数值按 0 处理。"""
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return 0.0


def today_text():
    """返回当前日期文本，格式 YYYY-MM-DD。"""
    return datetime.date.today().strftime("%Y-%m-%d")


def shift_days_text(days):
    """返回相对今天偏移指定天数后的日期文本。"""
    return (datetime.date.today() + datetime.timedelta(days=days)).strftime("%Y-%m-%d")


def generate_dispatch_code(conn):
    """生成指令下发编号，形如 DPS-20261027-0001。"""
    day_text = datetime.date.today().strftime("%Y%m%d")
    row = conn.execute(
        "SELECT COUNT(1) FROM iot_control_log WHERE dispatch_code LIKE ?",
        ("%s-%s-%%" % (DISPATCH_CODE_PREFIX, day_text),)
    ).fetchone()
    amount = row[0] if row else 0
    return "%s-%s-%04d" % (DISPATCH_CODE_PREFIX, day_text, amount + 1)


# ==================================================================
# 指令字典维护
# ==================================================================
def fetch_control_commands():
    """读取全部远程指令模板，附加类型名称与风险等级名称。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT * FROM iot_control_cmd ORDER BY cmd_id").fetchall()
    finally:
        conn.close()
    result = []
    for item in dict_list(rows):
        item["type_label"] = COMMAND_TYPE_DICT.get(item["cmd_type"], "未分类")
        item["risk_label"] = RISK_LEVEL_DICT.get(item["risk_level"], "未分级")
        result.append(item)
    return result


def save_control_command(payload):
    """新增或更新一条指令模板，返回统一结果字典。"""
    cmd_id = payload.get("cmd_id")
    cmd_code = (payload.get("cmd_code") or "").strip()
    cmd_name = (payload.get("cmd_name") or "").strip()
    cmd_type = (payload.get("cmd_type") or "").strip()
    risk_level = (payload.get("risk_level") or "").strip()
    param_text = (payload.get("param_text") or "{}").strip()

    if not cmd_code:
        return {"success": False, "message": "指令编码不能为空"}
    if not cmd_name:
        return {"success": False, "message": "指令名称不能为空"}
    if cmd_type not in COMMAND_TYPE_DICT:
        return {"success": False, "message": "指令类型编码不合法"}
    if risk_level not in RISK_LEVEL_DICT:
        return {"success": False, "message": "风险等级编码不合法"}

    enabled = 1 if str(payload.get("enabled_flag", 1)) in ("1", "true", "True") else 0
    conn = connect_db()
    try:
        if cmd_id:
            conn.execute(
                "UPDATE iot_control_cmd SET cmd_name=?, cmd_type=?, param_text=?, "
                "risk_level=?, enabled_flag=?, updated_at=? WHERE cmd_id=?",
                (cmd_name, cmd_type, param_text, risk_level, enabled,
                 now_text(), cmd_id),
            )
            message = "指令模板已更新"
        else:
            exist = conn.execute(
                "SELECT cmd_id FROM iot_control_cmd WHERE cmd_code=?", (cmd_code,)
            ).fetchone()
            if exist is not None:
                return {"success": False, "message": "指令编码 %s 已存在" % cmd_code}
            conn.execute(
                "INSERT INTO iot_control_cmd (cmd_code, cmd_name, cmd_type, "
                "param_text, risk_level, enabled_flag) VALUES (?,?,?,?,?,?)",
                (cmd_code, cmd_name, cmd_type, param_text, risk_level, enabled),
            )
            message = "指令模板已新增"
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": message}


def seed_demo_commands():
    """首次启动时灌入默认指令模板，已存在则不覆盖。"""
    conn = connect_db()
    try:
        if conn.execute("SELECT COUNT(1) FROM iot_control_cmd").fetchone()[0] > 0:
            return 0
        for cmd_code, cmd_type, cmd_name, param_text, risk_level in DEFAULT_COMMAND_LIST:
            conn.execute(
                "INSERT INTO iot_control_cmd (cmd_code, cmd_name, cmd_type, "
                "param_text, risk_level, enabled_flag) VALUES (?,?,?,?,?,1)",
                (cmd_code, cmd_name, cmd_type, param_text, risk_level),
            )
        conn.commit()
    finally:
        conn.close()
    return len(DEFAULT_COMMAND_LIST)


# ==================================================================
# 指令下发与回执
# ==================================================================
def dispatch_control_command(device_code, cmd_code, dispatch_person,
                             param_text="", confirm_flag=False):
    """向指定设备下发一条远程指令，返回统一结果字典。

    校验点：设备必须存在、指令模板必须启用、高危指令必须携带二次确认标记。
    下发后先置为「已下发待回执」，再由回执接口推进为成功或失败。
    """
    device_code = (device_code or "").strip()
    cmd_code = (cmd_code or "").strip()
    dispatch_person = (dispatch_person or "").strip()

    if not device_code:
        return {"success": False, "message": "设备编号不能为空"}
    if not cmd_code:
        return {"success": False, "message": "指令编码不能为空"}
    if not dispatch_person:
        return {"success": False, "message": "指令下发人不能为空"}

    device = fetch_device_by_code(device_code)
    if device is None:
        return {"success": False, "message": "设备编号 %s 不存在" % device_code}

    conn = connect_db()
    try:
        cmd_row = conn.execute(
            "SELECT * FROM iot_control_cmd WHERE cmd_code=?", (cmd_code,)
        ).fetchone()
        if cmd_row is None:
            return {"success": False, "message": "指令编码 %s 不存在" % cmd_code}
        cmd = dict(cmd_row)
        if cmd["enabled_flag"] != 1:
            return {"success": False, "message": "指令 %s 已停用，不可下发" % cmd["cmd_name"]}
        if cmd["risk_level"] == "HIGH" and not confirm_flag:
            return {"success": False,
                    "message": "指令「%s」为高风险指令，需勾选二次确认后下发"
                               % cmd["cmd_name"]}
        if device["run_state"] in ("OFFLINE", "FAULT"):
            return {"success": False,
                    "message": "设备当前%s，指令无法送达"
                               % DEVICE_RUN_STATE.get(device["run_state"], "不可用")}

        dispatch_code = generate_dispatch_code(conn)
        final_param = (param_text or "").strip() or cmd["param_text"]
        conn.execute(
            "INSERT INTO iot_control_log (dispatch_code, device_code, device_name, "
            "gateway_code, cmd_code, cmd_name, cmd_type, param_text, risk_level, "
            "dispatch_person, dispatch_time, exec_state) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,'SENT')",
            (dispatch_code, device_code, device["device_name"],
             device["gateway_code"], cmd_code, cmd["cmd_name"], cmd["cmd_type"],
             final_param, cmd["risk_level"], dispatch_person, now_text()),
        )
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "指令「%s」已下发至 %s，等待网关回执"
                       % (cmd["cmd_name"], device["device_name"]),
            "dispatch_code": dispatch_code}


def ack_control_command(dispatch_code, success_flag=True, ack_text=""):
    """登记网关回执：成功置 SUCCESS，失败置 FAILED 并记录失败原因。"""
    dispatch_code = (dispatch_code or "").strip()
    if not dispatch_code:
        return {"success": False, "message": "下发编号不能为空"}

    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_control_log WHERE dispatch_code=?", (dispatch_code,)
        ).fetchone()
        if row is None:
            return {"success": False, "message": "下发编号 %s 不存在" % dispatch_code}
        log = dict(row)
        if log["exec_state"] in ("SUCCESS", "FAILED", "REJECT"):
            return {"success": False,
                    "message": "该指令已终结于「%s」，不可重复回执"
                               % EXEC_STATE_DICT.get(log["exec_state"], "")}
        ack_at = now_text()
        if success_flag:
            state_code = "SUCCESS"
            text = (ack_text or "").strip() or "设备回执：指令执行成功"
        else:
            state_code = "FAILED"
            text = (ack_text or "").strip() or "设备回执：指令执行失败，设备未响应"
        conn.execute(
            "UPDATE iot_control_log SET exec_state=?, ack_text=?, ack_time=? "
            "WHERE dispatch_code=?", (state_code, text, ack_at, dispatch_code),
        )
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "指令回执已登记，执行状态更新为「%s」"
                       % EXEC_STATE_DICT[state_code],
            "exec_state": state_code}


def simulate_control_ack(dispatch_code):
    """模拟网关回执：按设备当前在线状态判定执行成功或失败。

    真实部署时由 MQTT 下行主题的回执报文驱动，此处按设备状态给出确定性
    结果，保证无 broker 环境下也能完整演示远程控制闭环。
    """
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_control_log WHERE dispatch_code=?", (dispatch_code,)
        ).fetchone()
        if row is None:
            return {"success": False, "message": "下发编号 %s 不存在" % dispatch_code}
        log = dict(row)
    finally:
        conn.close()
    device = fetch_device_by_code(log["device_code"])
    if device is None:
        return ack_control_command(dispatch_code, False, "设备已注销，指令无法执行")
    if device["run_state"] in ("OFFLINE", "FAULT"):
        return ack_control_command(
            dispatch_code, False,
            "设备%s，指令下发后未收到响应"
            % DEVICE_RUN_STATE.get(device["run_state"], "不可用"))
    if device["run_state"] == "SLEEP":
        return ack_control_command(dispatch_code, False, "设备处于待机状态，指令被拒绝")
    return ack_control_command(dispatch_code, True, "设备已确认执行，回执正常")


def redo_control_command(dispatch_code, dispatch_person):
    """对执行失败的指令重新下发：复制参数生成一条新的下发记录。"""
    dispatch_person = (dispatch_person or "").strip()
    if not dispatch_person:
        return {"success": False, "message": "重发操作人不能为空"}
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_control_log WHERE dispatch_code=?", (dispatch_code,)
        ).fetchone()
        if row is None:
            return {"success": False, "message": "下发编号 %s 不存在" % dispatch_code}
        log = dict(row)
    finally:
        conn.close()
    if log["exec_state"] != "FAILED":
        return {"success": False, "message": "仅执行失败的指令可以重发"}
    return dispatch_control_command(
        log["device_code"], log["cmd_code"], dispatch_person,
        log["param_text"], True)


# ==================================================================
# 查询与统计
# ==================================================================
def build_control_condition(device_code="", cmd_type="", exec_state="",
                            risk_level="", keyword="", date_from="", date_to=""):
    """拼装指令下发记录筛选条件。"""
    clauses = []
    params = []
    if device_code:
        clauses.append("device_code = ?")
        params.append(device_code)
    if cmd_type:
        clauses.append("cmd_type = ?")
        params.append(cmd_type)
    if exec_state:
        clauses.append("exec_state = ?")
        params.append(exec_state)
    if risk_level:
        clauses.append("risk_level = ?")
        params.append(risk_level)
    if keyword:
        clauses.append("(dispatch_code LIKE ? OR device_code LIKE ? OR "
                       "device_name LIKE ? OR dispatch_person LIKE ?)")
        like_text = "%%%s%%" % keyword
        params.extend([like_text, like_text, like_text, like_text])
    if date_from:
        clauses.append("substr(dispatch_time,1,10) >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("substr(dispatch_time,1,10) <= ?")
        params.append(date_to)
    where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where_sql, params


def fetch_control_logs(device_code="", cmd_type="", exec_state="", risk_level="",
                       keyword="", date_from="", date_to="", page=1, size=CONTROL_PAGE_SIZE):
    """分页查询指令下发记录，附加状态与类型名称。"""
    try:
        page_no = max(int(page), 1)
    except (TypeError, ValueError):
        page_no = 1
    try:
        page_size = max(int(size), 1)
    except (TypeError, ValueError):
        page_size = CONTROL_PAGE_SIZE
    offset = (page_no - 1) * page_size

    where_sql, params = build_control_condition(
        device_code, cmd_type, exec_state, risk_level, keyword, date_from, date_to)
    conn = connect_db()
    try:
        total = conn.execute(
            "SELECT COUNT(1) FROM iot_control_log" + where_sql, params).fetchone()[0]
        rows = conn.execute(
            "SELECT * FROM iot_control_log" + where_sql +
            " ORDER BY log_id DESC LIMIT ? OFFSET ?",
            params + [page_size, offset],
        ).fetchall()
    finally:
        conn.close()
    records = []
    for item in dict_list(rows):
        item["state_label"] = EXEC_STATE_DICT.get(item["exec_state"], "未知状态")
        item["type_label"] = COMMAND_TYPE_DICT.get(item["cmd_type"], "未分类")
        item["risk_label"] = RISK_LEVEL_DICT.get(item["risk_level"], "未分级")
        records.append(item)
    total_page = (total + page_size - 1) // page_size if page_size else 1
    return records, total, total_page, page_no


def build_control_summary():
    """统计指令下发总量、成功量、失败量、待回执量与执行成功率。"""
    conn = connect_db()
    try:
        total = conn.execute("SELECT COUNT(1) FROM iot_control_log").fetchone()[0]
        success = conn.execute(
            "SELECT COUNT(1) FROM iot_control_log WHERE exec_state='SUCCESS'"
        ).fetchone()[0]
        failed = conn.execute(
            "SELECT COUNT(1) FROM iot_control_log WHERE exec_state='FAILED'"
        ).fetchone()[0]
        pending = conn.execute(
            "SELECT COUNT(1) FROM iot_control_log WHERE exec_state='SENT'"
        ).fetchone()[0]
        high_risk = conn.execute(
            "SELECT COUNT(1) FROM iot_control_log WHERE risk_level='HIGH'"
        ).fetchone()[0]
        device_amount = conn.execute(
            "SELECT COUNT(DISTINCT device_code) FROM iot_control_log").fetchone()[0]
        today_amount = conn.execute(
            "SELECT COUNT(1) FROM iot_control_log "
            "WHERE substr(dispatch_time,1,10)=?", (today_text(),)).fetchone()[0]
    finally:
        conn.close()
    done = success + failed
    return {
        "total": total,
        "success": success,
        "failed": failed,
        "pending": pending,
        "high_risk": high_risk,
        "device_amount": device_amount,
        "today_amount": today_amount,
        "success_rate": round2(success * 100.0 / done) if done else 0.0,
    }


def build_control_charts():
    """构建指令状态、类型、风险分布、近 7 日趋势与设备指令排行五类图表数据。"""
    conn = connect_db()
    try:
        state_rows = conn.execute(
            "SELECT exec_state, COUNT(1) AS amount FROM iot_control_log "
            "GROUP BY exec_state").fetchall()
        type_rows = conn.execute(
            "SELECT cmd_type, COUNT(1) AS amount FROM iot_control_log "
            "GROUP BY cmd_type").fetchall()
        risk_rows = conn.execute(
            "SELECT risk_level, COUNT(1) AS amount FROM iot_control_log "
            "GROUP BY risk_level").fetchall()
        day_rows = conn.execute(
            "SELECT substr(dispatch_time,1,10) AS day_tag, COUNT(1) AS amount, "
            "SUM(CASE WHEN exec_state='SUCCESS' THEN 1 ELSE 0 END) AS success_amount "
            "FROM iot_control_log GROUP BY day_tag ORDER BY day_tag DESC LIMIT 7"
        ).fetchall()
        device_rows = conn.execute(
            "SELECT device_code, device_name, COUNT(1) AS amount, "
            "SUM(CASE WHEN exec_state='SUCCESS' THEN 1 ELSE 0 END) AS success_amount, "
            "SUM(CASE WHEN exec_state='FAILED' THEN 1 ELSE 0 END) AS failed_amount "
            "FROM iot_control_log GROUP BY device_code, device_name "
            "ORDER BY amount DESC LIMIT 8").fetchall()
    finally:
        conn.close()

    state_distribution = [
        {"name": EXEC_STATE_DICT.get(row["exec_state"], row["exec_state"]),
         "value": row["amount"]}
        for row in state_rows
    ]
    type_distribution = [
        {"name": COMMAND_TYPE_DICT.get(row["cmd_type"], row["cmd_type"]),
         "value": row["amount"]}
        for row in type_rows
    ]
    risk_distribution = [
        {"name": RISK_LEVEL_DICT.get(row["risk_level"], row["risk_level"]),
         "value": row["amount"]}
        for row in risk_rows
    ]
    day_trend = [
        {"day": row["day_tag"], "amount": row["amount"],
         "success_amount": row["success_amount"]}
        for row in reversed(day_rows)
    ]
    device_ranking = [
        {"device_code": row["device_code"], "device_name": row["device_name"],
         "amount": row["amount"], "success_amount": row["success_amount"],
         "failed_amount": row["failed_amount"]}
        for row in device_rows
    ]
    return {
        "state_distribution": state_distribution,
        "type_distribution": type_distribution,
        "risk_distribution": risk_distribution,
        "day_trend": day_trend,
        "device_ranking": device_ranking,
    }


def build_control_payload(device_code="", cmd_type="", exec_state="", risk_level="",
                          keyword="", date_from="", date_to="", page=1):
    """组装远程控制页面所需的全部数据：指令模板、汇总、图表、分页明细。"""
    records, total, total_page, page_no = fetch_control_logs(
        device_code, cmd_type, exec_state, risk_level, keyword, date_from, date_to, page)
    nodes = fetch_device_nodes()
    return {
        "result": "OK",
        "commands": fetch_control_commands(),
        "summary": build_control_summary(),
        "charts": build_control_charts(),
        "records": records,
        "total": total,
        "total_page": total_page,
        "page": page_no,
        "options": {
            "type_dict": COMMAND_TYPE_DICT,
            "risk_dict": RISK_LEVEL_DICT,
            "state_dict": EXEC_STATE_DICT,
            "device_list": [
                {"device_code": node["device_code"], "device_name": node["device_name"],
                 "run_state": node["run_state"],
                 "run_label": DEVICE_RUN_STATE.get(node["run_state"], node["run_state"])}
                for node in nodes
            ],
        },
    }


def build_control_brief():
    """远程控制大屏摘要，供实时监控大屏嵌入。"""
    summary = build_control_summary()
    return {
        "total": summary["total"],
        "pending": summary["pending"],
        "failed": summary["failed"],
        "success_rate": summary["success_rate"],
        "text": "待回执 %d 条 / 成功率 %.1f%%" % (summary["pending"], summary["success_rate"]),
    }


def seed_demo_control():
    """灌入演示指令下发记录：覆盖各执行状态，首日启动即可看到面板效果。"""
    seed_demo_commands()
    conn = connect_db()
    try:
        if conn.execute("SELECT COUNT(1) FROM iot_control_log").fetchone()[0] > 0:
            return 0
    finally:
        conn.close()

    online_nodes = [node for node in fetch_device_nodes()
                    if node["run_state"] == "ONLINE"]
    if not online_nodes:
        return 0
    # 演示计划：依次覆盖成功、失败与待回执三种执行状态
    # （指令编码、下发人、回执动作：ack_success / ack_failed / keep_pending）
    plan = [
        ("CMD-SAMPLE", "王工程师", "ack_success"),
        ("CMD-PARAM", "王工程师", "ack_success"),
        ("CMD-REBOOT", "李工程师", "ack_success"),
        ("CMD-CALIB", "李工程师", "ack_failed"),
        ("CMD-SWITCH", "张工程师", "ack_success"),
        ("CMD-UPGRADE", "张工程师", "keep_pending"),
    ]
    amount = 0
    for index, (cmd_code, person, action) in enumerate(plan):
        node = online_nodes[index % len(online_nodes)]
        result = dispatch_control_command(
            node["device_code"], cmd_code, person, "", True)
        if not result.get("success"):
            continue
        amount += 1
        dispatch_code = result["dispatch_code"]
        if action == "ack_success":
            simulate_control_ack(dispatch_code)
        elif action == "ack_failed":
            ack_control_command(dispatch_code, False,
                                "设备回执超时，%d 秒内未收到执行确认" % ACK_TIMEOUT_SEC)
    return amount

# --------------------------------------------------------------------------
# 第 7 天数据表脚本增量：iot_schema.sql（M，已内联为字符串常量 DAY7_SCHEMA_SQL）
# --------------------------------------------------------------------------

DAY7_SCHEMA_SQL = r"""
-- ==================================================================
-- 第 7 天新增：设备远程控制指令模板表
-- ==================================================================
-- 指令模板表：按「一条指令一个模板」维护可远程下发的操作，含指令类型、
-- 默认参数 JSON、风险等级与启用状态，风险等级为 HIGH 的指令下发前须
-- 进行二次确认，防止误操作影响临床使用
CREATE TABLE IF NOT EXISTS iot_control_cmd (
    cmd_id       INTEGER PRIMARY KEY AUTOINCREMENT,  -- 指令主键
    cmd_code     TEXT    NOT NULL UNIQUE,            -- 指令编码
    cmd_name     TEXT    NOT NULL,                   -- 指令名称
    cmd_type     TEXT    DEFAULT 'PARAM',            -- 指令类型（见 COMMAND_TYPE_DICT）
    param_text   TEXT    DEFAULT '{}',               -- 默认参数（JSON 文本）
    risk_level   TEXT    DEFAULT 'LOW',              -- 风险等级（见 RISK_LEVEL_DICT）
    enabled_flag INTEGER DEFAULT 1,                  -- 是否启用：1 启用 / 0 停用
    updated_at   TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- ==================================================================
-- 第 7 天新增：设备远程控制指令下发记录表
-- ==================================================================
-- 下发记录表：按「一次下发一条」记录指令下发全过程，含下发人、下发时间、
-- 下发参数、执行状态、网关回执文本与回执时间，支撑指令重发、失败追溯与
-- 远程操作审计留痕
CREATE TABLE IF NOT EXISTS iot_control_log (
    log_id          INTEGER PRIMARY KEY AUTOINCREMENT, -- 下发记录主键
    dispatch_code   TEXT    DEFAULT '',                -- 下发编号（DPS-日期-序号）
    device_code     TEXT    NOT NULL,                  -- 设备编号
    device_name     TEXT    DEFAULT '',                -- 设备名称快照
    gateway_code    TEXT    DEFAULT '',                -- 所属网关编号
    cmd_code        TEXT    NOT NULL,                  -- 指令编码
    cmd_name        TEXT    DEFAULT '',                -- 指令名称快照
    cmd_type        TEXT    DEFAULT 'PARAM',           -- 指令类型快照
    param_text      TEXT    DEFAULT '{}',              -- 下发参数（JSON 文本）
    risk_level      TEXT    DEFAULT 'LOW',             -- 风险等级快照
    dispatch_person TEXT    DEFAULT '',                -- 指令下发人
    dispatch_time   TEXT    DEFAULT '',                -- 指令下发时间
    exec_state      TEXT    DEFAULT 'SENT',            -- 执行状态（见 EXEC_STATE_DICT）
    ack_text        TEXT    DEFAULT '',                -- 网关回执文本
    ack_time        TEXT    DEFAULT '',                -- 网关回执时间
    retry_amount    INTEGER DEFAULT 0                  -- 重发次数
);

CREATE INDEX IF NOT EXISTS idx_ctrlcmd_code   ON iot_control_cmd (cmd_code);
CREATE INDEX IF NOT EXISTS idx_ctrllog_device ON iot_control_log (device_code);
CREATE INDEX IF NOT EXISTS idx_ctrllog_state  ON iot_control_log (exec_state);
CREATE INDEX IF NOT EXISTS idx_ctrllog_time   ON iot_control_log (dispatch_time);
CREATE INDEX IF NOT EXISTS idx_ctrllog_type   ON iot_control_log (cmd_type);
"""


def apply_day7_schema(conn):
    """把第 7 天新增的数据表脚本应用到已有数据库。"""
    conn.executescript(DAY7_SCHEMA_SQL)
    conn.commit()
    return True

# --------------------------------------------------------------------------
# 第 7 天主程序新增路由与接口：app.py（仅增量片段）
# --------------------------------------------------------------------------

# 开发日期：2026-10-27（第 7 天 / 共 15 天）
#           阈值、能耗与远程控制演示数据灌入、拉起 MQTT 订阅服务（或
#           模拟采集器），并注册实时监控大屏、设备接入、在线状态、实时
#           参数采集、阈值告警中心、能耗统计与远程控制七类页面路由，提供
#           设备注册增删改查、心跳上报、离线扫描识别、实时参数采集存储、
#           动态曲线、阈值扫描与告警处置、能耗分时采集、峰谷电费测算与
#           节能建议，以及远程指令模板维护、指令下发、网关回执登记与失败
#           重发 JSON 接口

from iot_control import (
    build_control_payload,
    build_control_brief,
    dispatch_control_command,
    ack_control_command,
    simulate_control_ack,
    redo_control_command,
    save_control_command,
    fetch_control_commands,
    seed_demo_control,
    COMMAND_TYPE_DICT,
    EXEC_STATE_DICT,
    RISK_LEVEL_DICT,
)

# 【函数内插入】def api_monitor():
#    payload["control_brief"] = build_control_brief()

# ==================================================================
# 以下为第 7 天新增：设备远程控制页面与接口
# ==================================================================


@app.route("/iot/control")
def page_control():
    """设备远程控制页面：指令概览、五类图表、指令模板与下发记录。"""
    return render_template(
        "page_control.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        refresh_interval=REFRESH_INTERVAL_SEC,
    )


@app.route("/api/iot/control/panel")
def api_control_panel():
    """远程控制数据接口：指令模板、汇总指标、五类图表与分页明细。"""
    return jsonify(build_control_payload(
        device_code=(request.args.get("device_code") or "").strip(),
        cmd_type=(request.args.get("cmd_type") or "").strip(),
        exec_state=(request.args.get("exec_state") or "").strip(),
        risk_level=(request.args.get("risk_level") or "").strip(),
        keyword=(request.args.get("keyword") or "").strip(),
        date_from=(request.args.get("date_from") or "").strip(),
        date_to=(request.args.get("date_to") or "").strip(),
        page=int(request.args.get("page", 1)),
    ))


@app.route("/api/iot/control/command", methods=["GET", "POST"])
def api_control_command():
    """指令模板接口：GET 读取全部模板，POST 新增或修改模板。"""
    if request.method == "GET":
        return jsonify({"result": "OK", "commands": fetch_control_commands()})
    body = request.get_json(silent=True) or {}
    result = save_control_command(body)
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/control/dispatch", methods=["POST"])
def api_control_dispatch():
    """向指定设备下发一条远程控制指令，高危指令需二次确认标记。"""
    body = request.get_json(silent=True) or {}
    result = dispatch_control_command(
        (body.get("device_code") or "").strip(),
        (body.get("cmd_code") or "").strip(),
        (body.get("dispatch_person") or "").strip(),
        (body.get("param_text") or "").strip(),
        bool(body.get("confirm_flag")),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/control/ack", methods=["POST"])
def api_control_ack():
    """登记网关回执：成功置执行成功，失败置执行失败并记录原因。"""
    body = request.get_json(silent=True) or {}
    result = ack_control_command(
        (body.get("dispatch_code") or "").strip(),
        bool(body.get("success_flag", True)),
        (body.get("ack_text") or "").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/control/simulate", methods=["POST"])
def api_control_simulate():
    """按设备当前运行状态模拟网关回执，供无 broker 环境演示闭环。"""
    body = request.get_json(silent=True) or {}
    result = simulate_control_ack((body.get("dispatch_code") or "").strip())
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/control/redo", methods=["POST"])
def api_control_redo():
    """对执行失败的指令重新下发，生成新的下发编号。"""
    body = request.get_json(silent=True) or {}
    result = redo_control_command(
        (body.get("dispatch_code") or "").strip(),
        (body.get("dispatch_person") or "").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)



# 【函数内插入】def bootstrap():
#    seed_demo_control()

# 【函数内插入】if __name__ == "__main__":
#    print("远程控制： http://127.0.0.1:%d/iot/control" % PORT)

# --------------------------------------------------------------------------
# 第 7 天新增/修改的前端资源清单（文件本体见工程 templates/、static/ 目录）
# --------------------------------------------------------------------------

#   [新增] static/iot_control.js            新增 431 行
#   [修改] templates/iot_base.html          新增 1 行
#   [新增] templates/page_control.html      新增 210 行
