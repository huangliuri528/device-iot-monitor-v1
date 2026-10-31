# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-31（第 11 天 / 共 15 天）
# 模块功能：本文件仅含该软件第 11 天新增与修改的源代码，不含第 1~10 天已有代码
#           本次迭代主题：固件版本与OTA升级管理模块 + 固件升级页面
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）
# 接入说明：以下内容为当日增量，需与前面各天的代码合并后运行——
#           新增模块按文件名放入工程目录；建表脚本增量追加进对应 .sql；
#           主程序增量片段插入 app.py 中 Flask 实例之后；前端资源按文末清单
#           放入 templates/ 与 static/ 目录


# --------------------------------------------------------------------------
# 第 11 天新增业务模块 1：iot_upgrade.py（完整源码）
# --------------------------------------------------------------------------

# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-31（第 11 天 / 共 15 天）
# 模块功能：固件版本与 OTA 升级管理模块。实现固件版本档案登记、稳定版标记
#           与升级台次统计，按单台设备、设备分组或全量设备创建 OTA 升级任务、
#           一键下发升级并逐台登记升级日志，离线与故障设备判定为升级失败并
#           支持重升，升级后异常支持回滚到升级前版本；输出版本覆盖分布、任务
#           状态分布、升级结论分布、版本升级台次排行与近十次任务成功率趋势
#           五类统计图表，形成「发版本 → 建任务 → 下发升级 → 失败重升 →
#           异常回滚」的固件升级闭环
# 依赖包：sqlite3（Python 内置）、datetime（Python 内置）、random（Python 内置）

import datetime
import random

from iot_store import connect_db, dict_list, now_text, fetch_device_nodes
from iot_store import fetch_gateway_rows

# ------------------------------------------------------------------
# 升级范围字典：支持单台、分组与全量三种下发方式
# ------------------------------------------------------------------
UPGRADE_SCOPE_DICT = {
    "ALL": "全量设备",
    "GROUP": "设备分组",
    "DEVICE": "单台设备",
}

# 升级任务状态字典：随下发执行逐级流转
UPGRADE_TASK_DICT = {
    "WAIT": "待下发",
    "RUNNING": "下发中",
    "DONE": "已完成",
    "PARTIAL": "部分失败",
    "FAIL": "下发失败",
}

# 升级结论字典：由设备运行状态与模拟执行结果判定
UPGRADE_LOG_DICT = {
    "PENDING": "待升级",
    "SUCCESS": "升级成功",
    "FAIL": "升级失败",
    "ROLLBACK": "已回滚",
}

# 设备出厂基线版本号：未查询到升级记录的设备按此版本统计
BASE_VERSION_NO = "v1.0.0"

# 各运行状态对应的升级成功率（%），离线与故障设备无法完成升级
STATE_SUCCESS_RATE = {
    "ONLINE": 92,
    "SLEEP": 70,
    "OFFLINE": 0,
    "FAULT": 0,
}

# 升级失败原因字典：按设备运行状态给出可追溯的失败原因
FAIL_REASON_MAP = {
    "OFFLINE": "设备离线，无法建立升级通道",
    "FAULT": "设备故障待修，暂停固件升级",
    "SLEEP": "设备休眠中，唤醒超时",
    "ONLINE": "固件校验失败，包签名不匹配",
}

# 单台设备升级耗时基数与随机波动（秒）
UPGRADE_BASE_SEC = 8
UPGRADE_RANDOM_SEC = 6

# 固件版本编号前缀，编号形如 FW-0001
VERSION_CODE_PREFIX = "FW"

# 升级日志编号前缀，编号形如 UL-0001
LOG_CODE_PREFIX = "UL"

# 升级任务编号前缀，编号形如 UT-20261031221503123
TASK_CODE_PREFIX = "UT"

# 演示数据随机种子，保证每次启动生成的升级记录一致
DEMO_RANDOM_SEED = 20261031


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


def build_version_code():
    """生成固件版本编号，形如 FW-0001。"""
    return "%s-%04d" % (VERSION_CODE_PREFIX,
                        next_serial("iot_firmware_version", "version_code",
                                    VERSION_CODE_PREFIX + "-"))


def build_log_code():
    """生成升级日志编号，形如 UL-0001。"""
    return "%s-%04d" % (LOG_CODE_PREFIX,
                        next_serial("iot_upgrade_log", "log_code",
                                    LOG_CODE_PREFIX + "-"))


def build_task_code():
    """生成升级任务编号，形如 UT-20261031221503123。

    末三位为毫秒序号，保证同一秒内连续创建的多个任务编号不冲突。
    """
    stamp = datetime.datetime.now()
    return "%s-%s%03d" % (TASK_CODE_PREFIX, stamp.strftime("%Y%m%d%H%M%S"),
                          stamp.microsecond // 1000)


def fetch_version_row(version_code):
    """按版本编号读取固件版本档案，未找到返回 None。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_firmware_version WHERE version_code=?",
            (version_code,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row is not None else None


def fetch_task_row(task_code):
    """按任务编号读取升级任务，未找到返回 None。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_upgrade_task WHERE task_code=?",
            (task_code,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row is not None else None


def fetch_device_version_map():
    """读取每台设备当前的固件版本号。

    取该设备最近一条升级成功的日志记录，无升级记录时按出厂基线版本统计。
    """
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT device_code, new_version FROM iot_upgrade_log "
            "WHERE upgrade_state='SUCCESS' ORDER BY log_id ASC").fetchall()
    finally:
        conn.close()
    version_map = {}
    for item in rows:
        version_map[item["device_code"]] = item["new_version"]
    return version_map


def current_device_version(device_code):
    """读取单台设备当前的固件版本号。"""
    return fetch_device_version_map().get(device_code, BASE_VERSION_NO)


# ------------------------------------------------------------------
# 固件版本维护
# ------------------------------------------------------------------
def save_firmware_version(payload):
    """登记或更新一条固件版本档案。

    传入已存在的版本编号时执行更新，否则按新编号建档；版本号与适用设备
    类型为必填项，稳定版标记决定该版本是否可被升级任务选择。

    :return: 含 success、message 与 version_code 的执行结果字典
    """
    firmware_name = (payload.get("firmware_name") or "").strip()
    device_type = (payload.get("device_type") or "").strip()
    version_no = (payload.get("version_no") or "").strip()
    if not firmware_name:
        return {"success": False, "message": "固件包名称不能为空"}
    if not device_type:
        return {"success": False, "message": "适用设备类型不能为空"}
    if not version_no:
        return {"success": False, "message": "版本号不能为空"}
    try:
        size = int(payload.get("file_size_kb") or 0)
    except (TypeError, ValueError):
        size = 0
    stable = 1 if str(payload.get("stable_flag", 1)) in ("1", "true", "True") else 0
    operator = (payload.get("operator_name") or "admin").strip()
    conn = connect_db()
    try:
        code = (payload.get("version_code") or "").strip()
        exist = None
        if code:
            exist = conn.execute(
                "SELECT version_code FROM iot_firmware_version "
                "WHERE version_code=?", (code,)).fetchone()
        if exist is not None:
            code = exist["version_code"]
            conn.execute(
                "UPDATE iot_firmware_version SET firmware_name=?, device_type=?, "
                "version_no=?, file_size_kb=?, version_desc=?, release_date=?, "
                "stable_flag=?, operator_name=? WHERE version_code=?",
                (firmware_name, device_type, version_no, size,
                 (payload.get("version_desc") or "").strip(),
                 (payload.get("release_date") or "").strip() or today_text(),
                 stable, operator, code))
        else:
            code = build_version_code()
            conn.execute(
                "INSERT INTO iot_firmware_version (version_code, firmware_name, "
                "device_type, version_no, file_size_kb, version_desc, "
                "release_date, stable_flag, operator_name) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (code, firmware_name, device_type, version_no, size,
                 (payload.get("version_desc") or "").strip(),
                 (payload.get("release_date") or "").strip() or today_text(),
                 stable, operator))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "固件版本 %s 已保存" % code,
            "version_code": code}


def toggle_version_stable(version_code, stable_flag):
    """切换固件版本的稳定版标记，测试版本不允许下发升级。"""
    row = fetch_version_row((version_code or "").strip())
    if row is None:
        return {"success": False, "message": "固件版本不存在"}
    flag = 1 if stable_flag else 0
    conn = connect_db()
    try:
        conn.execute(
            "UPDATE iot_firmware_version SET stable_flag=? WHERE version_code=?",
            (flag, row["version_code"]))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "已标记为%s" % ("稳定版" if flag else "测试版")}


def remove_firmware_version(version_code):
    """删除固件版本档案，已产生升级日志的版本不允许删除。"""
    row = fetch_version_row((version_code or "").strip())
    if row is None:
        return {"success": False, "message": "固件版本不存在"}
    conn = connect_db()
    try:
        used = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_upgrade_log WHERE version_code=?",
            (row["version_code"],)).fetchone()["c"]
        if used:
            return {"success": False,
                    "message": "该版本已产生 %d 条升级日志，不可删除" % used}
        conn.execute(
            "DELETE FROM iot_firmware_version WHERE version_code=?",
            (row["version_code"],))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "固件版本已删除"}


# ------------------------------------------------------------------
# 升级任务
# ------------------------------------------------------------------
def resolve_upgrade_targets(scope_type, scope_code):
    """按升级范围解析目标设备清单。

    :return: 设备节点字典列表
    """
    nodes = fetch_device_nodes()
    scope_type = (scope_type or "ALL").strip()
    scope_code = (scope_code or "").strip()
    if scope_type == "DEVICE" and scope_code:
        return [item for item in nodes if item["device_code"] == scope_code]
    if scope_type == "GROUP" and scope_code:
        conn = connect_db()
        try:
            rows = conn.execute(
                "SELECT device_code FROM iot_group_bind WHERE group_code=?",
                (scope_code,)).fetchall()
        finally:
            conn.close()
        codes = [item["device_code"] for item in rows]
        return [item for item in nodes if item["device_code"] in codes]
    return nodes


def create_upgrade_task(payload):
    """创建 OTA 升级任务，并为每台目标设备登记一条待升级日志。

    仅稳定版固件可被选为升级目标；目标设备为空时拒绝创建任务。

    :return: 含 success、message 与 task_code 的执行结果字典
    """
    version_code = (payload.get("version_code") or "").strip()
    version_row = fetch_version_row(version_code)
    if version_row is None:
        return {"success": False, "message": "请选择目标固件版本"}
    if not version_row["stable_flag"]:
        return {"success": False, "message": "测试版本不允许下发升级"}
    scope_type = (payload.get("scope_type") or "ALL").strip()
    if scope_type not in UPGRADE_SCOPE_DICT:
        return {"success": False, "message": "升级范围 %s 不合法" % scope_type}
    targets = resolve_upgrade_targets(scope_type, payload.get("scope_code"))
    if not targets:
        return {"success": False, "message": "该范围内没有可升级的设备"}
    operator = (payload.get("operator_name") or "admin").strip()
    version_map = fetch_device_version_map()
    task_code = build_task_code()
    # 日志编号基准在创建前取一次，循环内递增，避免同事务内重复查询撞号
    serial = next_serial("iot_upgrade_log", "log_code", LOG_CODE_PREFIX + "-")
    conn = connect_db()
    try:
        conn.execute(
            "INSERT INTO iot_upgrade_task (task_code, task_name, version_code, "
            "scope_type, scope_code, device_amount, task_state, schedule_time, "
            "operator_name) VALUES (?,?,?,?,?,?,'WAIT',?,?)",
            (task_code,
             (payload.get("task_name") or "").strip() or
             "%s 固件升级" % version_row["version_no"],
             version_row["version_code"], scope_type,
             (payload.get("scope_code") or "").strip(), len(targets),
             (payload.get("schedule_time") or "").strip() or now_text(),
             operator))
        for index, device in enumerate(targets):
            conn.execute(
                "INSERT INTO iot_upgrade_log (log_code, task_code, device_code, "
                "device_name, version_code, old_version, upgrade_state) "
                "VALUES (?,?,?,?,?,?,'PENDING')",
                (("%s-%04d" % (LOG_CODE_PREFIX, serial + index)),
                 task_code, device["device_code"], device["device_name"],
                 version_row["version_code"],
                 version_map.get(device["device_code"], BASE_VERSION_NO)))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "升级任务已创建，覆盖设备 %d 台" % len(targets),
            "task_code": task_code}


def run_upgrade_task(task_code, operator="admin"):
    """下发执行升级任务，逐台模拟升级结果并回写任务统计。

    升级成功率按设备运行状态取值：在线设备成功率最高，休眠设备次之，离线
    与故障设备直接判定失败；执行完成后按成功率判定任务状态。

    :return: 含 success、message 与统计信息的执行结果字典
    """
    task = fetch_task_row((task_code or "").strip())
    if task is None:
        return {"success": False, "message": "升级任务不存在"}
    if task["task_state"] not in ("WAIT", "PARTIAL", "FAIL"):
        return {"success": False, "message": "该任务已执行完成，无需重复下发"}
    version_row = fetch_version_row(task["version_code"])
    if version_row is None:
        return {"success": False, "message": "目标固件版本已不存在"}

    nodes = {item["device_code"]: item for item in fetch_device_nodes()}
    randomizer = random.Random(DEMO_RANDOM_SEED + len(task["task_code"]))
    conn = connect_db()
    try:
        logs = conn.execute(
            "SELECT * FROM iot_upgrade_log WHERE task_code=? AND "
            "upgrade_state IN ('PENDING','FAIL') ORDER BY log_id",
            (task["task_code"],)).fetchall()
        if not logs:
            return {"success": False, "message": "该任务没有待升级的设备"}
        start_text = now_text()
        success = 0
        fail = 0
        for item in logs:
            device = nodes.get(item["device_code"], {})
            state = device.get("run_state", "OFFLINE")
            rate = STATE_SUCCESS_RATE.get(state, 0)
            ok = randomizer.randint(1, 100) <= rate
            stamp = datetime.datetime.now() + datetime.timedelta(
                seconds=UPGRADE_BASE_SEC + randomizer.randint(0, UPGRADE_RANDOM_SEC))
            finish_text = stamp.strftime("%Y-%m-%d %H:%M:%S")
            if ok:
                success += 1
                conn.execute(
                    "UPDATE iot_upgrade_log SET upgrade_state='SUCCESS', "
                    "progress_rate=100, new_version=?, error_text='', "
                    "start_time=?, finish_time=? WHERE log_code=?",
                    (version_row["version_no"], start_text, finish_text,
                     item["log_code"]))
            else:
                fail += 1
                conn.execute(
                    "UPDATE iot_upgrade_log SET upgrade_state='FAIL', "
                    "progress_rate=?, new_version='', error_text=?, "
                    "start_time=?, finish_time=? WHERE log_code=?",
                    (round2(randomizer.randint(10, 65)),
                     FAIL_REASON_MAP.get(state, "升级通道建立失败"),
                     start_text, finish_text, item["log_code"]))
        total = success + fail
        task_state = "DONE"
        if success == 0:
            task_state = "FAIL"
        elif fail > 0:
            task_state = "PARTIAL"
        done_amount = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_upgrade_log WHERE task_code=? AND "
            "upgrade_state='SUCCESS'", (task["task_code"],)).fetchone()["c"]
        progress = round2(done_amount * 100.0 / task["device_amount"]) \
            if task["device_amount"] else 0.0
        conn.execute(
            "UPDATE iot_upgrade_task SET task_state=?, success_count=?, "
            "fail_count=?, progress_rate=?, start_time=?, finish_time=? "
            "WHERE task_code=?",
            (task_state, success, fail, progress, start_text,
             finish_text, task["task_code"]))
        conn.execute(
            "UPDATE iot_firmware_version SET upgrade_count=upgrade_count+? "
            "WHERE version_code=?", (success, version_row["version_code"]))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "升级下发完成，成功 %d 台、失败 %d 台" % (success, fail),
            "success_count": success, "fail_count": fail, "total": total}


def retry_upgrade_log(log_code, operator="admin"):
    """对失败的升级日志发起重升，设备在线时重新执行升级。

    :return: 含 success 与 message 的执行结果字典
    """
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_upgrade_log WHERE log_code=?",
            ((log_code or "").strip(),)).fetchone()
        if row is None:
            return {"success": False, "message": "升级记录不存在"}
        if row["upgrade_state"] not in ("FAIL", "PENDING"):
            return {"success": False, "message": "仅失败或待升级的记录可重升"}
        device = conn.execute(
            "SELECT run_state FROM iot_device_node WHERE device_code=?",
            (row["device_code"],)).fetchone()
        version = conn.execute(
            "SELECT version_no FROM iot_firmware_version WHERE version_code=?",
            (row["version_code"],)).fetchone()
    finally:
        conn.close()
    state = device["run_state"] if device else "OFFLINE"
    version_no = version["version_no"] if version else ""
    if state in ("OFFLINE", "FAULT"):
        return {"success": False,
                "message": "设备当前%s，无法重升" % ("离线" if state == "OFFLINE" else "故障")}
    randomizer = random.Random(DEMO_RANDOM_SEED + len(log_code))
    ok = randomizer.randint(1, 100) <= STATE_SUCCESS_RATE.get(state, 90)
    start_text = now_text()
    finish_text = (datetime.datetime.now() + datetime.timedelta(
        seconds=UPGRADE_BASE_SEC)).strftime("%Y-%m-%d %H:%M:%S")
    conn = connect_db()
    try:
        if ok:
            conn.execute(
                "UPDATE iot_upgrade_log SET upgrade_state='SUCCESS', "
                "progress_rate=100, new_version=?, error_text='', start_time=?, "
                "finish_time=? WHERE log_code=?",
                (version_no, start_text, finish_text, row["log_code"]))
            conn.execute(
                "UPDATE iot_upgrade_task SET success_count=success_count+1, "
                "fail_count=CASE WHEN fail_count>0 THEN fail_count-1 ELSE 0 END "
                "WHERE task_code=?", (row["task_code"],))
        else:
            conn.execute(
                "UPDATE iot_upgrade_log SET upgrade_state='FAIL', "
                "progress_rate=?, error_text=?, start_time=?, finish_time=? "
                "WHERE log_code=?",
                (round2(randomizer.randint(20, 70)), "重升后固件校验仍未通过",
                 start_text, finish_text, row["log_code"]))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "重升%s" % ("成功" if ok else "仍未成功")}


def rollback_upgrade_log(log_code, operator="admin"):
    """把已升级的设备回滚到升级前的固件版本。

    :return: 含 success 与 message 的执行结果字典
    """
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_upgrade_log WHERE log_code=?",
            ((log_code or "").strip(),)).fetchone()
        if row is None:
            return {"success": False, "message": "升级记录不存在"}
        if row["upgrade_state"] != "SUCCESS":
            return {"success": False, "message": "仅升级成功的记录可回滚"}
        conn.execute(
            "UPDATE iot_upgrade_log SET upgrade_state='ROLLBACK', "
            "progress_rate=100, new_version=?, error_text=?, finish_time=? "
            "WHERE log_code=?",
            (row["old_version"], "升级后运行异常，已回滚至升级前版本",
             now_text(), row["log_code"]))
        conn.execute(
            "UPDATE iot_upgrade_task SET success_count=CASE WHEN "
            "success_count>0 THEN success_count-1 ELSE 0 END, "
            "fail_count=fail_count+1 WHERE task_code=?", (row["task_code"],))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "已回滚至 %s" % row["old_version"]}


# ------------------------------------------------------------------
# 查询
# ------------------------------------------------------------------
def fetch_version_rows(keyword="", device_type=""):
    """按条件查询固件版本档案，并附带稳定版标签。"""
    where = []
    params = []
    if keyword:
        where.append("(version_code LIKE ? OR firmware_name LIKE ? OR "
                     "version_no LIKE ?)")
        like = "%%%s%%" % keyword
        params.extend([like, like, like])
    if device_type:
        where.append("device_type=?")
        params.append(device_type)
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT * FROM iot_firmware_version" + clause +
            " ORDER BY version_id DESC", params).fetchall()
    finally:
        conn.close()
    records = []
    for item in rows:
        row = dict(item)
        row["stable_label"] = "稳定版" if row["stable_flag"] else "测试版"
        records.append(row)
    return records


def fetch_task_rows(task_state="", limit=20):
    """按状态查询升级任务，并附带状态与范围标签。"""
    where = []
    params = []
    if task_state:
        where.append("task_state=?")
        params.append(task_state)
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT t.*, v.version_no, v.firmware_name FROM iot_upgrade_task t "
            "LEFT JOIN iot_firmware_version v ON t.version_code=v.version_code"
            + clause + " ORDER BY t.task_id DESC LIMIT ?",
            params + [limit]).fetchall()
    finally:
        conn.close()
    records = []
    for item in rows:
        row = dict(item)
        row["state_label"] = UPGRADE_TASK_DICT.get(row["task_state"], row["task_state"])
        row["scope_label"] = UPGRADE_SCOPE_DICT.get(row["scope_type"], row["scope_type"])
        records.append(row)
    return records


def fetch_log_rows(task_code="", upgrade_state="", keyword="", limit=40):
    """按任务与升级结论查询升级日志，并附带结论标签。"""
    where = []
    params = []
    if task_code:
        where.append("task_code=?")
        params.append(task_code)
    if upgrade_state:
        where.append("upgrade_state=?")
        params.append(upgrade_state)
    if keyword:
        where.append("(device_code LIKE ? OR device_name LIKE ?)")
        like = "%%%s%%" % keyword
        params.extend([like, like])
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT * FROM iot_upgrade_log" + clause +
            " ORDER BY log_id DESC LIMIT ?", params + [limit]).fetchall()
    finally:
        conn.close()
    records = []
    for item in rows:
        row = dict(item)
        row["state_label"] = UPGRADE_LOG_DICT.get(row["upgrade_state"],
                                                  row["upgrade_state"])
        records.append(row)
    return records


# ------------------------------------------------------------------
# 统计
# ------------------------------------------------------------------
def build_upgrade_summary():
    """汇总固件升级核心指标：版本数、任务数、覆盖台次、成功率与失败台次。"""
    conn = connect_db()
    try:
        version_amount = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_firmware_version").fetchone()["c"]
        stable_amount = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_firmware_version WHERE stable_flag=1"
        ).fetchone()["c"]
        task_rows = conn.execute(
            "SELECT task_state, COUNT(1) AS c FROM iot_upgrade_task "
            "GROUP BY task_state").fetchall()
        log_rows = conn.execute(
            "SELECT upgrade_state, COUNT(1) AS c FROM iot_upgrade_log "
            "GROUP BY upgrade_state").fetchall()
        device_amount = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_device_node").fetchone()["c"]
    finally:
        conn.close()
    task_counter = {item["task_state"]: item["c"] for item in task_rows}
    log_counter = {item["upgrade_state"]: item["c"] for item in log_rows}
    success = log_counter.get("SUCCESS", 0)
    fail = log_counter.get("FAIL", 0)
    rollback = log_counter.get("ROLLBACK", 0)
    judged = success + fail + rollback
    return {
        "version_amount": version_amount,
        "stable_amount": stable_amount,
        "task_amount": sum(task_counter.values()),
        "running_amount": task_counter.get("RUNNING", 0) +
                          task_counter.get("WAIT", 0),
        "device_amount": device_amount,
        "success_amount": success,
        "fail_amount": fail,
        "rollback_amount": rollback,
        "pending_amount": log_counter.get("PENDING", 0),
        "success_rate": round2(success * 100.0 / judged) if judged else 0.0,
    }


def build_upgrade_charts():
    """构建五类图表数据：版本覆盖分布、任务状态分布、升级结论分布、版本
    升级台次排行与近十次任务成功率趋势。"""
    conn = connect_db()
    try:
        version_rows = conn.execute(
            "SELECT version_code, version_no, firmware_name, device_type, "
            "upgrade_count FROM iot_firmware_version ORDER BY version_id").fetchall()
        task_state_rows = conn.execute(
            "SELECT task_state, COUNT(1) AS amount FROM iot_upgrade_task "
            "GROUP BY task_state").fetchall()
        log_state_rows = conn.execute(
            "SELECT upgrade_state, COUNT(1) AS amount FROM iot_upgrade_log "
            "GROUP BY upgrade_state").fetchall()
        task_rows = conn.execute(
            "SELECT task_code, device_amount, success_count, fail_count, "
            "created_at FROM iot_upgrade_task ORDER BY task_id DESC LIMIT 10"
        ).fetchall()
        device_rows = conn.execute(
            "SELECT device_code, device_name, COUNT(1) AS amount, "
            "SUM(CASE WHEN upgrade_state='SUCCESS' THEN 1 ELSE 0 END) AS ok_amount "
            "FROM iot_upgrade_log GROUP BY device_code, device_name "
            "ORDER BY amount DESC LIMIT 8").fetchall()
    finally:
        conn.close()

    version_map = fetch_device_version_map()
    cover_counter = {}
    for code, version_no in version_map.items():
        cover_counter[version_no] = cover_counter.get(version_no, 0) + 1
    version_distribution = [
        {"name": key, "value": value} for key, value in
        sorted(cover_counter.items(), key=lambda x: x[1], reverse=True)
    ]

    task_distribution = [
        {"name": UPGRADE_TASK_DICT.get(code, code), "value": amount}
        for code, amount in task_state_rows
    ]
    log_distribution = [
        {"name": UPGRADE_LOG_DICT.get(code, code), "value": amount}
        for code, amount in log_state_rows
    ]
    version_rank = [
        {"name": row["version_no"] or row["firmware_name"],
         "value": row["upgrade_count"] or 0}
        for row in version_rows
    ]
    version_rank.sort(key=lambda x: x["value"], reverse=True)
    task_trend = [
        {"name": (row["created_at"] or "")[-8:] or row["task_code"][-6:],
         "success_rate": round2(row["success_count"] * 100.0 /
                                (row["success_count"] + row["fail_count"]))
         if (row["success_count"] + row["fail_count"]) else 0.0,
         "success": row["success_count"], "fail": row["fail_count"]}
        for row in reversed(task_rows)
    ]
    device_rank = [
        {"name": row["device_name"], "value": row["amount"],
         "success": row["ok_amount"]}
        for row in device_rows
    ]
    return {
        "version_distribution": version_distribution,
        "task_distribution": task_distribution,
        "log_distribution": log_distribution,
        "version_rank": version_rank[:6],
        "task_trend": task_trend,
        "device_rank": device_rank,
    }


def fetch_upgrade_options():
    """固件升级页下拉选项：字典项、设备候选、分组候选与版本候选。"""
    conn = connect_db()
    try:
        device_rows = conn.execute(
            "SELECT device_code, device_name, device_type, run_state "
            "FROM iot_device_node ORDER BY device_code").fetchall()
        group_rows = conn.execute(
            "SELECT group_code, group_name FROM iot_device_group "
            "ORDER BY group_id").fetchall()
    finally:
        conn.close()
    version_map = fetch_device_version_map()
    device_list = []
    for item in device_rows:
        row = dict(item)
        row["current_version"] = version_map.get(row["device_code"], BASE_VERSION_NO)
        device_list.append(row)
    return {
        "device_list": device_list,
        "group_list": dict_list(group_rows),
        "version_list": fetch_version_rows(),
        "scope_options": [{"code": k, "label": v}
                          for k, v in UPGRADE_SCOPE_DICT.items()],
        "task_options": [{"code": k, "label": v}
                         for k, v in UPGRADE_TASK_DICT.items()],
        "log_options": [{"code": k, "label": v} for k, v in UPGRADE_LOG_DICT.items()],
    }


def build_upgrade_payload(version_keyword="", task_state="", log_task="",
                          log_state="", log_keyword=""):
    """组装固件版本与 OTA 升级管理页面所需的全部数据。"""
    return {
        "summary": build_upgrade_summary(),
        "charts": build_upgrade_charts(),
        "versions": fetch_version_rows(version_keyword),
        "tasks": fetch_task_rows(task_state),
        "logs": fetch_log_rows(log_task, log_state, log_keyword),
        "options": fetch_upgrade_options(),
        "result": "OK",
    }


def build_upgrade_brief():
    """固件升级看板摘要，供实时监控大屏嵌入。"""
    summary = build_upgrade_summary()
    return {
        "version_amount": summary["version_amount"],
        "task_amount": summary["task_amount"],
        "success_amount": summary["success_amount"],
        "fail_amount": summary["fail_amount"],
        "pending_amount": summary["pending_amount"],
        "success_rate": summary["success_rate"],
    }


# ------------------------------------------------------------------
# 演示数据
# ------------------------------------------------------------------
def seed_demo_upgrade():
    """灌入固件升级演示数据：版本建档、任务创建与升级下发。

    :return: 生成的固件版本条数
    """
    conn = connect_db()
    try:
        already = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_firmware_version").fetchone()["c"]
        if already:
            return 0
    finally:
        conn.close()

    demo_version_list = (
        ("监护类设备主控固件", "监护类", "v2.3.1", 2048,
         "修复血氧采样漂移问题，优化低功耗唤醒策略"),
        ("影像类设备采集固件", "影像类", "v1.8.4", 8192,
         "提升图像采集帧率，修复偶发条纹干扰"),
        ("检验类设备控制固件", "检验类", "v3.0.2", 4096,
         "新增试剂余量检测，优化温控闭环参数"),
        ("治疗类设备驱动固件", "治疗类", "v2.0.7", 3072,
         "修正输注速度偏差，增强管路阻塞检测"),
    )
    version_codes = []
    today = today_text()
    for index, (name, device_type, version_no, size, desc) \
            in enumerate(demo_version_list):
        outcome = save_firmware_version({
            "firmware_name": name,
            "device_type": device_type,
            "version_no": version_no,
            "file_size_kb": size,
            "version_desc": desc,
            "release_date": (
                datetime.date.today() - datetime.timedelta(days=30 - index * 7)
            ).strftime("%Y-%m-%d"),
            "stable_flag": 0 if index == 3 else 1,
            "operator_name": "信息科-韩工",
        })
        if outcome.get("success"):
            version_codes.append(outcome["version_code"])

    # 演示任务：全量升级、分组升级、单台升级与一次待下发任务
    task_plan = (
        ("ALL", ""),
        ("GROUP", ""),
        ("DEVICE", ""),
        ("ALL", ""),
    )
    group_rows = fetch_upgrade_options()["group_list"]
    device_rows = fetch_device_nodes()
    amount = len(version_codes)
    for index, (scope_type, _scope) in enumerate(task_plan):
        if not version_codes:
            break
        scope_code = ""
        if scope_type == "GROUP" and group_rows:
            scope_code = group_rows[0]["group_code"]
        elif scope_type == "DEVICE" and device_rows:
            scope_code = device_rows[index % len(device_rows)]["device_code"]
        outcome = create_upgrade_task({
            "task_name": "%s 第 %d 批固件升级" % (today, index + 1),
            # 演示版本清单末位为测试版，任务只在前三个稳定版中轮换
            "version_code": version_codes[index % 3] if len(version_codes) >= 3
            else version_codes[index % len(version_codes)],
            "scope_type": scope_type,
            "scope_code": scope_code,
            "operator_name": "信息科-韩工",
        })
        if not outcome.get("success"):
            continue
        # 最后一批保留为待下发，用于演示任务状态分布与手动下发
        if index == len(task_plan) - 1:
            continue
        run_upgrade_task(outcome["task_code"], "信息科-韩工")

    # 演示回滚：把第一条升级成功的记录回滚，展示异常回滚闭环
    success_logs = fetch_log_rows(upgrade_state="SUCCESS")
    if len(success_logs) > 2:
        rollback_upgrade_log(success_logs[-1]["log_code"], "信息科-韩工")
    return amount

# --------------------------------------------------------------------------
# 第 11 天数据表脚本增量：iot_schema.sql（M，已内联为字符串常量 DAY11_SCHEMA_SQL）
# --------------------------------------------------------------------------

DAY11_SCHEMA_SQL = r"""
-- ==================================================================
-- 第 11 天新增：固件版本档案表
-- ==================================================================
-- 固件版本表：按「一个设备类型一个固件包」记录版本号、包体积、发布日期与
-- 稳定版标记，是 OTA 升级任务的升级目标依据；稳定版标记用于区分正式发布包
-- 与测试包，下发升级时仅允许选择稳定版本
CREATE TABLE IF NOT EXISTS iot_firmware_version (
    version_id     INTEGER PRIMARY KEY AUTOINCREMENT,  -- 版本主键
    version_code   TEXT    NOT NULL UNIQUE,            -- 版本编号（FW-0001）
    firmware_name  TEXT    DEFAULT '',                 -- 固件包名称
    device_type    TEXT    DEFAULT '',                 -- 适用设备类型
    version_no     TEXT    DEFAULT '',                 -- 版本号（如 v2.3.1）
    file_size_kb   INTEGER DEFAULT 0,                  -- 固件包体积（KB）
    version_desc   TEXT    DEFAULT '',                 -- 版本说明（更新内容）
    release_date   TEXT    DEFAULT '',                 -- 发布日期
    stable_flag    INTEGER DEFAULT 1,                  -- 稳定版标记（1=稳定版 0=测试版）
    upgrade_count  INTEGER DEFAULT 0,                  -- 累计升级台次
    operator_name  TEXT    DEFAULT 'admin',            -- 版本发布人
    created_at     TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- ==================================================================
-- 第 11 天新增：OTA 升级任务表
-- ==================================================================
-- 升级任务表：按「一次下发一个任务」记录升级目标版本、目标范围（单台设备、
-- 设备分组或全量设备）、计划执行时间与执行结果统计；任务状态随下发执行
-- 逐级流转，执行完成后回写成功台次、失败台次与完成进度
CREATE TABLE IF NOT EXISTS iot_upgrade_task (
    task_id        INTEGER PRIMARY KEY AUTOINCREMENT,  -- 任务主键
    task_code      TEXT    NOT NULL UNIQUE,            -- 任务编号（UT-年月日时分秒毫秒）
    task_name      TEXT    DEFAULT '',                 -- 任务名称
    version_code   TEXT    NOT NULL,                   -- 目标固件版本编号
    scope_type     TEXT    DEFAULT 'ALL',              -- 升级范围（见 UPGRADE_SCOPE_DICT）
    scope_code     TEXT    DEFAULT '',                 -- 范围编码（设备编号或分组编号）
    device_amount  INTEGER DEFAULT 0,                  -- 目标设备台数
    success_count  INTEGER DEFAULT 0,                  -- 升级成功台次
    fail_count     INTEGER DEFAULT 0,                  -- 升级失败台次
    progress_rate  REAL    DEFAULT 0,                  -- 任务进度（%）
    task_state     TEXT    DEFAULT 'WAIT',             -- 任务状态（见 UPGRADE_TASK_DICT）
    schedule_time  TEXT    DEFAULT '',                 -- 计划下发时间
    start_time     TEXT    DEFAULT '',                 -- 开始执行时间
    finish_time    TEXT    DEFAULT '',                 -- 执行完成时间
    operator_name  TEXT    DEFAULT 'admin',            -- 任务创建人
    created_at     TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- ==================================================================
-- 第 11 天新增：设备升级日志表
-- ==================================================================
-- 升级日志表：按「一台设备一次升级」记录升级前后版本号、升级进度与执行结论，
-- 离线或故障设备判定为升级失败并可发起重升，升级后异常可回滚到升级前版本，
-- 支撑固件升级全过程的可追溯管理
CREATE TABLE IF NOT EXISTS iot_upgrade_log (
    log_id         INTEGER PRIMARY KEY AUTOINCREMENT,  -- 日志主键
    log_code       TEXT    NOT NULL UNIQUE,            -- 日志编号（UL-0001）
    task_code      TEXT    NOT NULL,                   -- 关联任务编号
    device_code    TEXT    NOT NULL,                   -- 设备编号
    device_name    TEXT    DEFAULT '',                 -- 设备名称快照
    version_code   TEXT    NOT NULL,                   -- 目标固件版本编号
    old_version    TEXT    DEFAULT '',                 -- 升级前版本号
    new_version    TEXT    DEFAULT '',                 -- 升级后版本号
    upgrade_state  TEXT    DEFAULT 'PENDING',          -- 升级结论（见 UPGRADE_LOG_DICT）
    progress_rate  REAL    DEFAULT 0,                  -- 升级进度（%）
    error_text     TEXT    DEFAULT '',                 -- 失败原因
    start_time     TEXT    DEFAULT '',                 -- 开始升级时间
    finish_time    TEXT    DEFAULT '',                 -- 升级完成时间
    created_at     TEXT    DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_fwversion_code   ON iot_firmware_version (version_code);
CREATE INDEX IF NOT EXISTS idx_fwversion_type   ON iot_firmware_version (device_type);
CREATE INDEX IF NOT EXISTS idx_uptask_code      ON iot_upgrade_task (task_code);
CREATE INDEX IF NOT EXISTS idx_uptask_version   ON iot_upgrade_task (version_code);
CREATE INDEX IF NOT EXISTS idx_uptask_state     ON iot_upgrade_task (task_state);
CREATE INDEX IF NOT EXISTS idx_uplog_code       ON iot_upgrade_log (log_code);
CREATE INDEX IF NOT EXISTS idx_uplog_task       ON iot_upgrade_log (task_code);
CREATE INDEX IF NOT EXISTS idx_uplog_device     ON iot_upgrade_log (device_code);
CREATE INDEX IF NOT EXISTS idx_uplog_state      ON iot_upgrade_log (upgrade_state);
"""


def apply_day11_schema(conn):
    """把第 11 天新增的数据表脚本应用到已有数据库。"""
    conn.executescript(DAY11_SCHEMA_SQL)
    conn.commit()
    return True

# --------------------------------------------------------------------------
# 第 11 天主程序新增路由与接口：app.py（仅增量片段）
# --------------------------------------------------------------------------

# 开发日期：2026-10-31（第 11 天 / 共 15 天）
#           阈值、能耗、远程控制、分组标签、网关管理、采集点位与固件版本
#           演示数据灌入、拉起 MQTT 订阅服务（或模拟采集器），并注册实时
#           监控大屏、设备接入、在线状态、实时参数采集、阈值告警中心、能耗
#           统计、远程控制、分组标签管理、物联网网关管理、采集点位转发与
#           固件 OTA 升级十一类页面路由，提供设备注册增删改查、心跳上报、
#           离线扫描识别、实时参数采集存储、动态曲线、阈值扫描与告警处置、
#           能耗分时采集、峰谷电费测算与节能建议、远程指令模板维护、指令
#           下发回执与重发、设备分组维护、入组退组、标签维护与批量打标、
#           网关档案维护、链路探测、远程重启、

from iot_upgrade import (
    build_upgrade_payload,
    build_upgrade_brief,
    save_firmware_version,
    toggle_version_stable,
    remove_firmware_version,
    create_upgrade_task,
    run_upgrade_task,
    retry_upgrade_log,
    rollback_upgrade_log,
    fetch_task_rows,
    fetch_log_rows,
    seed_demo_upgrade,
    UPGRADE_SCOPE_DICT,
    UPGRADE_TASK_DICT,
    UPGRADE_LOG_DICT,
)

# 【函数内插入】def api_monitor():
#    # 第 10 天新增：大屏叠加采集点位摘要，供采集概况卡渲染
#    # 第 11 天新增：大屏叠加固件升级摘要，供升级概况卡渲染
#    payload["upgrade_brief"] = build_upgrade_brief()

@app.route("/iot/upgrade")
def page_upgrade():
    """固件版本与 OTA 升级管理页面：版本档案、升级任务与设备升级日志。"""
    return render_template(
        "page_upgrade.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        refresh_interval=REFRESH_INTERVAL_SEC,
    )


@app.route("/api/iot/upgrade/panel")
def api_upgrade_panel():
    """固件升级数据接口：汇总指标、五类图表、版本任务与日志明细。"""
    return jsonify(build_upgrade_payload(
        version_keyword=(request.args.get("version_keyword") or "").strip(),
        task_state=(request.args.get("task_state") or "").strip(),
        log_task=(request.args.get("log_task") or "").strip(),
        log_state=(request.args.get("log_state") or "").strip(),
        log_keyword=(request.args.get("log_keyword") or "").strip(),
    ))


@app.route("/api/iot/upgrade/version/save", methods=["POST"])
def api_upgrade_version_save():
    """固件版本登记接口：传入编号则更新，留空编号则按序号建档。"""
    body = request.get_json(silent=True) or {}
    result = save_firmware_version(body)
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/upgrade/version/stable", methods=["POST"])
def api_upgrade_version_stable():
    """固件版本稳定版标记切换接口，测试版本不允许下发升级。"""
    body = request.get_json(silent=True) or {}
    result = toggle_version_stable(
        (body.get("version_code") or "").strip(),
        str(body.get("stable_flag", 1)) in ("1", "true", "True"),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/upgrade/version/remove", methods=["POST"])
def api_upgrade_version_remove():
    """固件版本删除接口，已产生升级日志的版本不允许删除。"""
    body = request.get_json(silent=True) or {}
    result = remove_firmware_version((body.get("version_code") or "").strip())
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/upgrade/task/create", methods=["POST"])
def api_upgrade_task_create():
    """升级任务创建接口：按范围解析目标设备并逐台登记待升级日志。"""
    body = request.get_json(silent=True) or {}
    result = create_upgrade_task(body)
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/upgrade/task/run", methods=["POST"])
def api_upgrade_task_run():
    """升级任务下发接口：逐台模拟升级结果并回写任务统计。"""
    body = request.get_json(silent=True) or {}
    result = run_upgrade_task(
        (body.get("task_code") or "").strip(),
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/upgrade/log/retry", methods=["POST"])
def api_upgrade_log_retry():
    """升级失败重升接口：对失败或待升级的设备重新发起升级。"""
    body = request.get_json(silent=True) or {}
    result = retry_upgrade_log(
        (body.get("log_code") or "").strip(),
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/upgrade/log/rollback", methods=["POST"])
def api_upgrade_log_rollback():
    """升级回滚接口：把已升级设备回滚到升级前的固件版本。"""
    body = request.get_json(silent=True) or {}
    result = rollback_upgrade_log(
        (body.get("log_code") or "").strip(),
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/upgrade/tasks")
def api_upgrade_tasks():
    """升级任务清单接口：按状态返回任务及其执行统计。"""
    return jsonify({
        "result": "OK",
        "tasks": fetch_task_rows((request.args.get("task_state") or "").strip(),
                                 int(request.args.get("limit", 20))),
    })


@app.route("/api/iot/upgrade/logs")
def api_upgrade_logs():
    """升级日志清单接口：按任务与升级结论返回设备升级明细。"""
    return jsonify({
        "result": "OK",
        "logs": fetch_log_rows(
            (request.args.get("task_code") or "").strip(),
            (request.args.get("upgrade_state") or "").strip(),
            (request.args.get("keyword") or "").strip(),
            int(request.args.get("limit", 40))),
    })


@app.route("/api/iot/upgrade/options")
def api_upgrade_options():
    """固件升级下拉选项接口：升级范围、任务状态与升级结论字典。"""
    return jsonify({
        "result": "OK",
        "scope_options": [{"code": k, "label": v}
                          for k, v in UPGRADE_SCOPE_DICT.items()],
        "task_options": [{"code": k, "label": v}
                         for k, v in UPGRADE_TASK_DICT.items()],
        "log_options": [{"code": k, "label": v} for k, v in UPGRADE_LOG_DICT.items()],
    })



# 【函数内插入】def bootstrap():
#    seed_demo_upgrade()

# 【函数内插入】if __name__ == "__main__":
#    print("固件升级： http://127.0.0.1:%d/iot/upgrade" % PORT)

# --------------------------------------------------------------------------
# 第 11 天新增/修改的前端资源清单（文件本体见工程 templates/、static/ 目录）
# --------------------------------------------------------------------------

#   [新增] static/iot_upgrade.js            新增 479 行
#   [修改] templates/iot_base.html          新增 1 行
#   [新增] templates/page_upgrade.html      新增 258 行
