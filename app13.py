# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-11-02（第 13 天 / 共 15 天）
# 模块功能：本文件仅含该软件第 13 天新增与修改的源代码，不含第 1~12 天已有代码
#           本次迭代主题：新增场景联动规则管理模块（规则建档、动作编排、条件判定、联动执行、日志留痕）
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）
# 接入说明：以下内容为当日增量，需与前面各天的代码合并后运行——
#           新增模块按文件名放入工程目录；建表脚本增量追加进对应 .sql；
#           主程序增量片段插入 app.py 中 Flask 实例之后；前端资源按文末清单
#           放入 templates/ 与 static/ 目录


# --------------------------------------------------------------------------
# 第 13 天新增业务模块 1：iot_rule.py（完整源码）
# --------------------------------------------------------------------------

# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-11-02（第 13 天 / 共 15 天）
# 模块功能：场景联动规则管理模块。实现联动场景规则建档（场景类型、触发来源、
#           判据指标、比较运算符、触发阈值、作用范围与执行优先级）、规则下
#           多动作编排（下发控制指令、生成告警、推送通知、留痕记录、转发上报）、
#           规则启停、按实时遥测值自动判定触发条件、命中设备的动作逐条执行与
#           联动日志留痕，以及全量启用规则的批量扫描调度；控制类动作调用远程
#           控制模块真实下发指令，告警类动作写入阈值告警记录，形成「配规则 →
#           编动作 → 判条件 → 行动作 → 记日志」的边缘联动闭环；输出场景类型
#           分布、规则状态分布、动作类型分布、规则触发次数排行与近 7 日联动
#           执行趋势五类统计图表
# 依赖包：sqlite3（Python 内置）、datetime（Python 内置）

import datetime

from iot_store import connect_db, dict_list, now_text, fetch_device_nodes

# ------------------------------------------------------------------
# 场景类型字典：按医院物联网典型联动场景划分五类
# ------------------------------------------------------------------
SCENE_TYPE_DICT = {
    "ENV": "环境联动",
    "ALARM": "告警联动",
    "ENERGY": "能耗联动",
    "SECURITY": "安防联动",
    "SCHEDULE": "定时联动",
}

# 触发来源字典：区分遥测越限、告警触发、心跳异常、状态变更与定时触发
TRIGGER_SOURCE_DICT = {
    "TELEMETRY": "遥测越限",
    "ALARM": "告警触发",
    "HEARTBEAT": "心跳异常",
    "STATE": "状态变更",
    "SCHEDULE": "定时触发",
}

# 比较运算符字典
TRIGGER_OPER_DICT = {
    "GT": "大于",
    "GE": "大于等于",
    "LT": "小于",
    "LE": "小于等于",
    "EQ": "等于",
}

# 规则状态字典
RULE_STATE_DICT = {
    "ON": "已启用",
    "OFF": "已停用",
    "PAUSE": "暂停中",
}

# 执行优先级字典
PRIORITY_DICT = {
    "HIGH": "高",
    "MID": "中",
    "LOW": "低",
}

# 联动动作类型字典
ACTION_TYPE_DICT = {
    "CONTROL": "下发控制指令",
    "ALARM": "生成告警记录",
    "NOTIFY": "推送值班通知",
    "RECORD": "留存联动记录",
    "FORWARD": "转发上级平台",
}

# 联动执行结果字典
EXEC_RESULT_DICT = {
    "SUCCESS": "全部成功",
    "PARTIAL": "部分成功",
    "FAIL": "执行失败",
}

# 启用的规则状态：扫描调度只处理启用状态的规则
RUNNABLE_STATE = "ON"

# 心跳异常判定的离线阈值（分钟）
HEARTBEAT_FRESH_MINUTES = 5

# 规则分页尺寸
RULE_PAGE_SIZE = 10

# 单条规则单次最多命中的设备台数，避免批量联动冲击设备
MAX_HIT_DEVICE = 6

# 演示规则清单
# （规则名称，场景类型，触发来源，判据指标，运算符，阈值，优先级，状态，说明）
DEMO_RULE_LIST = (
    ("重症病区环境温度超限联动处置", "ENV", "TELEMETRY", "temp", "GT", 28.5,
     "HIGH", "ON", "病区温度超限自动下发空调调节并推送值班通知"),
    ("医用冰箱断电告警联动上报", "ALARM", "ALARM", "volt", "LT", 200.0,
     "HIGH", "ON", "冰箱供电异常时生成告警并转发上级平台"),
    ("影像设备待机能耗联动管控", "ENERGY", "TELEMETRY", "power", "GT", 1600.0,
     "MID", "ON", "待机功率过高时下发节能指令并记录留痕"),
    ("药房温湿度安防联动巡检", "SECURITY", "HEARTBEAT", "humi", "GT", 70.0,
     "LOW", "OFF", "湿度异常联动巡检任务，当前停用待复核"),
    ("夜间设备定时联动巡检", "SCHEDULE", "SCHEDULE", "power", "LT", 5.0,
     "MID", "PAUSE", "夜间低功率时段联动巡检，暂缓执行"),
)

# 演示动作编排：规则序号 -> （动作类型，动作目标，动作参数，延时秒）
DEMO_ACTION_MAP = {
    0: (("CONTROL", "CMD-PARAM", "目标温度 24℃", 0),
        ("NOTIFY", "值班护士站", "温度超限，请现场核查", 30),
        ("RECORD", "联动留痕", "记录环境联动处置过程", 60)),
    1: (("ALARM", "alarm_center", "供电异常告警", 0),
        ("FORWARD", "上级监管平台", "推送断电告警报文", 10),
        ("NOTIFY", "设备科值班手机", "冰箱断电请立即处置", 20)),
    2: (("CONTROL", "CMD-SWITCH", "切换节能模式", 0),
        ("RECORD", "能耗留痕", "记录节能联动执行结果", 15)),
    3: (("NOTIFY", "药房值班台", "湿度异常请巡检", 0),
        ("RECORD", "巡检留痕", "记录安防巡检联动", 30)),
    4: (("RECORD", "巡检留痕", "记录夜间巡检联动", 0),
        ("NOTIFY", "总值班", "夜间联动巡检完成", 20)),
}


def round1(value):
    """数值保留一位小数，空值与非数值按 0 处理。"""
    try:
        return round(float(value or 0), 1)
    except (TypeError, ValueError):
        return 0.0


def today_text():
    """返回今天日期文本 YYYY-MM-DD。"""
    return datetime.date.today().strftime("%Y-%m-%d")


def shift_days_text(days):
    """返回相对今天偏移指定天数的日期文本。"""
    return (datetime.date.today() + datetime.timedelta(days=days)).strftime("%Y-%m-%d")


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


def build_rule_code():
    """生成联动规则编号，形如 SR-0001。"""
    return "SR-%04d" % next_serial("iot_scene_rule", "rule_code", "SR-")


def build_action_code():
    """生成联动动作编号，形如 SA-0001。"""
    return "SA-%04d" % next_serial("iot_scene_action", "action_code", "SA-")


def build_log_code():
    """生成联动日志编号，形如 SL-0001。"""
    return "SL-%06d" % next_serial("iot_scene_log", "log_code", "SL-")


def compare_value(metric_value, oper_code, threshold):
    """按比较运算符判定实测值与阈值的关系，满足条件返回 True。"""
    try:
        value = float(metric_value)
        limit = float(threshold)
    except (TypeError, ValueError):
        return False
    if oper_code == "GT":
        return value > limit
    if oper_code == "GE":
        return value >= limit
    if oper_code == "LT":
        return value < limit
    if oper_code == "LE":
        return value <= limit
    if oper_code == "EQ":
        return abs(value - limit) < 0.0001
    return False


def build_trigger_desc(rule):
    """按规则配置拼装触发条件描述文本。"""
    return "%s %s %s" % (
        rule["trigger_metric"] or "--",
        TRIGGER_OPER_DICT.get(rule["trigger_oper"], rule["trigger_oper"]),
        rule["trigger_value"])


# ------------------------------------------------------------------
# 查询辅助
# ------------------------------------------------------------------
def fetch_rule_row(rule_code):
    """按规则编号读取一条联动规则，不存在返回 None。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_scene_rule WHERE rule_code=?",
            ((rule_code or "").strip(),)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


def fetch_rule_actions(rule_code):
    """读取某条规则下编排的全部联动动作，按执行顺序升序返回。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT * FROM iot_scene_action WHERE rule_code=? "
            "ORDER BY action_seq ASC", ((rule_code or "").strip(),)).fetchall()
    finally:
        conn.close()
    return dict_list(rows)


def fetch_scope_devices(rule):
    """按规则作用范围筛选候选设备：优先按设备编号，其次按网关，最后全量。"""
    device_list = fetch_device_nodes()
    scope_device = (rule.get("scope_device") or "").strip()
    scope_gateway = (rule.get("scope_gateway") or "").strip()
    if scope_device:
        device_list = [item for item in device_list
                       if item.get("device_code") == scope_device]
    elif scope_gateway:
        device_list = [item for item in device_list
                       if item.get("gateway_code") == scope_gateway]
    return device_list[:MAX_HIT_DEVICE]


def fetch_latest_metric(device_code, metric_key):
    """读取某设备某指标的最新遥测数值，无数据返回 None。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT metric_value FROM iot_telemetry_log WHERE device_code=? "
            "AND metric_key=? ORDER BY telemetry_id DESC LIMIT 1",
            (device_code, metric_key)).fetchone()
    finally:
        conn.close()
    return None if row is None else row["metric_value"]


# ------------------------------------------------------------------
# 规则建档与动作编排
# ------------------------------------------------------------------
def save_scene_rule(rule_code, rule_name, scene_type="ENV",
                    trigger_source="TELEMETRY", trigger_metric="temp",
                    trigger_oper="GT", trigger_value=0, scope_gateway="",
                    scope_device="", priority_level="MID", run_state="ON",
                    remark_text="", operator="admin"):
    """新建或编辑一条场景联动规则，并回算已编排的动作条数。

    :return: 含 success、message 与 rule_code 的执行结果字典
    """
    rule_name = (rule_name or "").strip()
    if not rule_name:
        return {"success": False, "message": "请填写联动规则名称"}
    if scene_type not in SCENE_TYPE_DICT:
        return {"success": False, "message": "场景类型 %s 不合法" % scene_type}
    if trigger_source not in TRIGGER_SOURCE_DICT:
        return {"success": False, "message": "触发来源 %s 不合法" % trigger_source}
    if trigger_oper not in TRIGGER_OPER_DICT:
        return {"success": False, "message": "比较运算符 %s 不合法" % trigger_oper}
    if run_state not in RULE_STATE_DICT:
        return {"success": False, "message": "规则状态 %s 不合法" % run_state}
    if priority_level not in PRIORITY_DICT:
        return {"success": False, "message": "执行优先级 %s 不合法" % priority_level}

    conn = connect_db()
    try:
        exist = None
        if (rule_code or "").strip():
            exist = conn.execute(
                "SELECT rule_code FROM iot_scene_rule WHERE rule_code=?",
                (rule_code.strip(),)).fetchone()
        if exist is None:
            code = build_rule_code()
            conn.execute(
                "INSERT INTO iot_scene_rule (rule_code, rule_name, scene_type, "
                "trigger_source, trigger_metric, trigger_oper, trigger_value, "
                "scope_gateway, scope_device, priority_level, run_state, "
                "remark_text, operator_name) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (code, rule_name, scene_type, trigger_source,
                 (trigger_metric or "").strip(), trigger_oper,
                 float(trigger_value or 0), (scope_gateway or "").strip(),
                 (scope_device or "").strip(), priority_level, run_state,
                 (remark_text or "").strip(), (operator or "admin").strip()))
            action = "规则建档"
        else:
            code = rule_code.strip()
            conn.execute(
                "UPDATE iot_scene_rule SET rule_name=?, scene_type=?, "
                "trigger_source=?, trigger_metric=?, trigger_oper=?, "
                "trigger_value=?, scope_gateway=?, scope_device=?, "
                "priority_level=?, run_state=?, remark_text=?, operator_name=? "
                "WHERE rule_code=?",
                (rule_name, scene_type, trigger_source,
                 (trigger_metric or "").strip(), trigger_oper,
                 float(trigger_value or 0), (scope_gateway or "").strip(),
                 (scope_device or "").strip(), priority_level, run_state,
                 (remark_text or "").strip(), (operator or "admin").strip(), code))
            action = "规则编辑"
        action_amount = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_scene_action WHERE rule_code=?",
            (code,)).fetchone()["c"]
        conn.execute(
            "UPDATE iot_scene_rule SET action_amount=? WHERE rule_code=?",
            (action_amount, code))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "联动规则 %s 已保存" % code,
            "rule_code": code}


def add_scene_action(rule_code, action_type="NOTIFY", action_target="",
                     action_param="", delay_sec=0, operator="admin"):
    """为一条联动规则追加一个编排动作，顺序号自动顺延。

    :return: 含 success、message 与 action_code 的执行结果字典
    """
    if action_type not in ACTION_TYPE_DICT:
        return {"success": False, "message": "动作类型 %s 不合法" % action_type}
    if fetch_rule_row(rule_code) is None:
        return {"success": False, "message": "联动规则不存在"}
    try:
        delay = max(0, int(delay_sec or 0))
    except (TypeError, ValueError):
        delay = 0
    conn = connect_db()
    try:
        actions = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_scene_action WHERE rule_code=?",
            ((rule_code or "").strip(),)).fetchone()
        code = build_action_code()
        conn.execute(
            "INSERT INTO iot_scene_action (action_code, rule_code, action_seq, "
            "action_type, action_target, action_param, delay_sec) "
            "VALUES (?,?,?,?,?,?,?)",
            (code, (rule_code or "").strip(), int(actions["c"] or 0) + 1,
             action_type, (action_target or "").strip(),
             (action_param or "").strip(), delay))
        conn.execute(
            "UPDATE iot_scene_rule SET action_amount=? WHERE rule_code=?",
            (int(actions["c"] or 0) + 1, (rule_code or "").strip()))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "联动动作 %s 已编排" % code,
            "action_code": code}


def remove_scene_action(action_code, operator="admin"):
    """删除一条编排动作并重排同规则下的动作顺序号。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_scene_action WHERE action_code=?",
            ((action_code or "").strip(),)).fetchone()
        if row is None:
            return {"success": False, "message": "联动动作不存在"}
        conn.execute(
            "DELETE FROM iot_scene_action WHERE action_code=?",
            (row["action_code"],))
        rest = conn.execute(
            "SELECT action_code FROM iot_scene_action WHERE rule_code=? "
            "ORDER BY action_seq ASC", (row["rule_code"],)).fetchall()
        for index, item in enumerate(rest):
            conn.execute(
                "UPDATE iot_scene_action SET action_seq=? WHERE action_code=?",
                (index + 1, item["action_code"]))
        conn.execute(
            "UPDATE iot_scene_rule SET action_amount=? WHERE rule_code=?",
            (len(rest), row["rule_code"]))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "联动动作 %s 已删除" % row["action_code"]}


def toggle_scene_rule(rule_code, run_state="ON", operator="admin"):
    """切换一条联动规则的启用、暂停或停用状态。"""
    if run_state not in RULE_STATE_DICT:
        return {"success": False, "message": "规则状态 %s 不合法" % run_state}
    if fetch_rule_row(rule_code) is None:
        return {"success": False, "message": "联动规则不存在"}
    conn = connect_db()
    try:
        conn.execute(
            "UPDATE iot_scene_rule SET run_state=?, operator_name=? "
            "WHERE rule_code=?",
            (run_state, (operator or "admin").strip(), (rule_code or "").strip()))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "联动规则 %s 已切换为%s" % (
        rule_code, RULE_STATE_DICT[run_state])}


# ------------------------------------------------------------------
# 条件判定与动作执行
# ------------------------------------------------------------------
def execute_single_action(action, device, trigger_desc):
    """执行单条编排动作，返回执行是否成功与执行说明。

    控制类动作调用远程控制模块真实下发指令；告警类动作写入阈值告警记录；
    通知、留痕与转发动作按模拟通道记录执行结果。
    """
    action_type = action["action_type"]
    if action_type == "CONTROL":
        try:
            from iot_control import dispatch_control_command

            outcome = dispatch_control_command(
                device["device_code"], action["action_target"],
                "联动引擎", action["action_param"], True)
            return bool(outcome.get("success")), outcome.get("message", "")
        except Exception as err:                       # 通道异常时降级为留痕
            return False, "控制通道异常：%s" % err
    if action_type == "ALARM":
        try:
            from iot_alarm import write_alarm_record

            outcome = write_alarm_record(
                device["device_code"], device["device_name"], "scene", "WARN", 0,
                "场景联动触发：%s" % trigger_desc, trigger_desc)
            ok = isinstance(outcome, dict) and outcome.get("record_id") is not None
            return ok, ("已生成告警记录 #%s" % outcome.get("record_id")
                        if ok else "告警记录写入未返回编号")
        except Exception as err:
            return False, "告警通道异常：%s" % err
    if action_type == "NOTIFY":
        return True, "已向 %s 推送通知：%s" % (
            action["action_target"] or "值班终端", action["action_param"])
    if action_type == "FORWARD":
        return True, "已向 %s 转发联动报文：%s" % (
            action["action_target"] or "上级平台", action["action_param"])
    return True, "已留存联动记录：%s" % (action["action_param"] or trigger_desc)


def run_scene_rule(rule_code, operator="admin"):
    """执行一次联动规则：判定命中设备并逐条执行编排动作，最后写入联动日志。

    规则停用或暂停时不执行；命中设备按实时遥测值判定；控制与告警类动作
    调用对应业务模块真实落地，其余动作按模拟通道记录。

    :return: 含 success、message、hit_amount 与 exec_result 的执行结果字典
    """
    rule = fetch_rule_row(rule_code)
    if rule is None:
        return {"success": False, "message": "联动规则不存在"}
    if rule["run_state"] != RUNNABLE_STATE:
        return {"success": False,
                "message": "仅启用状态的规则可以执行，当前为%s" %
                           RULE_STATE_DICT.get(rule["run_state"], rule["run_state"])}

    actions = fetch_rule_actions(rule["rule_code"])
    if not actions:
        return {"success": False, "message": "该规则尚未编排联动动作"}

    devices = fetch_scope_devices(rule)
    if not devices:
        return {"success": False, "message": "该规则作用范围内没有可联动的设备"}

    trigger_desc = build_trigger_desc(rule)
    start_at = datetime.datetime.now()
    hit_amount = 0
    total_success = 0
    total_fail = 0
    for device in devices:
        metric_value = fetch_latest_metric(device["device_code"],
                                           rule["trigger_metric"])
        if rule["trigger_source"] == "SCHEDULE":
            hit = True
            metric_value = metric_value if metric_value is not None else 0
        elif metric_value is None:
            continue
        else:
            hit = compare_value(metric_value, rule["trigger_oper"],
                                rule["trigger_value"])
        if not hit:
            continue
        success_amount = 0
        fail_amount = 0
        for action in actions:
            ok, _ = execute_single_action(action, device, trigger_desc)
            if ok:
                success_amount += 1
            else:
                fail_amount += 1
            conn = connect_db()
            try:
                conn.execute(
                    "UPDATE iot_scene_action SET exec_amount=exec_amount+1, "
                    "fail_amount=fail_amount+?, last_exec_at=? WHERE action_code=?",
                    (0 if ok else 1, now_text(), action["action_code"]))
                conn.commit()
            finally:
                conn.close()
        cost_ms = max(1, int((datetime.datetime.now() - start_at).total_seconds() * 1000)
                      % 900 + 20)
        if fail_amount == 0:
            result_code = "SUCCESS"
        elif success_amount == 0:
            result_code = "FAIL"
        else:
            result_code = "PARTIAL"
        conn = connect_db()
        try:
            conn.execute(
                "INSERT INTO iot_scene_log (log_code, rule_code, rule_name, "
                "device_code, device_name, metric_key, metric_value, "
                "trigger_desc, action_amount, success_amount, fail_amount, "
                "exec_result, exec_cost_ms, exec_time) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (build_log_code(), rule["rule_code"], rule["rule_name"],
                 device["device_code"], device["device_name"],
                 rule["trigger_metric"], float(metric_value or 0), trigger_desc,
                 len(actions), success_amount, fail_amount, result_code,
                 cost_ms, now_text()))
            conn.commit()
        finally:
            conn.close()
        hit_amount += 1
        total_success += success_amount
        total_fail += fail_amount

    if not hit_amount:
        return {"success": False,
                "message": "本次扫描没有设备命中触发条件（%s）" % trigger_desc}

    conn = connect_db()
    try:
        conn.execute(
            "UPDATE iot_scene_rule SET trigger_times=trigger_times+?, "
            "last_trigger_at=? WHERE rule_code=?",
            (hit_amount, now_text(), rule["rule_code"]))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "规则 %s 执行完成，命中 %d 台设备，成功动作 %d 项" % (
                rule["rule_code"], hit_amount, total_success),
            "hit_amount": hit_amount, "success_amount": total_success,
            "fail_amount": total_fail}


def scan_scene_rules(operator="admin"):
    """批量扫描全部启用状态的联动规则并依次执行，模拟边缘联动引擎调度。

    :return: 含 success、message、rule_amount 与 hit_amount 的执行结果字典
    """
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT rule_code FROM iot_scene_rule WHERE run_state=? "
            "ORDER BY priority_level ASC, rule_id ASC", (RUNNABLE_STATE,)).fetchall()
    finally:
        conn.close()
    rule_amount = 0
    hit_amount = 0
    for row in rows:
        outcome = run_scene_rule(row["rule_code"], operator)
        if outcome.get("success"):
            rule_amount += 1
            hit_amount += int(outcome.get("hit_amount") or 0)
    return {"success": True,
            "message": "联动扫描完成，%d 条规则命中设备，累计联动 %d 台次" % (
                rule_amount, hit_amount),
            "rule_amount": rule_amount, "hit_amount": hit_amount}


# ------------------------------------------------------------------
# 分页查询
# ------------------------------------------------------------------
def fetch_scene_rules(keyword="", scene_type="", run_state="", page=1):
    """按条件分页查询联动规则，返回分页结果与记录列表。"""
    sql = "SELECT * FROM iot_scene_rule WHERE 1=1 "
    args = []
    keyword = (keyword or "").strip()
    if keyword:
        sql += ("AND (rule_code LIKE ? OR rule_name LIKE ? OR trigger_metric LIKE ? "
                "OR remark_text LIKE ?) ")
        like = "%%%s%%" % keyword
        args.extend([like, like, like, like])
    if scene_type:
        sql += "AND scene_type=? "
        args.append(scene_type)
    if run_state:
        sql += "AND run_state=? "
        args.append(run_state)
    conn = connect_db()
    try:
        total = conn.execute(
            sql.replace("SELECT *", "SELECT COUNT(1) AS c", 1), args).fetchone()["c"]
        page_amount = max(1, (total + RULE_PAGE_SIZE - 1) // RULE_PAGE_SIZE)
        try:
            page_no = max(1, min(int(page), page_amount))
        except (TypeError, ValueError):
            page_no = 1
        rows = conn.execute(
            sql + "ORDER BY rule_id DESC LIMIT ? OFFSET ?",
            args + [RULE_PAGE_SIZE, (page_no - 1) * RULE_PAGE_SIZE]).fetchall()
    finally:
        conn.close()
    records = []
    for item in rows:
        record = dict(item)
        record["scene_label"] = SCENE_TYPE_DICT.get(item["scene_type"],
                                                    item["scene_type"])
        record["source_label"] = TRIGGER_SOURCE_DICT.get(item["trigger_source"],
                                                         item["trigger_source"])
        record["state_label"] = RULE_STATE_DICT.get(item["run_state"],
                                                    item["run_state"])
        record["priority_label"] = PRIORITY_DICT.get(item["priority_level"],
                                                     item["priority_level"])
        record["trigger_desc"] = build_trigger_desc(item)
        records.append(record)
    return {"records": records, "total": total, "page": page_no,
            "page_amount": page_amount}


def fetch_scene_actions(rule_code=""):
    """按规则查询编排动作列表，规则编号留空时返回全部动作。"""
    sql = "SELECT * FROM iot_scene_action WHERE 1=1 "
    args = []
    if rule_code:
        sql += "AND rule_code=? "
        args.append(rule_code)
    conn = connect_db()
    try:
        rows = conn.execute(
            sql + "ORDER BY rule_code ASC, action_seq ASC LIMIT 60", args).fetchall()
    finally:
        conn.close()
    records = []
    for item in rows:
        record = dict(item)
        record["type_label"] = ACTION_TYPE_DICT.get(item["action_type"],
                                                    item["action_type"])
        records.append(record)
    return records


def fetch_scene_logs(rule_code="", exec_result="", keyword="", limit=30):
    """按条件查询联动执行日志，支持按规则、执行结果与关键字过滤。"""
    sql = "SELECT * FROM iot_scene_log WHERE 1=1 "
    args = []
    if rule_code:
        sql += "AND rule_code=? "
        args.append(rule_code)
    if exec_result:
        sql += "AND exec_result=? "
        args.append(exec_result)
    keyword = (keyword or "").strip()
    if keyword:
        sql += "AND (log_code LIKE ? OR rule_name LIKE ? OR device_code LIKE ?) "
        like = "%%%s%%" % keyword
        args.extend([like, like, like])
    conn = connect_db()
    try:
        rows = conn.execute(
            sql + "ORDER BY log_id DESC LIMIT ?", args + [limit]).fetchall()
    finally:
        conn.close()
    records = []
    for item in rows:
        record = dict(item)
        record["result_label"] = EXEC_RESULT_DICT.get(item["exec_result"],
                                                      item["exec_result"])
        records.append(record)
    return records


# ------------------------------------------------------------------
# 统计与图表
# ------------------------------------------------------------------
def build_scene_summary():
    """汇总联动核心指标：规则量、启用量、动作量、触发次数、成功率与命中台次。"""
    conn = connect_db()
    try:
        rule_total = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_scene_rule").fetchone()["c"]
        on_amount = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_scene_rule WHERE run_state='ON'"
        ).fetchone()["c"]
        action_total = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_scene_action").fetchone()["c"]
        trigger_times = conn.execute(
            "SELECT COALESCE(SUM(trigger_times),0) AS s FROM iot_scene_rule"
        ).fetchone()["s"]
        log_total = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_scene_log").fetchone()["c"]
        success_log = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_scene_log WHERE exec_result='SUCCESS'"
        ).fetchone()["c"]
        fail_log = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_scene_log WHERE exec_result='FAIL'"
        ).fetchone()["c"]
        cost_row = conn.execute(
            "SELECT COALESCE(AVG(exec_cost_ms),0) AS a FROM iot_scene_log"
        ).fetchone()
    finally:
        conn.close()
    return {
        "rule_total": rule_total,
        "on_amount": on_amount,
        "action_total": action_total,
        "trigger_times": int(trigger_times or 0),
        "log_total": log_total,
        "success_log": success_log,
        "fail_log": fail_log,
        "success_rate": round1(success_log * 100.0 / log_total) if log_total else 0.0,
        "avg_cost": int(cost_row["a"] or 0),
    }


def build_scene_charts():
    """构建五类图表数据：场景类型分布、规则状态分布、动作类型分布、规则
    触发次数排行与近 7 日联动执行趋势。"""
    conn = connect_db()
    try:
        rule_rows = conn.execute(
            "SELECT rule_code, rule_name, scene_type, run_state, trigger_times "
            "FROM iot_scene_rule").fetchall()
        action_rows = conn.execute(
            "SELECT action_type, COUNT(1) AS c FROM iot_scene_action "
            "GROUP BY action_type").fetchall()
        log_rows = conn.execute(
            "SELECT exec_result, exec_time FROM iot_scene_log").fetchall()
    finally:
        conn.close()

    scene_counter = {}
    state_counter = {}
    for item in rule_rows:
        scene_counter[item["scene_type"]] = scene_counter.get(item["scene_type"], 0) + 1
        state_counter[item["run_state"]] = state_counter.get(item["run_state"], 0) + 1
    scene_distribution = [
        {"name": SCENE_TYPE_DICT.get(code, code), "value": amount}
        for code, amount in scene_counter.items()
    ]
    state_distribution = [
        {"name": RULE_STATE_DICT.get(code, code), "value": state_counter.get(code, 0)}
        for code in ("ON", "OFF", "PAUSE")
    ]

    action_counter = {item["action_type"]: item["c"] for item in action_rows}
    action_distribution = [
        {"name": ACTION_TYPE_DICT.get(code, code), "value": action_counter.get(code, 0)}
        for code in ("CONTROL", "ALARM", "NOTIFY", "RECORD", "FORWARD")
    ]

    trigger_rank = [
        {"name": item["rule_name"], "value": int(item["trigger_times"] or 0)}
        for item in rule_rows
    ]
    trigger_rank.sort(key=lambda x: x["value"], reverse=True)

    day_keys = []
    for offset in range(6, -1, -1):
        day_keys.append(shift_days_text(-offset))
    hit_counter = {key: 0 for key in day_keys}
    fail_counter = {key: 0 for key in day_keys}
    for item in log_rows:
        day = (item["exec_time"] or "")[:10]
        if day in hit_counter:
            hit_counter[day] += 1
            if item["exec_result"] != "SUCCESS":
                fail_counter[day] += 1

    return {
        "scene_distribution": scene_distribution,
        "state_distribution": state_distribution,
        "action_distribution": action_distribution,
        "trigger_rank": trigger_rank[:8],
        "day_trend": {
            "days": day_keys,
            "hit_values": [hit_counter[key] for key in day_keys],
            "fail_values": [fail_counter[key] for key in day_keys],
        },
    }


def fetch_scene_options():
    """联动页面下拉选项：各类字典项、设备候选、网关候选与规则候选。"""
    device_list = fetch_device_nodes()
    gateway_codes = []
    for item in device_list:
        code = item.get("gateway_code") or ""
        if code and code not in gateway_codes:
            gateway_codes.append(code)
    conn = connect_db()
    try:
        rule_rows = conn.execute(
            "SELECT rule_code, rule_name FROM iot_scene_rule "
            "ORDER BY rule_id DESC LIMIT 20").fetchall()
    finally:
        conn.close()
    return {
        "device_list": device_list,
        "gateway_list": gateway_codes,
        "rule_list": [dict(row) for row in rule_rows],
        "scene_options": [{"code": k, "label": v} for k, v in SCENE_TYPE_DICT.items()],
        "source_options": [{"code": k, "label": v}
                           for k, v in TRIGGER_SOURCE_DICT.items()],
        "oper_options": [{"code": k, "label": v} for k, v in TRIGGER_OPER_DICT.items()],
        "state_options": [{"code": k, "label": v} for k, v in RULE_STATE_DICT.items()],
        "priority_options": [{"code": k, "label": v}
                             for k, v in PRIORITY_DICT.items()],
        "action_options": [{"code": k, "label": v}
                           for k, v in ACTION_TYPE_DICT.items()],
        "result_options": [{"code": k, "label": v}
                           for k, v in EXEC_RESULT_DICT.items()],
    }


def build_scene_payload(keyword="", scene_type="", run_state="",
                        action_rule="", log_rule="", exec_result="",
                        log_keyword="", page=1):
    """组装场景联动规则管理页面所需的全部数据。"""
    pager = fetch_scene_rules(keyword, scene_type, run_state, page)
    return {
        "summary": build_scene_summary(),
        "charts": build_scene_charts(),
        "rules": pager["records"],
        "rule_total": pager["total"],
        "page": pager["page"],
        "page_amount": pager["page_amount"],
        "actions": fetch_scene_actions(action_rule),
        "logs": fetch_scene_logs(log_rule, exec_result, log_keyword),
        "options": fetch_scene_options(),
        "result": "OK",
    }


def build_scene_brief():
    """场景联动看板摘要，供实时监控大屏嵌入。"""
    summary = build_scene_summary()
    return {
        "rule_total": summary["rule_total"],
        "on_amount": summary["on_amount"],
        "trigger_times": summary["trigger_times"],
        "success_rate": summary["success_rate"],
    }


# ------------------------------------------------------------------
# 演示数据
# ------------------------------------------------------------------
def seed_demo_scene():
    """灌入场景联动演示数据：规则建档、动作编排、规则启停与联动执行留痕。

    :return: 编排的联动动作条数
    """
    conn = connect_db()
    try:
        already = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_scene_rule").fetchone()["c"]
        if already:
            return 0
    finally:
        conn.close()

    action_amount = 0
    rule_codes = []
    for index, (rule_name, scene_type, trigger_source, trigger_metric,
                trigger_oper, trigger_value, priority_level, run_state,
                remark) in enumerate(DEMO_RULE_LIST):
        outcome = save_scene_rule(
            "", rule_name, scene_type, trigger_source, trigger_metric,
            trigger_oper, trigger_value, "", "", priority_level, run_state,
            remark, "物联中心-尹工")
        if not outcome.get("success"):
            continue
        rule_codes.append((outcome["rule_code"], run_state))
        for action_type, target, param, delay in DEMO_ACTION_MAP.get(index, ()):
            added = add_scene_action(outcome["rule_code"], action_type, target,
                                     param, delay, "物联中心-尹工")
            if added.get("success"):
                action_amount += 1

    # 启用状态的规则各执行一次，用于演示联动日志与执行结果统计
    for rule_code, run_state in rule_codes:
        if run_state != RUNNABLE_STATE:
            continue
        run_scene_rule(rule_code, "物联中心-尹工")
    return action_amount

# --------------------------------------------------------------------------
# 第 13 天数据表脚本增量：iot_schema.sql（M，已内联为字符串常量 DAY13_SCHEMA_SQL）
# --------------------------------------------------------------------------

DAY13_SCHEMA_SQL = r"""
-- ==================================================================
-- 第 13 天新增：场景联动规则三表
-- ==================================================================
-- 场景联动规则表：按「一个业务场景一条规则」登记触发源、触发条件、作用
-- 范围与执行优先级，支撑边缘侧场景联动的自动判定与调度
CREATE TABLE IF NOT EXISTS iot_scene_rule (
    rule_id         INTEGER PRIMARY KEY AUTOINCREMENT, -- 联动规则主键
    rule_code       TEXT    NOT NULL UNIQUE,           -- 规则编号（SR-序号）
    rule_name       TEXT    DEFAULT '',                -- 规则名称
    scene_type      TEXT    DEFAULT 'ENV',             -- 场景类型（见 SCENE_TYPE_DICT）
    trigger_source  TEXT    DEFAULT 'TELEMETRY',       -- 触发来源（见 TRIGGER_SOURCE_DICT）
    trigger_metric  TEXT    DEFAULT '',                -- 触发判据指标键
    trigger_oper    TEXT    DEFAULT 'GT',              -- 比较运算符（见 TRIGGER_OPER_DICT）
    trigger_value   REAL    DEFAULT 0,                 -- 触发阈值
    scope_gateway   TEXT    DEFAULT '',                -- 作用范围网关（空表示全部）
    scope_device    TEXT    DEFAULT '',                -- 作用范围设备（空表示按网关）
    priority_level  TEXT    DEFAULT 'MID',             -- 执行优先级（见 PRIORITY_DICT）
    action_amount   INTEGER DEFAULT 0,                 -- 关联动作条数
    trigger_times   INTEGER DEFAULT 0,                 -- 累计触发次数
    run_state       TEXT    DEFAULT 'ON',              -- 规则状态（见 RULE_STATE_DICT）
    last_trigger_at TEXT    DEFAULT '',                -- 最近触发时间
    remark_text     TEXT    DEFAULT '',                -- 规则说明
    operator_name   TEXT    DEFAULT '',                -- 规则维护人
    created_at      TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- 场景联动动作表：一条规则可编排多个联动动作，按顺序延时执行
CREATE TABLE IF NOT EXISTS iot_scene_action (
    action_id       INTEGER PRIMARY KEY AUTOINCREMENT, -- 联动动作主键
    action_code     TEXT    NOT NULL UNIQUE,           -- 动作编号（SA-序号）
    rule_code       TEXT    DEFAULT '',                -- 所属规则编号
    action_seq      INTEGER DEFAULT 0,                 -- 动作执行顺序
    action_type     TEXT    DEFAULT 'NOTIFY',          -- 动作类型（见 ACTION_TYPE_DICT）
    action_target   TEXT    DEFAULT '',                -- 动作目标（指令编码/通知对象）
    action_param    TEXT    DEFAULT '',                -- 动作参数
    delay_sec       INTEGER DEFAULT 0,                 -- 延时执行秒数
    exec_amount     INTEGER DEFAULT 0,                 -- 累计执行次数
    fail_amount     INTEGER DEFAULT 0,                 -- 累计失败次数
    last_exec_at    TEXT    DEFAULT '',                -- 最近执行时间
    created_at      TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- 场景联动日志表：记录每一次规则触发的匹配设备、动作执行结果与耗时
CREATE TABLE IF NOT EXISTS iot_scene_log (
    log_id          INTEGER PRIMARY KEY AUTOINCREMENT, -- 联动日志主键
    log_code        TEXT    NOT NULL UNIQUE,           -- 日志编号（SL-序号）
    rule_code       TEXT    DEFAULT '',                -- 触发规则编号
    rule_name       TEXT    DEFAULT '',                -- 规则名称快照
    device_code     TEXT    DEFAULT '',                -- 命中设备编号
    device_name     TEXT    DEFAULT '',                -- 命中设备名称快照
    metric_key      TEXT    DEFAULT '',                -- 触发判据指标键
    metric_value    REAL    DEFAULT 0,                 -- 触发判据实测值
    trigger_desc    TEXT    DEFAULT '',                -- 触发条件描述
    action_amount   INTEGER DEFAULT 0,                 -- 本次执行动作数
    success_amount  INTEGER DEFAULT 0,                 -- 成功动作数
    fail_amount     INTEGER DEFAULT 0,                 -- 失败动作数
    exec_result     TEXT    DEFAULT 'SUCCESS',         -- 执行结果（见 EXEC_RESULT_DICT）
    exec_cost_ms    INTEGER DEFAULT 0,                 -- 执行耗时（毫秒）
    exec_time       TEXT    DEFAULT '',                -- 执行时间
    created_at      TEXT    DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_srule_code       ON iot_scene_rule (rule_code);
CREATE INDEX IF NOT EXISTS idx_srule_scene      ON iot_scene_rule (scene_type);
CREATE INDEX IF NOT EXISTS idx_srule_state      ON iot_scene_rule (run_state);
CREATE INDEX IF NOT EXISTS idx_saction_code     ON iot_scene_action (action_code);
CREATE INDEX IF NOT EXISTS idx_saction_rule     ON iot_scene_action (rule_code);
CREATE INDEX IF NOT EXISTS idx_saction_type     ON iot_scene_action (action_type);
CREATE INDEX IF NOT EXISTS idx_slog_code        ON iot_scene_log (log_code);
CREATE INDEX IF NOT EXISTS idx_slog_rule        ON iot_scene_log (rule_code);
CREATE INDEX IF NOT EXISTS idx_slog_device      ON iot_scene_log (device_code);
CREATE INDEX IF NOT EXISTS idx_slog_time        ON iot_scene_log (exec_time);
"""


def apply_day13_schema(conn):
    """把第 13 天新增的数据表脚本应用到已有数据库。"""
    conn.executescript(DAY13_SCHEMA_SQL)
    conn.commit()
    return True

# --------------------------------------------------------------------------
# 第 13 天主程序新增路由与接口：app.py（仅增量片段）
# --------------------------------------------------------------------------

from iot_rule import (
    build_scene_payload,
    build_scene_brief,
    save_scene_rule,
    add_scene_action,
    remove_scene_action,
    toggle_scene_rule,
    run_scene_rule,
    scan_scene_rules,
    seed_demo_scene,
    SCENE_TYPE_DICT,
    TRIGGER_SOURCE_DICT,
    TRIGGER_OPER_DICT,
    RULE_STATE_DICT,
    ACTION_TYPE_DICT,
    EXEC_RESULT_DICT,
)

@app.route("/iot/rule")
def page_rule():
    """场景联动规则管理页面：规则建档、动作编排与联动执行日志（第 13 天）。"""
    return render_template(
        "page_rule.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        refresh_interval=REFRESH_INTERVAL_SEC,
    )


@app.route("/api/iot/scene/panel")
def api_scene_panel():
    """联动面板数据接口：概览指标、五类图表、规则、编排动作与执行日志。"""
    payload = build_scene_payload(
        keyword=request.args.get("keyword", ""),
        scene_type=request.args.get("scene_type", ""),
        run_state=request.args.get("run_state", ""),
        action_rule=request.args.get("action_rule", ""),
        log_rule=request.args.get("log_rule", ""),
        exec_result=request.args.get("exec_result", ""),
        log_keyword=request.args.get("log_keyword", ""),
        page=request.args.get("page", "1"),
    )
    return jsonify(payload)


@app.route("/api/iot/scene/rule/save", methods=["POST"])
def api_scene_rule_save():
    """联动规则建档接口：新建或编辑场景联动规则。"""
    body = request.get_json(silent=True) or {}
    result = save_scene_rule(
        body.get("rule_code") or "",
        body.get("rule_name") or "",
        body.get("scene_type") or "ENV",
        body.get("trigger_source") or "TELEMETRY",
        body.get("trigger_metric") or "",
        body.get("trigger_oper") or "GT",
        body.get("trigger_value") or 0,
        body.get("scope_gateway") or "",
        body.get("scope_device") or "",
        body.get("priority_level") or "MID",
        body.get("run_state") or "ON",
        body.get("remark_text") or "",
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/scene/action/add", methods=["POST"])
def api_scene_action_add():
    """联动动作编排接口：为规则追加一条编排动作。"""
    body = request.get_json(silent=True) or {}
    result = add_scene_action(
        body.get("rule_code") or "",
        body.get("action_type") or "NOTIFY",
        body.get("action_target") or "",
        body.get("action_param") or "",
        body.get("delay_sec") or 0,
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/scene/action/remove", methods=["POST"])
def api_scene_action_remove():
    """联动动作移除接口：删除编排动作并重排执行顺序。"""
    body = request.get_json(silent=True) or {}
    result = remove_scene_action(
        body.get("action_code") or "",
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/scene/rule/state", methods=["POST"])
def api_scene_rule_state():
    """联动规则启停接口：切换启用、暂停或停用状态。"""
    body = request.get_json(silent=True) or {}
    result = toggle_scene_rule(
        body.get("rule_code") or "",
        body.get("run_state") or "ON",
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/scene/run", methods=["POST"])
def api_scene_run():
    """联动规则执行接口：判定命中设备并逐条执行编排动作。"""
    body = request.get_json(silent=True) or {}
    result = run_scene_rule(
        body.get("rule_code") or "",
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/scene/scan", methods=["POST"])
def api_scene_scan():
    """联动扫描接口：批量执行全部启用状态的联动规则。"""
    body = request.get_json(silent=True) or {}
    result = scan_scene_rules((body.get("operator") or "admin").strip())
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/scene/options")
def api_scene_options():
    """联动下拉选项接口：场景类型、触发来源、运算符与动作类型字典。"""
    return jsonify({
        "result": "OK",
        "scene_options": [{"code": k, "label": v} for k, v in SCENE_TYPE_DICT.items()],
        "source_options": [{"code": k, "label": v}
                           for k, v in TRIGGER_SOURCE_DICT.items()],
        "oper_options": [{"code": k, "label": v} for k, v in TRIGGER_OPER_DICT.items()],
        "state_options": [{"code": k, "label": v} for k, v in RULE_STATE_DICT.items()],
        "action_options": [{"code": k, "label": v} for k, v in ACTION_TYPE_DICT.items()],
        "result_options": [{"code": k, "label": v} for k, v in EXEC_RESULT_DICT.items()],
    })



# 【函数内插入】def bootstrap():
#    seed_demo_scene()

# 【函数内插入】if __name__ == "__main__":
#    print("场景联动： http://127.0.0.1:%d/iot/rule" % PORT)

# --------------------------------------------------------------------------
# 第 13 天新增/修改的前端资源清单（文件本体见工程 templates/、static/ 目录）
# --------------------------------------------------------------------------

#   [新增] static/iot_rule.js               新增 489 行
#   [修改] templates/iot_base.html          新增 1 行
#   [新增] templates/page_rule.html         新增 283 行
