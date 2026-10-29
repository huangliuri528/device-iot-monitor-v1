# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-29（第 9 天 / 共 15 天）
# 模块功能：本文件仅含该软件第 9 天新增与修改的源代码，不含第 1~8 天已有代码
#           本次迭代主题：物联网网关管理模块 + 网关管理页面
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）
# 接入说明：以下内容为当日增量，需与前面各天的代码合并后运行——
#           新增模块按文件名放入工程目录；建表脚本增量追加进对应 .sql；
#           主程序增量片段插入 app.py 中 Flask 实例之后；前端资源按文末清单
#           放入 templates/ 与 static/ 目录


# --------------------------------------------------------------------------
# 第 9 天新增业务模块 1：iot_gateway.py（完整源码）
# --------------------------------------------------------------------------

# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-29（第 9 天 / 共 15 天）
# 模块功能：物联网网关管理模块。实现网关运行档案的维护与容量负载测算、
#           上行链路的时延丢包信号探测与链路质量评分分级、网关远程重启与
#           固件升级的指令下发留痕、以及网关运行事件的登记与处置闭环；
#           输出网关状态分布、链路质量等级分布、各网关时延对比、链路质量
#           走势与事件类型分布五类统计图表，形成「建档案 → 探链路 → 发指令
#           → 记事件 → 处置闭环」的网关运维管理闭环
# 依赖包：sqlite3（Python 内置）、datetime（Python 内置）、random（Python 内置）

import datetime
import random

from iot_store import connect_db, dict_list, now_text, fetch_gateway_rows
from iot_store import fetch_device_nodes

# ------------------------------------------------------------------
# 链路类型字典：区分有线、无线与蜂窝三种上行方式
# ------------------------------------------------------------------
LINK_TYPE_DICT = {
    "ETH": "有线以太网",
    "WIFI": "无线 WiFi",
    "4G": "蜂窝 4G",
    "5G": "蜂窝 5G",
}

# 网关运行状态字典
GATEWAY_STATE_DICT = {
    "ONLINE": "在线运行",
    "OFFLINE": "离线失联",
    "FAULT": "故障待修",
    "MAINTAIN": "维护停机",
}

# 链路质量等级字典：由时延、丢包与信号强度综合评分判定
QUALITY_LEVEL_DICT = {
    "EXCELLENT": "优秀",
    "GOOD": "良好",
    "FAIR": "一般",
    "POOR": "劣化",
}

# 链路质量等级阈值（评分下限由高到低匹配）
QUALITY_LEVEL_THRESHOLD = (
    ("EXCELLENT", 85),
    ("GOOD", 70),
    ("FAIR", 50),
    ("POOR", 0),
)

# 网关事件类型字典
GATEWAY_EVENT_DICT = {
    "ONLINE": "网关上线",
    "OFFLINE": "网关离线",
    "REBOOT": "远程重启",
    "UPGRADE": "固件升级",
    "LINK_DROP": "链路劣化",
    "ERROR": "运行异常",
}

# 事件级别字典
EVENT_LEVEL_DICT = {
    "INFO": "一般提示",
    "WARN": "告警",
    "FAULT": "故障",
}

# 链路质量评分权重：时延、丢包与信号强度各自的分值影响
QUALITY_WEIGHT_RULES = {
    "LATENCY": 0.25,     # 每超出基准时延 1 毫秒的扣分
    "LOSS": 6.0,         # 每 1% 丢包率的扣分
    "SIGNAL": 0.8,       # 信号强度每低于基准 1 dBm 的扣分
}

# 链路质量评分基准值：时延基准毫秒、信号强度基准 dBm
QUALITY_BASELINE = {
    "LATENCY": 20,
    "SIGNAL": -55,
}

# 链路劣化判定阈值：连续探测评分低于该分值即触发链路劣化事件
POOR_QUALITY_SCORE = 50

# 各链路类型的默认最大接入槽位
LINK_SLOT_DICT = {
    "ETH": 64,
    "WIFI": 24,
    "4G": 16,
    "5G": 32,
}

# 网关档案默认型号与固件版本
DEFAULT_GATEWAY_MODEL = "MIOT-GW-2000"
DEFAULT_FIRMWARE_VER = "V1.0.0"

# 网关档案分页尺寸与链路/事件展示条数
GATEWAY_PAGE_SIZE = 8
LINK_SHOW_LIMIT = 12
EVENT_SHOW_LIMIT = 12

# 演示数据随机种子，保证每次启动生成的链路指标一致
DEMO_RANDOM_SEED = 20261029


# ------------------------------------------------------------------
# 编码与评分工具
# ------------------------------------------------------------------
def code_hash(text):
    """把网关编号折算为稳定整数，用于派生演示链路指标的差异。"""
    return sum(ord(char) for char in (text or ""))


def round1(value):
    """保留一位小数，空值与非数值按 0 处理。"""
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):
        return 0.0


def build_link_code(seq_no):
    """生成链路探测编号：GL-年月日-序号。"""
    return "GL-%s-%03d" % (datetime.datetime.now().strftime("%Y%m%d"), seq_no)


def build_event_code(seq_no):
    """生成网关事件编号：GE-年月日-序号。"""
    return "GE-%s-%03d" % (datetime.datetime.now().strftime("%Y%m%d"), seq_no)


def next_seq_no(table_name):
    """读取指定业务表记录数并加一，用于拼装业务编码。"""
    conn = connect_db()
    try:
        amount = conn.execute(
            "SELECT COUNT(1) AS c FROM %s" % table_name).fetchone()["c"]
    finally:
        conn.close()
    return amount + 1


def judge_quality_level(quality_score):
    """按链路质量评分判定质量等级编码。"""
    for level_code, floor in QUALITY_LEVEL_THRESHOLD:
        if quality_score >= floor:
            return level_code
    return "POOR"


def calc_quality_score(latency_ms, packet_loss, signal_dbm):
    """按时延、丢包率与信号强度计算链路质量评分（百分制）。"""
    deduct = 0.0
    deduct += max(0, latency_ms - QUALITY_BASELINE["LATENCY"]) * QUALITY_WEIGHT_RULES["LATENCY"]
    deduct += max(0, packet_loss) * QUALITY_WEIGHT_RULES["LOSS"]
    deduct += max(0, QUALITY_BASELINE["SIGNAL"] - signal_dbm) * QUALITY_WEIGHT_RULES["SIGNAL"]
    return round(max(0.0, 100.0 - deduct), 1)


def fetch_profile_row(gateway_code):
    """按网关编号读取运行档案，未找到返回 None。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_gateway_profile WHERE gateway_code=?",
            (gateway_code,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row is not None else None


def count_gateway_devices(gateway_code):
    """统计某网关下已绑定设备数与在线设备数。

    :return: (绑定设备数, 在线设备数) 元组
    """
    nodes = fetch_device_nodes()
    bound = [item for item in nodes if item.get("gateway_code") == gateway_code]
    online = [item for item in bound if item.get("run_state") == "ONLINE"]
    return len(bound), len(online)


# ------------------------------------------------------------------
# 网关档案维护
# ------------------------------------------------------------------
def ensure_gateway_profiles():
    """为接入台账中的每台网关补齐运行档案，缺失时按默认参数建档。

    :return: 本次新建的档案条数
    """
    rows = fetch_gateway_rows()
    created = 0
    conn = connect_db()
    try:
        for index, item in enumerate(rows):
            exist = conn.execute(
                "SELECT profile_id FROM iot_gateway_profile WHERE gateway_code=?",
                (item["gateway_code"],)).fetchone()
            if exist is not None:
                continue
            link_type = ("ETH", "WIFI", "5G", "4G")[index % 4]
            conn.execute(
                "INSERT INTO iot_gateway_profile "
                "(gateway_code, gateway_name, gateway_model, ward_name, "
                "firmware_ver, link_type, run_state, max_slots, load_rate, "
                "online_rate, quality_score, quality_level, install_date, "
                "remark_text) VALUES (?,?,?,?,?,?,?,?,0,0,100,'EXCELLENT',?,?)",
                (item["gateway_code"], item["gateway_name"], DEFAULT_GATEWAY_MODEL,
                 item.get("ward_name", ""), DEFAULT_FIRMWARE_VER, link_type,
                 "ONLINE", LINK_SLOT_DICT[link_type],
                 (datetime.date.today() - datetime.timedelta(days=180 + index * 30)
                  ).strftime("%Y-%m-%d"),
                 "网关运行档案，由接入台账自动建档"))
            created += 1
        if created:
            conn.commit()
    finally:
        conn.close()
    return created


def save_gateway_profile(gateway_code, gateway_model="", link_type="", max_slots=0,
                         firmware_ver="", run_state="", remark_text="",
                         operator="admin"):
    """维护一台网关的运行档案参数。

    :return: 含 success 与 message 的执行结果字典
    """
    gateway_code = (gateway_code or "").strip()
    if not gateway_code:
        return {"success": False, "message": "网关编号不能为空"}
    profile = fetch_profile_row(gateway_code)
    if profile is None:
        return {"success": False, "message": "网关 %s 尚未建档" % gateway_code}
    if link_type and link_type not in LINK_TYPE_DICT:
        return {"success": False, "message": "链路类型 %s 不合法" % link_type}
    if run_state and run_state not in GATEWAY_STATE_DICT:
        return {"success": False, "message": "运行状态 %s 不合法" % run_state}
    try:
        slots = int(max_slots) if max_slots else 0
    except (TypeError, ValueError):
        return {"success": False, "message": "最大接入槽位必须为整数"}
    if slots < 0:
        return {"success": False, "message": "最大接入槽位不能为负数"}

    bound_amount, _ = count_gateway_devices(gateway_code)
    if slots and slots < bound_amount:
        return {"success": False,
                "message": "最大槽位 %d 小于已接入设备数 %d" % (slots, bound_amount)}
    new_link = link_type or profile["link_type"]
    new_slots = slots or (LINK_SLOT_DICT.get(new_link, profile["max_slots"])
                          if not slots else slots)
    load_rate = round1(bound_amount * 100.0 / new_slots) if new_slots else 0.0

    conn = connect_db()
    try:
        conn.execute(
            "UPDATE iot_gateway_profile SET gateway_model=?, link_type=?, "
            "max_slots=?, firmware_ver=?, run_state=?, remark_text=?, "
            "load_rate=?, updated_at=? WHERE gateway_code=?",
            ((gateway_model or "").strip() or profile["gateway_model"],
             new_link, new_slots,
             (firmware_ver or "").strip() or profile["firmware_ver"],
             (run_state or "").strip() or profile["run_state"],
             (remark_text or "").strip() or profile["remark_text"],
             load_rate, now_text(), gateway_code))
        conn.commit()
    finally:
        conn.close()
    if run_state:
        append_gateway_event(gateway_code,
                             "OFFLINE" if run_state == "OFFLINE" else "ONLINE",
                             "手动切换网关运行状态为「%s」"
                             % GATEWAY_STATE_DICT[run_state],
                             operator=operator)
    return {"success": True,
            "message": "网关 %s 档案更新成功，当前负载率 %.1f%%" % (gateway_code, load_rate),
            "load_rate": load_rate}


# ------------------------------------------------------------------
# 链路探测与事件留痕
# ------------------------------------------------------------------
def append_gateway_event(gateway_code, event_type, event_text,
                         event_level="INFO", operator="admin"):
    """登记一条网关运行事件。

    :return: 生成的事件编号
    """
    seq_no = next_seq_no("iot_gateway_event")
    event_code = build_event_code(seq_no)
    conn = connect_db()
    try:
        conn.execute(
            "INSERT INTO iot_gateway_event "
            "(event_code, gateway_code, event_type, event_level, event_text, "
            "handle_flag, handler_name, handle_note, event_time, handle_time) "
            "VALUES (?,?,?,?,?,0,'','',?,'')",
            (event_code, gateway_code, event_type, event_level, event_text, now_text()))
        conn.commit()
    finally:
        conn.close()
    return event_code


def probe_single_gateway(gateway_code, generator=None, stamp=""):
    """对单台网关执行一次链路探测并落库。

    探测指标由网关编号派生的稳定基线叠加瞬时抖动生成，保证同一台网关的
    历史曲线既有波动趋势又可复现；链路评分跌破劣化阈值时登记链路劣化事件。

    :return: 探测结果字典，网关未建档时返回 None
    """
    profile = fetch_profile_row(gateway_code)
    if profile is None:
        return None
    generator = generator or random.Random(code_hash(gateway_code) + DEMO_RANDOM_SEED)
    base = code_hash(gateway_code) % 5
    latency_ms = int(25 + base * 22 + generator.uniform(-6, 12))
    packet_loss = round1(base * 0.55 + generator.uniform(0, 0.9))
    signal_dbm = int(-55 - base * 7 + generator.randint(-4, 4))
    uplink_kbps = int(2400 - base * 320 + generator.randint(-180, 240))
    quality_score = calc_quality_score(latency_ms, packet_loss, signal_dbm)
    quality_level = judge_quality_level(quality_score)

    bound_amount, online_amount = count_gateway_devices(gateway_code)
    load_rate = round1(bound_amount * 100.0 / profile["max_slots"]) \
        if profile["max_slots"] else 0.0
    online_rate = round1(online_amount * 100.0 / bound_amount) if bound_amount else 0.0

    seq_no = next_seq_no("iot_gateway_link")
    link_code = build_link_code(seq_no)
    probe_time = stamp or now_text()
    conn = connect_db()
    try:
        conn.execute(
            "INSERT INTO iot_gateway_link "
            "(link_code, gateway_code, latency_ms, packet_loss, signal_dbm, "
            "uplink_kbps, quality_score, quality_level, probe_time) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (link_code, gateway_code, latency_ms, packet_loss, signal_dbm,
             uplink_kbps, quality_score, quality_level, probe_time))
        conn.execute(
            "UPDATE iot_gateway_profile SET load_rate=?, online_rate=?, "
            "quality_score=?, quality_level=?, last_probe_at=?, updated_at=? "
            "WHERE gateway_code=?",
            (load_rate, online_rate, quality_score, quality_level,
             probe_time, now_text(), gateway_code))
        conn.commit()
    finally:
        conn.close()

    # 链路评分跌破劣化阈值且此前未处于劣化状态时，登记一条链路劣化告警
    if quality_score < POOR_QUALITY_SCORE and profile["quality_level"] != "POOR":
        append_gateway_event(
            gateway_code, "LINK_DROP",
            "链路质量评分 %.1f（时延 %d ms、丢包 %.1f%%、信号 %d dBm），建议检查上行链路"
            % (quality_score, latency_ms, packet_loss, signal_dbm),
            event_level="WARN")
    return {
        "link_code": link_code,
        "gateway_code": gateway_code,
        "latency_ms": latency_ms,
        "packet_loss": packet_loss,
        "signal_dbm": signal_dbm,
        "uplink_kbps": uplink_kbps,
        "quality_score": quality_score,
        "quality_level": quality_level,
        "probe_time": probe_time,
    }


def probe_all_gateways(gateway_code=""):
    """对全部或单台网关执行一次链路探测。

    :return: 含 success、message 与探测条数的执行结果字典
    """
    ensure_gateway_profiles()
    gateway_code = (gateway_code or "").strip()
    if gateway_code:
        codes = [gateway_code]
    else:
        conn = connect_db()
        try:
            rows = conn.execute(
                "SELECT gateway_code FROM iot_gateway_profile "
                "ORDER BY gateway_code").fetchall()
        finally:
            conn.close()
        codes = [row["gateway_code"] for row in rows]
    if not codes:
        return {"success": False, "message": "暂无可探测的网关档案"}
    results = []
    for code in codes:
        outcome = probe_single_gateway(code)
        if outcome is not None:
            results.append(outcome)
    return {
        "success": True,
        "message": "链路探测完成，覆盖 %d 台网关，新增 %d 条探测记录"
                   % (len(codes), len(results)),
        "probe_amount": len(results),
        "results": results,
    }


def reboot_gateway(gateway_code, operator="admin", reason_text=""):
    """向网关下发一次远程重启指令并登记重启事件。

    :return: 含 success 与 message 的执行结果字典
    """
    gateway_code = (gateway_code or "").strip()
    profile = fetch_profile_row(gateway_code)
    if profile is None:
        return {"success": False, "message": "网关 %s 尚未建档" % gateway_code}
    if profile["run_state"] == "MAINTAIN":
        return {"success": False, "message": "网关处于维护停机状态，请先恢复运行"}
    conn = connect_db()
    try:
        conn.execute(
            "UPDATE iot_gateway_profile SET run_state='ONLINE', updated_at=? "
            "WHERE gateway_code=?", (now_text(), gateway_code))
        conn.commit()
    finally:
        conn.close()
    event_code = append_gateway_event(
        gateway_code, "REBOOT",
        "下发远程重启指令：%s" % ((reason_text or "").strip() or "例行巡检重启"),
        operator=operator)
    return {"success": True,
            "message": "网关 %s 重启指令已下发，事件编号 %s" % (gateway_code, event_code),
            "event_code": event_code}


def upgrade_gateway(gateway_code, target_ver, operator="admin", note_text=""):
    """登记一次网关固件升级并更新档案中的固件版本。

    :return: 含 success 与 message 的执行结果字典
    """
    gateway_code = (gateway_code or "").strip()
    target_ver = (target_ver or "").strip()
    if not target_ver:
        return {"success": False, "message": "目标固件版本不能为空"}
    profile = fetch_profile_row(gateway_code)
    if profile is None:
        return {"success": False, "message": "网关 %s 尚未建档" % gateway_code}
    if target_ver == profile["firmware_ver"]:
        return {"success": False, "message": "目标版本与当前版本一致，无需升级"}
    conn = connect_db()
    try:
        conn.execute(
            "UPDATE iot_gateway_profile SET firmware_ver=?, updated_at=? "
            "WHERE gateway_code=?", (target_ver, now_text(), gateway_code))
        conn.commit()
    finally:
        conn.close()
    event_code = append_gateway_event(
        gateway_code, "UPGRADE",
        "固件由 %s 升级至 %s：%s"
        % (profile["firmware_ver"], target_ver,
           (note_text or "").strip() or "修复链路重连稳定性问题"),
        operator=operator)
    return {"success": True,
            "message": "网关 %s 固件升级登记成功，当前版本 %s" % (gateway_code, target_ver),
            "event_code": event_code}


def handle_gateway_event(event_code, handler_name="admin", handle_note=""):
    """处置一条网关运行事件并写入处置人与处置时间。

    :return: 含 success 与 message 的执行结果字典
    """
    event_code = (event_code or "").strip()
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_gateway_event WHERE event_code=?",
            (event_code,)).fetchone()
        if row is None:
            return {"success": False, "message": "事件 %s 不存在" % event_code}
        event = dict(row)
        if event["handle_flag"]:
            return {"success": False, "message": "该事件已处置，不可重复登记"}
        conn.execute(
            "UPDATE iot_gateway_event SET handle_flag=1, handler_name=?, "
            "handle_note=?, handle_time=? WHERE event_code=?",
            ((handler_name or "admin").strip(), (handle_note or "").strip(),
             now_text(), event_code))
        # 链路劣化事件处置完成后，恢复网关运行状态为在线
        if event["event_type"] == "LINK_DROP":
            conn.execute(
                "UPDATE iot_gateway_profile SET run_state='ONLINE', updated_at=? "
                "WHERE gateway_code=? AND run_state<>'MAINTAIN'",
                (now_text(), event["gateway_code"]))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "事件 %s 处置完成" % event_code,
            "event_code": event_code}


# ------------------------------------------------------------------
# 查询与统计
# ------------------------------------------------------------------
def fetch_gateway_profiles(keyword="", ward_name="", link_type="", run_state="",
                           quality_level="", page=1):
    """按组合条件分页查询网关运行档案。"""
    where = []
    params = []
    if keyword:
        where.append("(gateway_code LIKE ? OR gateway_name LIKE ? OR "
                     "gateway_model LIKE ?)")
        like = "%%%s%%" % keyword
        params.extend([like, like, like])
    if ward_name:
        where.append("ward_name=?")
        params.append(ward_name)
    if link_type:
        where.append("link_type=?")
        params.append(link_type)
    if run_state:
        where.append("run_state=?")
        params.append(run_state)
    if quality_level:
        where.append("quality_level=?")
        params.append(quality_level)
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    try:
        page_no = int(page)
    except (TypeError, ValueError):
        page_no = 1
    if page_no < 1:
        page_no = 1
    offset = (page_no - 1) * GATEWAY_PAGE_SIZE

    conn = connect_db()
    try:
        total = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_gateway_profile" + clause,
            params).fetchone()["c"]
        rows = conn.execute(
            "SELECT * FROM iot_gateway_profile" + clause +
            " ORDER BY quality_score ASC, gateway_code ASC LIMIT ? OFFSET ?",
            params + [GATEWAY_PAGE_SIZE, offset]).fetchall()
    finally:
        conn.close()
    records = []
    for item in rows:
        row = dict(item)
        row["link_label"] = LINK_TYPE_DICT.get(row["link_type"], row["link_type"])
        row["state_label"] = GATEWAY_STATE_DICT.get(row["run_state"], row["run_state"])
        row["quality_label"] = QUALITY_LEVEL_DICT.get(row["quality_level"],
                                                      row["quality_level"])
        bound_amount, online_amount = count_gateway_devices(row["gateway_code"])
        row["bind_amount"] = bound_amount
        row["online_amount"] = online_amount
        row["free_slots"] = max(0, row["max_slots"] - bound_amount)
        records.append(row)
    return {
        "records": records,
        "total": total,
        "page": page_no,
        "page_size": GATEWAY_PAGE_SIZE,
        "page_amount": max(1, (total + GATEWAY_PAGE_SIZE - 1) // GATEWAY_PAGE_SIZE),
    }


def fetch_gateway_links(gateway_code="", limit=LINK_SHOW_LIMIT):
    """读取网关链路探测时序，默认返回最近 12 条。"""
    sql = "SELECT * FROM iot_gateway_link"
    params = []
    if gateway_code:
        sql += " WHERE gateway_code=?"
        params.append(gateway_code)
    sql += " ORDER BY link_id DESC LIMIT ?"
    params.append(limit)
    conn = connect_db()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    result = dict_list(rows)
    for row in result:
        row["quality_label"] = QUALITY_LEVEL_DICT.get(row["quality_level"],
                                                      row["quality_level"])
    result.reverse()
    return result


def fetch_gateway_events(gateway_code="", event_level="", handle_flag="",
                         limit=EVENT_SHOW_LIMIT):
    """读取网关运行事件，支持按网关、级别与处置状态过滤。"""
    where = []
    params = []
    if gateway_code:
        where.append("gateway_code=?")
        params.append(gateway_code)
    if event_level:
        where.append("event_level=?")
        params.append(event_level)
    if handle_flag in ("0", "1"):
        where.append("handle_flag=?")
        params.append(int(handle_flag))
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT * FROM iot_gateway_event" + clause +
            " ORDER BY event_id DESC LIMIT ?", params + [limit]).fetchall()
    finally:
        conn.close()
    result = []
    for item in rows:
        row = dict(item)
        row["type_label"] = GATEWAY_EVENT_DICT.get(row["event_type"], row["event_type"])
        row["level_label"] = EVENT_LEVEL_DICT.get(row["event_level"], row["event_level"])
        row["handle_label"] = "已处置" if row["handle_flag"] else "未处置"
        result.append(row)
    return result


def build_gateway_summary():
    """汇总网关管理核心指标：档案量、在线率、链路质量与负载情况。"""
    conn = connect_db()
    try:
        rows = conn.execute("SELECT * FROM iot_gateway_profile").fetchall()
        link_rows = conn.execute(
            "SELECT AVG(latency_ms) AS latency, AVG(packet_loss) AS loss, "
            "AVG(quality_score) AS score FROM iot_gateway_link").fetchall()
        event_total = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_gateway_event").fetchone()["c"]
        event_open = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_gateway_event WHERE handle_flag=0"
        ).fetchone()["c"]
    finally:
        conn.close()
    profiles = dict_list(rows)
    total = len(profiles)
    online_amount = len([p for p in profiles if p["run_state"] == "ONLINE"])
    fault_amount = len([p for p in profiles if p["run_state"] == "FAULT"])
    poor_amount = len([p for p in profiles if p["quality_level"] == "POOR"])
    good_amount = len([p for p in profiles
                       if p["quality_level"] in ("EXCELLENT", "GOOD")])
    bind_total = 0
    for profile in profiles:
        bound_amount, _ = count_gateway_devices(profile["gateway_code"])
        bind_total += bound_amount
    slot_total = sum(p["max_slots"] for p in profiles)
    avg = link_rows[0] if link_rows else None
    return {
        "gateway_total": total,
        "online_amount": online_amount,
        "fault_amount": fault_amount,
        "poor_amount": poor_amount,
        "good_amount": good_amount,
        "online_rate": round1(online_amount * 100.0 / total) if total else 0.0,
        "good_rate": round1(good_amount * 100.0 / total) if total else 0.0,
        "bind_total": bind_total,
        "slot_total": slot_total,
        "load_rate": round1(bind_total * 100.0 / slot_total) if slot_total else 0.0,
        "avg_latency": round1(avg["latency"]) if avg and avg["latency"] else 0.0,
        "avg_loss": round1(avg["loss"]) if avg and avg["loss"] else 0.0,
        "avg_score": round1(avg["score"]) if avg and avg["score"] else 0.0,
        "event_total": event_total,
        "event_open": event_open,
    }


def build_gateway_charts():
    """构建五类图表数据：状态分布、质量等级分布、时延对比、质量走势与事件分布。"""
    conn = connect_db()
    try:
        profiles = conn.execute("SELECT * FROM iot_gateway_profile").fetchall()
        link_rows = conn.execute(
            "SELECT gateway_code, latency_ms, packet_loss, quality_score, "
            "probe_time FROM iot_gateway_link ORDER BY link_id ASC").fetchall()
        event_rows = conn.execute(
            "SELECT event_type, event_level FROM iot_gateway_event").fetchall()
    finally:
        conn.close()
    profiles = dict_list(profiles)

    state_counter = {}
    for code in GATEWAY_STATE_DICT:
        state_counter[code] = 0
    for item in profiles:
        state_counter[item["run_state"]] = state_counter.get(item["run_state"], 0) + 1
    state_distribution = [
        {"name": GATEWAY_STATE_DICT[code], "value": state_counter[code]}
        for code in ("ONLINE", "OFFLINE", "FAULT", "MAINTAIN")
    ]

    quality_counter = {}
    for code in QUALITY_LEVEL_DICT:
        quality_counter[code] = 0
    for item in profiles:
        quality_counter[item["quality_level"]] = quality_counter.get(
            item["quality_level"], 0) + 1
    quality_distribution = [
        {"name": QUALITY_LEVEL_DICT[code], "value": quality_counter[code]}
        for code in ("EXCELLENT", "GOOD", "FAIR", "POOR")
    ]

    # 取每台网关最近一次探测的往返时延，与质量评分一并展示在对比图中
    latest_latency = {}
    for item in link_rows:
        latest_latency.setdefault(item["gateway_code"], item["latency_ms"])
    latency_compare = [
        {"name": item["gateway_name"] or item["gateway_code"],
         "value": round1(item["quality_score"]),
         "latency": latest_latency.get(item["gateway_code"], 0)}
        for item in profiles
    ]
    latency_compare.sort(key=lambda x: x["value"])

    name_map = {}
    for item in profiles:
        name_map[item["gateway_code"]] = item["gateway_name"] or item["gateway_code"]
    trend_map = {}
    for item in link_rows:
        code = item["gateway_code"]
        bucket = trend_map.setdefault(code, {"name": name_map.get(code, code),
                                             "values": [], "times": []})
        bucket["values"].append(round1(item["quality_score"]))
        bucket["times"].append((item["probe_time"] or "")[-8:-3])
    quality_trend = {
        "times": (list(trend_map.values())[0]["times"] if trend_map else []),
        "series": [{"name": bucket["name"], "values": bucket["values"]}
                   for bucket in trend_map.values()],
    }

    event_counter = {}
    for item in event_rows:
        event_counter[item["event_type"]] = event_counter.get(item["event_type"], 0) + 1
    event_distribution = [
        {"name": GATEWAY_EVENT_DICT.get(code, code), "value": amount}
        for code, amount in event_counter.items()
    ]

    return {
        "state_distribution": state_distribution,
        "quality_distribution": quality_distribution,
        "latency_compare": latency_compare,
        "quality_trend": quality_trend,
        "event_distribution": event_distribution,
    }


def fetch_gateway_options():
    """网关管理页下拉选项：网关候选、病区清单与各类字典。"""
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT gateway_code, gateway_name, ward_name FROM iot_gateway_profile "
            "ORDER BY gateway_code").fetchall()
    finally:
        conn.close()
    profiles = dict_list(rows)
    return {
        "gateway_list": profiles,
        "ward_list": sorted({p["ward_name"] for p in profiles if p["ward_name"]}),
        "link_options": [{"code": k, "label": v} for k, v in LINK_TYPE_DICT.items()],
        "state_options": [{"code": k, "label": v} for k, v in GATEWAY_STATE_DICT.items()],
        "quality_options": [{"code": k, "label": v} for k, v in QUALITY_LEVEL_DICT.items()],
        "event_type_options": [{"code": k, "label": v}
                               for k, v in GATEWAY_EVENT_DICT.items()],
        "event_level_options": [{"code": k, "label": v}
                                for k, v in EVENT_LEVEL_DICT.items()],
    }


def build_gateway_payload(keyword="", ward_name="", link_type="", run_state="",
                          quality_level="", page=1, gateway_code=""):
    """组装物联网网关管理页面所需的全部数据。"""
    ensure_gateway_profiles()
    pager = fetch_gateway_profiles(keyword, ward_name, link_type, run_state,
                                   quality_level, page)
    return {
        "summary": build_gateway_summary(),
        "charts": build_gateway_charts(),
        "records": pager["records"],
        "total": pager["total"],
        "page": pager["page"],
        "page_amount": pager["page_amount"],
        "links": fetch_gateway_links(gateway_code, LINK_SHOW_LIMIT),
        "events": fetch_gateway_events(gateway_code, "", "", EVENT_SHOW_LIMIT),
        "options": fetch_gateway_options(),
        "result": "OK",
    }


def build_gateway_brief():
    """网关管理看板摘要，供实时监控大屏嵌入。"""
    summary = build_gateway_summary()
    return {
        "gateway_total": summary["gateway_total"],
        "online_amount": summary["online_amount"],
        "online_rate": summary["online_rate"],
        "poor_amount": summary["poor_amount"],
        "avg_latency": summary["avg_latency"],
        "event_open": summary["event_open"],
    }


# ------------------------------------------------------------------
# 演示数据
# ------------------------------------------------------------------
def seed_demo_gateway():
    """灌入网关管理演示数据：建档、历史链路探测与运行事件样本。

    :return: 生成的网关档案条数
    """
    ensure_gateway_profiles()
    conn = connect_db()
    try:
        already = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_gateway_link").fetchone()["c"]
        if already:
            return 0
        rows = conn.execute(
            "SELECT gateway_code FROM iot_gateway_profile ORDER BY gateway_code"
        ).fetchall()
        codes = [row["gateway_code"] for row in rows]
    finally:
        conn.close()
    if not codes:
        return 0

    generator = random.Random(DEMO_RANDOM_SEED)
    for code in codes:
        # 每台网关补 6 条历史探测，时间逐次递增形成可观察的质量走势
        for step in range(6):
            stamp = (datetime.datetime.now()
                     - datetime.timedelta(minutes=(6 - step) * 20)
                     ).strftime("%Y-%m-%d %H:%M:%S")
            probe_single_gateway(code, generator, stamp)
        append_gateway_event(code, "ONLINE", "网关上线并完成设备注册", "INFO")
    if codes:
        # 首台网关：一次已处置的固件升级
        append_gateway_event(codes[0], "UPGRADE",
                             "固件由 V1.0.0 升级至 V1.2.3：优化链路重连策略", "INFO")
        conn = connect_db()
        try:
            conn.execute(
                "UPDATE iot_gateway_event SET handle_flag=1, "
                "handler_name='运维-张工', handle_note='升级后复测链路正常', "
                "handle_time=? WHERE gateway_code=? AND event_type='UPGRADE'",
                (now_text(), codes[0]))
            conn.execute(
                "UPDATE iot_gateway_profile SET firmware_ver='V1.2.3' "
                "WHERE gateway_code=?", (codes[0],))
            conn.commit()
        finally:
            conn.close()
    if len(codes) > 1:
        # 第二台网关：一次远程重启留痕
        event_code = append_gateway_event(
            codes[1], "REBOOT", "下发远程重启指令：例行巡检重启", "INFO")
        handle_gateway_event(event_code, "运维-张工", "重启后设备重新注册成功")
    # 链路质量最差的网关：置为故障待修并留下未处置的异常事件
    worst = None
    worst_score = None
    for code in codes:
        profile = fetch_profile_row(code)
        if profile is None:
            continue
        if worst_score is None or profile["quality_score"] < worst_score:
            worst, worst_score = code, profile["quality_score"]
    if worst is not None:
        append_gateway_event(worst, "ERROR",
                             "网关上行链路持续劣化，已置为故障待修", "FAULT")
        conn = connect_db()
        try:
            conn.execute(
                "UPDATE iot_gateway_profile SET run_state='FAULT' "
                "WHERE gateway_code=?", (worst,))
            conn.commit()
        finally:
            conn.close()
    return len(codes)

# --------------------------------------------------------------------------
# 第 9 天数据表脚本增量：iot_schema.sql（M，已内联为字符串常量 DAY9_SCHEMA_SQL）
# --------------------------------------------------------------------------

DAY9_SCHEMA_SQL = r"""
-- ==================================================================
-- 第 9 天新增：物联网网关运行档案表
-- ==================================================================
-- 网关档案表：按「一台网关一条」扩展网关接入台账，保存设备型号、固件版本、
-- 链路类型、运行状态、最大接入槽位、当前负载率与在线率，与第 1 天的网关
-- 接入表按网关编码一对一关联，支撑网关层面的容量规划与运行态势监控
CREATE TABLE IF NOT EXISTS iot_gateway_profile (
    profile_id    INTEGER PRIMARY KEY AUTOINCREMENT,  -- 档案主键
    gateway_code  TEXT    NOT NULL UNIQUE,            -- 网关编号（关联 iot_gateway_node）
    gateway_name  TEXT    DEFAULT '',                 -- 网关名称快照
    gateway_model TEXT    DEFAULT '',                 -- 网关型号
    ward_name     TEXT    DEFAULT '',                 -- 部署病区快照
    firmware_ver  TEXT    DEFAULT 'V1.0.0',           -- 当前固件版本
    link_type     TEXT    DEFAULT 'ETH',              -- 链路类型（见 LINK_TYPE_DICT）
    run_state     TEXT    DEFAULT 'ONLINE',           -- 运行状态（见 GATEWAY_STATE_DICT）
    max_slots     INTEGER DEFAULT 32,                 -- 最大接入槽位
    load_rate     REAL    DEFAULT 0,                  -- 当前负载率（已接设备 / 最大槽位）
    online_rate   REAL    DEFAULT 0,                  -- 下属设备在线率（%）
    quality_score REAL    DEFAULT 100,                -- 最近一次链路质量评分
    quality_level TEXT    DEFAULT 'EXCELLENT',        -- 链路质量等级（见 QUALITY_LEVEL_DICT）
    install_date  TEXT    DEFAULT '',                 -- 安装投用日期
    last_probe_at TEXT    DEFAULT '',                 -- 最近一次链路探测时间
    remark_text   TEXT    DEFAULT '',                 -- 备注说明
    updated_at    TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- ==================================================================
-- 第 9 天新增：网关链路质量探测表
-- ==================================================================
-- 链路探测表：按「一次探测一条」记录网关上行链路的通信质量时序，含往返
-- 时延、丢包率、信号强度、上行速率与质量评分等级，为链路劣化预警与网络
-- 优化提供连续的数据依据
CREATE TABLE IF NOT EXISTS iot_gateway_link (
    link_id      INTEGER PRIMARY KEY AUTOINCREMENT,   -- 探测主键
    link_code    TEXT    DEFAULT '',                  -- 探测编号（GL-年月日-序号）
    gateway_code TEXT    NOT NULL,                    -- 网关编号
    latency_ms   INTEGER DEFAULT 0,                   -- 往返时延（毫秒）
    packet_loss  REAL    DEFAULT 0,                   -- 丢包率（%）
    signal_dbm   INTEGER DEFAULT -60,                 -- 信号强度（dBm）
    uplink_kbps  INTEGER DEFAULT 0,                   -- 上行速率（kbps）
    quality_score REAL   DEFAULT 100,                 -- 链路质量评分
    quality_level TEXT   DEFAULT 'EXCELLENT',         -- 链路质量等级
    probe_time   TEXT    DEFAULT ''                   -- 探测时间
);

-- ==================================================================
-- 第 9 天新增：网关运行事件表
-- ==================================================================
-- 网关事件表：按「一次事件一条」记录网关上下线、远程重启、固件升级与链路
-- 劣化等运行事件，含事件级别、事件描述、处置标记与处置人，形成网关运维的
-- 完整留痕链条
CREATE TABLE IF NOT EXISTS iot_gateway_event (
    event_id     INTEGER PRIMARY KEY AUTOINCREMENT,   -- 事件主键
    event_code   TEXT    DEFAULT '',                  -- 事件编号（GE-年月日-序号）
    gateway_code TEXT    NOT NULL,                    -- 网关编号
    event_type   TEXT    DEFAULT 'ONLINE',            -- 事件类型（见 GATEWAY_EVENT_DICT）
    event_level  TEXT    DEFAULT 'INFO',              -- 事件级别（见 EVENT_LEVEL_DICT）
    event_text   TEXT    DEFAULT '',                  -- 事件描述
    handle_flag  INTEGER DEFAULT 0,                   -- 处置标记：0 未处置 / 1 已处置
    handler_name TEXT    DEFAULT '',                  -- 处置人
    handle_note  TEXT    DEFAULT '',                  -- 处置说明
    event_time   TEXT    DEFAULT '',                  -- 事件发生时间
    handle_time  TEXT    DEFAULT ''                   -- 事件处置时间
);

CREATE INDEX IF NOT EXISTS idx_gwprofile_code  ON iot_gateway_profile (gateway_code);
CREATE INDEX IF NOT EXISTS idx_gwprofile_state ON iot_gateway_profile (run_state);
CREATE INDEX IF NOT EXISTS idx_gwprofile_level ON iot_gateway_profile (quality_level);
CREATE INDEX IF NOT EXISTS idx_gwlink_code     ON iot_gateway_link (gateway_code);
CREATE INDEX IF NOT EXISTS idx_gwlink_time     ON iot_gateway_link (probe_time);
CREATE INDEX IF NOT EXISTS idx_gwevent_code    ON iot_gateway_event (gateway_code);
CREATE INDEX IF NOT EXISTS idx_gwevent_type    ON iot_gateway_event (event_type);
CREATE INDEX IF NOT EXISTS idx_gwevent_level   ON iot_gateway_event (event_level);
CREATE INDEX IF NOT EXISTS idx_gwevent_flag    ON iot_gateway_event (handle_flag);
"""


def apply_day9_schema(conn):
    """把第 9 天新增的数据表脚本应用到已有数据库。"""
    conn.executescript(DAY9_SCHEMA_SQL)
    conn.commit()
    return True

# --------------------------------------------------------------------------
# 第 9 天主程序新增路由与接口：app.py（仅增量片段）
# --------------------------------------------------------------------------

# 开发日期：2026-10-29（第 9 天 / 共 15 天）
#           阈值、能耗、远程控制、分组标签与网关管理演示数据灌入、拉起 MQTT
#           订阅服务（或模拟采集器），并注册实时监控大屏、设备接入、在线状态、
#           实时参数采集、阈值告警中心、能耗统计、远程控制、分组标签管理与
#           物联网网关管理九类页面路由，提供设备注册增删改查、心跳上报、
#           离线扫描识别、实时参数采集存储、动态曲线、阈值扫描与告警处置、
#           能耗分时采集、峰谷电费测算与节能建议、远程指令模板维护、指令
#           下发回执与重发、设备分组维护、入组退组、标签维护与批量打标，
#           以及网关档案维护、链路探测、远程重启、固件升级与运行事件处置
#           JSON 接口

from iot_gateway import (
    build_gateway_payload,
    build_gateway_brief,
    save_gateway_profile,
    probe_all_gateways,
    reboot_gateway,
    upgrade_gateway,
    handle_gateway_event,
    fetch_gateway_links,
    fetch_gateway_events,
    seed_demo_gateway,
    LINK_TYPE_DICT,
    GATEWAY_STATE_DICT,
    QUALITY_LEVEL_DICT,
    GATEWAY_EVENT_DICT,
)

# 【函数内插入】def api_monitor():
#    # 第 9 天新增：大屏叠加网关运行摘要，供网关概况卡渲染
#    payload["gateway_brief"] = build_gateway_brief()

# ==================================================================
# 以下为第 9 天新增：物联网网关管理页面与接口
# ==================================================================


@app.route("/iot/gateway")
def page_gateway():
    """物联网网关管理页面：网关态势、五类图表、档案明细与事件留痕。"""
    return render_template(
        "page_gateway.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        refresh_interval=REFRESH_INTERVAL_SEC,
    )


@app.route("/api/iot/gateway/panel")
def api_gateway_panel():
    """网关管理数据接口：汇总指标、五类图表、分页档案、链路时序与事件留痕。"""
    return jsonify(build_gateway_payload(
        keyword=(request.args.get("keyword") or "").strip(),
        ward_name=(request.args.get("ward_name") or "").strip(),
        link_type=(request.args.get("link_type") or "").strip(),
        run_state=(request.args.get("run_state") or "").strip(),
        quality_level=(request.args.get("quality_level") or "").strip(),
        page=int(request.args.get("page", 1)),
        gateway_code=(request.args.get("gateway_code") or "").strip(),
    ))


@app.route("/api/iot/gateway/save", methods=["POST"])
def api_gateway_save():
    """网关档案维护接口：更新型号、链路类型、槽位、固件版本与运行状态。"""
    body = request.get_json(silent=True) or {}
    result = save_gateway_profile(
        (body.get("gateway_code") or "").strip(),
        body.get("gateway_model") or "",
        (body.get("link_type") or "").strip(),
        body.get("max_slots", 0),
        body.get("firmware_ver") or "",
        (body.get("run_state") or "").strip(),
        body.get("remark_text") or "",
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/gateway/probe", methods=["POST"])
def api_gateway_probe():
    """链路探测接口：对全部或单台网关采集一次链路质量指标。"""
    body = request.get_json(silent=True) or {}
    result = probe_all_gateways((body.get("gateway_code") or "").strip())
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/gateway/reboot", methods=["POST"])
def api_gateway_reboot():
    """远程重启接口：向网关下发重启指令并登记重启事件。"""
    body = request.get_json(silent=True) or {}
    result = reboot_gateway(
        (body.get("gateway_code") or "").strip(),
        (body.get("operator") or "admin").strip(),
        body.get("reason_text") or "",
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/gateway/upgrade", methods=["POST"])
def api_gateway_upgrade():
    """固件升级接口：登记目标版本并写入升级事件。"""
    body = request.get_json(silent=True) or {}
    result = upgrade_gateway(
        (body.get("gateway_code") or "").strip(),
        (body.get("target_ver") or "").strip(),
        (body.get("operator") or "admin").strip(),
        body.get("note_text") or "",
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/gateway/event/handle", methods=["POST"])
def api_gateway_event_handle():
    """网关事件处置接口：确认并关闭一条运行事件。"""
    body = request.get_json(silent=True) or {}
    result = handle_gateway_event(
        (body.get("event_code") or "").strip(),
        (body.get("handler_name") or "admin").strip(),
        body.get("handle_note") or "",
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/gateway/links")
def api_gateway_links():
    """链路探测时序接口：按网关返回最近若干次探测记录。"""
    return jsonify({
        "result": "OK",
        "records": fetch_gateway_links(
            (request.args.get("gateway_code") or "").strip(),
            int(request.args.get("limit", 12))),
    })


@app.route("/api/iot/gateway/events")
def api_gateway_events():
    """网关事件查询接口：支持按网关、级别与处置状态过滤。"""
    return jsonify({
        "result": "OK",
        "records": fetch_gateway_events(
            (request.args.get("gateway_code") or "").strip(),
            (request.args.get("event_level") or "").strip(),
            (request.args.get("handle_flag") or "").strip(),
            int(request.args.get("limit", 12))),
    })


@app.route("/api/iot/gateway/options")
def api_gateway_options():
    """网关管理下拉选项接口：链路类型、运行状态与质量等级字典。"""
    return jsonify({
        "result": "OK",
        "link_options": [{"code": k, "label": v} for k, v in LINK_TYPE_DICT.items()],
        "state_options": [{"code": k, "label": v} for k, v in GATEWAY_STATE_DICT.items()],
        "quality_options": [{"code": k, "label": v} for k, v in QUALITY_LEVEL_DICT.items()],
        "event_type_options": [{"code": k, "label": v}
                               for k, v in GATEWAY_EVENT_DICT.items()],
    })



# 【函数内插入】def bootstrap():
#    seed_demo_gateway()

# 【函数内插入】if __name__ == "__main__":
#    print("网关管理： http://127.0.0.1:%d/iot/gateway" % PORT)

# --------------------------------------------------------------------------
# 第 9 天新增/修改的前端资源清单（文件本体见工程 templates/、static/ 目录）
# --------------------------------------------------------------------------

#   [新增] static/iot_gateway.js            新增 434 行
#   [修改] templates/iot_base.html          新增 1 行
#   [新增] templates/page_gateway.html      新增 301 行
