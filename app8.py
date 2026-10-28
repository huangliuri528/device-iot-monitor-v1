# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-28（第 8 天 / 共 15 天）
# 模块功能：本文件仅含该软件第 8 天新增与修改的源代码，不含第 1~7 天已有代码
#           本次迭代主题：设备分组与标签模块 + 分组标签页面
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）
# 接入说明：以下内容为当日增量，需与前面各天的代码合并后运行——
#           新增模块按文件名放入工程目录；建表脚本增量追加进对应 .sql；
#           主程序增量片段插入 app.py 中 Flask 实例之后；前端资源按文末清单
#           放入 templates/ 与 static/ 目录


# --------------------------------------------------------------------------
# 第 8 天新增业务模块 1：iot_group.py（完整源码）
# --------------------------------------------------------------------------

# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-28（第 8 天 / 共 15 天）
# 模块功能：设备分组与标签模块。实现设备分组的维护与设备入组退组、标签
#           体系的维护与设备打标、按网关病区运行状态批量打标、分组维度
#           的在线率与告警统计，并按分组、标签、运行状态组合筛选查询设备
#           卡片；输出分组设备分布、标签使用分布、分组在线率与分组告警数
#           四类统计图表，形成「建组 → 入组 → 打标 → 分组监控」的设备
#           组织化管理闭环
# 依赖包：sqlite3（Python 内置）、datetime（Python 内置）

import datetime

from iot_store import connect_db, dict_list, now_text, fetch_device_nodes

# ------------------------------------------------------------------
# 分组状态标签：按分组内设备的在线比例划分健康度
# ------------------------------------------------------------------
GROUP_HEALTH_DICT = {
    "GOOD": "运行良好",
    "WARN": "需关注",
    "BAD": "告警较多",
    "EMPTY": "组内无设备",
}

# 健康度判定阈值（在线率百分比下限，由高到低匹配）
GROUP_HEALTH_THRESHOLD = [
    ("GOOD", 80),
    ("WARN", 50),
    ("BAD", 0),
]

# 内置设备分组：（分组编码、分组名称、所属病区、分组说明）
DEFAULT_GROUP_LIST = [
    ("GRP-ICU", "重症监护组", "ICU 病区", "重症监护病区生命支持类设备"),
    ("GRP-OR", "手术麻醉组", "手术中心", "手术与麻醉相关设备"),
    ("GRP-LAB", "检验分析组", "检验中心", "检验分析类设备"),
    ("GRP-IMG", "影像诊断组", "影像中心", "大型影像诊断设备"),
]

# 病区关键字与分组编码的匹配规则：设备入组时按所属病区自动归组
WARD_GROUP_RULE = (
    ("ICU", "GRP-ICU"),
    ("手术", "GRP-OR"),
    ("检验", "GRP-LAB"),
)

# 病区无法匹配内置规则时的兜底分组
FALLBACK_GROUP_CODE = "GRP-IMG"

# 内置设备标签：（标签编码、标签名称、标签颜色、标签说明）
DEFAULT_TAG_LIST = [
    ("TAG-LIFE", "急救生命支持", "#f87171", "直接关系生命支持的急救与监护设备"),
    ("TAG-IMAGE", "大型影像", "#38bdf8", "单价高、能耗高的影像诊断设备"),
    ("TAG-MOBILE", "可移动设备", "#34d399", "可在病区间调配的移动式设备"),
    ("TAG-VALUE", "高值设备", "#fbbf24", "单价超过五十万元的高值设备"),
    ("TAG-KEY", "重点监控", "#a78bfa", "纳入重点监控与实时告警的设备"),
]

# 分组编码与标签编码前缀，用于生成新编码时保持风格统一
GROUP_CODE_PREFIX = "GRP"
TAG_CODE_PREFIX = "TAG"

# 设备卡片单页条数
GROUP_PAGE_SIZE = 12


def round1(value):
    """保留一位小数，空值与非数值按 0 处理。"""
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):
        return 0.0


def build_group_code(seq_no):
    """生成分组编码：GRP-四位序号。"""
    return "%s-%04d" % (GROUP_CODE_PREFIX, seq_no)


def build_tag_code(seq_no):
    """生成标签编码：TAG-四位序号。"""
    return "%s-%04d" % (TAG_CODE_PREFIX, seq_no)


def next_group_seq():
    """读取下一个分组序号。"""
    conn = connect_db()
    try:
        amount = conn.execute("SELECT COUNT(1) AS c FROM iot_device_group").fetchone()["c"]
    finally:
        conn.close()
    return amount + 1


def next_tag_seq():
    """读取下一个标签序号。"""
    conn = connect_db()
    try:
        amount = conn.execute("SELECT COUNT(1) AS c FROM iot_device_tag").fetchone()["c"]
    finally:
        conn.close()
    return amount + 1


# ------------------------------------------------------------------
# 分组维护
# ------------------------------------------------------------------
def match_group_by_ward(ward_name):
    """按设备所属病区匹配应归入的分组编码，未命中规则时返回兜底分组。"""
    text = (ward_name or "")
    for keyword, group_code in WARD_GROUP_RULE:
        if keyword in text:
            return group_code
    return FALLBACK_GROUP_CODE


def ensure_default_groups():
    """写入内置设备分组，已存在同名编码时跳过。"""
    conn = connect_db()
    amount = 0
    try:
        for item in DEFAULT_GROUP_LIST:
            exist = conn.execute(
                "SELECT group_id FROM iot_device_group WHERE group_code=?",
                (item[0],)).fetchone()
            if exist is not None:
                continue
            conn.execute(
                "INSERT INTO iot_device_group (group_code, group_name, ward_name, "
                "group_desc) VALUES (?,?,?,?)", item)
            amount += 1
        if amount:
            conn.commit()
    finally:
        conn.close()
    return amount


def ensure_default_tags():
    """写入内置设备标签，已存在同名编码时跳过。"""
    conn = connect_db()
    amount = 0
    try:
        for item in DEFAULT_TAG_LIST:
            exist = conn.execute(
                "SELECT tag_id FROM iot_device_tag WHERE tag_code=?",
                (item[0],)).fetchone()
            if exist is not None:
                continue
            conn.execute(
                "INSERT INTO iot_device_tag (tag_code, tag_name, tag_color, "
                "tag_desc) VALUES (?,?,?,?)", item)
            amount += 1
        if amount:
            conn.commit()
    finally:
        conn.close()
    return amount


def fetch_group_row(group_code):
    """按分组编码读取一条分组记录。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_device_group WHERE group_code=?",
            (group_code,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row is not None else None


def fetch_tag_row(tag_code):
    """按标签编码读取一条标签记录。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_device_tag WHERE tag_code=?", (tag_code,)).fetchone()
    finally:
        conn.close()
    return dict(row) if row is not None else None


def save_group(group_code, group_name, ward_name="", group_desc=""):
    """新增或修改一个设备分组。

    :return: 含 success 与 message 的执行结果字典
    """
    group_name = (group_name or "").strip()
    if not group_name:
        return {"success": False, "message": "分组名称不能为空"}
    group_code = (group_code or "").strip()
    if (group_code and not group_code.startswith(GROUP_CODE_PREFIX)
            and fetch_group_row(group_code) is None):
        return {"success": False,
                "message": "分组编码需以 %s- 开头" % GROUP_CODE_PREFIX}
    if not group_code:
        group_code = build_group_code(next_group_seq())
    conn = connect_db()
    try:
        exist = fetch_group_row(group_code)
        if exist is None:
            conn.execute(
                "INSERT INTO iot_device_group (group_code, group_name, ward_name, "
                "group_desc) VALUES (?,?,?,?)",
                (group_code, group_name, (ward_name or "").strip(),
                 (group_desc or "").strip()))
            tip = "新增分组 %s" % group_code
        else:
            conn.execute(
                "UPDATE iot_device_group SET group_name=?, ward_name=?, "
                "group_desc=? WHERE group_code=?",
                (group_name, (ward_name or "").strip(),
                 (group_desc or "").strip(), group_code))
            tip = "更新分组 %s" % group_code
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "%s 成功" % tip, "group_code": group_code}


def remove_group(group_code):
    """删除一个设备分组，并同步解除该组下全部设备的入组关系。"""
    if fetch_group_row((group_code or "").strip()) is None:
        return {"success": False, "message": "分组 %s 不存在" % group_code}
    conn = connect_db()
    try:
        bound = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_group_bind WHERE group_code=?",
            (group_code,)).fetchone()["c"]
        conn.execute("DELETE FROM iot_group_bind WHERE group_code=?", (group_code,))
        conn.execute("DELETE FROM iot_device_group WHERE group_code=?", (group_code,))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "分组 %s 已删除，解除 %d 台设备的入组关系"
                       % (group_code, bound)}


def bind_device_group(device_code, group_code):
    """把一台设备加入分组，一台设备同时只归属一个分组。"""
    device_code = (device_code or "").strip()
    group_code = (group_code or "").strip()
    if not device_code:
        return {"success": False, "message": "设备编号不能为空"}
    conn = connect_db()
    try:
        device = conn.execute(
            "SELECT device_code FROM iot_device_node WHERE device_code=?",
            (device_code,)).fetchone()
        if device is None:
            return {"success": False, "message": "设备 %s 不存在" % device_code}
        if group_code:
            if fetch_group_row(group_code) is None:
                return {"success": False, "message": "分组 %s 不存在" % group_code}
            conn.execute(
                "INSERT INTO iot_group_bind (device_code, group_code, bind_time) "
                "VALUES (?,?,?) ON CONFLICT(device_code) "
                "DO UPDATE SET group_code=excluded.group_code, "
                "bind_time=excluded.bind_time",
                (device_code, group_code, now_text()))
            tip = "设备 %s 已加入分组 %s" % (device_code, group_code)
        else:
            conn.execute(
                "DELETE FROM iot_group_bind WHERE device_code=?", (device_code,))
            tip = "设备 %s 已移出分组" % device_code
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": tip}


# ------------------------------------------------------------------
# 标签维护与打标
# ------------------------------------------------------------------
def save_tag(tag_code, tag_name, tag_color="#38bdf8", tag_desc="", enabled_flag=1):
    """新增或修改一个设备标签。"""
    tag_name = (tag_name or "").strip()
    if not tag_name:
        return {"success": False, "message": "标签名称不能为空"}
    tag_code = (tag_code or "").strip()
    if tag_code and not tag_code.startswith(TAG_CODE_PREFIX):
        if fetch_tag_row(tag_code) is None:
            return {"success": False,
                    "message": "标签编码需以 %s- 开头" % TAG_CODE_PREFIX}
    if not tag_code:
        tag_code = build_tag_code(next_tag_seq())
    conn = connect_db()
    try:
        exist = fetch_tag_row(tag_code)
        if exist is None:
            conn.execute(
                "INSERT INTO iot_device_tag (tag_code, tag_name, tag_color, "
                "tag_desc, enabled_flag) VALUES (?,?,?,?,?)",
                (tag_code, tag_name, (tag_color or "#38bdf8").strip(),
                 (tag_desc or "").strip(), 1 if enabled_flag else 0))
            tip = "新增标签 %s" % tag_code
        else:
            conn.execute(
                "UPDATE iot_device_tag SET tag_name=?, tag_color=?, tag_desc=?, "
                "enabled_flag=? WHERE tag_code=?",
                (tag_name, (tag_color or "#38bdf8").strip(),
                 (tag_desc or "").strip(), 1 if enabled_flag else 0, tag_code))
            tip = "更新标签 %s" % tag_code
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "%s 成功" % tip, "tag_code": tag_code}


def remove_tag(tag_code):
    """删除一个设备标签，并同步清除全部设备上的该标签。"""
    if fetch_tag_row((tag_code or "").strip()) is None:
        return {"success": False, "message": "标签 %s 不存在" % tag_code}
    conn = connect_db()
    try:
        used = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_tag_bind WHERE tag_code=?",
            (tag_code,)).fetchone()["c"]
        conn.execute("DELETE FROM iot_tag_bind WHERE tag_code=?", (tag_code,))
        conn.execute("DELETE FROM iot_device_tag WHERE tag_code=?", (tag_code,))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "标签 %s 已删除，清除 %d 台设备上的该标签"
                       % (tag_code, used)}


def mark_device_tags(device_code, tag_codes, operator="admin"):
    """设置一台设备的标签集合，采用覆盖式写入。

    :param tag_codes: 标签编码列表，为空表示清空该设备全部标签
    """
    device_code = (device_code or "").strip()
    if not device_code:
        return {"success": False, "message": "设备编号不能为空"}
    codes = []
    for item in tag_codes or []:
        code = (item or "").strip()
        if not code:
            continue
        if fetch_tag_row(code) is None:
            return {"success": False, "message": "标签 %s 不存在" % code}
        if code not in codes:
            codes.append(code)
    conn = connect_db()
    try:
        device = conn.execute(
            "SELECT device_code FROM iot_device_node WHERE device_code=?",
            (device_code,)).fetchone()
        if device is None:
            return {"success": False, "message": "设备 %s 不存在" % device_code}
        conn.execute("DELETE FROM iot_tag_bind WHERE device_code=?", (device_code,))
        for code in codes:
            conn.execute(
                "INSERT INTO iot_tag_bind (device_code, tag_code, bind_time, "
                "operator_name) VALUES (?,?,?,?)",
                (device_code, code, now_text(), operator))
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "设备 %s 标签已更新为 %d 个" % (device_code, len(codes))}


def batch_mark_tags(tag_codes, gateway_code="", ward_name="", run_state="",
                    group_code="", operator="admin"):
    """按网关、病区、运行状态与分组范围批量给设备打标（追加式）。

    :return: 含 success、message 与 amount（受影响设备数）的结果字典
    """
    codes = []
    for item in tag_codes or []:
        code = (item or "").strip()
        if not code:
            continue
        if fetch_tag_row(code) is None:
            return {"success": False, "message": "标签 %s 不存在" % code}
        if code not in codes:
            codes.append(code)
    if not codes:
        return {"success": False, "message": "请至少选择一个标签"}
    devices = pick_devices_by_scope(gateway_code, ward_name, run_state, group_code)
    if not devices:
        return {"success": False, "message": "筛选范围内没有匹配的设备"}
    conn = connect_db()
    amount = 0
    try:
        for device_code in devices:
            for code in codes:
                conn.execute(
                    "INSERT INTO iot_tag_bind (device_code, tag_code, bind_time, "
                    "operator_name) VALUES (?,?,?,?) "
                    "ON CONFLICT(device_code, tag_code) DO NOTHING",
                    (device_code, code, now_text(), operator))
            amount += 1
        conn.commit()
    finally:
        conn.close()
    return {"success": True,
            "message": "已为 %d 台设备追加 %d 个标签" % (amount, len(codes)),
            "amount": amount}


def pick_devices_by_scope(gateway_code="", ward_name="", run_state="",
                          group_code=""):
    """按筛选范围取出设备编号列表。"""
    sql = ("SELECT n.device_code AS device_code FROM iot_device_node n "
           "LEFT JOIN iot_group_bind b ON n.device_code=b.device_code WHERE 1=1")
    params = []
    if gateway_code:
        sql += " AND n.gateway_code=?"
        params.append(gateway_code)
    if ward_name:
        sql += " AND n.ward_name=?"
        params.append(ward_name)
    if run_state:
        sql += " AND n.run_state=?"
        params.append(run_state)
    if group_code:
        sql += " AND b.group_code=?"
        params.append(group_code)
    conn = connect_db()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return [dict(item)["device_code"] for item in rows]


# ------------------------------------------------------------------
# 查询与统计
# ------------------------------------------------------------------
def fetch_group_statistics():
    """统计每个分组的设备数、在线数、故障数与告警数。"""
    conn = connect_db()
    try:
        groups = conn.execute(
            "SELECT * FROM iot_device_group ORDER BY group_id").fetchall()
        rows = conn.execute(
            "SELECT n.device_code, n.run_state, b.group_code "
            "FROM iot_device_node n "
            "LEFT JOIN iot_group_bind b ON n.device_code=b.device_code").fetchall()
        alarm_rows = conn.execute(
            "SELECT device_code, COUNT(1) AS c FROM iot_alarm_record "
            "WHERE handle_state='OPEN' GROUP BY device_code").fetchall()
    finally:
        conn.close()
    alarm_map = {dict(item)["device_code"]: dict(item)["c"] for item in alarm_rows}
    device_rows = [dict(item) for item in rows]
    result = []
    for item in groups:
        group = dict(item)
        members = [x for x in device_rows if x["group_code"] == group["group_code"]]
        online = len([x for x in members if x["run_state"] == "ONLINE"])
        fault = len([x for x in members if x["run_state"] == "FAULT"])
        offline = len([x for x in members if x["run_state"] == "OFFLINE"])
        sleep = len([x for x in members if x["run_state"] == "SLEEP"])
        alarm = sum(alarm_map.get(x["device_code"], 0) for x in members)
        total = len(members)
        rate = round1(online * 100.0 / total) if total else 0.0
        group["device_amount"] = total
        group["online_amount"] = online
        group["fault_amount"] = fault
        group["offline_amount"] = offline
        group["sleep_amount"] = sleep
        group["alarm_amount"] = alarm
        group["online_rate"] = rate
        group["health_code"] = judge_group_health(rate, total)
        group["health_label"] = GROUP_HEALTH_DICT[group["health_code"]]
        result.append(group)
    return result


def judge_group_health(online_rate, device_amount):
    """按分组在线率判定分组健康度编码。"""
    if not device_amount:
        return "EMPTY"
    for code, floor in GROUP_HEALTH_THRESHOLD:
        if online_rate >= floor:
            return code
    return "BAD"


def fetch_tag_statistics():
    """统计每个标签的使用次数与关联设备。"""
    conn = connect_db()
    try:
        tags = conn.execute(
            "SELECT * FROM iot_device_tag ORDER BY tag_id").fetchall()
        rows = conn.execute(
            "SELECT tag_code, COUNT(1) AS c FROM iot_tag_bind "
            "GROUP BY tag_code").fetchall()
    finally:
        conn.close()
    counter = {dict(item)["tag_code"]: dict(item)["c"] for item in rows}
    result = []
    for item in tags:
        tag = dict(item)
        tag["use_amount"] = counter.get(tag["tag_code"], 0)
        result.append(tag)
    return result


def fetch_device_cards(keyword="", group_code="", tag_code="", run_state="",
                       gateway_code="", page=1):
    """按组合条件分页查询设备卡片，附带所属分组与标签清单。"""
    conn = connect_db()
    try:
        nodes = conn.execute("SELECT * FROM iot_device_node").fetchall()
        binds = conn.execute("SELECT * FROM iot_group_bind").fetchall()
        tag_binds = conn.execute("SELECT * FROM iot_tag_bind").fetchall()
        tags = conn.execute("SELECT tag_code, tag_name, tag_color "
                            "FROM iot_device_tag").fetchall()
        groups = conn.execute("SELECT group_code, group_name "
                              "FROM iot_device_group").fetchall()
    finally:
        conn.close()

    group_map = {dict(x)["group_code"]: dict(x)["group_name"] for x in groups}
    tag_map = {dict(x)["tag_code"]: dict(x) for x in tags}
    device_group = {dict(x)["device_code"]: dict(x)["group_code"] for x in binds}
    device_tags = {}
    for item in tag_binds:
        row = dict(item)
        device_tags.setdefault(row["device_code"], []).append(row["tag_code"])

    cards = []
    for item in nodes:
        node = dict(item)
        codes = device_tags.get(node["device_code"], [])
        node["tag_codes"] = codes
        node["tag_list"] = [tag_map[c] for c in codes if c in tag_map]
        node["group_code"] = device_group.get(node["device_code"], "")
        node["group_name"] = group_map.get(node["group_code"], "未分组")
        cards.append(node)

    if keyword:
        key = keyword.lower()
        cards = [x for x in cards
                 if key in x["device_code"].lower()
                 or key in x["device_name"].lower()
                 or key in x["ward_name"].lower()]
    if group_code:
        cards = [x for x in cards if x["group_code"] == group_code]
    if tag_code:
        cards = [x for x in cards if tag_code in x["tag_codes"]]
    if run_state:
        cards = [x for x in cards if x["run_state"] == run_state]
    if gateway_code:
        cards = [x for x in cards if x["gateway_code"] == gateway_code]

    try:
        page_no = int(page)
    except (TypeError, ValueError):
        page_no = 1
    if page_no < 1:
        page_no = 1
    offset = (page_no - 1) * GROUP_PAGE_SIZE
    return {
        "records": cards[offset:offset + GROUP_PAGE_SIZE],
        "total": len(cards),
        "page": page_no,
        "page_amount": max(1, (len(cards) + GROUP_PAGE_SIZE - 1) // GROUP_PAGE_SIZE),
    }


def build_group_summary():
    """汇总分组与标签概况：分组数、标签数、已分组设备数、未分组设备数。"""
    conn = connect_db()
    try:
        total_device = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_device_node").fetchone()["c"]
        bound = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_group_bind").fetchone()["c"]
        tagged = conn.execute(
            "SELECT COUNT(DISTINCT device_code) AS c FROM iot_tag_bind").fetchone()["c"]
    finally:
        conn.close()
    groups = fetch_group_statistics()
    tags = fetch_tag_statistics()
    online_groups = len([x for x in groups if x["health_code"] == "GOOD"])
    return {
        "group_amount": len(groups),
        "tag_amount": len([x for x in tags if x["enabled_flag"]]),
        "device_amount": total_device,
        "grouped_amount": bound,
        "ungrouped_amount": total_device - bound,
        "tagged_amount": tagged,
        "online_group_amount": online_groups,
        "alarm_amount": sum(x["alarm_amount"] for x in groups),
    }


def build_group_charts():
    """构建分组页四类图表：分组设备分布、标签使用分布、分组在线率与分组告警数。"""
    groups = fetch_group_statistics()
    tags = fetch_tag_statistics()
    return {
        "group_distribution": [
            {"name": x["group_name"], "value": x["device_amount"]} for x in groups
        ],
        "tag_distribution": [
            {"name": x["tag_name"], "value": x["use_amount"]}
            for x in tags if x["enabled_flag"]
        ],
        "group_online": [
            {"name": x["group_name"], "rate": x["online_rate"],
             "online": x["online_amount"], "total": x["device_amount"]}
            for x in groups
        ],
        "group_alarm": [
            {"name": x["group_name"], "value": x["alarm_amount"]} for x in groups
        ],
    }


def fetch_group_options():
    """分组页下拉选项：分组、标签、运行状态、网关与病区清单。"""
    conn = connect_db()
    try:
        gateway_rows = conn.execute(
            "SELECT DISTINCT gateway_code FROM iot_device_node "
            "WHERE gateway_code<>'' ORDER BY gateway_code").fetchall()
        ward_rows = conn.execute(
            "SELECT DISTINCT ward_name FROM iot_device_node "
            "WHERE ward_name<>'' ORDER BY ward_name").fetchall()
        state_rows = conn.execute(
            "SELECT DISTINCT run_state FROM iot_device_node "
            "WHERE run_state<>'' ORDER BY run_state").fetchall()
    finally:
        conn.close()
    return {
        "group_list": fetch_group_statistics(),
        "tag_list": fetch_tag_statistics(),
        "gateway_list": [dict(x)["gateway_code"] for x in gateway_rows],
        "ward_list": [dict(x)["ward_name"] for x in ward_rows],
        "state_list": [dict(x)["run_state"] for x in state_rows],
        "device_list": [{"code": x["device_code"], "label": x["device_code"]}
                        for x in fetch_device_nodes()],
    }


def build_group_payload(keyword="", group_code="", tag_code="", run_state="",
                        gateway_code="", page=1):
    """组装分组与标签页面所需的全部数据：汇总指标、四类图表、设备卡片与下拉选项。"""
    pager = fetch_device_cards(keyword, group_code, tag_code, run_state,
                               gateway_code, page)
    return {
        "summary": build_group_summary(),
        "charts": build_group_charts(),
        "records": pager["records"],
        "total": pager["total"],
        "page": pager["page"],
        "page_amount": pager["page_amount"],
        "options": fetch_group_options(),
        "result": "OK",
    }


def build_group_brief():
    """分组与标签大屏摘要，供实时监控大屏嵌入。"""
    summary = build_group_summary()
    return {
        "group_amount": summary["group_amount"],
        "tag_amount": summary["tag_amount"],
        "grouped_amount": summary["grouped_amount"],
        "ungrouped_amount": summary["ungrouped_amount"],
        "alarm_amount": summary["alarm_amount"],
    }


# ------------------------------------------------------------------
# 演示数据
# ------------------------------------------------------------------
def seed_demo_group():
    """灌入设备分组与标签演示数据：建分组、建标签、设备入组并打标。

    :return: 完成入组的设备台数
    """
    ensure_default_groups()
    ensure_default_tags()
    conn = connect_db()
    try:
        already = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_group_bind").fetchone()["c"]
        if already:
            return 0
    finally:
        conn.close()
    nodes = fetch_device_nodes()
    if not nodes:
        return 0
    group_codes = [x[0] for x in DEFAULT_GROUP_LIST]
    tag_codes = [x[0] for x in DEFAULT_TAG_LIST]
    amount = 0
    for index, node in enumerate(nodes):
        # 先按病区语义归组，病区未命中内置规则时落到兜底分组
        group_code = match_group_by_ward(node["ward_name"])
        if group_code not in group_codes:
            group_code = group_codes[index % len(group_codes)]
        result = bind_device_group(node["device_code"], group_code)
        if not result.get("success"):
            continue
        amount += 1
        # 每台设备按序号分配两到三个标签，保证标签分布有梯度
        picked = [tag_codes[index % len(tag_codes)]]
        picked.append(tag_codes[(index + 2) % len(tag_codes)])
        if index % 3 == 0:
            picked.append(tag_codes[(index + 4) % len(tag_codes)])
        mark_device_tags(node["device_code"], picked, "医工科-周工")
    return amount

# --------------------------------------------------------------------------
# 第 8 天数据表脚本增量：iot_schema.sql（M，已内联为字符串常量 DAY8_SCHEMA_SQL）
# --------------------------------------------------------------------------

DAY8_SCHEMA_SQL = r"""
-- ==================================================================
-- 第 8 天新增：设备分组档案表
-- ==================================================================
-- 分组档案表：按「一个管理单元一个分组」组织设备，按病区或业务条线划分，
-- 便于按组统计在线率、告警量与能耗，实现设备的组织化、批量化管理
CREATE TABLE IF NOT EXISTS iot_device_group (
    group_id    INTEGER PRIMARY KEY AUTOINCREMENT,   -- 分组主键
    group_code  TEXT    NOT NULL UNIQUE,             -- 分组编码（GRP-序号）
    group_name  TEXT    NOT NULL,                    -- 分组名称
    ward_name   TEXT    DEFAULT '',                  -- 主要覆盖病区
    group_desc  TEXT    DEFAULT '',                  -- 分组说明
    created_at  TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- ==================================================================
-- 第 8 天新增：设备标签档案表
-- ==================================================================
-- 标签档案表：按「一类属性一个标签」维护设备标签体系，如急救生命支持、
-- 大型影像、可移动设备、高值设备与重点监控，标签可多维度叠加，支撑按
-- 属性快速圈选设备并批量下发指令
CREATE TABLE IF NOT EXISTS iot_device_tag (
    tag_id       INTEGER PRIMARY KEY AUTOINCREMENT,  -- 标签主键
    tag_code     TEXT    NOT NULL UNIQUE,            -- 标签编码（TAG-序号）
    tag_name     TEXT    NOT NULL,                   -- 标签名称
    tag_color    TEXT    DEFAULT '#38bdf8',          -- 标签展示颜色
    tag_desc     TEXT    DEFAULT '',                 -- 标签说明
    enabled_flag INTEGER DEFAULT 1,                  -- 是否启用：1 启用 / 0 停用
    created_at   TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- ==================================================================
-- 第 8 天新增：设备分组绑定表
-- ==================================================================
-- 分组绑定表：按「一台设备一个分组一条」记录入组关系，一台设备同时只归属
-- 一个分组，重复入组时覆盖原分组，支撑按分组维度的在线率与告警统计
CREATE TABLE IF NOT EXISTS iot_group_bind (
    bind_id     INTEGER PRIMARY KEY AUTOINCREMENT,  -- 绑定主键
    device_code TEXT    NOT NULL UNIQUE,            -- 设备编号
    group_code  TEXT    NOT NULL,                   -- 分组编码
    bind_time   TEXT    DEFAULT ''                  -- 入组时间
);

-- ==================================================================
-- 第 8 天新增：设备标签绑定表
-- ==================================================================
-- 标签绑定表：按「一台设备一个标签一条」记录打标结果，一台设备可叠加多个
-- 标签，重复打标自动忽略，支撑多维度圈选设备与批量操作
CREATE TABLE IF NOT EXISTS iot_tag_bind (
    bind_id        INTEGER PRIMARY KEY AUTOINCREMENT, -- 绑定主键
    device_code    TEXT    NOT NULL,                  -- 设备编号
    tag_code       TEXT    NOT NULL,                  -- 标签编码
    bind_time      TEXT    DEFAULT '',                -- 打标时间
    operator_name  TEXT    DEFAULT 'admin',           -- 打标操作人
    UNIQUE (device_code, tag_code)
);

CREATE INDEX IF NOT EXISTS idx_groupcode      ON iot_device_group (group_code);
CREATE INDEX IF NOT EXISTS idx_tagcode        ON iot_device_tag (tag_code);
CREATE INDEX IF NOT EXISTS idx_groupbind_dev  ON iot_group_bind (device_code);
CREATE INDEX IF NOT EXISTS idx_groupbind_grp  ON iot_group_bind (group_code);
CREATE INDEX IF NOT EXISTS idx_tagbind_dev    ON iot_tag_bind (device_code);
CREATE INDEX IF NOT EXISTS idx_tagbind_tag    ON iot_tag_bind (tag_code);
"""


def apply_day8_schema(conn):
    """把第 8 天新增的数据表脚本应用到已有数据库。"""
    conn.executescript(DAY8_SCHEMA_SQL)
    conn.commit()
    return True

# --------------------------------------------------------------------------
# 第 8 天主程序新增路由与接口：app.py（仅增量片段）
# --------------------------------------------------------------------------

# 开发日期：2026-10-28（第 8 天 / 共 15 天）
#           阈值、能耗、远程控制与分组标签演示数据灌入、拉起 MQTT 订阅
#           服务（或模拟采集器），并注册实时监控大屏、设备接入、在线状态、
#           实时参数采集、阈值告警中心、能耗统计、远程控制与分组标签管理
#           八类页面路由，提供设备注册增删改查、心跳上报、离线扫描识别、
#           实时参数采集存储、动态曲线、阈值扫描与告警处置、能耗分时采集、
#           峰谷电费测算与节能建议、远程指令模板维护、指令下发回执与重发，
#           以及设备分组维护、入组退组、标签维护与批量打标 JSON 接口

from iot_group import (
    build_group_payload,
    build_group_brief,
    save_group,
    remove_group,
    bind_device_group,
    save_tag,
    remove_tag,
    mark_device_tags,
    batch_mark_tags,
    seed_demo_group,
    GROUP_HEALTH_DICT,
)

# 【函数内插入】def api_monitor():
#    # 第 8 天新增：大屏叠加分组与标签摘要，供分组概况卡渲染
#    payload["group_brief"] = build_group_brief()

@app.route("/iot/group")
def page_group():
    """设备分组与标签页面：分组档案、标签体系、设备入组与打标。"""
    return render_template(
        "page_group.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        refresh_interval=REFRESH_INTERVAL_SEC,
    )


@app.route("/api/iot/group/panel")
def api_group_panel():
    """分组与标签数据接口：汇总指标、四类图表、分组标签清单与设备卡片。"""
    payload = build_group_payload(
        keyword=(request.args.get("keyword") or "").strip(),
        group_code=(request.args.get("group_code") or "").strip(),
        tag_code=(request.args.get("tag_code") or "").strip(),
        run_state=(request.args.get("run_state") or "").strip(),
        gateway_code=(request.args.get("gateway_code") or "").strip(),
        page=int(request.args.get("page", 1)),
    )
    return jsonify(payload)


@app.route("/api/iot/group/save", methods=["POST"])
def api_group_save():
    """新增或修改一个设备分组。"""
    body = request.get_json(silent=True) or {}
    result = save_group(
        (body.get("group_code") or "").strip(),
        (body.get("group_name") or "").strip(),
        (body.get("ward_name") or "").strip(),
        (body.get("group_desc") or "").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/group/remove", methods=["POST"])
def api_group_remove():
    """删除一个设备分组并解除组内设备绑定。"""
    body = request.get_json(silent=True) or {}
    result = remove_group((body.get("group_code") or "").strip())
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/group/bind", methods=["POST"])
def api_group_bind():
    """把一台设备加入分组，分组编码为空表示移出分组。"""
    body = request.get_json(silent=True) or {}
    result = bind_device_group(
        (body.get("device_code") or "").strip(),
        (body.get("group_code") or "").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/tag/save", methods=["POST"])
def api_tag_save():
    """新增或修改一个设备标签。"""
    body = request.get_json(silent=True) or {}
    result = save_tag(
        (body.get("tag_code") or "").strip(),
        (body.get("tag_name") or "").strip(),
        (body.get("tag_color") or "#38bdf8").strip(),
        (body.get("tag_desc") or "").strip(),
        body.get("enabled_flag", 1),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/tag/remove", methods=["POST"])
def api_tag_remove():
    """删除一个设备标签并清除设备上该标签。"""
    body = request.get_json(silent=True) or {}
    result = remove_tag((body.get("tag_code") or "").strip())
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/tag/mark", methods=["POST"])
def api_tag_mark():
    """为单台设备设置标签集合（覆盖式写入）。"""
    body = request.get_json(silent=True) or {}
    result = mark_device_tags(
        (body.get("device_code") or "").strip(),
        body.get("tag_codes") or [],
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/tag/batch", methods=["POST"])
def api_tag_batch():
    """按网关、病区、运行状态与分组范围批量给设备追加标签。"""
    body = request.get_json(silent=True) or {}
    result = batch_mark_tags(
        body.get("tag_codes") or [],
        (body.get("gateway_code") or "").strip(),
        (body.get("ward_name") or "").strip(),
        (body.get("run_state") or "").strip(),
        (body.get("group_code") or "").strip(),
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)



# 【函数内插入】def bootstrap():
#    seed_demo_group()

# 【函数内插入】if __name__ == "__main__":
#    print("分组标签： http://127.0.0.1:%d/iot/group" % PORT)

# --------------------------------------------------------------------------
# 第 8 天新增/修改的前端资源清单（文件本体见工程 templates/、static/ 目录）
# --------------------------------------------------------------------------

#   [新增] static/iot_group.js              新增 531 行
#   [修改] templates/iot_base.html          新增 1 行
#   [新增] templates/page_group.html        新增 247 行
