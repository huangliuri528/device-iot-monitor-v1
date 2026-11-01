# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-11-01（第 12 天 / 共 15 天）
# 模块功能：本文件仅含该软件第 12 天新增与修改的源代码，不含第 1~11 天已有代码
#           本次迭代主题：设备空间位置与分布管理模块 + 空间位置页面
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）
# 接入说明：以下内容为当日增量，需与前面各天的代码合并后运行——
#           新增模块按文件名放入工程目录；建表脚本增量追加进对应 .sql；
#           主程序增量片段插入 app.py 中 Flask 实例之后；前端资源按文末清单
#           放入 templates/ 与 static/ 目录


# --------------------------------------------------------------------------
# 第 12 天新增业务模块 1：iot_location.py（完整源码）
# --------------------------------------------------------------------------

# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-11-01（第 12 天 / 共 15 天）
# 模块功能：设备空间位置与分布管理模块。实现院区空间区域建档（楼宇、楼层、
#           区域类型、面积与设备容纳上限）、设备位置绑定与平面坐标登记、
#           跨区域调拨自动生成移动轨迹、在位状态扫描（按运行状态与心跳新鲜
#           度判定在位、移动中、失联与未知）与区域容量超限校验；设备换区时
#           自动按区域中心点估算移动距离并留痕，形成可调阅的位置流转轨迹；
#           输出区域类型分布、定位状态分布、区域设备分布排行、楼层设备分布与
#           近 7 日位置变动趋势五类统计图表，形成「划区域 → 绑位置 → 扫在位
#           → 记调拨 → 查轨迹」的设备空间管理闭环
# 依赖包：sqlite3（Python 内置）、datetime（Python 内置）

import datetime
import math

from iot_store import connect_db, dict_list, now_text, fetch_device_nodes

# ------------------------------------------------------------------
# 区域类型字典：按医院使用场景划分四类空间区域
# ------------------------------------------------------------------
ZONE_TYPE_DICT = {
    "WARD": "住院病区",
    "OPD": "门诊诊区",
    "LAB": "医技检查",
    "PUB": "公共配套",
}

# 区域状态字典
ZONE_STATE_DICT = {
    "USE": "使用中",
    "IDLE": "空闲",
    "STOP": "停用",
}

# 定位状态字典：按运行状态与心跳新鲜度自动判定
LOCATE_STATE_DICT = {
    "ONLINE": "在位在线",
    "MOVING": "移动中",
    "OFFLINE": "失联",
    "UNKNOWN": "未知",
}

# 移动类型字典：区分新装入库、区域内调拨与报废迁出
MOVE_TYPE_DICT = {
    "IN": "新装入库",
    "MOVE": "区域调拨",
    "OUT": "迁出退库",
}

# 心跳新鲜度阈值（分钟）：超过该时长未上报心跳判定为失联
HEARTBEAT_FRESH_MINUTES = 5

# 调拨生效时长（分钟）：移动类设备在该时长内发生过位置变动判定为移动中
MOVE_ACTIVE_MINUTES = 30

# 定位状态与设备运行状态的映射关系
STATE_TO_LOCATE = {
    "ONLINE": "ONLINE",
    "SLEEP": "ONLINE",
    "OFFLINE": "OFFLINE",
    "FAULT": "OFFLINE",
}

# 各区域类型的默认中心坐标（米），用于估算跨区域移动距离
ZONE_CENTER_MAP = {
    "WARD": (18.0, 12.0),
    "OPD": (46.0, 14.0),
    "LAB": (74.0, 22.0),
    "PUB": (30.0, 46.0),
}

# 位置记录与轨迹的分页尺寸
LOCATION_PAGE_SIZE = 10
TRACK_PAGE_SIZE = 10

# 演示区域清单，用于首日灌入演示数据
# （区域名称，楼宇，楼层，区域类型，面积，容纳上限，区域状态）
DEMO_ZONE_LIST = (
    ("住院一部三楼重症病区", "住院一部", 3, "WARD", 320.0, 8, "USE"),
    ("住院一部五楼普通病区", "住院一部", 5, "WARD", 480.0, 10, "USE"),
    ("门诊二楼超声诊区", "门诊楼", 2, "OPD", 260.0, 8, "USE"),
    ("医技楼一层放射检查区", "医技楼", 1, "LAB", 560.0, 10, "USE"),
    ("医技楼二层检验中心", "医技楼", 2, "LAB", 430.0, 8, "USE"),
    ("后勤楼一层设备周转库", "后勤楼", 1, "PUB", 180.0, 20, "IDLE"),
)


def round1(value):
    """数值保留一位小数，空值与非数值按 0 处理。"""
    try:
        return round(float(value), 1)
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


def build_zone_code():
    """生成区域编号，形如 LZ-0001。"""
    return "LZ-%04d" % next_serial("iot_location_zone", "zone_code", "LZ-")


def build_loc_code():
    """生成位置编号，形如 LP-0001。"""
    return "LP-%04d" % next_serial("iot_device_location", "loc_code", "LP-")


def build_track_code():
    """生成轨迹编号，形如 LT-0001。"""
    return "LT-%04d" % next_serial("iot_location_track", "track_code", "LT-")


def parse_datetime_text(text):
    """把 YYYY-MM-DD HH:MM:SS 文本解析为日期时间对象，失败返回 None。"""
    text = (text or "").strip()
    if not text:
        return None
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(text[:19], pattern)
        except (TypeError, ValueError):
            continue
    return None


def calc_distance(pos_a, pos_b):
    """按平面坐标计算两点直线距离（米），坐标缺失时返回 0。"""
    try:
        return round(math.sqrt((float(pos_a[0]) - float(pos_b[0])) ** 2 +
                               (float(pos_a[1]) - float(pos_b[1])) ** 2), 2)
    except (TypeError, ValueError, IndexError):
        return 0.0


def zone_center(zone_row):
    """读取区域中心坐标，缺失时按区域类型回退到默认中心点。"""
    if not zone_row:
        return ZONE_CENTER_MAP["PUB"]
    try:
        pos_x = float(zone_row.get("pos_x") or 0)
        pos_y = float(zone_row.get("pos_y") or 0)
    except (TypeError, ValueError):
        return ZONE_CENTER_MAP.get(zone_row.get("zone_type"), ZONE_CENTER_MAP["PUB"])
    if pos_x or pos_y:
        return (pos_x, pos_y)
    return ZONE_CENTER_MAP.get(zone_row.get("zone_type"), ZONE_CENTER_MAP["PUB"])


# ------------------------------------------------------------------
# 空间区域维护
# ------------------------------------------------------------------
def save_zone(payload):
    """新建或编辑一个空间区域，编号留空表示新建。

    :param payload: 含 zone_code、zone_name、building_name、floor_no、
                    zone_type、area_sqm、device_limit、manager_name、
                    zone_state、remark_text 的字典
    :return: 含 success、message 与 zone_code 的执行结果字典
    """
    zone_name = (payload.get("zone_name") or "").strip()
    if not zone_name:
        return {"success": False, "message": "请填写区域名称"}
    zone_type = (payload.get("zone_type") or "WARD").strip()
    if zone_type not in ZONE_TYPE_DICT:
        return {"success": False, "message": "区域类型 %s 不合法" % zone_type}
    zone_state = (payload.get("zone_state") or "USE").strip()
    if zone_state not in ZONE_STATE_DICT:
        return {"success": False, "message": "区域状态 %s 不合法" % zone_state}
    try:
        floor_no = int(payload.get("floor_no") or 1)
    except (TypeError, ValueError):
        floor_no = 1
    conn = connect_db()
    try:
        exist = None
        code = (payload.get("zone_code") or "").strip()
        if code:
            exist = conn.execute(
                "SELECT zone_code FROM iot_location_zone WHERE zone_code=?",
                (code,)).fetchone()
        if exist is None:
            code = build_zone_code()
            conn.execute(
                "INSERT INTO iot_location_zone (zone_code, zone_name, "
                "building_name, floor_no, zone_type, area_sqm, device_limit, "
                "manager_name, zone_state, remark_text) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (code, zone_name, (payload.get("building_name") or "").strip(),
                 floor_no, zone_type, round1(payload.get("area_sqm") or 0),
                 int(payload.get("device_limit") or 0),
                 (payload.get("manager_name") or "").strip(), zone_state,
                 (payload.get("remark_text") or "").strip()))
        else:
            conn.execute(
                "UPDATE iot_location_zone SET zone_name=?, building_name=?, "
                "floor_no=?, zone_type=?, area_sqm=?, device_limit=?, "
                "manager_name=?, zone_state=?, remark_text=? WHERE zone_code=?",
                (zone_name, (payload.get("building_name") or "").strip(),
                 floor_no, zone_type, round1(payload.get("area_sqm") or 0),
                 int(payload.get("device_limit") or 0),
                 (payload.get("manager_name") or "").strip(), zone_state,
                 (payload.get("remark_text") or "").strip(), code))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "空间区域 %s 已保存" % code,
            "zone_code": code}


def remove_zone(zone_code):
    """删除空间区域，已绑定设备的区域不允许删除。"""
    zone_code = (zone_code or "").strip()
    if not zone_code:
        return {"success": False, "message": "请先选择空间区域"}
    conn = connect_db()
    try:
        used = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_device_location WHERE zone_code=?",
            (zone_code,)).fetchone()["c"]
        if used:
            return {"success": False,
                    "message": "该区域已绑定 %d 台设备，请先迁出设备" % used}
        row = conn.execute(
            "SELECT zone_code FROM iot_location_zone WHERE zone_code=?",
            (zone_code,)).fetchone()
        if row is None:
            return {"success": False, "message": "空间区域不存在"}
        conn.execute(
            "DELETE FROM iot_location_zone WHERE zone_code=?", (zone_code,))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "空间区域 %s 已删除" % zone_code}


def fetch_zone_rows(keyword="", zone_type="", zone_state=""):
    """按条件查询空间区域，并回算各区域已绑定设备台数与容量占用率。"""
    sql = "SELECT * FROM iot_location_zone WHERE 1=1 "
    args = []
    keyword = (keyword or "").strip()
    if keyword:
        sql += "AND (zone_code LIKE ? OR zone_name LIKE ? OR building_name LIKE ?) "
        like = "%%%s%%" % keyword
        args.extend([like, like, like])
    if zone_type:
        sql += "AND zone_type=? "
        args.append(zone_type)
    if zone_state:
        sql += "AND zone_state=? "
        args.append(zone_state)
    conn = connect_db()
    try:
        rows = conn.execute(
            sql + "ORDER BY floor_no ASC, zone_id ASC", args).fetchall()
        stat_rows = conn.execute(
            "SELECT zone_code, COUNT(1) AS c FROM iot_device_location "
            "GROUP BY zone_code").fetchall()
    finally:
        conn.close()
    stat_map = {row["zone_code"]: row["c"] for row in stat_rows}
    records = []
    for item in dict_list(rows):
        used = stat_map.get(item["zone_code"], 0)
        limit = int(item["device_limit"] or 0)
        item["device_amount"] = used
        item["use_rate"] = round1(used * 100.0 / limit) if limit else 0.0
        item["over_flag"] = 1 if limit and used > limit else 0
        item["type_label"] = ZONE_TYPE_DICT.get(item["zone_type"], item["zone_type"])
        item["state_label"] = ZONE_STATE_DICT.get(item["zone_state"], item["zone_state"])
        records.append(item)
    return records


def fetch_zone_row(zone_code):
    """按编号读取一条空间区域，不存在返回 None。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_location_zone WHERE zone_code=?",
            ((zone_code or "").strip(),)).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


# ------------------------------------------------------------------
# 设备位置绑定与调拨
# ------------------------------------------------------------------
def bind_device_location(payload):
    """绑定或变更设备位置，跨区域变更时自动写入一条移动轨迹。

    同一设备只保留一条位置记录；换区时按两区域中心点估算移动距离，并按
    是否首次绑定判定移动类型（新装入库或区域调拨）。区域容量超限时给出
    提示但仍允许绑定，便于演示容量校验告警。

    :param payload: 含 device_code、zone_code、pos_x、pos_y、move_flag、
                    remark_text、operator_name 的字典
    :return: 含 success、message 与 loc_code 的执行结果字典
    """
    device_code = (payload.get("device_code") or "").strip()
    zone_code = (payload.get("zone_code") or "").strip()
    if not device_code:
        return {"success": False, "message": "请先选择设备"}
    if not zone_code:
        return {"success": False, "message": "请先选择目标区域"}
    zone = fetch_zone_row(zone_code)
    if zone is None:
        return {"success": False, "message": "空间区域 %s 不存在" % zone_code}
    conn = connect_db()
    try:
        device = conn.execute(
            "SELECT device_name, run_state, gateway_code, heartbeat_time "
            "FROM iot_device_node WHERE device_code=?", (device_code,)).fetchone()
        if device is None:
            return {"success": False, "message": "设备编号 %s 不存在" % device_code}
        exist = conn.execute(
            "SELECT * FROM iot_device_location WHERE device_code=?",
            (device_code,)).fetchone()
        used = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_device_location WHERE zone_code=?",
            (zone_code,)).fetchone()["c"]
        pos_x = round1(payload.get("pos_x") or 0)
        pos_y = round1(payload.get("pos_y") or 0)
        if not (pos_x or pos_y):
            center = zone_center(zone)
            pos_x, pos_y = center[0], center[1]
        install_date = (payload.get("install_date") or "").strip() or today_text()
        locate_state = judge_locate_state(device["run_state"],
                                          device["heartbeat_time"])
        rssi_value = int(payload.get("rssi_value") or -65)
        if exist is None:
            loc_code = build_loc_code()
            conn.execute(
                "INSERT INTO iot_device_location (loc_code, device_code, "
                "device_name, zone_code, zone_name, pos_x, pos_y, install_date, "
                "move_flag, locate_state, last_seen, rssi_value, remark_text) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (loc_code, device_code, device["device_name"], zone_code,
                 zone["zone_name"], pos_x, pos_y, install_date,
                 1 if payload.get("move_flag") else 0, locate_state,
                 now_text(), rssi_value, (payload.get("remark_text") or "").strip()))
            move_type = "IN"
            from_name = "院外"
        else:
            loc_code = exist["loc_code"]
            old_zone = fetch_zone_row(exist["zone_code"])
            if exist["zone_code"] == zone_code:
                conn.execute(
                    "UPDATE iot_device_location SET pos_x=?, pos_y=?, "
                    "install_date=?, move_flag=?, locate_state=?, last_seen=?, "
                    "rssi_value=?, remark_text=? WHERE loc_code=?",
                    (pos_x, pos_y, install_date,
                     1 if payload.get("move_flag") else 0, locate_state,
                     now_text(), rssi_value,
                     (payload.get("remark_text") or "").strip(), loc_code))
                conn.commit()
                return {"success": True,
                        "message": "设备 %s 位置信息已更新" % device_code,
                        "loc_code": loc_code}
            conn.execute(
                "UPDATE iot_device_location SET zone_code=?, zone_name=?, "
                "pos_x=?, pos_y=?, install_date=?, move_flag=?, locate_state=?, "
                "last_seen=?, rssi_value=?, remark_text=? WHERE loc_code=?",
                (zone_code, zone["zone_name"], pos_x, pos_y, install_date,
                 1 if payload.get("move_flag") else 0, locate_state,
                 now_text(), rssi_value,
                 (payload.get("remark_text") or "").strip(), loc_code))
            move_type = "MOVE"
            from_name = old_zone["zone_name"] if old_zone else (exist["zone_name"] or "")
        distance = 0.0 if move_type == "IN" else calc_distance(
            zone_center(old_zone) if move_type == "MOVE" else (0.0, 0.0),
            zone_center(zone))
        conn.execute(
            "INSERT INTO iot_location_track (track_code, device_code, "
            "device_name, from_zone, to_zone, move_type, distance_m, "
            "move_time, operator_name, remark_text) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (build_track_code(), device_code, device["device_name"], from_name,
             zone["zone_name"], move_type, distance, now_text(),
             (payload.get("operator_name") or "admin").strip(),
             (payload.get("track_note") or "").strip() or
             "区域调拨登记" if move_type == "MOVE" else "新装入库登记"))
        conn.commit()
    finally:
        conn.close()
    message = "设备 %s 已绑定至 %s" % (device_code, zone["zone_name"])
    limit = int(zone["device_limit"] or 0)
    if limit and used + 1 > limit:
        message += "（该区域容量上限 %d 台，当前已超容，请关注）" % limit
    return {"success": True, "message": message, "loc_code": loc_code}


def unbind_device_location(device_code, operator="admin"):
    """设备迁出退库：清空位置绑定并写入一条迁出轨迹。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT * FROM iot_device_location WHERE device_code=?",
            ((device_code or "").strip(),)).fetchone()
        if row is None:
            return {"success": False, "message": "该设备尚未绑定位置"}
        conn.execute(
            "INSERT INTO iot_location_track (track_code, device_code, "
            "device_name, from_zone, to_zone, move_type, distance_m, "
            "move_time, operator_name, remark_text) VALUES (?,?,?,?,?,'OUT',?,?,?,?)",
            (build_track_code(), row["device_code"], row["device_name"],
             row["zone_name"], "设备周转库", 0.0, now_text(),
             (operator or "admin").strip(), "设备迁出退库"))
        conn.execute(
            "DELETE FROM iot_device_location WHERE device_code=?",
            (row["device_code"],))
        conn.commit()
    finally:
        conn.close()
    return {"success": True, "message": "设备 %s 已迁出退库" % row["device_code"]}


def judge_locate_state(run_state, heartbeat_time):
    """按运行状态与心跳新鲜度判定设备定位状态。"""
    locate = STATE_TO_LOCATE.get((run_state or "").strip().upper(), "UNKNOWN")
    if locate != "ONLINE":
        return locate
    stamp = parse_datetime_text(heartbeat_time)
    if stamp is None:
        return "UNKNOWN"
    delta = (datetime.datetime.now() - stamp).total_seconds()
    if delta > HEARTBEAT_FRESH_MINUTES * 60:
        return "OFFLINE"
    return "ONLINE"


def scan_location_state():
    """扫描全部已绑定设备的在位状态并回写，返回本次发生状态变化的条数。"""
    nodes = fetch_device_nodes()
    state_map = {row["device_code"]: row for row in nodes}
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT loc_code, device_code, locate_state, move_flag "
            "FROM iot_device_location").fetchall()
        move_rows = conn.execute(
            "SELECT device_code, MAX(move_time) AS last_move FROM "
            "iot_location_track GROUP BY device_code").fetchall()
        move_map = {row["device_code"]: row["last_move"] for row in move_rows}
        changed = 0
        for row in rows:
            node = state_map.get(row["device_code"])
            if node is None:
                continue
            locate = judge_locate_state(node["run_state"], node["heartbeat_time"])
            # 移动类设备刚完成调拨的一段时间内核实为移动中，稳定后回归在位在线
            if locate == "ONLINE" and row["move_flag"]:
                last_move = parse_datetime_text(
                    move_map.get(row["device_code"], ""))
                if last_move is not None and (
                        datetime.datetime.now() - last_move).total_seconds() \
                        <= MOVE_ACTIVE_MINUTES * 60:
                    locate = "MOVING"
            if locate != row["locate_state"]:
                conn.execute(
                    "UPDATE iot_device_location SET locate_state=?, last_seen=? "
                    "WHERE loc_code=?", (locate, now_text(), row["loc_code"]))
                changed += 1
            elif locate == "ONLINE":
                conn.execute(
                    "UPDATE iot_device_location SET last_seen=? WHERE loc_code=?",
                    (now_text(), row["loc_code"]))
        conn.commit()
    finally:
        conn.close()
    return changed


# ------------------------------------------------------------------
# 查询、统计与图表
# ------------------------------------------------------------------
def fetch_location_rows(keyword="", zone_code="", locate_state="", page=1):
    """按条件分页查询设备位置记录。"""
    sql = "SELECT * FROM iot_device_location WHERE 1=1 "
    args = []
    keyword = (keyword or "").strip()
    if keyword:
        sql += "AND (device_code LIKE ? OR device_name LIKE ?) "
        like = "%%%s%%" % keyword
        args.extend([like, like])
    if zone_code:
        sql += "AND zone_code=? "
        args.append(zone_code)
    if locate_state:
        sql += "AND locate_state=? "
        args.append(locate_state)
    conn = connect_db()
    try:
        total = conn.execute(
            sql.replace("SELECT *", "SELECT COUNT(1) AS c", 1), args).fetchone()["c"]
        page_amount = max(1, (total + LOCATION_PAGE_SIZE - 1) // LOCATION_PAGE_SIZE)
        try:
            page_no = max(1, min(int(page), page_amount))
        except (TypeError, ValueError):
            page_no = 1
        rows = conn.execute(
            sql + "ORDER BY zone_code ASC, loc_id ASC LIMIT ? OFFSET ?",
            args + [LOCATION_PAGE_SIZE, (page_no - 1) * LOCATION_PAGE_SIZE]).fetchall()
    finally:
        conn.close()
    records = []
    for item in dict_list(rows):
        item["state_label"] = LOCATE_STATE_DICT.get(item["locate_state"],
                                                    item["locate_state"])
        item["move_label"] = "移动设备" if item["move_flag"] else "固定设备"
        records.append(item)
    return {"records": records, "total": total, "page": page_no,
            "page_amount": page_amount}


def fetch_track_rows(device_code="", move_type="", limit=20):
    """按条件查询位置变动轨迹，按移动时间倒序。"""
    sql = "SELECT * FROM iot_location_track WHERE 1=1 "
    args = []
    if device_code:
        sql += "AND device_code=? "
        args.append(device_code)
    if move_type:
        sql += "AND move_type=? "
        args.append(move_type)
    conn = connect_db()
    try:
        rows = conn.execute(
            sql + "ORDER BY track_id DESC LIMIT ?", args + [limit]).fetchall()
    finally:
        conn.close()
    records = []
    for item in dict_list(rows):
        item["move_label"] = MOVE_TYPE_DICT.get(item["move_type"], item["move_type"])
        records.append(item)
    return records


def build_location_summary():
    """汇总空间位置核心指标：区域数、设备定位数、在位率、失联数与超容区域数。"""
    scan_location_state()
    zones = fetch_zone_rows()
    conn = connect_db()
    try:
        state_rows = conn.execute(
            "SELECT locate_state, COUNT(1) AS c FROM iot_device_location "
            "GROUP BY locate_state").fetchall()
        located = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_device_location").fetchone()["c"]
        track_amount = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_location_track").fetchone()["c"]
        distance_row = conn.execute(
            "SELECT COALESCE(SUM(distance_m),0) AS s FROM iot_location_track"
        ).fetchone()
    finally:
        conn.close()
    state_counter = {row["locate_state"]: row["c"] for row in state_rows}
    online = state_counter.get("ONLINE", 0) + state_counter.get("MOVING", 0)
    offline = state_counter.get("OFFLINE", 0)
    over_zone = sum(1 for item in zones if item["over_flag"])
    idle_zone = sum(1 for item in zones
                    if item["state_label"] == ZONE_STATE_DICT["IDLE"])
    return {
        "zone_total": len(zones),
        "idle_zone": idle_zone,
        "over_zone": over_zone,
        "located_amount": located,
        "online_amount": online,
        "offline_amount": offline,
        "unknown_amount": state_counter.get("UNKNOWN", 0),
        "online_rate": round1(online * 100.0 / located) if located else 0.0,
        "track_amount": track_amount,
        "move_distance": round1(distance_row["s"]),
    }


def build_location_charts():
    """构建五类图表数据：区域类型分布、定位状态分布、区域设备分布排行、
    楼层设备分布与近 7 日位置变动趋势。"""
    zones = fetch_zone_rows()
    conn = connect_db()
    try:
        loc_rows = conn.execute(
            "SELECT zone_code, zone_name, locate_state FROM "
            "iot_device_location").fetchall()
        track_rows = conn.execute(
            "SELECT move_time, move_type FROM iot_location_track").fetchall()
    finally:
        conn.close()

    type_counter = {}
    for zone in zones:
        type_counter[zone["zone_type"]] = type_counter.get(zone["zone_type"], 0) + 1
    type_distribution = [
        {"name": ZONE_TYPE_DICT.get(code, code), "value": type_counter.get(code, 0)}
        for code in ("WARD", "OPD", "LAB", "PUB")
    ]

    state_counter = {}
    for row in loc_rows:
        state_counter[row["locate_state"]] = state_counter.get(row["locate_state"], 0) + 1
    state_distribution = [
        {"name": LOCATE_STATE_DICT.get(code, code), "value": state_counter.get(code, 0)}
        for code in ("ONLINE", "MOVING", "OFFLINE", "UNKNOWN")
    ]

    zone_rank = [
        {"name": zone["zone_name"], "value": zone["device_amount"],
         "limit": zone["device_limit"], "rate": zone["use_rate"]}
        for zone in zones
    ]
    zone_rank.sort(key=lambda x: x["value"], reverse=True)

    floor_counter = {}
    for zone in zones:
        key = "%s %dF" % (zone["building_name"] or "未分配", zone["floor_no"])
        floor_counter[key] = floor_counter.get(key, 0) + zone["device_amount"]
    floor_distribution = [
        {"name": key, "value": value} for key, value in floor_counter.items()
    ]
    floor_distribution.sort(key=lambda x: x["value"], reverse=True)

    day_keys = []
    cursor = datetime.date.today()
    for _ in range(7):
        day_keys.append(cursor.strftime("%m-%d"))
        cursor = cursor - datetime.timedelta(days=1)
    day_keys.reverse()
    move_counter = {key: 0 for key in day_keys}
    for row in track_rows:
        stamp = parse_datetime_text(row["move_time"])
        if stamp is None:
            continue
        key = stamp.strftime("%m-%d")
        if key in move_counter:
            move_counter[key] += 1

    type_move_counter = {}
    for row in track_rows:
        type_move_counter[row["move_type"]] = type_move_counter.get(row["move_type"], 0) + 1
    move_distribution = [
        {"name": MOVE_TYPE_DICT.get(code, code), "value": type_move_counter.get(code, 0)}
        for code in ("IN", "MOVE", "OUT")
    ]

    return {
        "type_distribution": type_distribution,
        "state_distribution": state_distribution,
        "zone_rank": zone_rank[:8],
        "floor_distribution": floor_distribution,
        "move_distribution": move_distribution,
        "day_trend": {"days": day_keys,
                      "values": [move_counter[key] for key in day_keys]},
    }


def fetch_location_options():
    """空间位置页下拉选项：各类字典项、区域候选与设备候选。"""
    nodes = fetch_device_nodes()
    return {
        "zone_list": [{"code": row["zone_code"],
                       "label": "%s · %s %dF" % (row["zone_name"],
                                                 row["building_name"] or "未分配",
                                                 row["floor_no"])}
                      for row in fetch_zone_rows()],
        "device_list": [{"code": row["device_code"],
                         "label": row["device_code"] + " · " + row["device_name"]}
                        for row in nodes],
        "type_options": [{"code": k, "label": v} for k, v in ZONE_TYPE_DICT.items()],
        "state_options": [{"code": k, "label": v} for k, v in ZONE_STATE_DICT.items()],
        "locate_options": [{"code": k, "label": v} for k, v in LOCATE_STATE_DICT.items()],
        "move_options": [{"code": k, "label": v} for k, v in MOVE_TYPE_DICT.items()],
    }


def build_location_payload(keyword="", zone_code="", locate_state="",
                           track_device="", track_type="", page=1):
    """组装设备空间位置与分布管理页面所需的全部数据。"""
    pager = fetch_location_rows(keyword, zone_code, locate_state, page)
    return {
        "summary": build_location_summary(),
        "charts": build_location_charts(),
        "zones": fetch_zone_rows(),
        "locations": pager["records"],
        "location_total": pager["total"],
        "page": pager["page"],
        "page_amount": pager["page_amount"],
        "tracks": fetch_track_rows(track_device, track_type),
        "options": fetch_location_options(),
        "result": "OK",
    }


def build_location_brief():
    """空间位置看板摘要，供实时监控大屏嵌入。"""
    summary = build_location_summary()
    return {
        "zone_total": summary["zone_total"],
        "located_amount": summary["located_amount"],
        "online_amount": summary["online_amount"],
        "offline_amount": summary["offline_amount"],
        "over_zone": summary["over_zone"],
        "online_rate": summary["online_rate"],
        "track_amount": summary["track_amount"],
    }


# ------------------------------------------------------------------
# 演示数据
# ------------------------------------------------------------------
def seed_demo_location():
    """灌入空间位置演示数据：区域建档、设备位置绑定、调拨轨迹与在位状态。

    :return: 生成的空间区域条数
    """
    conn = connect_db()
    try:
        already = conn.execute(
            "SELECT COUNT(1) AS c FROM iot_location_zone").fetchone()["c"]
        if already:
            return 0
    finally:
        conn.close()
    zone_codes = []
    for zone_name, building, floor_no, zone_type, area, limit, state \
            in DEMO_ZONE_LIST:
        outcome = save_zone({
            "zone_name": zone_name, "building_name": building,
            "floor_no": floor_no, "zone_type": zone_type,
            "area_sqm": area, "device_limit": limit,
            "manager_name": "后勤保障部-郑工", "zone_state": state,
            "remark_text": "演示用空间区域",
        })
        if outcome.get("success"):
            zone_codes.append(outcome["zone_code"])
    if not zone_codes:
        return 0

    nodes = fetch_device_nodes()
    amount = 0
    for index, node in enumerate(nodes):
        # 前 7 个区域轮流承载设备，最后一个区域留作周转库
        zone_code = zone_codes[index % max(1, len(zone_codes) - 1)]
        bind_device_location({
            "device_code": node["device_code"],
            "zone_code": zone_code,
            "pos_x": round1(6.0 + (index % 5) * 8.5),
            "pos_y": round1(4.0 + (index % 3) * 7.5),
            "move_flag": 1 if index % 4 == 3 else 0,
            "remark_text": "演示用设备位置",
            "operator_name": "后勤保障部-郑工",
            "track_note": "新装入库并绑定区域",
        })
        amount = len(zone_codes)

    # 抽取三台设备演示跨区域调拨，形成移动轨迹与距离留痕
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT device_code FROM iot_device_location "
            "ORDER BY loc_id ASC LIMIT 3").fetchall()
    finally:
        conn.close()
    for step, row in enumerate(rows):
        target = zone_codes[(step + 2) % max(1, len(zone_codes) - 1)]
        bind_device_location({
            "device_code": row["device_code"],
            "zone_code": target,
            "pos_x": round1(12.0 + step * 9.5),
            "pos_y": round1(9.0 + step * 6.5),
            "move_flag": 1,
            "remark_text": "演示跨区域调拨",
            "operator_name": "后勤保障部-郑工",
            "track_note": "科室间设备调配",
        })

    # 抽取一台设备演示迁出退库后重新入库周转库，补齐移动类型的完整取值
    conn = connect_db()
    try:
        first_row = conn.execute(
            "SELECT device_code FROM iot_device_location "
            "ORDER BY loc_id DESC LIMIT 1").fetchone()
    finally:
        conn.close()
    if first_row:
        unbind_device_location(first_row["device_code"], "后勤保障部-郑工")
        bind_device_location({
            "device_code": first_row["device_code"],
            "zone_code": zone_codes[-1],
            "pos_x": 15.0, "pos_y": 20.0, "move_flag": 1,
            "remark_text": "退库后转入周转库待分配",
            "operator_name": "后勤保障部-郑工",
            "track_note": "退库检修后重新入库",
        })

    # 把较早的入库轨迹按天铺开，让近 7 日位置变动趋势具备层次
    conn = connect_db()
    try:
        track_rows = conn.execute(
            "SELECT track_code FROM iot_location_track "
            "ORDER BY track_id ASC").fetchall()
        spread = max(0, len(track_rows) - 4)
        for index, row in enumerate(track_rows[:spread]):
            offset = max(1, spread - index)
            stamp = datetime.datetime.now() - datetime.timedelta(days=offset)
            conn.execute(
                "UPDATE iot_location_track SET move_time=? WHERE track_code=?",
                (stamp.strftime("%Y-%m-%d %H:%M:%S"), row["track_code"]))
        conn.commit()
    finally:
        conn.close()
    scan_location_state()
    return amount

# --------------------------------------------------------------------------
# 第 12 天数据表脚本增量：iot_schema.sql（M，已内联为字符串常量 DAY12_SCHEMA_SQL）
# --------------------------------------------------------------------------

DAY12_SCHEMA_SQL = r"""
-- ------------------------------------------------------------------
-- 第 12 天新增：设备空间位置与分布管理
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iot_location_zone (
    zone_id        INTEGER PRIMARY KEY AUTOINCREMENT, -- 区域主键
    zone_code      TEXT    NOT NULL UNIQUE,           -- 区域编号
    zone_name      TEXT    DEFAULT '',               -- 区域名称
    building_name  TEXT    DEFAULT '',               -- 所属楼宇
    floor_no       INTEGER DEFAULT 1,                -- 所在楼层
    zone_type      TEXT    DEFAULT 'WARD',           -- 区域类型（见 ZONE_TYPE_DICT）
    area_sqm       REAL    DEFAULT 0,                -- 区域面积（平方米）
    device_limit   INTEGER DEFAULT 0,                -- 设备容纳上限
    manager_name   TEXT    DEFAULT '',               -- 区域责任人
    zone_state     TEXT    DEFAULT 'USE',            -- 区域状态（见 ZONE_STATE_DICT）
    remark_text    TEXT    DEFAULT '',               -- 备注说明
    created_at     TEXT    DEFAULT (datetime('now', 'localtime'))
);

CREATE TABLE IF NOT EXISTS iot_device_location (
    loc_id         INTEGER PRIMARY KEY AUTOINCREMENT, -- 位置主键
    loc_code       TEXT    NOT NULL UNIQUE,           -- 位置编号
    device_code    TEXT    DEFAULT '',               -- 设备编号
    device_name    TEXT    DEFAULT '',               -- 设备名称快照
    zone_code      TEXT    DEFAULT '',               -- 所在区域编号
    zone_name      TEXT    DEFAULT '',               -- 所在区域名称快照
    pos_x          REAL    DEFAULT 0,                -- 平面横坐标（米）
    pos_y          REAL    DEFAULT 0,                -- 平面纵坐标（米）
    install_date   TEXT    DEFAULT '',               -- 安装到位日期
    move_flag      INTEGER DEFAULT 0,                -- 是否移动类设备（1=是）
    locate_state   TEXT    DEFAULT 'UNKNOWN',        -- 定位状态（见 LOCATE_STATE_DICT）
    last_seen      TEXT    DEFAULT '',               -- 最近在位时间
    rssi_value     INTEGER DEFAULT 0,                -- 定位信号强度（dBm）
    remark_text    TEXT    DEFAULT '',               -- 位置备注
    created_at     TEXT    DEFAULT (datetime('now', 'localtime'))
);

CREATE TABLE IF NOT EXISTS iot_location_track (
    track_id       INTEGER PRIMARY KEY AUTOINCREMENT, -- 轨迹主键
    track_code     TEXT    NOT NULL UNIQUE,           -- 轨迹编号
    device_code    TEXT    DEFAULT '',               -- 设备编号
    device_name    TEXT    DEFAULT '',               -- 设备名称快照
    from_zone      TEXT    DEFAULT '',               -- 迁出区域
    to_zone        TEXT    DEFAULT '',               -- 迁入区域
    move_type      TEXT    DEFAULT 'MOVE',           -- 移动类型（见 MOVE_TYPE_DICT）
    distance_m     REAL    DEFAULT 0,                -- 移动距离（米）
    move_time      TEXT    DEFAULT '',               -- 移动时间
    operator_name  TEXT    DEFAULT '',               -- 登记人
    remark_text    TEXT    DEFAULT '',               -- 移动说明
    created_at     TEXT    DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_lzone_code       ON iot_location_zone (zone_code);
CREATE INDEX IF NOT EXISTS idx_lzone_type       ON iot_location_zone (zone_type);
CREATE INDEX IF NOT EXISTS idx_lzone_state      ON iot_location_zone (zone_state);
CREATE INDEX IF NOT EXISTS idx_dloc_code        ON iot_device_location (loc_code);
CREATE INDEX IF NOT EXISTS idx_dloc_device      ON iot_device_location (device_code);
CREATE INDEX IF NOT EXISTS idx_dloc_zone        ON iot_device_location (zone_code);
CREATE INDEX IF NOT EXISTS idx_dloc_state       ON iot_device_location (locate_state);
CREATE INDEX IF NOT EXISTS idx_ltrack_code      ON iot_location_track (track_code);
CREATE INDEX IF NOT EXISTS idx_ltrack_device    ON iot_location_track (device_code);
CREATE INDEX IF NOT EXISTS idx_ltrack_type      ON iot_location_track (move_type);
"""


def apply_day12_schema(conn):
    """把第 12 天新增的数据表脚本应用到已有数据库。"""
    conn.executescript(DAY12_SCHEMA_SQL)
    conn.commit()
    return True

# --------------------------------------------------------------------------
# 第 12 天主程序新增路由与接口：app.py（仅增量片段）
# --------------------------------------------------------------------------

# 开发日期：2026-11-01（第 12 天 / 共 15 天）
# 模块功能：Flask 主程序。启动时完成建表、演示拓扑初始化、遥测预置、阈值、
#           能耗、远程控制、分组标签、网关管理、采集点位、固件版本与空间位置
#           统计、远程控制、分组标签管理、物联网网关管理、采集点位转发、
#           固件 OTA 升级与设备空间位置十二类页面路由，提供设备注册增删改查、
#           心跳上报、离线扫描识别、实时参数采集存储、动态曲线、阈值扫描与
#           告警处置、能耗分时采集、峰谷电费测算与节能建议、远程指令模板维护、
#           指令下发回执与重发、设备分组维护、入组退组、标签维护与批量打标、

from iot_location import (
    build_location_payload,
    build_location_brief,
    save_zone,
    remove_zone,
    bind_device_location,
    unbind_device_location,
    scan_location_state,
    fetch_zone_rows,
    fetch_track_rows,
    seed_demo_location,
    ZONE_TYPE_DICT,
    ZONE_STATE_DICT,
    LOCATE_STATE_DICT,
    MOVE_TYPE_DICT,
)

# 【函数内插入】def api_monitor():
#    # 第 12 天新增：大屏叠加空间位置摘要，供位置概况卡渲染
#    payload["location_brief"] = build_location_brief()

@app.route("/iot/location")
def page_location():
    """设备空间位置与分布管理页面：空间区域、设备位置与位置变动轨迹。"""
    return render_template(
        "page_location.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        refresh_interval=REFRESH_INTERVAL_SEC,
    )


@app.route("/api/iot/location/panel")
def api_location_panel():
    """空间位置面板数据接口：概览、六类图表、区域、位置明细与变动轨迹。"""
    return jsonify(build_location_payload(
        keyword=request.args.get("keyword", ""),
        zone_code=request.args.get("zone_code", ""),
        locate_state=request.args.get("locate_state", ""),
        track_device=request.args.get("track_device", ""),
        track_type=request.args.get("track_type", ""),
        page=request.args.get("page", 1),
    ))


@app.route("/api/iot/location/zone/save", methods=["POST"])
def api_location_zone_save():
    """空间区域保存接口：新建或编辑楼宇楼层区域与容量上限。"""
    body = request.get_json(silent=True) or {}
    result = save_zone(body)
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/location/zone/remove", methods=["POST"])
def api_location_zone_remove():
    """空间区域删除接口：已绑定设备的区域不允许删除。"""
    body = request.get_json(silent=True) or {}
    result = remove_zone((body.get("zone_code") or "").strip())
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/location/bind", methods=["POST"])
def api_location_bind():
    """设备位置绑定接口：绑定或调拨设备位置并自动写入移动轨迹。"""
    body = request.get_json(silent=True) or {}
    result = bind_device_location(body)
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/location/unbind", methods=["POST"])
def api_location_unbind():
    """设备迁出退库接口：解除位置绑定并写入迁出轨迹。"""
    body = request.get_json(silent=True) or {}
    result = unbind_device_location(
        (body.get("device_code") or "").strip(),
        (body.get("operator") or "admin").strip(),
    )
    result["result"] = "OK" if result.get("success") else "FAIL"
    return jsonify(result)


@app.route("/api/iot/location/scan", methods=["POST"])
def api_location_scan():
    """在位状态扫描接口：按运行状态与心跳新鲜度回写设备定位状态。"""
    return jsonify({"result": "OK", "amount": scan_location_state()})


@app.route("/api/iot/location/options")
def api_location_options():
    """空间位置下拉选项接口：区域类型、区域状态、定位状态与移动类型字典。"""
    return jsonify({
        "result": "OK",
        "type_options": [{"code": k, "label": v} for k, v in ZONE_TYPE_DICT.items()],
        "state_options": [{"code": k, "label": v} for k, v in ZONE_STATE_DICT.items()],
        "locate_options": [{"code": k, "label": v} for k, v in LOCATE_STATE_DICT.items()],
        "move_options": [{"code": k, "label": v} for k, v in MOVE_TYPE_DICT.items()],
        "zone_list": [{"code": row["zone_code"], "label": row["zone_name"]}
                      for row in fetch_zone_rows()],
    })



# 【函数内插入】def bootstrap():
#    seed_demo_location()

# 【函数内插入】if __name__ == "__main__":
#    print("空间位置： http://127.0.0.1:%d/iot/location" % PORT)

# --------------------------------------------------------------------------
# 第 12 天新增/修改的前端资源清单（文件本体见工程 templates/、static/ 目录）
# --------------------------------------------------------------------------

#   [新增] static/iot_location.js           新增 441 行
#   [修改] static/iot_upgrade.js            新增 1 行
#   [修改] templates/iot_base.html          新增 1 行
#   [新增] templates/page_location.html     新增 258 行
