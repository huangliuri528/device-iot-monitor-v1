# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-26（第 6 天 / 共 15 天）
# 模块功能：本文件仅含该软件第 6 天新增与修改的源代码，不含第 1~5 天已有代码
#           本次迭代主题：设备能耗统计与节能建议模块 + 能耗统计页面
# 依赖包：Flask==3.0.3、paho-mqtt（可选，缺失自动降级）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）
# 接入说明：以下内容为当日增量，需与前面各天的代码合并后运行——
#           新增模块按文件名放入工程目录；建表脚本增量追加进对应 .sql；
#           主程序增量片段插入 app.py 中 Flask 实例之后；前端资源按文末清单
#           放入 templates/ 与 static/ 目录


# --------------------------------------------------------------------------
# 第 6 天新增业务模块 1：iot_energy.py（完整源码）
# --------------------------------------------------------------------------

# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-26（第 6 天 / 共 15 天）
# 模块功能：医疗设备能耗统计模块。负责按小时采集在线设备的实时功率并
#           折算电量，形成设备级与病区级的能耗汇总；支持峰谷时段判定与
#           峰谷电量、电费测算，输出 24 小时能耗趋势、病区能耗排名、
#           设备能耗 Top10、能耗等级分布与峰谷电量占比五类图表，并依据
#           待机耗电、夜间待机与高耗能设备生成节能建议
# 依赖包：sqlite3（Python 内置）、datetime（Python 内置）

import datetime

from iot_config import METRIC_FIELD_DICT, DEVICE_RUN_STATE, REFRESH_INTERVAL_SEC
from iot_store import connect_db, dict_list, now_text, fetch_device_nodes

# 电价配置（元/千瓦时）：平段基准价，峰段与谷段按系数折算
ENERGY_BASE_PRICE = 0.68
ENERGY_PRICE_COEFFICIENT = {
    "PEAK": 1.45,     # 峰段电价系数
    "FLAT": 1.00,     # 平段电价系数
    "VALLEY": 0.52,   # 谷段电价系数
}

# 时段类型字典：按用电时间划分峰、平、谷三段
PERIOD_TYPE_DICT = {
    "PEAK": "峰段",
    "FLAT": "平段",
    "VALLEY": "谷段",
}

# 峰平谷时段划分（小时）：峰段 8-11、18-21；平段 7-8、11-18、21-22；其余为谷段
PEAK_HOUR_LIST = [8, 9, 10, 18, 19, 20]
FLAT_HOUR_LIST = [7, 11, 12, 13, 14, 15, 16, 17, 21]

# 能耗等级字典：按单台设备当日用电量划分，用于识别高耗能设备
ENERGY_LEVEL_DICT = {
    "HIGH": "高耗能",
    "MIDDLE": "中等耗能",
    "LOW": "低耗能",
}

# 能耗等级判定阈值（千瓦时/日，由高到低匹配）
ENERGY_LEVEL_THRESHOLD = [
    ("HIGH", 30),
    ("MIDDLE", 10),
]

# 节能建议触发阈值
# 待机设备功率超过该值即认定「待机仍耗电」，建议夜间断电
STANDBY_POWER_THRESHOLD = 40
# 单台设备日用电超过该值即认定「高耗能」，建议错峰运行
HIGH_ENERGY_THRESHOLD = 30
# 夜间（谷段）电量占比低于该值即提示可优化运行排班
VALLEY_RATIO_THRESHOLD = 25

# 趋势图默认展示小时数
TREND_HOUR_AMOUNT = 24


def today_text():
    """返回当前日期文本，格式 YYYY-MM-DD。"""
    return datetime.date.today().strftime("%Y-%m-%d")


def current_hour_tag():
    """返回当前整点小时标签，格式 YYYY-MM-DD HH:00。"""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:00")


def shift_hour_tag(hours):
    """返回相对当前整点偏移指定小时后的小时标签。"""
    return (datetime.datetime.now() + datetime.timedelta(hours=hours)).strftime("%Y-%m-%d %H:00")


def parse_hour_value(hour_tag):
    """从小时标签中取出小时数值，用于峰谷判定。"""
    try:
        return int(str(hour_tag)[11:13])
    except (ValueError, TypeError):
        return datetime.datetime.now().hour


def judge_period_type(hour_value):
    """按小时判定用电时段类型：峰段、平段或谷段。"""
    if hour_value in PEAK_HOUR_LIST:
        return "PEAK", PERIOD_TYPE_DICT["PEAK"]
    if hour_value in FLAT_HOUR_LIST:
        return "FLAT", PERIOD_TYPE_DICT["FLAT"]
    return "VALLEY", PERIOD_TYPE_DICT["VALLEY"]


def calc_period_price(period_code):
    """按时段类型计算实际电价（元/千瓦时）。"""
    return round(ENERGY_BASE_PRICE * ENERGY_PRICE_COEFFICIENT.get(period_code, 1.0), 4)


def judge_energy_level(daily_kwh):
    """按单台设备当日用电量匹配能耗等级编码与名称。"""
    amount = float(daily_kwh or 0)
    for level_code, floor in ENERGY_LEVEL_THRESHOLD:
        if amount >= floor:
            return level_code, ENERGY_LEVEL_DICT[level_code]
    return "LOW", ENERGY_LEVEL_DICT["LOW"]


def fetch_latest_power(device_code):
    """读取某设备最新一条功率采集值（瓦），无数据时返回 0。"""
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT metric_value FROM iot_telemetry_log "
            "WHERE device_code=? AND metric_key='power' "
            "ORDER BY telemetry_id DESC LIMIT 1", (device_code,)
        ).fetchone()
    finally:
        conn.close()
    return float(row["metric_value"]) if row else 0.0


def save_energy_log(device_code, gateway_code, ward_name, hour_tag,
                    power_value, run_state=""):
    """写入或更新某设备某小时的能耗明细，同时折算电量与电费。

    功率取本小时内采集到的最大值，避免被瞬时低值拉低；
    电量按「功率 × 1 小时 / 1000」折算为千瓦时。
    """
    hour_value = parse_hour_value(hour_tag)
    period_code, period_name = judge_period_type(hour_value)
    if run_state and run_state not in DEVICE_RUN_STATE:
        run_state = ""
    price = calc_period_price(period_code)
    power_value = round(float(power_value or 0), 2)
    energy_kwh = round(power_value / 1000.0, 4)
    cost_amount = round(energy_kwh * price, 4)

    conn = connect_db()
    try:
        exist = conn.execute(
            "SELECT log_id, peak_power FROM iot_energy_log "
            "WHERE device_code=? AND stat_hour=?", (device_code, hour_tag)
        ).fetchone()
        if exist:
            conn.execute(
                "UPDATE iot_energy_log SET power_value=?, peak_power=?, "
                "energy_kwh=?, cost_amount=?, period_code=?, run_state=? "
                "WHERE log_id=?",
                (power_value, max(float(exist["peak_power"] or 0), power_value),
                 energy_kwh, cost_amount, period_code, run_state, exist["log_id"]))
        else:
            conn.execute(
                "INSERT INTO iot_energy_log "
                "(device_code, gateway_code, ward_name, stat_hour, stat_date, "
                "power_value, peak_power, energy_kwh, cost_amount, period_code, "
                "run_state) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (device_code, gateway_code, ward_name, hour_tag, str(hour_tag)[:10],
                 power_value, power_value, energy_kwh, cost_amount, period_code,
                 run_state))
        conn.commit()
    finally:
        conn.close()
    return {"device_code": device_code, "energy_kwh": energy_kwh, "cost_amount": cost_amount}


def collect_energy_snapshot():
    """执行一次能耗采集：扫描全部设备当前功率并写入当前小时能耗明细。

    离线与故障设备不再耗电，电量记 0，但保留一条记录以便统计覆盖率。
    """
    nodes = fetch_device_nodes()
    hour_tag = current_hour_tag()
    saved = []
    for node in nodes:
        power = 0.0
        if node["run_state"] in ("ONLINE", "SLEEP"):
            power = fetch_latest_power(node["device_code"])
        if node["run_state"] in ("OFFLINE", "FAULT"):
            power = 0.0
        saved.append(save_energy_log(
            node["device_code"], node["gateway_code"], node["ward_name"],
            hour_tag, power, node["run_state"]))
    refresh_energy_summary(today_text())
    return {
        "hour_tag": hour_tag,
        "device_amount": len(nodes),
        "energy_total": round(sum(x["energy_kwh"] for x in saved), 4),
        "cost_total": round(sum(x["cost_amount"] for x in saved), 4),
        "rows": saved,
    }


def refresh_energy_summary(stat_date=""):
    """按日期重算设备级能耗汇总：运行时长、电量、均功率、峰值与电费。"""
    stat_date = stat_date or today_text()
    conn = connect_db()
    try:
        rows = conn.execute(
            "SELECT device_code, ward_name, COUNT(1) AS hour_amount, "
            "COALESCE(SUM(energy_kwh), 0) AS energy_kwh, "
            "COALESCE(SUM(cost_amount), 0) AS cost_amount, "
            "COALESCE(AVG(power_value), 0) AS avg_power, "
            "COALESCE(MAX(peak_power), 0) AS peak_power "
            "FROM iot_energy_log WHERE stat_date=? "
            "GROUP BY device_code, ward_name", (stat_date,)
        ).fetchall()
        device_rows = conn.execute(
            "SELECT device_code, device_name FROM iot_device_node").fetchall()
    finally:
        conn.close()

    name_map = {r["device_code"]: r["device_name"] for r in device_rows}
    for item in [dict(r) for r in rows]:
        level_code, level_name = judge_energy_level(item["energy_kwh"])
        conn = connect_db()
        try:
            exist = conn.execute(
                "SELECT summary_id FROM iot_energy_summary "
                "WHERE device_code=? AND stat_date=?",
                (item["device_code"], stat_date)).fetchone()
            payload = (
                name_map.get(item["device_code"], item["device_code"]),
                item["ward_name"], item["hour_amount"],
                round(float(item["energy_kwh"]), 4),
                round(float(item["cost_amount"]), 2),
                round(float(item["avg_power"]), 2),
                round(float(item["peak_power"]), 2),
                level_code,
            )
            if exist:
                conn.execute(
                    "UPDATE iot_energy_summary SET device_name=?, ward_name=?, "
                    "run_hours=?, energy_kwh=?, cost_amount=?, avg_power=?, "
                    "peak_power=?, level_code=? WHERE summary_id=?",
                    payload + (exist["summary_id"],))
            else:
                conn.execute(
                    "INSERT INTO iot_energy_summary "
                    "(device_code, device_name, ward_name, stat_date, run_hours, "
                    "energy_kwh, cost_amount, avg_power, peak_power, level_code) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (item["device_code"],) + payload[:2] + (stat_date,) + payload[2:])
            conn.commit()
        finally:
            conn.close()
    return len(rows)


def fetch_energy_summary_rows(stat_date="", ward_name="", level_code=""):
    """读取设备级能耗汇总，附带等级名称与电价信息。"""
    stat_date = stat_date or today_text()
    conn = connect_db()
    try:
        sql = "SELECT * FROM iot_energy_summary WHERE stat_date=?"
        params = [stat_date]
        if ward_name:
            sql += " AND ward_name=?"
            params.append(ward_name)
        sql += " ORDER BY energy_kwh DESC"
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    records = []
    for item in [dict(r) for r in rows]:
        item["level_name"] = ENERGY_LEVEL_DICT.get(item["level_code"], "未分级")
        if level_code and item["level_code"] != level_code:
            continue
        records.append(item)
    return records


def build_energy_charts(stat_date=""):
    """生成能耗统计五类图表：趋势、病区排名、设备 Top10、等级分布、峰谷占比。"""
    stat_date = stat_date or today_text()
    conn = connect_db()
    try:
        hour_rows = conn.execute(
            "SELECT stat_hour, COALESCE(SUM(energy_kwh), 0) AS energy_kwh, "
            "COALESCE(SUM(cost_amount), 0) AS cost_amount, period_code "
            "FROM iot_energy_log WHERE stat_date=? "
            "GROUP BY stat_hour ORDER BY stat_hour", (stat_date,)
        ).fetchall()
        ward_rows = conn.execute(
            "SELECT ward_name, COALESCE(SUM(energy_kwh), 0) AS energy_kwh, "
            "COALESCE(SUM(cost_amount), 0) AS cost_amount "
            "FROM iot_energy_log WHERE stat_date=? "
            "GROUP BY ward_name ORDER BY energy_kwh DESC", (stat_date,)
        ).fetchall()
        period_rows = conn.execute(
            "SELECT period_code, COALESCE(SUM(energy_kwh), 0) AS energy_kwh, "
            "COALESCE(SUM(cost_amount), 0) AS cost_amount "
            "FROM iot_energy_log WHERE stat_date=? GROUP BY period_code",
            (stat_date,)
        ).fetchall()
        summary_rows = conn.execute(
            "SELECT device_code, device_name, ward_name, energy_kwh, cost_amount, "
            "level_code FROM iot_energy_summary WHERE stat_date=? "
            "ORDER BY energy_kwh DESC", (stat_date,)
        ).fetchall()
    finally:
        conn.close()

    hour_map = {r["stat_hour"]: r for r in hour_rows}
    hour_tags = []
    energy_series = []
    cost_series = []
    base = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)
    for offset in range(TREND_HOUR_AMOUNT - 1, -1, -1):
        tag = (base - datetime.timedelta(hours=offset)).strftime("%Y-%m-%d %H:00")
        hit = hour_map.get(tag)
        hour_tags.append(tag[11:16])
        energy_series.append(round(float(hit["energy_kwh"]), 3) if hit else 0.0)
        cost_series.append(round(float(hit["cost_amount"]), 2) if hit else 0.0)

    ward_rank = [{
        "name": r["ward_name"] or "未归属病区",
        "energy": round(float(r["energy_kwh"]), 3),
        "cost": round(float(r["cost_amount"]), 2),
    } for r in ward_rows]

    device_rank = [{
        "name": r["device_name"],
        "code": r["device_code"],
        "ward": r["ward_name"],
        "energy": round(float(r["energy_kwh"]), 3),
        "cost": round(float(r["cost_amount"]), 2),
        "level": ENERGY_LEVEL_DICT.get(r["level_code"], "未分级"),
    } for r in summary_rows[:10]]

    level_counter = {}
    for row in summary_rows:
        level_counter[row["level_code"]] = level_counter.get(row["level_code"], 0) + 1
    level_distribution = [{
        "name": ENERGY_LEVEL_DICT.get(code, "未分级"),
        "value": amount,
    } for code, amount in level_counter.items()]

    period_distribution = [{
        "name": PERIOD_TYPE_DICT.get(r["period_code"], "未分段"),
        "value": round(float(r["energy_kwh"]), 3),
        "cost": round(float(r["cost_amount"]), 2),
    } for r in period_rows]

    return {
        "hour_trend": {
            "categories": hour_tags,
            "energy_series": energy_series,
            "cost_series": cost_series,
        },
        "ward_rank": ward_rank,
        "device_rank": device_rank,
        "level_distribution": level_distribution,
        "period_distribution": period_distribution,
    }


def build_energy_advice(stat_date=""):
    """按当日能耗数据生成节能建议：待机耗电、夜间运行与高耗能设备三类。"""
    stat_date = stat_date or today_text()
    conn = connect_db()
    try:
        standby_rows = conn.execute(
            "SELECT device_code, ward_name, AVG(power_value) AS avg_power "
            "FROM iot_energy_log WHERE stat_date=? AND run_state='SLEEP' "
            "GROUP BY device_code, ward_name", (stat_date,)
        ).fetchall()
        valley_row = conn.execute(
            "SELECT COALESCE(SUM(energy_kwh), 0) AS valley_kwh FROM iot_energy_log "
            "WHERE stat_date=? AND period_code='VALLEY'", (stat_date,)
        ).fetchone()
        total_row = conn.execute(
            "SELECT COALESCE(SUM(energy_kwh), 0) AS total_kwh FROM iot_energy_log "
            "WHERE stat_date=?", (stat_date,)
        ).fetchone()
        high_rows = conn.execute(
            "SELECT device_code, device_name, ward_name, energy_kwh "
            "FROM iot_energy_summary WHERE stat_date=? AND level_code='HIGH' "
            "ORDER BY energy_kwh DESC LIMIT 5", (stat_date,)
        ).fetchall()
    finally:
        conn.close()

    advice_list = []
    for item in [dict(r) for r in standby_rows]:
        avg_power = float(item["avg_power"] or 0)
        if avg_power >= STANDBY_POWER_THRESHOLD:
            advice_list.append({
                "advice_type": "STANDBY",
                "type_label": "待机耗电",
                "target_text": "%s（%s）" % (item["device_code"], item["ward_name"]),
                "advice_text": "待机仍维持 %.0f W 功率，建议夜间集中断电或启用深度休眠" % avg_power,
                "save_hint": "预计每日可节约 %.1f%% 待机电量" % min(90.0, avg_power / 8),
            })

    total_kwh = float(total_row["total_kwh"]) if total_row else 0.0
    valley_kwh = float(valley_row["valley_kwh"]) if valley_row else 0.0
    valley_ratio = round(valley_kwh / total_kwh * 100, 1) if total_kwh else 0.0
    if total_kwh > 0 and valley_ratio < VALLEY_RATIO_THRESHOLD:
        advice_list.append({
            "advice_type": "SCHEDULE",
            "type_label": "排班优化",
            "target_text": "全院设备",
            "advice_text": "谷段电量占比仅 %.1f%%，建议将非紧急检查挪至谷段运行以降电费" % valley_ratio,
            "save_hint": "谷段电价为平段的 52%%，错峰可显著压降电费",
        })

    for item in [dict(r) for r in high_rows]:
        advice_list.append({
            "advice_type": "HIGH",
            "type_label": "高耗能设备",
            "target_text": "%s（%s）" % (item["device_name"], item["ward_name"]),
            "advice_text": "当日用电 %.1f kWh，超过高耗能阈值 %d kWh，建议核查运行工况" % (
                float(item["energy_kwh"]), HIGH_ENERGY_THRESHOLD),
            "save_hint": "可优先安排维护与参数优化",
        })

    if not advice_list:
        advice_list.append({
            "advice_type": "NORMAL",
            "type_label": "运行正常",
            "target_text": "全院设备",
            "advice_text": "当日能耗处于合理区间，暂无需处置的节能项",
            "save_hint": "继续保持现有运行策略",
        })
    return {"advice_list": advice_list, "valley_ratio": valley_ratio,
            "advice_amount": len(advice_list)}


def build_energy_payload(stat_date="", ward_name="", level_code=""):
    """组装能耗统计页面数据：汇总指标、五类图表、设备明细与节能建议。"""
    stat_date = stat_date or today_text()
    records = fetch_energy_summary_rows(stat_date, ward_name, level_code)
    conn = connect_db()
    try:
        total_row = conn.execute(
            "SELECT COUNT(DISTINCT device_code) AS device_amount, "
            "COALESCE(SUM(energy_kwh), 0) AS energy_total, "
            "COALESCE(SUM(cost_amount), 0) AS cost_total, "
            "COALESCE(MAX(peak_power), 0) AS peak_power "
            "FROM iot_energy_log WHERE stat_date=?", (stat_date,)
        ).fetchone()
        ward_rows = conn.execute(
            "SELECT DISTINCT ward_name FROM iot_energy_summary "
            "WHERE ward_name<>'' ORDER BY ward_name").fetchall()
        date_rows = conn.execute(
            "SELECT DISTINCT stat_date FROM iot_energy_summary "
            "ORDER BY stat_date DESC LIMIT 7").fetchall()
    finally:
        conn.close()

    item = dict(total_row) if total_row else {}
    energy_total = round(float(item.get("energy_total") or 0), 3)
    summary = {
        "stat_date": stat_date,
        "device_amount": int(item.get("device_amount") or 0),
        "energy_total": energy_total,
        "cost_total": round(float(item.get("cost_total") or 0), 2),
        "peak_power": round(float(item.get("peak_power") or 0), 2),
        "avg_power": round(energy_total * 1000 / 24, 2) if energy_total else 0.0,
    }

    return {
        "summary": summary,
        "charts": build_energy_charts(stat_date),
        "advice": build_energy_advice(stat_date),
        "records": records,
        "row_amount": len(records),
        "ward_options": [r["ward_name"] for r in ward_rows],
        "date_options": [r["stat_date"] for r in date_rows] or [stat_date],
        "level_options": [{"code": k, "label": v} for k, v in ENERGY_LEVEL_DICT.items()],
        "period_options": [{"code": k, "label": v, "price": calc_period_price(k)}
                           for k, v in PERIOD_TYPE_DICT.items()],
        "refresh_interval": REFRESH_INTERVAL_SEC,
        "metric_label": METRIC_FIELD_DICT.get("power", "实时功率"),
    }


def build_energy_brief():
    """生成大屏用能耗概览：当日总电量、电费与高耗能台数。"""
    stat_date = today_text()
    conn = connect_db()
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(energy_kwh), 0) AS energy_total, "
            "COALESCE(SUM(cost_amount), 0) AS cost_total "
            "FROM iot_energy_log WHERE stat_date=?", (stat_date,)
        ).fetchone()
        high_row = conn.execute(
            "SELECT COUNT(1) AS amount FROM iot_energy_summary "
            "WHERE stat_date=? AND level_code='HIGH'", (stat_date,)
        ).fetchone()
    finally:
        conn.close()
    item = dict(row) if row else {}
    return {
        "stat_date": stat_date,
        "energy_total": round(float(item.get("energy_total") or 0), 3),
        "cost_total": round(float(item.get("cost_total") or 0), 2),
        "high_amount": int(high_row["amount"]) if high_row else 0,
    }


def seed_demo_energy():
    """灌入演示能耗数据：为每台设备生成近 24 小时的能耗明细与当日汇总。"""
    conn = connect_db()
    try:
        exist = conn.execute(
            "SELECT COUNT(1) AS amount FROM iot_energy_log").fetchone()["amount"]
        if exist > 0:
            return 0
    finally:
        conn.close()

    nodes = fetch_device_nodes()
    if not nodes:
        return 0

    base_hour = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)
    amount = 0
    for index, node in enumerate(nodes):
        # 额定功率按设备序号差异化（900~2800W），保证能耗排名与等级分布有梯度
        rated_power = 900 + (index * 317) % 1900
        for offset in range(TREND_HOUR_AMOUNT - 1, -1, -1):
            hour_time = base_hour - datetime.timedelta(hours=offset)
            hour_tag = hour_time.strftime("%Y-%m-%d %H:00")
            hour_value = hour_time.hour
            period_code, _ = judge_period_type(hour_value)
            # 夜间与谷段功率下降，峰段满负荷，模拟真实用能曲线
            if period_code == "VALLEY":
                power = rated_power * 0.42
            elif period_code == "PEAK":
                power = rated_power * 0.96
            else:
                power = rated_power * 0.72
            run_state = node["run_state"]
            if node["run_state"] == "SLEEP":
                power = rated_power * 0.16
            elif node["run_state"] in ("OFFLINE", "FAULT"):
                power = 0
            elif index == 2 and period_code == "VALLEY":
                # 演示样本：该设备夜间进入待机，但待机仍有一定功率消耗
                power = rated_power * 0.16
                run_state = "SLEEP"
            power = round(power + (index * 7 + offset * 3) % 21, 2)
            save_energy_log(node["device_code"], node["gateway_code"],
                            node["ward_name"], hour_tag, power, run_state)
            amount += 1
    refresh_energy_summary(today_text())
    return amount

# --------------------------------------------------------------------------
# 第 6 天数据表脚本增量：iot_schema.sql（M，已内联为字符串常量 DAY6_SCHEMA_SQL）
# --------------------------------------------------------------------------

DAY6_SCHEMA_SQL = r"""
-- ==================================================================
-- 第 6 天新增：设备能耗分时明细表
-- ==================================================================
-- 能耗明细表：按「一台设备一个小时一条」记录实时功率、折算电量与电费，
-- 并标记所属峰谷时段与当时运行状态，支撑 24 小时能耗趋势与峰谷电费测算
CREATE TABLE IF NOT EXISTS iot_energy_log (
    log_id       INTEGER PRIMARY KEY AUTOINCREMENT,   -- 能耗明细主键
    device_code  TEXT    NOT NULL,                   -- 设备编号
    gateway_code TEXT    DEFAULT '',                 -- 网关编号
    ward_name    TEXT    DEFAULT '',                 -- 所在病区快照
    stat_hour    TEXT    NOT NULL,                   -- 统计小时（YYYY-MM-DD HH:00）
    stat_date    TEXT    DEFAULT '',                 -- 统计日期
    power_value  REAL    DEFAULT 0,                  -- 该小时实时功率（瓦）
    peak_power   REAL    DEFAULT 0,                  -- 该小时功率峰值（瓦）
    energy_kwh   REAL    DEFAULT 0,                  -- 折算电量（千瓦时）
    cost_amount  REAL    DEFAULT 0,                  -- 折算电费（元）
    period_code  TEXT    DEFAULT 'FLAT',             -- 峰谷时段（见 PERIOD_TYPE_DICT）
    run_state    TEXT    DEFAULT '',                 -- 当时运行状态
    collect_time TEXT    DEFAULT (datetime('now', 'localtime'))
);

-- ==================================================================
-- 第 6 天新增：设备能耗日汇总表
-- ==================================================================
-- 能耗汇总表：按「一台设备一天一条」汇总运行小时数、日用电量、日电费、
-- 平均功率与功率峰值，并标注能耗等级，支撑病区排名与节能建议生成
CREATE TABLE IF NOT EXISTS iot_energy_summary (
    summary_id   INTEGER PRIMARY KEY AUTOINCREMENT,  -- 汇总主键
    device_code  TEXT    NOT NULL,                   -- 设备编号
    device_name  TEXT    DEFAULT '',                 -- 设备名称
    ward_name    TEXT    DEFAULT '',                 -- 所在病区
    stat_date    TEXT    NOT NULL,                   -- 统计日期
    run_hours    INTEGER DEFAULT 0,                  -- 有效运行小时数
    energy_kwh   REAL    DEFAULT 0,                  -- 当日用电量（千瓦时）
    cost_amount  REAL    DEFAULT 0,                  -- 当日电费（元）
    avg_power    REAL    DEFAULT 0,                  -- 平均功率（瓦）
    peak_power   REAL    DEFAULT 0,                  -- 峰值功率（瓦）
    level_code   TEXT    DEFAULT 'LOW',              -- 能耗等级（见 ENERGY_LEVEL_DICT）
    updated_at   TEXT    DEFAULT (datetime('now', 'localtime')),
    UNIQUE (device_code, stat_date)
);

CREATE INDEX IF NOT EXISTS idx_energy_device ON iot_energy_log (device_code);
CREATE INDEX IF NOT EXISTS idx_energy_hour   ON iot_energy_log (stat_hour);
CREATE INDEX IF NOT EXISTS idx_energy_date   ON iot_energy_log (stat_date);
CREATE INDEX IF NOT EXISTS idx_energy_period ON iot_energy_log (period_code);
CREATE INDEX IF NOT EXISTS idx_esummary_date ON iot_energy_summary (stat_date);
CREATE INDEX IF NOT EXISTS idx_esummary_ward ON iot_energy_summary (ward_name);
"""


def apply_day6_schema(conn):
    """把第 6 天新增的数据表脚本应用到已有数据库。"""
    conn.executescript(DAY6_SCHEMA_SQL)
    conn.commit()
    return True

# --------------------------------------------------------------------------
# 第 6 天主程序新增路由与接口：app.py（仅增量片段）
# --------------------------------------------------------------------------

# 开发日期：2026-10-26（第 6 天 / 共 15 天）
#           阈值与能耗演示数据灌入、拉起 MQTT 订阅服务（或模拟采集器），
#           并注册实时监控大屏、设备接入、在线状态、实时参数采集、阈值
#           告警中心与能耗统计六类页面路由，提供设备注册增删改查、心跳
#           上报、离线扫描识别、实时参数采集存储、动态曲线、阈值扫描与
#           告警处置、能耗分时采集、峰谷电费测算与节能建议 JSON 接口

from iot_energy import (
    build_energy_payload,
    build_energy_brief,
    collect_energy_snapshot,
    refresh_energy_summary,
    seed_demo_energy,
    ENERGY_LEVEL_DICT,
    PERIOD_TYPE_DICT,
)

# 【函数内插入】def api_monitor():
#    payload["energy_brief"] = build_energy_brief()

# ==================================================================
# 以下为第 6 天新增：设备能耗统计接口
# ==================================================================


@app.route("/iot/energy")
def page_energy():
    """设备能耗统计页面：能耗概览、五图表、设备明细与节能建议。"""
    return render_template(
        "page_energy.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        refresh_interval=REFRESH_INTERVAL_SEC,
    )


@app.route("/api/iot/energy/panel")
def api_energy_panel():
    """能耗统计数据接口：汇总指标、五类图表、设备明细与节能建议。"""
    payload = build_energy_payload(
        stat_date=(request.args.get("stat_date") or "").strip(),
        ward_name=(request.args.get("ward_name") or "").strip(),
        level_code=(request.args.get("level_code") or "").strip(),
    )
    payload["result"] = "OK"
    return jsonify(payload)


@app.route("/api/iot/energy/collect", methods=["POST"])
def api_energy_collect():
    """能耗采集接口：扫描全部设备当前功率并写入当前小时能耗明细。"""
    snapshot = collect_energy_snapshot()
    return jsonify({
        "result": "OK",
        "success": True,
        "device_amount": snapshot["device_amount"],
        "energy_total": snapshot["energy_total"],
        "cost_total": snapshot["cost_total"],
        "message": "已采集 %d 台设备，本小时电量 %.3f kWh、电费 %.2f 元"
                   % (snapshot["device_amount"], snapshot["energy_total"],
                      snapshot["cost_total"]),
    })


@app.route("/api/iot/energy/refresh", methods=["POST"])
def api_energy_refresh():
    """能耗汇总重算接口：按日期重算设备级能耗汇总。"""
    body = request.get_json(silent=True) or {}
    stat_date = (body.get("stat_date") or "").strip()
    amount = refresh_energy_summary(stat_date)
    return jsonify({
        "result": "OK",
        "success": True,
        "message": "已重算 %s 的能耗汇总，覆盖 %d 台设备"
                   % (stat_date or "今日", amount),
    })


@app.route("/api/iot/energy/options")
def api_energy_options():
    """能耗页下拉选项接口：能耗等级、峰谷时段与对应电价。"""
    return jsonify({
        "result": "OK",
        "level_options": [{"code": k, "label": v} for k, v in ENERGY_LEVEL_DICT.items()],
        "period_options": [{"code": k, "label": v} for k, v in PERIOD_TYPE_DICT.items()],
    })


@app.route("/api/iot/energy/brief")
def api_energy_brief():
    """能耗概览接口：当日总电量、电费与高耗能台数，供大屏卡片使用。"""
    return jsonify({"result": "OK", "energy_brief": build_energy_brief()})



# 【函数内插入】def bootstrap():
#    seed_demo_energy()

# 【函数内插入】if __name__ == "__main__":
#    print("能耗统计： http://127.0.0.1:%d/iot/energy" % PORT)

# --------------------------------------------------------------------------
# 第 6 天新增/修改的前端资源清单（文件本体见工程 templates/、static/ 目录）
# --------------------------------------------------------------------------

#   [新增] static/iot_energy.js             新增 285 行
#   [修改] static/iot_screen.css            新增 57 行
#   [修改] templates/iot_base.html          新增 1 行
#   [新增] templates/page_energy.html       新增 100 行
