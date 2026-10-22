# 软件全称：医疗设备物联网实时管控平台 V1.0
# 版本：V1.0
# 开发日期：2026-10-22（第 2 天 / 共 15 天）
# 模块功能：Flask 主程序。启动时完成建表、演示拓扑初始化、遥测预置，
#           拉起 MQTT 订阅服务（或模拟采集器），并注册实时监控大屏与
#           设备接入页面路由、设备注册增删改查接口、实时数据 JSON 接口
# 依赖包：Flask==3.0.3、paho-mqtt（可选）
# 启动命令：python app.py （默认 http://127.0.0.1:5003/iot/monitor）

from flask import Flask, render_template, jsonify, redirect, url_for, request
from iot_config import HOST, PORT, DEBUG, SOFTWARE_FULL_NAME, SOFTWARE_VERSION, OFFLINE_TIMEOUT_SEC
from iot_store import (
    setup_storage,
    seed_demo_topology,
    seed_demo_telemetry,
    build_monitor_payload,
    fetch_telemetry_curve,
    fetch_device_nodes,
    fetch_device_by_code,
    count_telemetry_by_device,
)
from iot_mqtt import launch_ingest_service
from iot_device import (
    register_device_node,
    modify_device_node,
    unregister_device_node,
    switch_device_state,
    list_device_registry,
    build_registry_options,
    summarize_registry,
    build_device_topics,
)

app = Flask(__name__)
app.json.ensure_ascii = False

INGEST_WORKER = None
INGEST_MODE = "UNKNOWN"


@app.route("/")
def root_entry():
    """根路径跳转到实时监控大屏。"""
    return redirect(url_for("screen_monitor"))


@app.route("/iot/monitor")
def screen_monitor():
    """物联网实时监控大屏页面（第 12 天补充动态刷新与图表联动优化）。"""
    return render_template(
        "screen_monitor.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        offline_timeout=OFFLINE_TIMEOUT_SEC,
    )


@app.route("/iot/devices")
def page_devices():
    """物联网设备接入管理页面：设备注册、网关绑定、状态切换、注销（第 2 天实现）。"""
    return render_template(
        "page_devices.html",
        software_name=SOFTWARE_FULL_NAME,
        software_version=SOFTWARE_VERSION,
        offline_timeout=OFFLINE_TIMEOUT_SEC,
    )


# ==================================================================
# 以下为第 2 天新增：设备接入注册管理接口
# ==================================================================

@app.route("/api/iot/devices", methods=["GET", "POST"])
def api_device_collection():
    """设备集合接口：GET 查询接入台账，POST 注册新设备。"""
    if request.method == "GET":
        result = list_device_registry(
            keyword=request.args.get("keyword", ""),
            gateway_code=request.args.get("gateway_code", ""),
            run_state=request.args.get("run_state", ""),
        )
        result["result"] = "OK"
        result["summary"] = summarize_registry()
        return jsonify(result)

    payload = request.get_json(silent=True) or request.form.to_dict()
    outcome = register_device_node(payload, operator="admin")
    outcome["result"] = "OK" if outcome.get("success") else "FAIL"
    return jsonify(outcome)


@app.route("/api/iot/devices/<device_code>", methods=["GET", "PUT", "DELETE"])
def api_device_item(device_code):
    """设备单条接口：GET 详情，PUT 修改，DELETE 注销。"""
    if request.method == "GET":
        detail = fetch_device_by_code(device_code)
        if not detail:
            return jsonify({"result": "FAIL", "message": "设备不存在"})
        return jsonify({
            "result": "OK",
            "detail": detail,
            "topics": build_device_topics(device_code, detail["gateway_code"]),
            "telemetry_amount": count_telemetry_by_device(device_code),
        })

    if request.method == "PUT":
        payload = request.get_json(silent=True) or request.form.to_dict()
        outcome = modify_device_node(device_code, payload, operator="admin")
        outcome["result"] = "OK" if outcome.get("success") else "FAIL"
        return jsonify(outcome)

    outcome = unregister_device_node(device_code, operator="admin")
    outcome["result"] = "OK" if outcome.get("success") else "FAIL"
    return jsonify(outcome)


@app.route("/api/iot/devices/<device_code>/state", methods=["POST"])
def api_device_state(device_code):
    """设备运行状态切换接口。"""
    payload = request.get_json(silent=True) or request.form.to_dict()
    outcome = switch_device_state(device_code, payload.get("run_state", ""), operator="admin")
    outcome["result"] = "OK" if outcome.get("success") else "FAIL"
    return jsonify(outcome)


@app.route("/api/iot/registry/options")
def api_registry_options():
    """设备接入下拉选项接口：网关清单与运行状态字典。"""
    return jsonify({"result": "OK", "options": build_registry_options()})


@app.route("/api/iot/monitor")
def api_monitor():
    """监控大屏数据接口：在线/离线统计、状态分布、设备清单、温度曲线。"""
    payload = build_monitor_payload()
    payload["ingest_mode"] = INGEST_MODE
    payload["result"] = "OK"
    return jsonify(payload)


@app.route("/api/iot/curve")
def api_curve():
    """实时曲线接口：按设备编号与指标名返回最近 N 条遥测数据。"""
    from flask import request
    device_code = request.args.get("device_code", "")
    metric_key = request.args.get("metric_key", "temp")
    limit = int(request.args.get("limit", 30))
    if not device_code:
        nodes = fetch_device_nodes()
        device_code = nodes[0]["device_code"] if nodes else ""
    data = fetch_telemetry_curve(device_code, metric_key, limit)
    return jsonify({
        "result": "OK",
        "device_code": device_code,
        "metric_key": metric_key,
        "points": data,
    })


@app.route("/api/iot/health")
def api_health():
    """健康检查接口。"""
    return jsonify({
        "result": "OK",
        "software": SOFTWARE_FULL_NAME,
        "version": SOFTWARE_VERSION,
        "ingest_mode": INGEST_MODE,
    })


def bootstrap():
    """启动引导：建表 → 初始化拓扑 → 预置遥测 → 拉起采集服务。"""
    global INGEST_WORKER, INGEST_MODE
    setup_storage()
    seed_demo_topology()
    seed_demo_telemetry()
    INGEST_WORKER, INGEST_MODE = launch_ingest_service()


if __name__ == "__main__":
    bootstrap()
    print("=" * 62)
    print("%s %s 启动中..." % (SOFTWARE_FULL_NAME, SOFTWARE_VERSION))
    print("数据接入模式：%s" % INGEST_MODE)
    print("访问地址： http://127.0.0.1:%d/iot/monitor" % PORT)
    print("=" * 62)
    app.run(host=HOST, port=PORT, debug=DEBUG, use_reloader=False)
