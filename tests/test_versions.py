"""端到端自测：保管链版本复核（质控按样本编号 + 版本号查询指定提交版本）。

运行：python -m tests.test_versions
"""
import os
import tempfile

_tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_tmp, "test_versions.db")
os.environ["SEED_ON_START"] = "false"

from fastapi.testclient import TestClient  # noqa: E402

from app.db import init_db  # noqa: E402
from app.main import app  # noqa: E402

init_db()
client = TestClient(app)
client.__enter__()  # 触发 lifespan（启动钩子）
API = "/api/v1"
PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def reg(code, collector="张三", seal=None, when="2026-09-30T01:00:00Z"):
    return client.post(f"{API}/samples", json={
        "sample_code": code, "collected_at": when,
        "collector": collector, "seal_no": seal or f"SEAL-{code}"})


def ev(payload):
    return client.post(f"{API}/events", json=payload)


def correction(body):
    return client.post(f"{API}/corrections", json=body)


def batch(body):
    return client.post(f"{API}/transfer-batches", json=body)


def versions(code):
    return client.get(f"{API}/samples/{code}/versions")


def ver(code, n):
    return client.get(f"{API}/samples/{code}/versions/{n}")


def verj(code, n):
    return ver(code, n).json()


def chain(code):
    return client.get(f"{API}/samples/{code}/chain").json()


def standard_chain(code, seal):
    ev({"event_id": f"{code}-T1", "event_type": "transfer", "sample_code": code,
        "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": f"TR-{code}",
        "from_custodian": "张三", "to_custodian": "李四"})
    ev({"event_id": f"{code}-R1", "event_type": "receive", "sample_code": code,
        "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": f"TR-{code}",
        "receiver": "李四", "seal_no": seal})
    ev({"event_id": f"{code}-C1", "event_type": "seal_check", "sample_code": code,
        "occurred_at": "2026-09-30T03:30:00Z", "checked_seal_no": seal, "seal_intact": True})
    ev({"event_id": f"{code}-S1", "event_type": "storage", "sample_code": code,
        "occurred_at": "2026-09-30T04:00:00Z", "custodian": "实验室",
        "storage_location": "冷库A"})


# ---------------------------------------------------------------- 1. 版本 0 与逐事件递增
print("1) 登记即版本 0；每次真正提交单事件版本 +1")
reg("V-BASIC")
r = versions("V-BASIC")
check("版本清单 200", r.status_code == 200, r.text)
j = r.json()
check("仅版本 0", j["total"] == 1 and j["latest_version"] == 0, str(j))
check("版本 0 change=register", j["items"][0]["change"]["type"] == "register", str(j["items"][0]))
s0 = verj("V-BASIC", 0)
check("版本 0 为登记时 pending", s0["status"] == "pending" and s0["timeline"] == [], str(s0))
check("版本 0 当前保管人=采集人", s0["current_custodian"] == "张三")
check("版本 0 未核定", s0["approval"]["approved"] is False and s0["approval"]["approved_at"] is None)
check("版本快照回带当前最新版本号", s0["latest_version"] == 0)
check("chain 响应带 latest_version", chain("V-BASIC")["latest_version"] == 0)

standard_chain("V-BASIC", "SEAL-V-BASIC")
j = versions("V-BASIC").json()
check("4 个事件后共 5 个版本、最新为 4", j["total"] == 5 and j["latest_version"] == 4, str(j["total"]))
check("版本顺序 0..4 且 change 类型正确",
      [it["change"]["type"] for it in j["items"]] == ["register", "event", "event", "event", "event"],
      str([it["change"]["type"] for it in j["items"]]))
check("版本清单逐项引用事件号",
      [it["change"]["event_id"] for it in j["items"][1:]] ==
      ["V-BASIC-T1", "V-BASIC-R1", "V-BASIC-C1", "V-BASIC-S1"], str(j))

# ---------------------------------------------------------------- 2. 历史版本快照
print("2) 指定版本快照复刻当时核验结论/原因/保管人/有效时间线")
s1 = verj("V-BASIC", 1)
check("v1 仅含移交、pending、保管人张三",
      [e["event_id"] for e in s1["timeline"]] == ["V-BASIC-T1"]
      and s1["status"] == "pending" and s1["current_custodian"] == "张三", str(s1))
check("v1 原因含缺签收与缺入库",
      any("缺少签收" in x for x in s1["reasons"]) and any("缺少实验室入库" in x for x in s1["reasons"]),
      str(s1["reasons"]))
s2 = verj("V-BASIC", 2)
check("v2 签收后仅缺入库、保管人李四",
      len(s2["timeline"]) == 2 and s2["status"] == "pending"
      and s2["current_custodian"] == "李四", str(s2))
s4 = verj("V-BASIC", 4)
check("v4 完整链 valid、保管人实验室",
      s4["status"] == "valid" and s4["current_custodian"] == "实验室"
      and len(s4["timeline"]) == 4, str(s4))
check("历史快照的 latest_version 回带最新 4", s1["latest_version"] == 4 and s4["latest_version"] == 4)
check("时间线按发生时间排列（即便提交顺序不同，本例即序）",
      [e["occurred_at"] for e in s4["timeline"]] == sorted(e["occurred_at"] for e in s4["timeline"]))

# ---------------------------------------------------------------- 3. 同号同内容重试 / 失败不产生版本
print("3) 幂等重试与失败操作不产生版本")
same = ev({"event_id": "V-BASIC-T1", "event_type": "transfer", "sample_code": "V-BASIC",
           "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-V-BASIC",
           "from_custodian": "张三", "to_custodian": "李四"})
check("同内容重试 200 幂等", same.status_code == 200 and same.json()["idempotent"] is True)
conflict = ev({"event_id": "V-BASIC-T1", "event_type": "transfer", "sample_code": "V-BASIC",
               "occurred_at": "2026-09-30T02:10:00Z", "transfer_no": "TR-V-BASIC",
               "from_custodian": "张三", "to_custodian": "李四"})
check("异内容 409 冲突", conflict.status_code == 409)
bad = ev({"event_id": "V-BASIC-BAD", "event_type": "transfer", "sample_code": "V-BASIC",
          "occurred_at": "2026-09-30T02:00:00Z", "from_custodian": "张三"})
check("载荷校验失败 422", bad.status_code == 422)
check("失败/重试后版本数不变", versions("V-BASIC").json()["total"] == 5)

# ---------------------------------------------------------------- 4. 补录早发生时间事件不改旧版
print("4) 版本按提交先后记录；补录发生时间更早的事件只进新版本")
ev({"event_id": "V-BASIC-C0", "event_type": "seal_check", "sample_code": "V-BASIC",
    "occurred_at": "2026-09-30T01:30:00Z", "checked_seal_no": "SEAL-V-BASIC", "seal_intact": True})
j = versions("V-BASIC").json()
check("补录后最新版本=5", j["latest_version"] == 5 and j["total"] == 6, str(j))
s1b = verj("V-BASIC", 1)
check("旧版 v1 时间线不被补录改写", [e["event_id"] for e in s1b["timeline"]] == ["V-BASIC-T1"],
      str(s1b["timeline"]))
s5 = verj("V-BASIC", 5)
check("新版 v5 按发生时间含补录事件（排在最前）",
      [e["event_id"] for e in s5["timeline"]] ==
      ["V-BASIC-C0", "V-BASIC-T1", "V-BASIC-R1", "V-BASIC-C1", "V-BASIC-S1"], str(s5["timeline"]))

# ---------------------------------------------------------------- 5. 纠错版本（作废 / 替换）
print("5) 作废/替换纠错各递增一次，旧版保留当时有效集")
reg("V-FIX", seal="SEAL-V-FIX")
ev({"event_id": "V-FIX-T1", "event_type": "transfer", "sample_code": "V-FIX",
    "occurred_at": "2026-09-30T02:00:00Z", "transfer_no": "TR-F",
    "from_custodian": "张三", "to_custodian": "李四"})
ev({"event_id": "V-FIX-R1", "event_type": "receive", "sample_code": "V-FIX",
    "occurred_at": "2026-09-30T03:00:00Z", "transfer_no": "TR-F",
    "receiver": "李四", "seal_no": "WRONG"})
before = versions("V-FIX").json()["latest_version"]
check("作废前链 invalid", verj("V-FIX", before)["status"] == "invalid")
r = correction({"correction_id": "VFIX-1", "sample_code": "V-FIX",
                "target_event_id": "V-FIX-R1", "action": "void",
                "operator": "质控员", "reason": "封签录错，作废"})
check("作废 201", r.status_code == 201, r.text)
v_void = versions("V-FIX").json()["latest_version"]
check("作废产生新版本", v_void == before + 1)
ch = verj("V-FIX", v_void)["change"]
check("作废版本 change=correction_void 且引用纠错号/目标事件",
      ch["type"] == "correction_void" and ch["correction_id"] == "VFIX-1"
      and ch["event_id"] == "V-FIX-R1", str(ch))
sv = verj("V-FIX", v_void)
check("作废版有效时间线剔除目标、superseded 留痕",
      "V-FIX-R1" not in [e["event_id"] for e in sv["timeline"]]
      and [e["event_id"] for e in sv["superseded_events"]] == ["V-FIX-R1"]
      and sv["superseded_events"][0]["superseded_by"] == "VFIX-1"
      and [c["correction_id"] for c in sv["corrections"]] == ["VFIX-1"], str(sv))
check("作废前旧版仍是当时结论（invalid）且无纠错留痕",
      verj("V-FIX", before)["status"] == "invalid"
      and verj("V-FIX", before)["corrections"] == []
      and verj("V-FIX", before)["superseded_events"] == [])

# 同纠错号同内容重试幂等，不新增版本
r = correction({"correction_id": "VFIX-1", "sample_code": "V-FIX",
                "target_event_id": "V-FIX-R1", "action": "void",
                "operator": "质控员", "reason": "封签录错，作废"})
check("纠错重试幂等 200", r.status_code == 200)
check("纠错重试不新增版本", versions("V-FIX").json()["latest_version"] == v_void)

# 替换
ev({"event_id": "V-FIX-C1", "event_type": "seal_check", "sample_code": "V-FIX",
    "occurred_at": "2026-09-30T03:30:00Z", "checked_seal_no": "BAD", "seal_intact": True})
before2 = versions("V-FIX").json()["latest_version"]
r = correction({"correction_id": "VFIX-2", "sample_code": "V-FIX",
                "target_event_id": "V-FIX-C1", "action": "replace",
                "operator": "质控员", "reason": "扫码错误，整体替换",
                "new_event": {"event_id": "V-FIX-C2", "event_type": "seal_check",
                              "occurred_at": "2026-09-30T03:30:00Z",
                              "checked_seal_no": "SEAL-V-FIX", "seal_intact": True}})
check("替换 201", r.status_code == 201, r.text)
v_rep = versions("V-FIX").json()["latest_version"]
check("替换产生新版本且 change 引用新事件号",
      v_rep == before2 + 1
      and verj("V-FIX", v_rep)["change"]["event_id"] == "V-FIX-C2"
      and verj("V-FIX", v_rep)["change"]["type"] == "correction_replace")
sr = verj("V-FIX", v_rep)
check("替换版时间线含新事件并标记 replaces_event_id，原事件进 superseded",
      any(e["event_id"] == "V-FIX-C2" and e["replaces_event_id"] == "V-FIX-C1" for e in sr["timeline"])
      and [e["event_id"] for e in sr["superseded_events"]] == ["V-FIX-R1", "V-FIX-C1"], str(sr))
check("被替换前一版仍含旧事件、不含新事件",
      "V-FIX-C1" in [e["event_id"] for e in verj("V-FIX", before2)["timeline"]]
      and "V-FIX-C2" not in [e["event_id"] for e in verj("V-FIX", before2)["timeline"]])
check("替换失败（新号占用）不产生版本",
      correction({"correction_id": "VFIX-X", "sample_code": "V-FIX",
                  "target_event_id": "V-FIX-T1", "action": "replace",
                  "operator": "质控员", "reason": "试探",
                  "new_event": {"event_id": "V-FIX-C2", "event_type": "seal_check",
                                "occurred_at": "2026-09-30T03:31:00Z",
                                "checked_seal_no": "SEAL-V-FIX", "seal_intact": True}}).status_code == 409
      and versions("V-FIX").json()["latest_version"] == v_rep)

# ---------------------------------------------------------------- 6. 核定版本与核定后旧版可查
print("6) 入库核定产生版本并冻结；核定后旧版仍可查询")
ev({"event_id": "V-FIX-R2", "event_type": "receive", "sample_code": "V-FIX",
    "occurred_at": "2026-09-30T03:40:00Z", "transfer_no": "TR-F",
    "receiver": "李四", "seal_no": "SEAL-V-FIX"})
ev({"event_id": "V-FIX-S1", "event_type": "storage", "sample_code": "V-FIX",
    "occurred_at": "2026-09-30T04:00:00Z", "custodian": "实验室"})
before3 = versions("V-FIX").json()["latest_version"]
r = client.post(f"{API}/samples/V-FIX/approve", json={"approver": "质量主管", "note": "准予入库"})
check("有效链核定 200", r.status_code == 200, r.text)
v_app = versions("V-FIX").json()["latest_version"]
check("核定产生 approval 版本", v_app == before3 + 1
      and verj("V-FIX", v_app)["change"]["type"] == "approval")
sa = verj("V-FIX", v_app)
check("核定版带核定信息",
      sa["approval"]["approved"] is True and sa["approval"]["approver"] == "质量主管"
      and sa["approval"]["note"] == "准予入库" and sa["approval"]["approved_at"] is not None,
      str(sa["approval"]))
check("核定前版本 approval 为空", verj("V-FIX", before3)["approval"]["approved"] is False)
check("重复核定幂等不产生版本",
      client.post(f"{API}/samples/V-FIX/approve", json={"approver": "质量主管"}).status_code == 200
      and versions("V-FIX").json()["latest_version"] == v_app)
check("核定后追加事件 409 且不产生版本",
      ev({"event_id": "V-FIX-LATE", "event_type": "seal_check", "sample_code": "V-FIX",
          "occurred_at": "2026-09-30T05:00:00Z", "checked_seal_no": "SEAL-V-FIX"}).status_code == 409
      and versions("V-FIX").json()["latest_version"] == v_app)
check("核定后各旧版仍可查询", all(ver("V-FIX", n).status_code == 200 for n in range(v_app + 1)))
check("核定后纠错被拒且不产生版本",
      correction({"correction_id": "VFIX-LATE", "sample_code": "V-FIX",
                  "target_event_id": "V-FIX-T1", "action": "void",
                  "operator": "质控员", "reason": "核定后试探"}).status_code == 409
      and versions("V-FIX").json()["latest_version"] == v_app)
check("核定快照版本状态=valid", sa["status"] == "valid")

# ---------------------------------------------------------------- 7. 批量移交版本
print("7) 批量移交中该样本事件使批内各样本各递增一次，整批同提交或同失败")
for code in ("V-B1", "V-B2"):
    reg(code)
body = {"batch_id": "VBAT-1", "occurred_at": "2026-09-30T02:00:00Z",
        "from_custodian": "张三", "to_custodian": "李四",
        "items": [
            {"sample_code": "V-B1", "transfer_no": "TR-VB1", "event_id": "VBE-1"},
            {"sample_code": "V-B2", "transfer_no": "TR-VB2", "event_id": "VBE-2"},
        ]}
r = batch(body)
check("批量提交 201", r.status_code == 201, r.text)
for code, eid in (("V-B1", "VBE-1"), ("V-B2", "VBE-2")):
    jj = versions(code).json()
    check(f"{code} 批次后仅 2 个版本（0、1）", jj["total"] == 2 and jj["latest_version"] == 1, str(jj))
    vv = verj(code, 1)
    check(f"{code} v1=batch_transfer 引用批次与事件",
          vv["change"]["type"] == "batch_transfer" and vv["change"]["batch_id"] == "VBAT-1"
          and vv["change"]["event_id"] == eid, str(vv["change"]))
    check(f"{code} v1 时间线为该份批量移交事件",
          [e["event_id"] for e in vv["timeline"]] == [eid]
          and vv["timeline"][0]["batch_id"] == "VBAT-1", str(vv["timeline"]))

# 同批次号同内容重试（换序）幂等，不新增版本
body["items"] = list(reversed(body["items"]))
check("批次重试 200 幂等", batch(body).status_code == 200)
check("批次重试不新增版本", all(versions(c).json()["total"] == 2 for c in ("V-B1", "V-B2")))

# 整批失败：样本不存在 + 事件号占用 → 受影响样本版本不变、批次不留
bad = {"batch_id": "VBAT-BAD", "occurred_at": "2026-09-30T02:00:00Z",
       "from_custodian": "张三", "to_custodian": "李四",
       "items": [
           {"sample_code": "V-B1", "transfer_no": "TR-X", "event_id": "VBE-9"},
           {"sample_code": "V-NOPE", "transfer_no": "TR-Y", "event_id": "VBE-8"},
       ]}
check("整批失败 4xx", batch(bad).status_code in (409, 422))
check("失败批不增加任何样本版本", all(versions(c).json()["total"] == 2 for c in ("V-B1", "V-B2")))
check("失败批次查不到", client.get(f"{API}/transfer-batches/VBAT-BAD").status_code == 404)
check("失败批次事件号未被占用，可重新使用",
      ev({"event_id": "VBE-9", "event_type": "transfer", "sample_code": "V-B2",
          "occurred_at": "2026-09-30T02:30:00Z", "transfer_no": "TR-9",
          "from_custodian": "张三", "to_custodian": "李四"}).status_code == 201)

# 批内某样本随后作废批量事件：只影响此后版本，v1 仍为有效批量事件
r = correction({"correction_id": "VFIX-B", "sample_code": "V-B1",
                "target_event_id": "VBE-1", "action": "void",
                "operator": "质控员", "reason": "误上车次"})
check("批内作废 201", r.status_code == 201, r.text)
check("作废后 v1 仍显示批量事件有效（旧版不改写）",
      [e["event_id"] for e in verj("V-B1", 1)["timeline"]] == ["VBE-1"])
vv = versions("V-B1").json()
check("作废新增 v2，当前最新 2", vv["latest_version"] == 2
      and verj("V-B1", 2)["timeline"] == [])

# ---------------------------------------------------------------- 8. 未知样本 / 不存在版本明确报错
print("8) 未知样本与不存在的版本明确报错")
check("未知样本版本清单 404 sample_not_found",
      versions("V-NO-SUCH").status_code == 404
      and versions("V-NO-SUCH").json()["error"]["code"] == "sample_not_found")
check("未知样本指定版本 404 sample_not_found",
      ver("V-NO-SUCH", 0).status_code == 404
      and ver("V-NO-SUCH", 0).json()["error"]["code"] == "sample_not_found")
r = ver("V-BASIC", 999)
check("超出版本 404 version_not_found 并回带最新版本",
      r.status_code == 404 and r.json()["error"]["code"] == "version_not_found"
      and r.json()["error"]["details"]["latest_version"]
      == versions("V-BASIC").json()["latest_version"], r.text)
r = ver("V-BASIC", -1)
check("负版本号 404 version_not_found",
      r.status_code == 404 and r.json()["error"]["code"] == "version_not_found", r.text)
r = client.get(f"{API}/samples/V-BASIC/versions/x")
check("非整数版本号 422", r.status_code == 422, r.text)

# ---------------------------------------------------------------- 9. 升级基线（旧库样本以升级时状态为版本 0）
print("9) 升级前样本以升级时状态为版本 0，不推断更早历史")
import sqlite3 as _sqlite3

old_path = os.path.join(_tmp, "test_versions_old.db")
if os.path.exists(old_path):
    os.remove(old_path)
ocon = _sqlite3.connect(old_path)
ocon.executescript("""
CREATE TABLE sample (
  id INTEGER PRIMARY KEY AUTOINCREMENT, sample_code TEXT NOT NULL UNIQUE,
  collected_at TEXT NOT NULL, collector TEXT NOT NULL, seal_no TEXT NOT NULL,
  content_hash TEXT NOT NULL, approved_storage_at TEXT, approved_by TEXT,
  approve_note TEXT, created_at TEXT NOT NULL);
CREATE TABLE custody_event (
  id INTEGER PRIMARY KEY AUTOINCREMENT, sample_id INTEGER NOT NULL,
  event_id TEXT NOT NULL UNIQUE, event_type TEXT NOT NULL, occurred_at TEXT NOT NULL,
  payload_json TEXT NOT NULL, content_hash TEXT NOT NULL, created_at TEXT NOT NULL,
  superseded_by TEXT, replaces_event_id TEXT, batch_id TEXT);
CREATE TABLE event_correction (
  id INTEGER PRIMARY KEY AUTOINCREMENT, correction_id TEXT NOT NULL UNIQUE,
  sample_id INTEGER NOT NULL, target_event_id TEXT NOT NULL, action TEXT NOT NULL,
  operator TEXT NOT NULL, reason TEXT NOT NULL, new_event_id TEXT,
  new_event_row_id INTEGER, request_hash TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE transfer_batch (id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL UNIQUE,
  occurred_at TEXT NOT NULL, from_custodian TEXT NOT NULL, to_custodian TEXT NOT NULL,
  item_count INTEGER NOT NULL, request_hash TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE transfer_batch_item (id INTEGER PRIMARY KEY AUTOINCREMENT, batch_pk INTEGER NOT NULL,
  sample_id INTEGER NOT NULL, sample_code TEXT NOT NULL, transfer_no TEXT NOT NULL,
  event_id TEXT NOT NULL, event_row_id INTEGER NOT NULL, item_index INTEGER NOT NULL);
""")
ocon.execute("INSERT INTO sample VALUES (1,'OLD-V','2026-09-30T01:00:00Z','张三','S-OLD','h',"
             "NULL,NULL,NULL,'2026-09-30T01:05:00Z')")
ocon.execute("INSERT INTO custody_event (id,sample_id,event_id,event_type,occurred_at,payload_json,"
             "content_hash,created_at) VALUES (1,1,'OLD-E1','transfer','2026-09-30T02:00:00Z',"
             "'{\"transfer_no\":\"TR-O\",\"from_custodian\":\"张三\",\"to_custodian\":\"李四\"}',"
             "'h','2026-09-30T02:05:00Z')")
ocon.commit(); ocon.close()

# 用独立连接对旧库执行迁移（不经 HTTP，避免切换全局 DB_PATH）
from app.db import db_session, get_connection  # noqa: E402
import app.config as cfg  # noqa: E402
from pathlib import Path  # noqa: E402
cfg.DB_PATH = Path(old_path)
import app.db as dbmod  # noqa: E402
dbmod.config = cfg
init_db()
with db_session() as conn:
    from app.service import get_sample_version_snapshot, list_sample_versions
    srow = conn.execute("SELECT * FROM sample WHERE sample_code='OLD-V'").fetchone()
    snap = get_sample_version_snapshot(conn, "OLD-V", 0)
    lst = list_sample_versions(conn, "OLD-V")
check("旧样本升级后仅版本 0", lst["total"] == 1 and lst["latest_version"] == 0, str(lst))
check("基线 change=baseline", snap["change"]["type"] == "baseline", str(snap["change"]))
check("基线复刻升级时状态（含事件，未核定）",
      [e["event_id"] for e in snap["timeline"]] == ["OLD-E1"]
      and snap["approval"]["approved"] is False,
      str(snap))

# 升级后新提交从 v1 开始
from app.schemas import EventIn  # noqa: E402
from app.service import create_event  # noqa: E402
with db_session(immediate=True) as conn:
    srow = conn.execute("SELECT * FROM sample WHERE sample_code='OLD-V'").fetchone()
    create_event(conn, EventIn(
        event_id="OLD-NEW", event_type="seal_check", sample_code="OLD-V",
        occurred_at="2026-09-30T01:30:00Z", checked_seal_no="S-OLD", seal_intact=True))
with db_session() as conn:
    lst2 = list_sample_versions(conn, "OLD-V")
    snap0 = get_sample_version_snapshot(conn, "OLD-V", 0)
    snap1 = get_sample_version_snapshot(conn, "OLD-V", 1)
check("升级后新提交 v0→v1", [i["version"] for i in lst2["items"]] == [0, 1], str(lst2))
check("基线版本不被早发生时间补录改写",
      [e["event_id"] for e in snap0["timeline"]] == ["OLD-E1"], str(snap0["timeline"]))
check("v1 含补录事件（按发生时间在前）",
      [e["event_id"] for e in snap1["timeline"]] == ["OLD-NEW", "OLD-E1"], str(snap1["timeline"]))

print(f"\n结果: {PASS} 通过, {FAIL} 失败")
raise SystemExit(1 if FAIL else 0)
