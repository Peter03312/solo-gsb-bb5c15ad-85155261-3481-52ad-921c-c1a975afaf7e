"""领域服务：登记、事件写入（幂等/冲突）、保管链重算、事件纠错与入库核定。

核验规则（与 README 一致）：
  * 移交必须由当前保管人发起；移交至签收期间不得再次移交；
  * 签收必须匹配移交单号、接收人、封签号，且发生在移交之后；重复签收无效；
  * 封签检查封签号不符或封签破损 → 无效；
  * 缺签收 / 缺入库 → 待补证；
  * 断链、重复签收、封签不符 → 无效，无效优先于待补证；
  * 乱序补录按发生时间排序后整体重算；
  * 纠错（作废/替换）仅核定前可提交，只作用于当前有效版本，原记录与纠错关系永久留痕；
    核验仅纳入有效版本，被替换记录可经链查询追溯；
  * 仅有效链可核定入库；核定后历史冻结。
"""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from . import config
from .schemas import ApproveIn, CorrectionIn, EventIn, NewEventIn, SampleIn, TransferBatchIn

LAB_CUSTODIAN = "实验室"

EVENT_TYPES = ("transfer", "receive", "seal_check", "storage")

# 可选参数“未提供”哨兵（区别于显式传入 None）
_UNSET = object()


class DomainError(Exception):
    def __init__(self, status_code: int, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details or {}


# --------------------------------------------------------------------------- 通用工具

def to_utc_iso(dt: datetime) -> str:
    """统一规范为 UTC ISO8601（秒精度、Z 结尾），无时区输入按 UTC 处理。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _fingerprint(obj: dict[str, Any]) -> str:
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_sample_or_404(conn: sqlite3.Connection, sample_code: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM sample WHERE sample_code = ?", (sample_code,)).fetchone()
    if row is None:
        raise DomainError(404, "sample_not_found", f"样本不存在: {sample_code}")
    return row


def ensure_unlocked(sample: sqlite3.Row) -> None:
    if config.LOCK_AFTER_APPROVAL and sample["approved_storage_at"]:
        raise DomainError(
            409,
            "history_locked",
            f"样本 {sample['sample_code']} 已核定入库（{sample['approved_storage_at']}），历史记录冻结，禁止改写",
        )


def evaluation_summary(result: dict[str, Any], sample: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    """从 evaluate 结果与样本行抽取对外的核验摘要（列表/批次等多处复用）。"""
    return {
        "sample_code": result["sample_code"],
        "status": result["status"],
        "reasons": result["reasons"],
        "current_custodian": result["current_custodian"],
        "pending_transfer": result["pending_transfer"],
        "has_storage": result["has_storage"],
        "event_count": result["event_count"],
        "collected_at": sample["collected_at"],
        "collector": sample["collector"],
        "seal_no": sample["seal_no"],
        "approved_storage_at": sample["approved_storage_at"],
        "approved_by": sample["approved_by"],
    }


# --------------------------------------------------------------------------- 保管链版本

# 版本变更类型 → 中文说明（版本清单/快照的 change 字段）
CHANGE_LABELS: dict[str, str] = {
    "register": "登记（版本0）",
    "baseline": "升级基线（版本0）",
    "event": "单事件提交",
    "batch_transfer": "批量移交中的该样本事件",
    "correction_void": "作废纠错",
    "correction_replace": "替换纠错",
    "approval": "入库核定",
}


def _row_tips(conn: sqlite3.Connection, sample_id: int) -> tuple[int, int]:
    """该样本当前 custody_event / event_correction 的最大行号（版本水位）。"""
    erow = conn.execute(
        "SELECT COALESCE(MAX(id),0) FROM custody_event WHERE sample_id = ?", (sample_id,)
    ).fetchone()[0]
    crow = conn.execute(
        "SELECT COALESCE(MAX(id),0) FROM event_correction WHERE sample_id = ?", (sample_id,)
    ).fetchone()[0]
    return int(erow), int(crow)


def insert_sample_version(
    conn: sqlite3.Connection,
    sample: sqlite3.Row | dict[str, Any],
    change_type: str,
    *,
    ref_event_id: str | None = None,
    ref_batch_id: str | None = None,
    ref_correction_id: str | None = None,
    created_at: str | None = None,
    approved_storage_at: Any = _UNSET,
    approved_by: Any = _UNSET,
) -> None:
    """在写事务内追加一个样本版本。版本号 = 该样本已有最大版本 +1（0 起）。

    必须与产生变更的写入处于同一事务：整批各样本的版本随批次一起提交或一起回滚；
    重试（幂等）与失败路径不得调用本函数，故不产生版本。
    """
    from .db import utcnow_iso

    sample_id = sample["id"]
    latest = conn.execute(
        "SELECT COALESCE(MAX(version), -1) FROM sample_version WHERE sample_id = ?", (sample_id,)
    ).fetchone()[0]
    event_tip, correction_tip = _row_tips(conn, sample_id)
    conn.execute(
        "INSERT INTO sample_version"
        " (sample_id, version, change_type, event_tip, correction_tip,"
        "  ref_event_id, ref_batch_id, ref_correction_id,"
        "  approved_storage_at, approved_by, created_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            sample_id,
            int(latest) + 1,
            change_type,
            event_tip,
            correction_tip,
            ref_event_id,
            ref_batch_id,
            ref_correction_id,
            sample["approved_storage_at"] if approved_storage_at is _UNSET else approved_storage_at,
            sample["approved_by"] if approved_by is _UNSET else approved_by,
            created_at or utcnow_iso(),
        ),
    )


def get_latest_version_no(conn: sqlite3.Connection, sample_id: int) -> int:
    row = conn.execute(
        "SELECT MAX(version) FROM sample_version WHERE sample_id = ?", (sample_id,)
    ).fetchone()[0]
    return int(row)  # 任何现存样本至少有版本 0


def _change_dict(vrow: sqlite3.Row) -> dict[str, Any]:
    return {
        "type": vrow["change_type"],
        "label": CHANGE_LABELS.get(vrow["change_type"], vrow["change_type"]),
        "event_id": vrow["ref_event_id"],
        "batch_id": vrow["ref_batch_id"],
        "correction_id": vrow["ref_correction_id"],
        "committed_at": vrow["created_at"],
    }


def _evaluate_version(conn: sqlite3.Connection, sample: sqlite3.Row, vrow: sqlite3.Row) -> dict[str, Any]:
    """按版本水位复刻该版本提交时点的有效事件/纠错集并重算。"""
    event_rows = conn.execute(
        "SELECT * FROM custody_event WHERE sample_id = ? AND id <= ? ORDER BY occurred_at ASC, id ASC",
        (sample["id"], vrow["event_tip"]),
    ).fetchall()
    corr_rows = conn.execute(
        "SELECT * FROM event_correction WHERE sample_id = ? AND id <= ? ORDER BY id ASC",
        (sample["id"], vrow["correction_tip"]),
    ).fetchall()
    return _evaluate_state(
        dict(sample),
        event_rows,
        corr_rows,
        approved_storage_at=vrow["approved_storage_at"],
    )


def get_sample_version_snapshot(
    conn: sqlite3.Connection, sample_code: str, version: int
) -> dict[str, Any]:
    """指定提交版本的保管链复核快照：当时核验结论/原因/当前保管人/有效时间线/纠错/核定。"""
    sample = get_sample_or_404(conn, sample_code)
    vrow = conn.execute(
        "SELECT * FROM sample_version WHERE sample_id = ? AND version = ?",
        (sample["id"], version),
    ).fetchone()
    if vrow is None:
        latest = get_latest_version_no(conn, sample["id"])
        raise DomainError(
            404,
            "version_not_found",
            f"样本 {sample_code} 版本 {version} 不存在（当前最新版本为 {latest}）",
            {"sample_code": sample_code, "requested_version": version, "latest_version": latest},
        )

    result = _evaluate_version(conn, sample, vrow)
    # 核定备注不随链重算变化，随首个核定版本快照；此处从样本行取核定备注用于展示
    approve_note = sample["approve_note"] if vrow["approved_storage_at"] else None
    return {
        "sample": {
            "sample_code": sample["sample_code"],
            "collected_at": sample["collected_at"],
            "collector": sample["collector"],
            "seal_no": sample["seal_no"],
        },
        "version": version,
        "latest_version": get_latest_version_no(conn, sample["id"]),
        "change": _change_dict(vrow),
        "committed_at": vrow["created_at"],
        "status": result["status"],
        "reasons": result["reasons"],
        "current_custodian": result["current_custodian"],
        "pending_transfer": result["pending_transfer"],
        "has_storage": result["has_storage"],
        "timeline": result["timeline"],
        "corrections": result["corrections"],
        "superseded_events": result["superseded_events"],
        "approval": {
            "approved": vrow["approved_storage_at"] is not None,
            "approved_at": vrow["approved_storage_at"],
            "approver": vrow["approved_by"],
            "note": approve_note,
        },
    }


def list_sample_versions(conn: sqlite3.Connection, sample_code: str) -> dict[str, Any]:
    """版本清单：按版本号（提交先后）给出每次变更类型、引用对象与当时结论。"""
    sample = get_sample_or_404(conn, sample_code)
    vrows = conn.execute(
        "SELECT * FROM sample_version WHERE sample_id = ? ORDER BY version ASC", (sample["id"],)
    ).fetchall()
    items: list[dict[str, Any]] = []
    for v in vrows:
        result = _evaluate_version(conn, sample, v)
        items.append({
            "version": v["version"],
            "change": _change_dict(v),
            "committed_at": v["created_at"],
            "status": result["status"],
            "current_custodian": result["current_custodian"],
            "approved": v["approved_storage_at"] is not None,
            "approved_storage_at": v["approved_storage_at"],
        })
    return {
        "sample_code": sample_code,
        "latest_version": items[-1]["version"] if items else 0,
        "total": len(items),
        "items": items,
    }


# --------------------------------------------------------------------------- 样本登记

def register_sample(conn: sqlite3.Connection, data: SampleIn) -> tuple[sqlite3.Row, bool]:
    """返回 (样本行, 是否幂等命中已有记录)。"""
    canonical = {
        "sample_code": data.sample_code,
        "collected_at": to_utc_iso(data.collected_at),
        "collector": data.collector,
        "seal_no": data.seal_no,
    }
    h = _fingerprint(canonical)
    existing = conn.execute("SELECT * FROM sample WHERE sample_code = ?", (data.sample_code,)).fetchone()
    if existing is not None:
        if existing["content_hash"] != h:
            raise DomainError(
                409,
                "sample_conflict",
                f"样本编号 {data.sample_code} 已登记但内容不一致（同编号异内容冲突）",
                {"existing_content_hash": existing["content_hash"], "incoming_content_hash": h},
            )
        return existing, True

    from .db import utcnow_iso

    created_at = utcnow_iso()
    cur = conn.execute(
        "INSERT INTO sample (sample_code, collected_at, collector, seal_no, content_hash, created_at)"
        " VALUES (?,?,?,?,?,?)",
        (data.sample_code, canonical["collected_at"], data.collector, data.seal_no, h, created_at),
    )
    row = conn.execute("SELECT * FROM sample WHERE id = ?", (cur.lastrowid,)).fetchone()
    # 新登记样本从版本 0 开始
    insert_sample_version(conn, row, "register", created_at=created_at)
    return row, False


# --------------------------------------------------------------------------- 事件写入

def _event_canonical(data: EventIn | NewEventIn, sample_code: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    """构造参与指纹的规范字典与落库 payload（字段集随事件类型固定）。

    sample_code 用于纠错替换：NewEventIn 本身不含样本编号（不得跨样本），
    以纠错单的 sample_code 落库。
    """
    code = sample_code or getattr(data, "sample_code")
    occurred = to_utc_iso(data.occurred_at)
    payload: dict[str, Any]
    if data.event_type == "transfer":
        payload = {
            "transfer_no": data.transfer_no,
            "from_custodian": data.from_custodian,
            "to_custodian": data.to_custodian,
        }
    elif data.event_type == "receive":
        payload = {"transfer_no": data.transfer_no, "receiver": data.receiver, "seal_no": data.seal_no}
    elif data.event_type == "seal_check":
        payload = {"checked_seal_no": data.checked_seal_no, "seal_intact": bool(data.seal_intact)}
    else:  # storage
        payload = {"custodian": data.custodian or LAB_CUSTODIAN}
        if data.storage_location is not None:
            payload["storage_location"] = data.storage_location

    canonical: dict[str, Any] = {
        "event_id": data.event_id,
        "event_type": data.event_type,
        "sample_code": code,
        "occurred_at": occurred,
        **payload,
    }
    if data.note is not None:
        canonical["note"] = data.note
        payload = {**payload, "note": data.note}
    return canonical, payload


def create_event(conn: sqlite3.Connection, data: EventIn) -> tuple[sqlite3.Row, bool]:
    """写入事件。返回 (事件行, 是否幂等重复提交)。

    同 event_id + 同内容 → 幂等返回；同 event_id + 异内容 → 409 冲突。
    """
    sample = get_sample_or_404(conn, data.sample_code)

    canonical, payload = _event_canonical(data)
    h = _fingerprint(canonical)
    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    # 幂等查重优先于冻结检查：既有规则（同 event_id 同内容 200 / 异内容 409）
    # 在核定入库后也保持兼容；新增事件才受历史冻结限制。
    existing = conn.execute("SELECT * FROM custody_event WHERE event_id = ?", (data.event_id,)).fetchone()
    if existing is not None:
        if existing["content_hash"] != h:
            raise DomainError(
                409,
                "event_conflict",
                f"事件号 {data.event_id} 已存在但提交内容不一致（同事件号异内容冲突）",
                {
                    "event_id": data.event_id,
                    "existing": {
                        "event_type": existing["event_type"],
                        "occurred_at": existing["occurred_at"],
                        "content_hash": existing["content_hash"],
                    },
                    "incoming": {"event_type": data.event_type, "occurred_at": canonical["occurred_at"], "content_hash": h},
                },
            )
        return existing, True

    ensure_unlocked(sample)

    from .db import utcnow_iso

    try:
        cur = conn.execute(
            "INSERT INTO custody_event (sample_id, event_id, event_type, occurred_at, payload_json, content_hash, created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (sample["id"], data.event_id, data.event_type, canonical["occurred_at"], payload_json, h, utcnow_iso()),
        )
    except sqlite3.IntegrityError:
        # 并发下唯一键竞争：重新读取后按幂等/冲突处理
        existing = conn.execute("SELECT * FROM custody_event WHERE event_id = ?", (data.event_id,)).fetchone()
        if existing is not None and existing["content_hash"] == h:
            return existing, True
        raise DomainError(409, "event_conflict", f"事件号 {data.event_id} 并发写入且内容冲突")

    row = conn.execute("SELECT * FROM custody_event WHERE id = ?", (cur.lastrowid,)).fetchone()
    # 真正提交的单事件：受影响样本版本 +1（幂等重试与失败路径不走此处，不产生版本）
    insert_sample_version(conn, sample, "event", ref_event_id=data.event_id)
    return row, False


# --------------------------------------------------------------------------- 保管链重算

def _correction_rows(conn: sqlite3.Connection, sample_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM event_correction WHERE sample_id = ? ORDER BY id ASC",
        (sample_id,),
    ).fetchall()


def correction_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    """纠错留痕的对外结构（原事件与纠错关系永久可查）。"""
    return {
        "correction_id": row["correction_id"],
        "target_event_id": row["target_event_id"],
        "action": row["action"],
        "operator": row["operator"],
        "reason": row["reason"],
        "new_event_id": row["new_event_id"],
        "created_at": row["created_at"],
    }


def evaluate(conn: sqlite3.Connection, sample: sqlite3.Row) -> dict[str, Any]:
    """按发生时间重算该样本当前的完整证据链，输出结论。

    仅纳入当前有效版本（superseded_by IS NULL）；被作废/替换的事件不参与核验，
    其内容经 superseded_events 与 corrections 永久留痕、可追溯。
    """
    all_rows = conn.execute(
        "SELECT * FROM custody_event WHERE sample_id = ? ORDER BY occurred_at ASC, id ASC",
        (sample["id"],),
    ).fetchall()
    correction_rows = _correction_rows(conn, sample["id"])
    return _evaluate_state(dict(sample), all_rows, correction_rows)


def _evaluate_state(
    sample: dict[str, Any],
    all_rows: list[sqlite3.Row],
    correction_rows: list[sqlite3.Row],
    approved_storage_at: Any = _UNSET,
) -> dict[str, Any]:
    """给定某一时点的事件全集与纠错全集，按发生时间重算保管链结论。

    版本历史查询复用本函数：
      * all_rows 只传入该版本水位（行号）以内已提交的事件——故补录但发生时间更早的
        事件不会出现在更早版本中；
      * 事件是否有效由 correction_rows 推导（superseded_by 是现状列，历史时点可能尚未作废）；
      * approved_storage_at 传入 None 表示未核定；省略（...）表示沿用 sample 行上的现值。
    """
    # 该时点每条纠错使 target_event_id 失效（同一有效版本并发只允许一次纠错，目标不冲突）
    superseded_ids = {r["target_event_id"]: r["correction_id"] for r in correction_rows}
    events = [e for e in all_rows if e["event_id"] not in superseded_ids]
    superseded_rows = [e for e in all_rows if e["event_id"] in superseded_ids]
    if approved_storage_at is _UNSET:
        approved_storage_at = sample["approved_storage_at"]

    current = sample["collector"]                       # 当前保管人（初始为采集人）
    open_transfer: dict[str, Any] | None = None         # 在途移交（已移交未签收）
    closed_transfer_nos: set[str] = set()               # 已正常签收的移交单
    last_receive_at: str | None = None
    stored = False
    storage_without_chain = False                       # 入库节点缺失前序签收（链不完整）
    invalid_reasons: list[str] = []
    timeline: list[dict[str, Any]] = []

    # 预索引全部移交单（含乱序补录），用于把"签收早于移交"与"单号根本不存在"区分开
    transfer_by_no: dict[str, dict[str, Any]] = {}
    for ev0 in events:
        if ev0["event_type"] == "transfer":
            transfer_by_no[json.loads(ev0["payload_json"])["transfer_no"]] = {
                "row": ev0,
                "payload": json.loads(ev0["payload_json"]),
            }

    for ev in events:
        p = json.loads(ev["payload_json"])
        ref = f"[事件 {ev['event_id']}]"
        timeline.append(
            {
                "event_id": ev["event_id"],
                "event_type": ev["event_type"],
                "occurred_at": ev["occurred_at"],
                "recorded_at": ev["created_at"],
                "payload": p,
                "replaces_event_id": ev["replaces_event_id"],
                "batch_id": ev["batch_id"],
            }
        )

        # 任何事件早于采集时间：记录无效（不可能在登记采集前发生保管动作）
        if ev["occurred_at"] < sample["collected_at"]:
            invalid_reasons.append(f"{ref} 事件发生时间 {ev['occurred_at']} 早于样本采集时间 {sample['collected_at']}")
            continue

        et = ev["event_type"]

        if et == "transfer":
            if open_transfer is not None:
                invalid_reasons.append(
                    f"{ref} 移交至签收期间再次移交：移交单 {open_transfer['transfer_no']}"
                    f"（{open_transfer['from_custodian']} → {open_transfer['to_custodian']}）尚未签收"
                )
                continue
            frm, to, no = p["from_custodian"], p["to_custodian"], p["transfer_no"]
            if frm != current:
                invalid_reasons.append(
                    f"{ref} 移交人 {frm} 不是当前保管人 {current}（移交单 {no}，保管人断链）"
                )
                continue
            open_transfer = {"transfer_no": no, "from_custodian": frm, "to_custodian": to, "occurred_at": ev["occurred_at"]}
            # 签收完成前保管人不变，仍为移交人

        elif et == "receive":
            no = p["transfer_no"]
            if no in closed_transfer_nos:
                invalid_reasons.append(f"{ref} 重复签收：移交单 {no} 已被签收")
                continue
            known = transfer_by_no.get(no)
            if open_transfer is None or open_transfer["transfer_no"] != no:
                if known is not None and ev["occurred_at"] <= known["row"]["occurred_at"]:
                    invalid_reasons.append(
                        f"{ref} 签收时间 {ev['occurred_at']} 不晚于移交时间 {known['row']['occurred_at']}（单号 {no}）"
                    )
                    # 该移交到达时在途状态仍可能被后续正确签收关闭，故此处不改在途状态
                    continue
                invalid_reasons.append(f"{ref} 签收无对应在途移交：单号 {no} 未移交或不匹配（保管人断链）")
                continue

            mismatch = False
            if p["receiver"] != open_transfer["to_custodian"]:
                invalid_reasons.append(
                    f"{ref} 签收接收人不匹配：签收人 {p['receiver']} ≠ 移交单 {no} 指定接收人 {open_transfer['to_custodian']}"
                )
                mismatch = True
            if p["seal_no"] != sample["seal_no"]:
                invalid_reasons.append(
                    f"{ref} 封签号不符：签收封签 {p['seal_no']} ≠ 登记封签 {sample['seal_no']}"
                )
                mismatch = True
            if ev["occurred_at"] <= open_transfer["occurred_at"]:
                invalid_reasons.append(
                    f"{ref} 签收时间 {ev['occurred_at']} 不晚于移交时间 {open_transfer['occurred_at']}（单号 {no}）"
                )
                mismatch = True
            if mismatch:
                # 在途移交保留：允许后续按发生时间补录正确签收
                continue

            closed_transfer_nos.add(no)
            current = p["receiver"]
            last_receive_at = ev["occurred_at"]
            open_transfer = None

        elif et == "seal_check":
            if p["checked_seal_no"] != sample["seal_no"]:
                invalid_reasons.append(
                    f"{ref} 封签检查封签号不符：检查封签 {p['checked_seal_no']} ≠ 登记封签 {sample['seal_no']}"
                )
            if not p.get("seal_intact", True):
                invalid_reasons.append(f"{ref} 封签检查发现封签破损/不完好")

        else:  # storage
            if last_receive_at is not None and ev["occurred_at"] < last_receive_at:
                invalid_reasons.append(
                    f"{ref} 入库时间 {ev['occurred_at']} 早于最终签收时间 {last_receive_at}（保管链不成立）"
                )
                continue
            if open_transfer is not None or last_receive_at is None:
                # 尚无已完成签收（或还有在途移交）：入库节点缺失前序保管链 → 待补证，保管人不变更
                storage_without_chain = True
                continue
            stored = True
            current = p.get("custodian") or LAB_CUSTODIAN

    # ------------------------------------------------------------ 汇总（无效优先）
    pending_reasons: list[str] = []
    if open_transfer is not None:
        pending_reasons.append(
            f"移交单 {open_transfer['transfer_no']} 已移交但缺少签收"
            f"（{open_transfer['from_custodian']} → {open_transfer['to_custodian']}，移交时间 {open_transfer['occurred_at']}）"
        )
    elif storage_without_chain:
        pending_reasons.append("存在入库记录但缺少此前完整的移交-签收链，入库不能成立")
    if not stored:
        pending_reasons.append("缺少实验室入库记录")

    if invalid_reasons:
        status = "invalid"
        reasons = invalid_reasons + [f"另有待补证：{r}" for r in pending_reasons]
    elif pending_reasons:
        status = "pending"
        reasons = pending_reasons
    else:
        status = "valid"
        reasons = [f"保管链完整：采集 {sample['collector']} 至当前保管人 {current}，封签一致，已入库"]

    # 被作废/替换的原始事件（按发生时间排列），仅用于追溯，不参与上面的核验
    superseded_events = [
        {
            "event_id": ev["event_id"],
            "event_type": ev["event_type"],
            "occurred_at": ev["occurred_at"],
            "recorded_at": ev["created_at"],
            "payload": json.loads(ev["payload_json"]),
            "superseded_by": superseded_ids[ev["event_id"]],
            "replaces_event_id": ev["replaces_event_id"],
            "batch_id": ev["batch_id"],
        }
        for ev in superseded_rows
    ]
    corrections = [correction_to_dict(r) for r in correction_rows]

    return {
        "sample_code": sample["sample_code"],
        "status": status,
        "reasons": reasons,
        "current_custodian": current,
        "pending_transfer": open_transfer,
        "has_storage": stored,
        "approved_storage_at": approved_storage_at,
        "event_count": len(events),
        "timeline": timeline,
        "corrections": corrections,
        "superseded_events": superseded_events,
    }


# --------------------------------------------------------------------------- 事件纠错（作废 / 替换）

def _correction_fingerprint(data: CorrectionIn, new_canonical: dict[str, Any] | None) -> str:
    obj = {
        "correction_id": data.correction_id,
        "sample_code": data.sample_code,
        "target_event_id": data.target_event_id,
        "action": data.action,
        "operator": data.operator,
        "reason": data.reason,
    }
    if new_canonical is not None:
        obj["new_event"] = new_canonical
    return _fingerprint(obj)


def _get_correction(conn: sqlite3.Connection, correction_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM event_correction WHERE correction_id = ?", (correction_id,)
    ).fetchone()


def submit_correction(conn: sqlite3.Connection, data: CorrectionIn) -> tuple[dict[str, Any], bool]:
    """提交作废/替换纠错。返回 (纠错结果, 是否幂等重试)。

    规则：
      * 仅核定入库前可纠错；纠错不得跨样本；
      * 只能针对当前有效版本（已被作废/替换的事件再次纠错 → 409，失败不改链）；
      * replace 必须给完整新事件内容且新事件号未被占用（含已留痕的旧号）；
      * 同 correction_id 同内容 → 幂等返回 200；异内容 → 409；
      * 并发针对同一有效版本只允许一次成功（行级条件更新守卫，失败回滚）。
    """
    from .db import utcnow_iso

    sample = get_sample_or_404(conn, data.sample_code)

    new_canonical: dict[str, Any] | None = None
    new_payload_json: str | None = None
    new_hash: str | None = None
    if data.new_event is not None:
        new_canonical, new_payload = _event_canonical(data.new_event, sample_code=data.sample_code)
        new_payload_json = json.dumps(new_payload, ensure_ascii=False, separators=(",", ":"))
        new_hash = _fingerprint(new_canonical)

    request_hash = _correction_fingerprint(data, new_canonical)

    # 1) 同纠错号：同内容幂等 / 异内容冲突（与既有事件上报语义一致）
    existing = _get_correction(conn, data.correction_id)
    if existing is not None:
        if existing["request_hash"] != request_hash:
            raise DomainError(
                409,
                "correction_conflict",
                f"纠错号 {data.correction_id} 已使用但提交内容不一致（同纠错号异内容冲突）",
                {"correction_id": data.correction_id,
                 "existing_request_hash": existing["request_hash"],
                 "incoming_request_hash": request_hash},
            )
        return _correction_result(conn, sample, existing), True

    # 2) 核定入库后拒绝纠错
    ensure_unlocked(sample)

    # 3) 目标事件必须存在、属于该样本、且仍是当前有效版本
    target = conn.execute(
        "SELECT * FROM custody_event WHERE event_id = ?", (data.target_event_id,)
    ).fetchone()
    if target is None:
        raise DomainError(404, "event_not_found", f"目标事件不存在: {data.target_event_id}")
    if target["sample_id"] != sample["id"]:
        raise DomainError(
            409,
            "correction_cross_sample",
            f"纠错不得跨样本：事件 {data.target_event_id} 不属于样本 {data.sample_code}",
            {"target_event_id": data.target_event_id,
               "target_sample_id": target["sample_id"],
               "request_sample_code": data.sample_code},
        )
    if target["superseded_by"] is not None:
        raise DomainError(
            409,
            "event_not_effective",
            f"事件 {data.target_event_id} 已被纠错 {target['superseded_by']} 作废/替换，"
            "仅可针对当前有效版本纠错",
            {"target_event_id": data.target_event_id, "superseded_by": target["superseded_by"]},
        )

    # 4) 替换：新事件号必须未被占用（有效版本与历史留痕共用全局唯一事件号）
    new_row_id: int | None = None
    if data.action == "replace":
        occupied = conn.execute(
            "SELECT 1 FROM custody_event WHERE event_id = ?", (data.new_event.event_id,)
        ).fetchone()
        if occupied is not None:
            raise DomainError(
                409,
                "new_event_id_occupied",
                f"新事件号 {data.new_event.event_id} 已被占用，替换须使用未占用的新事件号",
                {"new_event_id": data.new_event.event_id},
            )

    # 5) 原子生效：条件更新仅命中仍为有效版本的目标行。
    #    本事务在 IMMEDIATE 写锁内，其他纠错/核定无法穿插；若目标行已被并发事务
    #    抢先纠错（本事务持锁后重读必可见），rowcount=0，整体回滚，链不变。
    cur = conn.execute(
        "UPDATE custody_event SET superseded_by = ? WHERE id = ? AND superseded_by IS NULL",
        (data.correction_id, target["id"]),
    )
    if cur.rowcount == 0:
        raise DomainError(
            409,
            "event_not_effective",
            f"事件 {data.target_event_id} 已被并发纠错，同一有效版本仅允许一次纠错成功",
            {"target_event_id": data.target_event_id},
        )

    if data.action == "replace":
        ins = conn.execute(
            "INSERT INTO custody_event"
            " (sample_id, event_id, event_type, occurred_at, payload_json, content_hash,"
            "  created_at, superseded_by, replaces_event_id)"
            " VALUES (?,?,?,?,?,?,?,NULL,?)",
            (sample["id"], data.new_event.event_id, data.new_event.event_type,
             new_canonical["occurred_at"], new_payload_json, new_hash,
             utcnow_iso(), data.target_event_id),
        )
        new_row_id = ins.lastrowid

    corr_cur = conn.execute(
        "INSERT INTO event_correction"
        " (correction_id, sample_id, target_event_id, action, operator, reason,"
        "  new_event_id, new_event_row_id, request_hash, created_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (data.correction_id, sample["id"], data.target_event_id, data.action,
         data.operator, data.reason,
         data.new_event.event_id if data.new_event else None, new_row_id,
         request_hash, utcnow_iso()),
    )
    corr_row = conn.execute(
        "SELECT * FROM event_correction WHERE id = ?", (corr_cur.lastrowid,)
    ).fetchone()
    # 作废 / 替换纠错各使受影响样本版本 +1
    insert_sample_version(
        conn,
        sample,
        "correction_replace" if data.action == "replace" else "correction_void",
        ref_event_id=(data.new_event.event_id if data.new_event else data.target_event_id),
        ref_correction_id=data.correction_id,
    )
    return _correction_result(conn, sample, corr_row), False


def _correction_result(
    conn: sqlite3.Connection, sample: sqlite3.Row, corr_row: sqlite3.Row
) -> dict[str, Any]:
    """纠错提交后的响应体（含按最终有效链重算的结论、原因、当前保管人）。"""
    result = evaluate(conn, sample)
    return {"correction": correction_to_dict(corr_row), "evaluation": result}


def list_corrections(conn: sqlite3.Connection, sample_code: str) -> list[dict[str, Any]]:
    sample = get_sample_or_404(conn, sample_code)
    return [correction_to_dict(r) for r in _correction_rows(conn, sample["id"])]


# --------------------------------------------------------------------------- 入库核定

def approve_storage(conn: sqlite3.Connection, sample_code: str, data: ApproveIn) -> dict[str, Any]:
    sample = get_sample_or_404(conn, sample_code)
    result = evaluate(conn, sample)

    if sample["approved_storage_at"]:
        # 已核定：幂等返回，不允许借重复核定改写
        result["approval"] = {"approved": True, "approved_at": sample["approved_storage_at"], "approver": sample["approved_by"]}
        return result

    if result["status"] != "valid":
        raise DomainError(
            422,
            "approval_rejected",
            f"样本 {sample_code} 当前结论为 {result['status']}，仅有效（valid）保管链可核定入库",
            {"evaluation": result},
        )

    from .db import utcnow_iso

    approved_at = utcnow_iso()
    conn.execute(
        "UPDATE sample SET approved_storage_at = ?, approved_by = ?, approve_note = ? WHERE id = ?",
        (approved_at, data.approver, data.note, sample["id"]),
    )
    # 入库核定使受影响样本版本 +1（重复核定走幂等提前返回，不产生版本）
    insert_sample_version(
        conn,
        sample,
        "approval",
        approved_storage_at=approved_at,
        approved_by=data.approver,
    )
    result["approved_storage_at"] = approved_at
    result["approval"] = {"approved": True, "approved_at": approved_at, "approver": data.approver, "note": data.note}
    return result


# --------------------------------------------------------------------------- 批量移交（同一交接车次多份样本）

def _batch_fingerprint(data: TransferBatchIn) -> tuple[str, str]:
    """返回 (规范后的统一移交时间 UTC ISO, 整批内容指纹)。

    指纹对 items 的提交顺序不敏感（按样本编号/事件号归一化排序）；
    批内重复编号属于请求不合法，由提交函数另行判定（不进入幂等/冲突语义）。
    """
    occurred = to_utc_iso(data.occurred_at)
    items = sorted(
        (
            {"sample_code": it.sample_code, "transfer_no": it.transfer_no, "event_id": it.event_id}
            for it in data.items
        ),
        key=lambda x: (x["sample_code"], x["event_id"]),
    )
    canonical = {
        "batch_id": data.batch_id,
        "occurred_at": occurred,
        "from_custodian": data.from_custodian,
        "to_custodian": data.to_custodian,
        "items": items,
    }
    return occurred, _fingerprint(canonical)


def submit_transfer_batch(conn: sqlite3.Connection, data: TransferBatchIn) -> tuple[dict[str, Any], bool]:
    """同一交接车次多份样本一次提交：各份均作为既有 transfer 事件原子落库。

    返回 (批次查询视图, 是否幂等重试)。整批语义：
      * 批内样本编号/事件号重复、样本不存在、样本已核定、任一事件号已占用
        → 整个批次失败，事务回滚，不留下事件或批次；
      * 同批次号同内容重试 → 幂等返回 200（即使样本此后已核定）；
      * 同批次号异内容 → 409 batch_conflict；
      * 并发提交以 BEGIN IMMEDIATE 持锁顺序串行判定（事件号唯一键兜底）。
    """
    from .db import utcnow_iso

    occurred, request_hash = _batch_fingerprint(data)

    # 1) 同批次号：同内容幂等 / 异内容冲突。优先于一切业务校验，
    #    与既有事件接口一致——核定后同号同内容重试仍 200。
    existing = conn.execute(
        "SELECT * FROM transfer_batch WHERE batch_id = ?", (data.batch_id,)
    ).fetchone()
    if existing is not None:
        if existing["request_hash"] != request_hash:
            raise DomainError(
                409,
                "batch_conflict",
                f"批次号 {data.batch_id} 已使用但提交内容不一致（同批次号异内容冲突）",
                {"batch_id": data.batch_id,
                 "existing_request_hash": existing["request_hash"],
                 "incoming_request_hash": request_hash},
            )
        return get_transfer_batch(conn, data.batch_id), True

    # 2) 整批预校验：任一份不通过即整体拒绝（此刻尚未写入任何行）。
    #    conflict_items（已核定/事件号占用）优先 → 409；其余（批内重复/样本不存在）→ 422。
    conflict_items: list[dict[str, Any]] = []
    invalid_items: list[dict[str, Any]] = []
    seen_samples: dict[str, int] = {}
    seen_events: dict[str, int] = {}
    sample_rows: dict[str, sqlite3.Row] = {}

    for idx, it in enumerate(data.items):
        if it.sample_code in seen_samples:
            invalid_items.append({
                "index": idx, "sample_code": it.sample_code, "event_id": it.event_id,
                "code": "duplicate_sample_in_batch",
                "message": f"批内样本编号重复：{it.sample_code}"
                           f"（第 {seen_samples[it.sample_code] + 1} 份与第 {idx + 1} 份）",
            })
        else:
            seen_samples[it.sample_code] = idx
        if it.event_id in seen_events:
            invalid_items.append({
                "index": idx, "sample_code": it.sample_code, "event_id": it.event_id,
                "code": "duplicate_event_in_batch",
                "message": f"批内事件号重复：{it.event_id}"
                           f"（第 {seen_events[it.event_id] + 1} 份与第 {idx + 1} 份）",
            })
        else:
            seen_events[it.event_id] = idx

        srow = conn.execute(
            "SELECT * FROM sample WHERE sample_code = ?", (it.sample_code,)
        ).fetchone()
        if srow is None:
            invalid_items.append({
                "index": idx, "sample_code": it.sample_code, "event_id": it.event_id,
                "code": "sample_not_found",
                "message": f"样本不存在: {it.sample_code}",
            })
            continue
        sample_rows[it.sample_code] = srow

        if config.LOCK_AFTER_APPROVAL and srow["approved_storage_at"]:
            conflict_items.append({
                "index": idx, "sample_code": it.sample_code, "event_id": it.event_id,
                "code": "sample_approved",
                "message": f"样本 {it.sample_code} 已核定入库（{srow['approved_storage_at']}），禁止追加移交",
            })
        occupied = conn.execute(
            "SELECT 1 FROM custody_event WHERE event_id = ?", (it.event_id,)
        ).fetchone()
        if occupied is not None:
            conflict_items.append({
                "index": idx, "sample_code": it.sample_code, "event_id": it.event_id,
                "code": "event_id_occupied",
                "message": f"事件号 {it.event_id} 已被占用",
            })

    if conflict_items or invalid_items:
        if conflict_items:
            raise DomainError(
                409,
                "batch_rejected",
                f"批次 {data.batch_id} 存在冲突项（样本已核定/事件号占用），整批失败且不写入任何事件或批次",
                {"batch_id": data.batch_id, "items": conflict_items + invalid_items},
            )
        raise DomainError(
            422,
            "batch_rejected",
            f"批次 {data.batch_id} 存在不合法项（批内重复/样本不存在），整批失败且不写入任何事件或批次",
            {"batch_id": data.batch_id, "items": invalid_items},
        )

    # 3) 全部通过：写批次头、各份既有 transfer 事件、批次明细。
    #    在 IMMEDIATE 写事务内，批次头唯一键与事件号唯一键共同保证并发原子性：
    #    任一插入失败（如并发抢先占用事件号）→ 整体回滚，不留批次与事件。
    created_at = utcnow_iso()
    bc = conn.execute(
        "INSERT INTO transfer_batch"
        " (batch_id, occurred_at, from_custodian, to_custodian, item_count, request_hash, created_at)"
        " VALUES (?,?,?,?,?,?,?)",
        (data.batch_id, occurred, data.from_custodian, data.to_custodian,
         len(data.items), request_hash, created_at),
    )
    batch_pk = bc.lastrowid

    for idx, it in enumerate(data.items):
        srow = sample_rows[it.sample_code]
        event = EventIn(
            event_id=it.event_id,
            event_type="transfer",
            sample_code=it.sample_code,
            occurred_at=data.occurred_at,
            transfer_no=it.transfer_no,
            from_custodian=data.from_custodian,
            to_custodian=data.to_custodian,
        )
        canonical, payload = _event_canonical(event)
        h = _fingerprint(canonical)
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        try:
            ec = conn.execute(
                "INSERT INTO custody_event"
                " (sample_id, event_id, event_type, occurred_at, payload_json, content_hash,"
                "  created_at, superseded_by, replaces_event_id, batch_id)"
                " VALUES (?,?,?,?,?,?,?,NULL,NULL,?)",
                (srow["id"], it.event_id, "transfer", occurred, payload_json, h,
                 created_at, data.batch_id),
            )
        except sqlite3.IntegrityError:
            raise DomainError(
                409,
                "batch_rejected",
                f"批次 {data.batch_id} 并发提交冲突：事件号 {it.event_id} 已被占用，整批失败",
                {"batch_id": data.batch_id,
                 "items": [{"index": idx, "sample_code": it.sample_code,
                            "event_id": it.event_id, "code": "event_id_occupied"}]},
            )
        conn.execute(
            "INSERT INTO transfer_batch_item"
            " (batch_pk, sample_id, sample_code, transfer_no, event_id, event_row_id, item_index)"
            " VALUES (?,?,?,?,?,?,?)",
            (batch_pk, srow["id"], it.sample_code, it.transfer_no,
             it.event_id, ec.lastrowid, idx),
        )

    # 4) 批内每份样本各递增一次版本；与批次、事件同事务，整批一起提交或一起失败
    for idx, it in enumerate(data.items):
        insert_sample_version(
            conn,
            sample_rows[it.sample_code],
            "batch_transfer",
            ref_event_id=it.event_id,
            ref_batch_id=data.batch_id,
            created_at=created_at,
        )

    return get_transfer_batch(conn, data.batch_id), False


def _effective_event_id(conn: sqlite3.Connection, row_id: int) -> str | None:
    """沿替换链（replaces_event_id）走到当前有效版本事件号。

    原事件仍有效 → 返回原事件号；经 replace 链最终版本有效 → 返回最终事件号；
    被 void 作废（无替换后继）或链终点已失效 → None。
    """
    first = conn.execute("SELECT event_id FROM custody_event WHERE id = ?", (row_id,)).fetchone()
    current_evt = first["event_id"]
    while True:
        cur = conn.execute(
            "SELECT superseded_by FROM custody_event WHERE event_id = ?", (current_evt,)
        ).fetchone()
        if cur["superseded_by"] is None:
            return current_evt
        child = conn.execute(
            "SELECT event_id FROM custody_event WHERE replaces_event_id = ? ORDER BY id ASC",
            (current_evt,),
        ).fetchone()
        if child is None:
            return None
        current_evt = child["event_id"]


def get_transfer_batch(conn: sqlite3.Connection, batch_id: str) -> dict[str, Any]:
    """批次查询：批次头 + 逐份样本的当前核验结论与原事件是否仍有效。

    后续纠错（作废/替换）只改变有效链与本查询结果；批次头与明细的原始归属永久留痕。
    """
    batch = conn.execute(
        "SELECT * FROM transfer_batch WHERE batch_id = ?", (batch_id,)
    ).fetchone()
    if batch is None:
        raise DomainError(404, "batch_not_found", f"批次不存在: {batch_id}")

    items: list[dict[str, Any]] = []
    item_rows = conn.execute(
        "SELECT * FROM transfer_batch_item WHERE batch_pk = ? ORDER BY item_index ASC",
        (batch["id"],),
    ).fetchall()
    for it in item_rows:
        sample = conn.execute("SELECT * FROM sample WHERE id = ?", (it["sample_id"],)).fetchone()
        result = evaluate(conn, sample)
        orig = conn.execute(
            "SELECT superseded_by FROM custody_event WHERE id = ?", (it["event_row_id"],)
        ).fetchone()
        items.append({
            "index": it["item_index"],
            "sample_code": it["sample_code"],
            "transfer_no": it["transfer_no"],
            "event_id": it["event_id"],
            "event_effective": orig["superseded_by"] is None,
            "superseded_by": orig["superseded_by"],
            "current_event_id": _effective_event_id(conn, it["event_row_id"]),
            "evaluation": evaluation_summary(result, sample),
        })

    return {
        "batch_id": batch["batch_id"],
        "occurred_at": batch["occurred_at"],
        "from_custodian": batch["from_custodian"],
        "to_custodian": batch["to_custodian"],
        "item_count": batch["item_count"],
        "created_at": batch["created_at"],
        "items": items,
    }
