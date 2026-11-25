"""data_reconcile 领域服务：数据对账与差异检测。

业务逻辑：
1. 数据对账：左源表与右源表按对账字段逐行对比
2. 对账链路：data_reconcile → changelog（对账结论/失败写入变更日志追溯）
3. 失败场景：源表未登记、源表暂不可读、差异阈值超限、字段类型不匹配、对账超时
4. 与上下游流水线：为 changelog 提供对账结论与真实读点

跨模块数据流：
  data_reconcile → changelog（对账结论、失败待重试、重试关闭均写入变更日志）

读取状态约定（见 ``app.store.read_table``）：
  - missing    表未登记：表名拼错或尚未接入，属于异常，必须说明原因
  - unreadable 暂不可读：表已登记但当前读不到，属于异常，必须说明原因并提示重试
  - empty      真实空表：表可读且确实没有数据，单侧差异是真实业务差异
  - ok         正常读取

只有 missing / unreadable 会进入「待重试」队列；真实空表产生的差异是正常对账结论，
不会被当成失败，也不会出现在待重试队列里（兼容历史遗留的空表差异记录）。
"""
from __future__ import annotations

import threading
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import (
    TableRead,
    store,
)


class DataReconcileService(BaseModuleService):
    MODULE = "data_reconcile"
    ENTITY = "对账任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "左源表", "右源表", "对账字段", "差异阈值", "执行时间", "差异行数", "对账结论"]
    REQUIRED_FIELDS = ["任务编号", "左源表", "右源表"]
    STATUS_ORDER = ["待执行", "执行中", "待重试", "一致", "存在差异"]
    ACTION_RULES = {
        "发起对账": "执行中",
        "重试对账": "待重试",
        "标记一致": "一致",
        "标记差异": "存在差异",
    }
    NEGATIVE_ACTIONS = []

    PIPELINE = "reconcile_pipeline"
    DOWNSTREAM_MODULES = ["changelog"]
    PRODUCES_ARTIFACT = "数据对账结果记录"

    # 可执行对账的动作：这些动作会真正读取源表，而不是只改状态
    EXECUTE_ACTIONS = {"发起对账", "重试对账", "执行对账"}
    RETRY_HINT = "请修正源表名称或等待源表恢复后重试；恢复后会按实际读取的两侧数据重新对账"

    def __init__(self) -> None:
        super().__init__()
        # 首轮成功读取的左表快照：右表暂不可读时保留下来，恢复后据此判断
        # 应沿用首轮左表，还是重新读取两侧。
        self._pending: dict[int, dict[str, Any]] = {}
        # 最近一次真实对账结果：并发提交相同重试时，读到的两侧数据摘要一致，
        # 直接复用首次结论，不重复写变更日志。
        self._last_result: dict[int, dict[str, Any]] = {}
        self._entry_locks: dict[int, threading.RLock] = {}
        self._locks_guard = threading.Lock()

    # ------------------------------------------------------------------
    # 对账入口
    # ------------------------------------------------------------------
    def execute_reconcile(
        self, entry_id: int, *, reuse_first_round: bool = True
    ) -> dict[str, Any]:
        """执行对账：比较左右源表数据差异。

        读取失败（表未登记 / 暂不可读）不会把源表当成空表，也不会产生单侧差异，
        而是返回失败原因与重试提示，并把任务挂到「待重试」，同时写入变更日志。

        恢复后重试时：
        - 左表与首轮读点摘要一致 → 沿用首轮左表（left_policy=reuse_first_round）
        - 左表在等待期间被更正（摘要变化）→ 重新读取两侧（left_policy=reread_both）
        - 左表当前也不可读但首轮快照还在 → 沿用首轮左表并标记 left_policy=reuse_first_round_left_unreadable

        无论走哪种策略，日志里都会登记实际使用的左右读点，便于复核。
        """
        lock = self._entry_lock(entry_id)
        with lock:
            return self._execute_locked(entry_id, reuse_first_round=reuse_first_round)

    def _execute_locked(self, entry_id: int, *, reuse_first_round: bool) -> dict[str, Any]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "entry_id": entry_id, "reason": f"任务 {entry_id} 不存在"}

        status = entry.get("status")
        if status not in ("待执行", "待重试", "一致", "存在差异"):
            return {
                "ok": False,
                "entry_id": entry_id,
                "reason": f"任务状态 {status} 非可对账状态",
            }

        left_table = str(entry.get("左源表", ""))
        right_table = str(entry.get("右源表", ""))
        reconcile_field = str(entry.get("对账字段", "id")) or "id"
        threshold = self._parse_threshold(entry.get("差异阈值", "5"))

        pending = self._pending_for(entry_id, left_table, right_table)
        retrying = pending is not None or status == "待重试"
        first_left: TableRead | None = pending.get("first_left") if pending else None

        # ---- 读取左表 ----
        left_now = store.read_table(left_table)
        left_used = left_now
        # 首轮读取：fresh；重试但无首轮快照可沿用：reread_both（重新读取两侧）。
        left_policy = "reread_both" if retrying else "fresh"
        left_reused = False

        if retrying and first_left is not None and reuse_first_round:
            if left_now.readable:
                if left_now.digest == first_left.digest:
                    # 左表在等待期间没有变化，沿用首轮读点，避免把同一版本读两次。
                    left_used = first_left
                    left_policy = "reuse_first_round"
                    left_reused = True
                else:
                    # 左表收到更正，首轮快照已过期；必须重新读取两侧再对账。
                    left_used = left_now
                    left_policy = "reread_both"
            else:
                # 左表当前也读不到，但首轮快照是真实读到的数据，仍可用于对账。
                left_used = first_left
                left_policy = "reuse_first_round_left_unreadable"
                left_reused = True

        if not left_used.readable:
            return self._fail(
                entry=entry,
                entry_id=entry_id,
                left_table=left_table,
                right_table=right_table,
                failed_side="left",
                failed_read=left_now,
                left_used=left_used,
                left_now=left_now,
                right_read=None,
                left_policy=left_policy,
                left_reused=left_reused,
            )

        # ---- 读取右表 ----
        right_now = store.read_table(right_table)
        if not right_now.readable:
            first_to_keep = first_left or (left_used if left_used.readable else None)
            self._remember_pending(
                entry_id=entry_id,
                left_table=left_table,
                right_table=right_table,
                first_left=first_to_keep,
                entry=entry,
            )
            return self._fail(
                entry=entry,
                entry_id=entry_id,
                left_table=left_table,
                right_table=right_table,
                failed_side="right",
                failed_read=right_now,
                left_used=left_used,
                left_now=left_now,
                right_read=right_now,
                left_policy=left_policy,
                left_reused=left_reused,
            )

        # ---- 两侧都真实读到，执行比对 ----
        # 幂等键用「两侧数据摘要 + 对账口径」，而不是读点：每次真实读取都会分配新读点，
        # 但只要数据内容相同，就是同一个对账结论。
        left_point_key = (left_used.digest, right_now.digest, reconcile_field, threshold)
        cached = self._last_result.get(entry_id)
        summary = entry.get("对账读点摘要") or {}
        already_recorded = (
            summary.get("left") == left_used.digest
            and summary.get("right") == right_now.digest
        )
        if (
            cached is not None
            and cached.get("key") == left_point_key
            and already_recorded
            and self._pending_failure_record(entry_id) is None
        ):
            # 任务已按这两份数据记过真实对账（含并发提交的相同重试），不重复写日志。
            reused = dict(cached["result"])
            reused["deduplicated"] = True
            reused["read_points"] = cached["read_points"]
            return reused

        result = self._compare(
            left_rows=left_used.rows,
            right_rows=right_now.rows,
            reconcile_field=reconcile_field,
            threshold=threshold,
        )
        passed = result["passed"]
        diff_count = result["diff_count"]
        conclusion = "一致" if passed else "存在差异"

        read_points = self._read_points(
            left_used=left_used,
            left_now=left_now,
            right_read=right_now,
            left_policy=left_policy,
            left_reused=left_reused,
        )

        # 先关闭历史待重试记录（保留原失败经过），再写本次真实对账结论。
        self._close_failure_record(entry_id, conclusion, read_points)

        entry["差异行数"] = str(diff_count)
        entry["对账结论"] = conclusion
        entry["执行时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["status"] = conclusion
        entry["pending"] = not passed
        entry["abnormal"] = not passed
        entry["读取状态"] = {
            "left": left_used.status_label,
            "right": right_now.status_label,
        }
        entry["读点"] = read_points
        entry["对账读点摘要"] = {"left": left_used.digest, "right": right_now.digest}
        entry["差异明细"] = {
            "only_left": result["only_left"],
            "only_right": result["only_right"],
            "mismatches": result["mismatches"],
        }
        entry.pop("失败原因", None)
        entry.pop("重试提示", None)
        entry.pop("失败源表", None)
        self._pending.pop(entry_id, None)

        # 对账结论同步到变更日志清单（一致也记，保留完整审计链）。
        store.append_changelog({
            "目标表": left_table,
            "变更类型": "对账差异" if not passed else "对账一致",
            "变更内容": self._conclusion_text(
                result, read_points, left_reused=left_reused),
            "操作时间": time.time(),
            "操作来源": "reconcile",
            "影响行数": diff_count,
            "回滚版本": "",
            "对账任务": entry_id,
            "任务编号": entry.get("任务编号"),
            "左源表": left_table,
            "右源表": right_table,
            "对账结论": conclusion,
            "差异明细": entry["差异明细"],
            "读点": read_points,
            "左读点": read_points["left_read_point"],
            "右读点": read_points["right_read_point"],
            "左表沿用首轮": left_reused,
            "左侧读点策略": left_policy,
        })

        batch_id = f"RECONCILE_{entry_id}_{int(time.time() * 1000)}"
        result_payload = [{
            "task": entry.get("任务编号"),
            "diff_count": diff_count,
            "passed": passed,
            "left_table": left_table,
            "right_table": right_table,
            "left_read_point": read_points["left_read_point"],
            "right_read_point": read_points["right_read_point"],
            "left_policy": left_policy,
        }]
        self.produce_artifact(result_payload, batch_id)

        response = {
            "ok": True,
            "entry_id": entry_id,
            "left_table": left_table,
            "right_table": right_table,
            "diff_count": diff_count,
            "passed": passed,
            "only_left": result["only_left"],
            "only_right": result["only_right"],
            "mismatches": result["mismatches"],
            "conclusion": conclusion,
            "读取状态": entry["读取状态"],
            "left_status": left_used.status,
            "right_status": right_now.status,
            "left_read_point": read_points["left_read_point"],
            "right_read_point": read_points["right_read_point"],
            "read_points": read_points,
            "left_policy": left_policy,
            "left_reused": left_reused,
        }
        self._last_result[entry_id] = {
            "key": left_point_key,
            "read_points": read_points,
            "result": response,
        }
        return response

    # ------------------------------------------------------------------
    # 失败处理
    # ------------------------------------------------------------------
    def _fail(
        self,
        *,
        entry: dict[str, Any],
        entry_id: int,
        left_table: str,
        right_table: str,
        failed_side: str,
        failed_read: TableRead,
        left_used: TableRead,
        left_now: TableRead,
        right_read: TableRead | None,
        left_policy: str,
        left_reused: bool,
    ) -> dict[str, Any]:
        """登记读取失败：说明原因、提示重试，不产生单侧差异。"""
        failed_table = failed_read.table or (left_table if failed_side == "left" else right_table)
        side_label = "左源表" if failed_side == "left" else "右源表"
        reason = f"{side_label} {failed_table} {failed_read.status_label}：{failed_read.detail or failed_read.reason}"
        hint = self.RETRY_HINT

        read_points = self._read_points(
            left_used=left_used,
            left_now=left_now,
            right_read=right_read,
            left_policy=left_policy,
            left_reused=left_reused,
        )
        read_points["failed_side"] = failed_side
        read_points["failed_table"] = failed_table

        entry["status"] = "待重试"
        entry["pending"] = True
        entry["abnormal"] = True
        entry["差异行数"] = ""
        entry["对账结论"] = "待重试"
        entry["执行时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["失败源表"] = failed_table
        entry["失败原因"] = reason
        entry["重试提示"] = hint
        # 读取状态反映本次实际尝试的两侧读取结果；沿用的首轮读点在「读点」里单独登记。
        entry["读取状态"] = {
            "left": left_now.status_label,
            "right": right_read.status_label if right_read is not None else "未读取",
        }
        entry["读点"] = read_points

        record = self._upsert_failure_record(
            entry=entry,
            entry_id=entry_id,
            left_table=left_table,
            right_table=right_table,
            failed_side=failed_side,
            failed_table=failed_table,
            failed_read=failed_read,
            reason=reason,
            hint=hint,
            read_points=read_points,
        )

        return {
            "ok": False,
            "entry_id": entry_id,
            "left_table": left_table,
            "right_table": right_table,
            "reason": reason,
            "retry_hint": hint,
            "retryable": True,
            "failed_side": failed_side,
            "failed_source": failed_table,
            "failed_status": failed_read.status,
            "failed_status_label": failed_read.status_label,
            "status": "待重试",
            "读取状态": entry["读取状态"],
            "left_status": left_now.status,
            "right_status": (
                failed_read.status if failed_side == "right"
                else (right_read.status if right_read else "unread")
            ),
            "left_read_point": read_points["left_read_point"],
            "right_read_point": read_points["right_read_point"],
            "read_points": read_points,
            "left_policy": left_policy,
            "left_reused": left_reused,
            "changelog_record": record,
        }

    def _upsert_failure_record(
        self,
        *,
        entry: dict[str, Any],
        entry_id: int,
        left_table: str,
        right_table: str,
        failed_side: str,
        failed_table: str,
        failed_read: TableRead,
        reason: str,
        hint: str,
        read_points: dict[str, Any],
    ) -> dict[str, Any]:
        """写入/更新一条「对账失败、待重试」记录；同一任务只保留一条待办。"""
        existing = self._pending_failure_record(entry_id)
        snapshot = {
            "失败源表": failed_table,
            "失败侧": failed_side,
            "失败状态": failed_read.status,
            "失败原因": reason,
            "时间": time.time(),
            "读点": read_points,
        }
        if existing is None:
            record: dict[str, Any] = {
                "目标表": left_table,
                "变更类型": "对账失败",
                "变更内容": f"{reason}；{hint}",
                "操作时间": time.time(),
                "操作来源": "reconcile",
                "影响行数": 0,
                "回滚版本": "",
                "对账任务": entry_id,
                "任务编号": entry.get("任务编号"),
                "左源表": left_table,
                "右源表": right_table,
                "失败源表": failed_table,
                "失败侧": failed_side,
                "失败状态": failed_read.status,
                "失败状态说明": failed_read.status_label,
                "失败原因": reason,
                "重试提示": hint,
                "重试状态": "待重试",
                "重试次数": 0,
                "读点": read_points,
                "左读点": read_points["left_read_point"],
                "右读点": read_points["right_read_point"],
                "首次失败": snapshot,
            }
            store.append_changelog(record)
            return record

        # 保留首次失败经过，仅刷新最近一次失败与重试次数。
        existing["重试次数"] = int(existing.get("重试次数", 0)) + 1
        existing["最近失败"] = snapshot
        existing["失败源表"] = failed_table
        existing["失败侧"] = failed_side
        existing["失败状态"] = failed_read.status
        existing["失败状态说明"] = failed_read.status_label
        existing["失败原因"] = reason
        existing["重试提示"] = hint
        existing["变更内容"] = f"{reason}；{hint}"
        existing["操作时间"] = time.time()
        existing["读点"] = read_points
        existing["左读点"] = read_points["left_read_point"]
        existing["右读点"] = read_points["right_read_point"]
        existing["重试状态"] = "待重试"
        return existing

    def _pending_failure_record(self, entry_id: int) -> dict[str, Any] | None:
        for record in reversed(store._global_changelog):
            if (
                record.get("变更类型") == "对账失败"
                and record.get("对账任务") == entry_id
                and record.get("重试状态") == "待重试"
            ):
                return record
        return None

    def _close_failure_record(
        self, entry_id: int, conclusion: str, read_points: dict[str, Any]
    ) -> int:
        """成功重跑后关闭原待办，但保留失败经过（不清空原记录）。"""
        closed = 0
        for record in store._global_changelog:
            if (
                record.get("变更类型") == "对账失败"
                and record.get("对账任务") == entry_id
                and record.get("重试状态") == "待重试"
            ):
                record["重试状态"] = "已关闭"
                record["关闭时间"] = time.time()
                record["重试结果"] = conclusion
                record["关闭读点"] = read_points
                closed += 1
        return closed

    # ------------------------------------------------------------------
    # 重试队列
    # ------------------------------------------------------------------
    def retry_pending(
        self,
        failed_source_table: str | None = None,
        *,
        source_table: str | None = None,
    ) -> dict[str, Any]:
        """从变更日志按失败源表复查待重试记录，逐条重跑。

        成功重跑会关闭原待办（保留失败经过）；仍失败则继续留在待重试队列。
        真实空表产生的差异记录不是「对账失败」，天然不会进入该队列。
        """
        table = failed_source_table or source_table
        pending = self.pending_retry_records(table)
        results: list[dict[str, Any]] = []
        closed = 0
        still_pending = 0
        for record in pending:
            entry_id = record.get("对账任务")
            if entry_id is None:
                continue
            outcome = self.execute_reconcile(int(entry_id))
            results.append(outcome)
            if outcome.get("ok"):
                closed += 1
            else:
                still_pending += 1
        return {
            "ok": True,
            "failed_source_table": table,
            "attempted": len(results),
            "closed": closed,
            "still_pending": still_pending,
            "results": results,
        }

    def pending_retry_records(self, failed_source_table: str | None = None) -> list[dict[str, Any]]:
        """列出待重试的对账失败记录（可按失败源表过滤）。"""
        records = [
            record for record in store._global_changelog
            if record.get("变更类型") == "对账失败"
            and record.get("重试状态") == "待重试"
        ]
        if failed_source_table:
            records = [
                r for r in records
                if r.get("失败源表") == failed_source_table
            ]
        return records

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _entry_lock(self, entry_id: int) -> threading.RLock:
        with self._locks_guard:
            lock = self._entry_locks.get(entry_id)
            if lock is None:
                lock = threading.RLock()
                self._entry_locks[entry_id] = lock
            return lock

    def _pending_for(
        self, entry_id: int, left_table: str, right_table: str
    ) -> dict[str, Any] | None:
        pending = self._pending.get(entry_id)
        if pending is None:
            return None
        if pending.get("left_table") != left_table or pending.get("right_table") != right_table:
            # 任务被改成别的源表，旧的首轮快照不再适用。
            self._pending.pop(entry_id, None)
            return None
        return pending

    def _remember_pending(
        self,
        *,
        entry_id: int,
        left_table: str,
        right_table: str,
        first_left: TableRead | None,
        entry: dict[str, Any],
    ) -> None:
        if first_left is None:
            return
        self._pending[entry_id] = {
            "entry_id": entry_id,
            "left_table": left_table,
            "right_table": right_table,
            "first_left": first_left,
            "task_no": entry.get("任务编号"),
            "remembered_at": time.time(),
        }

    @staticmethod
    def _parse_threshold(value: Any) -> float:
        try:
            return float(str(value))
        except (TypeError, ValueError):
            return 5.0

    @staticmethod
    def _compare(
        *,
        left_rows: list[dict[str, Any]],
        right_rows: list[dict[str, Any]],
        reconcile_field: str,
        threshold: float,
    ) -> dict[str, Any]:
        left_map = {
            r.get(reconcile_field): r for r in left_rows
            if r.get(reconcile_field) is not None
        }
        right_map = {
            r.get(reconcile_field): r for r in right_rows
            if r.get(reconcile_field) is not None
        }
        only_left = [k for k in left_map if k not in right_map]
        only_right = [k for k in right_map if k not in left_map]
        mismatches = 0
        mismatch_keys: list[Any] = []
        ignored = {"id", "pending", "abnormal", "status"}
        for key in left_map:
            if key not in right_map:
                continue
            left_val = left_map[key]
            right_val = right_map[key]
            common_fields = set(left_val.keys()) & set(right_val.keys()) - ignored
            field_diff = any(
                str(left_val.get(f)) != str(right_val.get(f)) for f in common_fields
            )
            if field_diff:
                mismatches += 1
                mismatch_keys.append(key)

        diff_count = len(only_left) + len(only_right) + mismatches
        # 沿用既有对账取数逻辑：差异行数不超过阈值即视为一致。
        passed = diff_count <= threshold
        return {
            "only_left": len(only_left),
            "only_right": len(only_right),
            "mismatches": mismatches,
            "diff_count": diff_count,
            "passed": passed,
            "only_left_keys": only_left,
            "only_right_keys": only_right,
            "mismatch_keys": mismatch_keys,
        }

    @staticmethod
    def _read_points(
        *,
        left_used: TableRead,
        left_now: TableRead,
        right_read: TableRead | None,
        left_policy: str,
        left_reused: bool,
    ) -> dict[str, Any]:
        right_info = right_read.as_dict() if right_read is not None else None
        left_info = left_used.as_dict()
        right_point = right_info.get("read_point", "") if right_info else ""
        return {
            "left": left_info,
            "right": right_info,
            "used": {"left": left_info, "right": right_info},
            "current": {"left": left_now.as_dict(), "right": right_info},
            "left_policy": left_policy,
            "left_reused": left_reused,
            "left_read_point": left_info.get("read_point", ""),
            "right_read_point": right_point,
        }

    @staticmethod
    def _conclusion_text(
        result: dict[str, Any], read_points: dict[str, Any], *, left_reused: bool
    ) -> str:
        left_point = read_points.get("left_read_point", "") or "未读取"
        right_point = read_points.get("right_read_point", "") or "未读取"
        left_mode = "沿用首轮左表" if left_reused else "本次重新读取左表"
        return (
            f"only_left: {result['only_left']}, "
            f"only_right: {result['only_right']}, "
            f"mismatches: {result['mismatches']}；"
            f"左读点 {left_point}（{left_mode}），右读点 {right_point}"
        )

    # ------------------------------------------------------------------
    # 动作入口：发起/重试对账会真正读取源表
    # ------------------------------------------------------------------
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action in self.EXECUTE_ACTIONS:
            result = self.execute_reconcile(entry_id)
            if result.get("ok"):
                return entry, (
                    f"对账完成：{entry.get('对账结论')}（差异 {entry.get('差异行数')} 行）"
                )
            # 读取失败不是「执行成功」：返回 None 让接口以失败呈现，
            # 任务本身已在仓库中登记为待重试，列表刷新后可见失败原因与重试提示。
            reason = str(result.get("reason") or "对账失败")
            hint = str(result.get("retry_hint") or "")
            return None, f"{reason}；{hint}" if hint else reason
        return super().run_action(entry_id, action)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["pending_tasks"] = sum(1 for r in rows if r.get("status") == "待执行")
        base["pending_retry"] = sum(1 for r in rows if r.get("status") == "待重试")
        base["pending_retry_records"] = len(self.pending_retry_records())
        return base
