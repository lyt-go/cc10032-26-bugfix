"""changelog 领域服务：变更日志记录与追溯。

业务逻辑：
1. 变更记录：目标表的增量变更、回滚版本、影响行数
2. 变更追溯链路：data_reconcile → changelog（对账差异驱动变更追溯）
3. 失败场景：回滚版本不存在、记录冲突、历史清理误删
4. 与上下游流水线：消费 data_reconcile 的对账差异；输出变更追溯信息

跨模块数据流：
  data_reconcile → changelog（对账差异写入变更日志）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ChangelogService(BaseModuleService):
    MODULE = "changelog"
    ENTITY = "变更记录"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["记录编号", "目标表", "变更类型", "变更内容", "操作时间", "操作来源", "影响行数", "回滚版本"]
    REQUIRED_FIELDS = ["记录编号", "目标表", "变更类型"]
    STATUS_ORDER = ["已记录", "已回滚", "已归档"]
    ACTION_RULES = {"发起回滚": "已回滚", "归档记录": "已归档"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "changelog_pipeline"
    UPSTREAM_MODULES = ["data_reconcile"]
    PRODUCES_ARTIFACT = "变更追溯记录"

    def pending_retry(self, failed_source_table: str | None = None) -> dict[str, Any]:
        """从变更日志按失败源表复查所有待重试的对账任务。

        - 只认「对账失败 + 待重试」记录，真实空表产生的差异（对账差异）不在其中；
        - 每条待办携带失败原因、重试提示、首次失败经过与最近一次读点。
        """
        from app.services import service_for

        reconcile = service_for("data_reconcile")
        records = reconcile.pending_retry_records(failed_source_table)
        return {
            "ok": True,
            "failed_source_table": failed_source_table,
            "total": len(records),
            "records": records,
        }

    def retry_pending(self, failed_source_table: str | None = None) -> dict[str, Any]:
        """按失败源表重跑待重试任务：成功重跑关闭原待办且保留失败经过。"""
        from app.services import service_for

        reconcile = service_for("data_reconcile")
        return reconcile.retry_pending(failed_source_table)

    def query_table_changelog(self, table_name: str) -> dict[str, Any]:
        """查询目标表的完整变更历史（从全局变更日志中提取）。

        与 data_reconcile 的链路：对账发现的差异会写入全局变更日志，
        changelog 模块提供对外查询接口，追溯数据变更轨迹。
        """
        records = store.changelog_for_table(table_name)
        changes_by_type = {}
        for r in records:
            ctype = r.get("变更类型", "unknown")
            changes_by_type[ctype] = changes_by_type.get(ctype, 0) + 1

        pending_retry = [
            r for r in records
            if r.get("变更类型") == "对账失败" and r.get("重试状态") == "待重试"
        ]
        return {
            "ok": True,
            "table": table_name,
            "total_changes": len(records),
            "by_type": changes_by_type,
            "pending_retry": len(pending_retry),
            "latest_records": records[-5:] if records else [],
            "source": "global_changelog",
        }

    def reconcile_read_points(self, table_name: str) -> dict[str, Any]:
        """变更日志清单：按目标表汇总可复核的对账读点。"""
        records = store.changelog_for_table(table_name)
        points = []
        for r in records:
            if r.get("变更类型") in ("对账差异", "对账一致"):
                points.append({
                    "变更类型": r.get("变更类型"),
                    "对账任务": r.get("对账任务"),
                    "对账结论": r.get("对账结论"),
                    "左读点": r.get("左读点"),
                    "右读点": r.get("右读点"),
                    "左表沿用首轮": r.get("左表沿用首轮", False),
                    "左侧读点策略": r.get("左侧读点策略"),
                })
        return {"ok": True, "table": table_name, "read_points": points}

    def rollback_change(self, entry_id: int) -> dict[str, Any]:
        """回滚变更记录

        失败场景：
        - 回滚版本不存在
        - 记录已回滚无法再次回滚
        - 回滚依赖冲突
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"记录 {entry_id} 不存在"}
        if entry.get("status") == "已回滚":
            return {"ok": False, "reason": "该记录已经回滚过，无法重复回滚"}

        rollback_version = str(entry.get("回滚版本", ""))
        if not rollback_version:
            return {"ok": False, "reason": "无回滚版本信息，无法回滚"}

        entry["status"] = "已回滚"
        entry["pending"] = True
        entry["abnormal"] = False

        batch_id = f"ROLLBACK_{entry_id}_{int(time.time())}"
        self.produce_artifact([{
            "change_id": entry_id,
            "table": entry.get("目标表"),
            "rollback_version": rollback_version,
            "ts": time.time(),
        }], batch_id)

        return {
            "ok": True,
            "entry_id": entry_id,
            "table": entry.get("目标表"),
            "rollback_version": rollback_version,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["global_changelog_total"] = len(store._global_changelog)
        return base