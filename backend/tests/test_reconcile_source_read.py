"""对账源表读取状态与失败重试链路的回归测试。

覆盖用户验收场景：
1. 右表名拼错 → 表未登记，不把左表行算成单侧差异，不写差异记录
2. 右表改为真实空表 → 正常对账，单侧差异是真实业务差异（不进入待重试）
3. 右表补入相同业务键 → 对账一致
4. 右表暂不可读 → 失败原因 + 重试提示，全部读点（含首轮左表）登记在日志
5. 等待期间左表金额 20→30，右表恢复后为 20 → 重新读取两侧，识别金额差异
6. 等待期间左表未变 → 沿用首轮左表读点
7. 并发提交相同重试 → 变更日志只记录一次真实对账及其读点
8. 按失败源表复查待重试记录，成功重跑关闭原待办且保留失败经过
9. 真实空表差异不出现在待重试队列
"""
from __future__ import annotations

import threading

import pytest

from app.services import service_for
from app.store import READ_EMPTY, READ_MISSING, READ_OK, READ_UNREADABLE, store


@pytest.fixture()
def reconcile():
    svc = service_for("data_reconcile")
    # 每个测试用独立的源表名，避免共享内存仓库互相污染。
    return svc


def _new_task(left: str, right: str, *, threshold: str = "0") -> int:
    rows = store.rows("data_reconcile")
    entry_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    rows.append({
        "id": entry_id,
        "status": "待执行",
        "pending": True,
        "abnormal": False,
        "任务编号": f"DATA-TEST-{entry_id}",
        "左源表": left,
        "右源表": right,
        "对账字段": "id",
        "差异阈值": threshold,
        "执行时间": "",
        "差异行数": "",
        "对账结论": "",
    })
    return entry_id


def _changelog_for(entry_id: int) -> list[dict]:
    return [c for c in store._global_changelog if c.get("对账任务") == entry_id]


# ---------------------------------------------------------------------------
# 读取状态：空态三分
# ---------------------------------------------------------------------------
class TestTableReadStates:
    def test_missing_table_is_not_empty(self):
        read = store.read_table("no_such_table_xyz")
        assert read.status == READ_MISSING
        assert read.readable is False
        assert read.reason and read.detail

    def test_unregistered_read_does_not_create_table(self):
        store.read_table("ghost_table_xyz")
        assert "ghost_table_xyz" not in store._tables

    def test_unreadable_table_reports_reason(self):
        store.register_table("unreadable_case")
        store.mark_table_unreadable("unreadable_case", "副本延迟")
        read = store.read_table("unreadable_case")
        assert read.status == READ_UNREADABLE
        assert read.readable is False
        assert "副本延迟" in read.detail
        store.mark_table_readable("unreadable_case")

    def test_registered_but_empty_is_empty(self):
        store.register_table("empty_case")
        read = store.read_table("empty_case")
        assert read.status == READ_EMPTY
        assert read.readable is True
        assert read.is_empty_table is True

    def test_read_point_is_stable_and_unique(self):
        store.register_table("point_case")
        store.rows("point_case").append({"id": 1, "v": "a"})
        first = store.read_table("point_case")
        second = store.read_table("point_case")
        assert first.status == READ_OK
        assert first.digest == second.digest
        assert first.read_point != second.read_point


# ---------------------------------------------------------------------------
# 验收场景：拼错 / 空表 / 补入相同键
# ---------------------------------------------------------------------------
class TestAcceptanceScenario:
    def test_misspelled_right_table(self, reconcile):
        store.register_table("acc_left_a")
        store.rows("acc_left_a").extend([
            {"id": 1, "amount": 20}, {"id": 2, "amount": 40},
        ])
        entry_id = _new_task("acc_left_a", "acc_rihgt_a")

        result = reconcile.execute_reconcile(entry_id)

        assert result["ok"] is False
        assert result["failed_source"] == "acc_rihgt_a"
        assert result["failed_status"] == READ_MISSING
        assert "表未登记" in result["reason"]
        assert result["retry_hint"]
        # 不把左表已有行算成单侧差异
        assert "only_left" not in result
        # 不产生虚假的差异日志，只写一条待重试失败记录
        records = _changelog_for(entry_id)
        assert [r["变更类型"] for r in records] == ["对账失败"]
        assert records[0]["变更类型"] != "对账差异"
        entry = store.find("data_reconcile", entry_id)
        assert entry["status"] == "待重试"
        assert entry["失败原因"]

    def test_right_table_real_empty_then_filled(self, reconcile):
        store.register_table("acc_left_b")
        store.register_table("acc_right_b")
        store.rows("acc_left_b").extend([
            {"id": 1, "amount": 20}, {"id": 2, "amount": 40},
        ])
        entry_id = _new_task("acc_left_b", "acc_right_b")

        empty_result = reconcile.execute_reconcile(entry_id)
        assert empty_result["ok"] is True
        assert empty_result["right_status"] == READ_EMPTY
        assert empty_result["only_left"] == 2
        assert empty_result["conclusion"] == "存在差异"

        store.rows("acc_right_b").extend([
            {"id": 1, "amount": 20}, {"id": 2, "amount": 40},
        ])
        filled_result = reconcile.execute_reconcile(entry_id)
        assert filled_result["ok"] is True
        assert filled_result["diff_count"] == 0
        assert filled_result["conclusion"] == "一致"

        types = [r["变更类型"] for r in _changelog_for(entry_id)]
        assert types == ["对账差异", "对账一致"]

    def test_real_empty_diff_not_in_retry_queue(self, reconcile):
        store.register_table("acc_left_c")
        store.register_table("acc_right_c")
        store.rows("acc_left_c").append({"id": 1, "amount": 20})
        entry_id = _new_task("acc_left_c", "acc_right_c")

        reconcile.execute_reconcile(entry_id)

        assert reconcile.pending_retry_records("acc_right_c") == []
        pending = service_for("changelog").pending_retry()
        assert all(r.get("对账任务") != entry_id for r in pending["records"])


# ---------------------------------------------------------------------------
# 暂不可读 + 恢复：读点复核
# ---------------------------------------------------------------------------
class TestUnreadableAndRecovery:
    def test_unreadable_right_registers_read_points(self, reconcile):
        store.register_table("urec_left")
        store.register_table("urec_right")
        store.rows("urec_left").append({"id": 1, "amount": 20})
        store.mark_table_unreadable("urec_right", "存储节点重启中")
        entry_id = _new_task("urec_left", "urec_right")

        result = reconcile.execute_reconcile(entry_id)

        assert result["ok"] is False
        assert result["failed_status"] == READ_UNREADABLE
        assert "暂不可读" in result["reason"]
        assert "重试" in result["retry_hint"]
        # 首轮左表真实读取的读点必须留下
        assert result["left_read_point"].startswith("urec_left@")
        record = _changelog_for(entry_id)[0]
        assert record["变更类型"] == "对账失败"
        assert record["重试状态"] == "待重试"
        assert record["左读点"] == result["left_read_point"]
        store.mark_table_readable("urec_right")

    def test_left_corrected_while_waiting_rereads_both(self, reconcile):
        store.register_table("wait_left")
        store.register_table("wait_right")
        store.rows("wait_left").append({"id": 1, "amount": 20})
        store.mark_table_unreadable("wait_right", "网络分区")
        entry_id = _new_task("wait_left", "wait_right")

        first = reconcile.execute_reconcile(entry_id)
        assert first["ok"] is False
        first_left_point = first["left_read_point"]

        # 等待期间左表金额被更正为 30
        store.rows("wait_left")[0]["amount"] = 30
        # 右表恢复，金额为 20
        store.rows("wait_right").append({"id": 1, "amount": 20})
        store.mark_table_readable("wait_right")

        recovered = reconcile.execute_reconcile(entry_id)

        assert recovered["ok"] is True
        assert recovered["left_policy"] == "reread_both"
        assert recovered["left_reused"] is False
        assert recovered["left_read_point"] != first_left_point
        # 金额 30 vs 20 属于同一业务键的字段不一致
        assert recovered["mismatches"] == 1
        assert recovered["diff_count"] == 1
        assert recovered["conclusion"] == "存在差异"

        diff_record = _changelog_for(entry_id)[-1]
        assert diff_record["变更类型"] == "对账差异"
        assert diff_record["左表沿用首轮"] is False
        assert diff_record["左侧读点策略"] == "reread_both"
        assert diff_record["左读点"] == recovered["left_read_point"]
        assert diff_record["右读点"] == recovered["right_read_point"]

    def test_unchanged_left_reuses_first_round(self, reconcile):
        store.register_table("same_left")
        store.register_table("same_right")
        store.rows("same_left").append({"id": 1, "amount": 20})
        store.mark_table_unreadable("same_right", "副本延迟")
        entry_id = _new_task("same_left", "same_right")

        first = reconcile.execute_reconcile(entry_id)
        first_left_point = first["left_read_point"]

        store.rows("same_right").append({"id": 1, "amount": 20})
        store.mark_table_readable("same_right")
        recovered = reconcile.execute_reconcile(entry_id)

        assert recovered["conclusion"] == "一致"
        assert recovered["left_policy"] == "reuse_first_round"
        assert recovered["left_reused"] is True
        assert recovered["left_read_point"] == first_left_point

        record = _changelog_for(entry_id)[-1]
        assert record["左表沿用首轮"] is True
        assert record["左读点"] == first_left_point
        assert record["右读点"] == recovered["right_read_point"]

    def test_recovery_closes_pending_todo_and_keeps_history(self, reconcile):
        store.register_table("close_left")
        store.register_table("close_right")
        store.rows("close_left").append({"id": 1, "amount": 20})
        store.mark_table_unreadable("close_right", "权限校验失败")
        entry_id = _new_task("close_left", "close_right")

        reconcile.execute_reconcile(entry_id)
        assert len(reconcile.pending_retry_records("close_right")) == 1

        store.rows("close_right").append({"id": 1, "amount": 20})
        store.mark_table_readable("close_right")
        reconcile.execute_reconcile(entry_id)

        assert reconcile.pending_retry_records("close_right") == []
        failure_record = _changelog_for(entry_id)[0]
        assert failure_record["变更类型"] == "对账失败"
        assert failure_record["重试状态"] == "已关闭"
        assert failure_record["失败原因"]
        assert failure_record["首次失败"]
        assert failure_record["关闭读点"]["left_read_point"]


# ---------------------------------------------------------------------------
# 并发相同重试
# ---------------------------------------------------------------------------
class TestConcurrentRetry:
    def test_same_retry_submitted_concurrently_logs_once(self, reconcile):
        store.register_table("conc_left")
        store.register_table("conc_right")
        store.rows("conc_left").append({"id": 1, "amount": 5})
        store.mark_table_unreadable("conc_right", "只读副本切换")
        entry_id = _new_task("conc_left", "conc_right")

        reconcile.execute_reconcile(entry_id)

        store.rows("conc_right").append({"id": 1, "amount": 5})
        store.mark_table_readable("conc_right")

        results: list[dict] = []

        def worker() -> None:
            results.append(reconcile.execute_reconcile(entry_id))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert all(r["ok"] for r in results)
        conclusions = {r["conclusion"] for r in results}
        assert conclusions == {"一致"}
        deduplicated = [r for r in results if r.get("deduplicated")]
        assert len(deduplicated) == len(results) - 1

        records = _changelog_for(entry_id)
        success_records = [r for r in records if r["变更类型"] == "对账一致"]
        assert len(success_records) == 1
        assert success_records[0]["左读点"]
        assert success_records[0]["右读点"]

    def test_new_data_change_is_reconciled_again(self, reconcile):
        store.register_table("chg_left")
        store.register_table("chg_right")
        store.rows("chg_left").extend([
            {"id": 1, "amount": 20}, {"id": 2, "amount": 40},
        ])
        store.rows("chg_right").extend([
            {"id": 1, "amount": 20}, {"id": 2, "amount": 40},
        ])
        entry_id = _new_task("chg_left", "chg_right")

        first = reconcile.execute_reconcile(entry_id)
        assert first["conclusion"] == "一致"
        assert first.get("deduplicated") is not True

        # 数据真的发生变化时，必须重新对账并写出新的结论记录
        store.rows("chg_right")[1]["amount"] = 41
        second = reconcile.execute_reconcile(entry_id)
        assert second["conclusion"] == "存在差异"
        assert second["mismatches"] == 1
        assert second["left_read_point"] != first["left_read_point"]

        types = [r["变更类型"] for r in _changelog_for(entry_id)]
        assert types == ["对账一致", "对账差异"]


# ---------------------------------------------------------------------------
# 变更日志：按失败源表复查待重试队列
# ---------------------------------------------------------------------------
class TestRetryQueue:
    def test_retry_by_failed_source_table(self, reconcile):
        store.register_table("queue_left")
        store.register_table("queue_right")
        store.rows("queue_left").append({"id": 1, "amount": 20})
        store.mark_table_unreadable("queue_right", "维护窗口")
        entry_id = _new_task("queue_left", "queue_right")

        reconcile.execute_reconcile(entry_id)

        changelog = service_for("changelog")
        pending = changelog.pending_retry("queue_right")
        assert pending["total"] == 1
        assert pending["records"][0]["失败源表"] == "queue_right"
        assert pending["records"][0]["变更类型"] == "对账失败"

        store.rows("queue_right").append({"id": 1, "amount": 20})
        store.mark_table_readable("queue_right")

        outcome = changelog.retry_pending("queue_right")
        assert outcome["attempted"] == 1
        assert outcome["closed"] == 1
        assert changelog.pending_retry("queue_right")["total"] == 0

        records = _changelog_for(entry_id)
        assert [r["变更类型"] for r in records] == ["对账失败", "对账一致"]
        assert records[0]["重试状态"] == "已关闭"
        assert records[0]["首次失败"]

    def test_retry_queue_filters_by_failed_source(self, reconcile):
        store.register_table("qf_left")
        store.register_table("qf_right_a")
        store.register_table("qf_right_b")
        store.rows("qf_left").append({"id": 1, "amount": 20})
        store.mark_table_unreadable("qf_right_a", "A 不可读")
        store.mark_table_unreadable("qf_right_b", "B 不可读")
        task_a = _new_task("qf_left", "qf_right_a")
        task_b = _new_task("qf_left", "qf_right_b")

        reconcile.execute_reconcile(task_a)
        reconcile.execute_reconcile(task_b)

        only_a = reconcile.pending_retry_records("qf_right_a")
        assert [r["对账任务"] for r in only_a] == [task_a]
        store.mark_table_readable("qf_right_a")
        store.mark_table_readable("qf_right_b")

    def test_still_unreadable_stays_pending(self, reconcile):
        store.register_table("still_left")
        store.register_table("still_right")
        store.rows("still_left").append({"id": 1, "amount": 20})
        store.mark_table_unreadable("still_right", "持续不可读")
        entry_id = _new_task("still_left", "still_right")

        reconcile.execute_reconcile(entry_id)
        outcome = reconcile.retry_pending("still_right")

        assert outcome["attempted"] == 1
        assert outcome["closed"] == 0
        assert outcome["still_pending"] == 1
        record = reconcile.pending_retry_records("still_right")[0]
        assert record["重试状态"] == "待重试"
        assert record["重试次数"] == 1
        assert record["首次失败"]
        store.mark_table_readable("still_right")


# ---------------------------------------------------------------------------
# 动作入口：前端「发起对账」走真实读取逻辑
# ---------------------------------------------------------------------------
class TestActionEntry:
    def test_run_action_executes_real_reconcile(self, reconcile):
        store.register_table("act_left")
        store.register_table("act_right")
        store.rows("act_left").append({"id": 1, "amount": 20})
        store.rows("act_right").append({"id": 1, "amount": 20})
        entry_id = _new_task("act_left", "act_right")

        entry, message = reconcile.run_action(entry_id, "发起对账")

        assert entry is not None
        assert entry["status"] == "一致"
        assert "对账完成" in message
        assert entry["读点"]["left_read_point"]
        assert entry["读点"]["right_read_point"]

    def test_run_action_surfaces_failure_reason(self, reconcile):
        store.register_table("actf_left")
        store.rows("actf_left").append({"id": 1, "amount": 20})
        entry_id = _new_task("actf_left", "actf_missing_right")

        entry, message = reconcile.run_action(entry_id, "发起对账")

        # 读取失败要如实报失败，而不是伪装成执行成功
        assert entry is None
        assert "表未登记" in message
        assert "重试" in message
        task = store.find("data_reconcile", entry_id)
        assert task["status"] == "待重试"
        assert task["失败原因"]
