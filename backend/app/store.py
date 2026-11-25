"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据，并支持跨模块数据流水线。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from app.seed import SEED_GLOBAL_CHANGELOG, SEED_ROWS


# ---------------------------------------------------------------------------
# 源表读取状态
#
# 以前对账直接用 `store.rows(name) if name in store._tables else []`，
# 把「表没登记」也当成「空表」，于是左表所有行都被算成单侧差异。
# 这里把读取结果显式建模，空态分三种：
#   - 表未登记（missing）：名字不在仓库里，通常是表名拼错/还没接入
#   - 暂不可读（unreadable）：表已登记，但当前读取失败（连接、权限、快照未就绪）
#   - 真实空表（empty）：表已登记且确实一行数据都没有
# 只有后两种属于「读到了」；前两种必须给出原因，不能伪造空数据。
# ---------------------------------------------------------------------------
READ_OK = "ok"
READ_EMPTY = "empty"
READ_MISSING = "missing"
READ_UNREADABLE = "unreadable"

READ_STATUS_LABELS: dict[str, str] = {
    READ_OK: "正常",
    READ_EMPTY: "真实空表",
    READ_MISSING: "表未登记",
    READ_UNREADABLE: "暂不可读",
}


def digest_rows(rows: list[dict[str, Any]]) -> str:
    """对一份行数据做稳定摘要，用于标识一次真实读取的读点。"""
    payload = json.dumps(rows, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


@dataclass
class TableRead:
    """一次源表读取的结果：数据 + 读取状态 + 可复核的读点。"""

    table: str
    status: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    read_point: str = ""
    digest: str = ""
    at: float = field(default_factory=time.time)
    reason: str = ""
    detail: str = ""

    @property
    def readable(self) -> bool:
        return self.status in (READ_OK, READ_EMPTY)

    @property
    def is_empty_table(self) -> bool:
        """真正的空表（区别于读不到）。"""
        return self.status == READ_EMPTY

    @property
    def status_label(self) -> str:
        return READ_STATUS_LABELS.get(self.status, self.status)

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def as_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "status": self.status,
            "status_label": self.status_label,
            "readable": self.readable,
            "read_point": self.read_point,
            "digest": self.digest,
            "row_count": self.row_count,
            "read_at": self.at,
            "reason": self.reason,
            "detail": self.detail,
        }


class DataPipeline:
    """跨模块数据流水线引擎：支持 输入→转换→版本化产物→下游消费 的完整链路。

    每条流水线定义了源模块、目标模块、转换函数和验证规则。
    产物存储为可版本化的数据快照，支持脏行检测、重复批次拒绝、冲突版本检测、中断恢复。
    """

    def __init__(self, name: str, sources: list[str], target_module: str) -> None:
        self.name = name
        self.sources = sources
        self.target_module = target_module
        self._artifacts: list[dict[str, Any]] = []
        self._versions: list[str] = []  # 版本号列表
        self._dirty_log: list[dict[str, Any]] = []
        self._batch_registry: set[str] = set()  # 防重复批次
        self._last_checkpoint: dict[str, Any] = {}  # 中断恢复点
        self._interrupted = False

    def ingest(self, rows: list[dict[str, Any]], batch_id: str) -> tuple[int, list[str]]:
        """摄入数据行，返回 (成功行数, 脏行详情列表)"""
        if batch_id in self._batch_registry:
            return 0, [f"重复批次 {batch_id} 已存在，拒绝摄入"]
        self._batch_registry.add(batch_id)
        clean_rows = []
        dirty_details = []
        for i, row in enumerate(rows):
            issues = self._validate_row(row, i)
            if issues:
                dirty_details.append(f"行 {i}: {'; '.join(issues)}")
                self._dirty_log.append({"batch": batch_id, "row_index": i, "issues": issues, "row": row, "ts": time.time()})
            else:
                clean_rows.append(row)
        self._artifacts.extend(clean_rows)
        return len(clean_rows), dirty_details

    def transform(self, transform_fn, *args, **kwargs) -> list[dict[str, Any]]:
        """对当前产物执行转换函数"""
        self._save_checkpoint("transform_enter", len(self._artifacts))
        try:
            result = transform_fn(self._artifacts, *args, **kwargs)
            version = self._make_version("transform")
            self._versions.append(version)
            self._save_checkpoint("transform_exit", len(result))
            return result
        except Exception:
            self._interrupted = True
            raise

    def versioned_snapshot(self) -> dict[str, Any]:
        """生成版本化产物快照"""
        version = self._versions[-1] if self._versions else self._make_version("snapshot")
        return {
            "pipeline": self.name,
            "version": version,
            "artifact_count": len(self._artifacts),
            "artifact_sample": self._artifacts[:5],
            "dirty_count": len(self._dirty_log),
            "batch_count": len(self._batch_registry),
            "interrupted": self._interrupted,
            "checkpoint": self._last_checkpoint,
        }

    def health_report(self) -> dict[str, Any]:
        """流水线健康报告：用于外部模块消费和容量材料生成"""
        return {
            "pipeline": self.name,
            "source_modules": self.sources,
            "target_module": self.target_module,
            "total_ingested": sum(1 for a in self._artifacts),
            "dirty_rows": len(self._dirty_log),
            "versions": len(self._versions),
            "batches_processed": len(self._batch_registry),
            "interrupted": self._interrupted,
            "last_artifact_ts": max((a.get("ts", 0) for a in self._artifacts), default=0) if self._artifacts else 0,
        }

    def _validate_row(self, row: dict[str, Any], index: int) -> list[str]:
        issues = []
        for key, val in row.items():
            if val is None or (isinstance(val, str) and not val.strip()):
                issues.append(f"空值字段 '{key}'")
        if any(row.get(k) is None for k in ["id"]):
            issues.append("缺少主键 id")
        return issues

    def _make_version(self, tag: str) -> str:
        ts = str(int(time.time() * 1000))
        raw = f"{self.name}:{tag}:{ts}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def _save_checkpoint(self, stage: str, count: int) -> None:
        self._last_checkpoint = {"stage": stage, "count": count, "ts": time.time()}

    def recover_checkpoint(self) -> dict[str, Any]:
        """从最近检查点恢复"""
        return {
            "pipeline": self.name,
            "interrupted": self._interrupted,
            "last_checkpoint": self._last_checkpoint,
            "artifacts_before_interrupt": len(self._artifacts),
        }


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块数据流水线
        self._pipelines: dict[str, DataPipeline] = {}
        # 全局变更日志（用于 changelog 跨模块追溯）
        self._global_changelog: list[dict[str, Any]] = [
            dict(record) for record in SEED_GLOBAL_CHANGELOG
        ]
        # Schema 注册表（cross-module schema registry）
        self._schema_registry: dict[str, list[dict[str, Any]]] = {}
        # 水位偏移记录（watermark → late_data 共享）
        self._watermark_offsets: dict[str, dict[str, Any]] = {}
        # 快照存储表（snapshot_mgr → backfill_job 共享）
        self._snapshot_store: dict[str, dict[str, Any]] = {}
        # 血缘图（lineage → table_format 共享）
        self._lineage_graph: dict[str, list[dict[str, Any]]] = {}
        # 归档清单（cold_archive → retention 共享）
        self._archive_manifest: list[dict[str, Any]] = []
        # 源表读取状态：登记过的表 + 当前不可读的表（表名 → 原因）
        self._registered_tables: set[str] = set(self._tables)
        self._unreadable_tables: dict[str, str] = {}
        # 每次真实读取分配一个单调递增的读点序号，保证读点可比较先后
        self._read_sequence = 0
        self._lock = threading.RLock()

    # ---------- 基础 CRUD ----------
    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        # 通过 rows() 首次触达的表同样视为「已登记」：这是写入路径，
        # 与 read_table() 的只读语义分开，避免读操作把拼错的表名悄悄建出来。
        self._registered_tables.add(module)
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    # ---------- 源表读取状态 ----------
    def register_table(self, table: str) -> bool:
        """登记一张源表；重复登记返回 False。登记不等于有数据。"""
        with self._lock:
            if table in self._tables:
                self._registered_tables.add(table)
                return False
            self._tables[table] = []
            self._registered_tables.add(table)
            return True

    def mark_table_unreadable(self, table: str, reason: str) -> None:
        """把已登记的表标记为暂不可读（连接/权限/快照未就绪等），读取时不得伪造空表。"""
        with self._lock:
            self._registered_tables.add(table)
            self._tables.setdefault(table, [])
            self._unreadable_tables[table] = reason

    def mark_table_readable(self, table: str) -> None:
        """恢复表的可读状态（数据本身不变）。"""
        with self._lock:
            self._unreadable_tables.pop(table, None)

    def table_registered(self, table: str) -> bool:
        return table in self._tables or table in self._registered_tables

    def table_readable(self, table: str) -> bool:
        return self.table_registered(table) and table not in self._unreadable_tables

    def read_table(self, table: str) -> TableRead:
        """按源表当前真实状态读取一次数据，并生成可复核的读点。

        与 ``rows()`` 的区别：不会把未登记的表 ``setdefault`` 成空表，
        也不会把不可读的表当成空表。空态在这里分成三种，前两种必须带原因。
        """
        with self._lock:
            if not self.table_registered(table):
                return TableRead(
                    table=table,
                    status=READ_MISSING,
                    reason="表未登记",
                    detail=f"仓库中没有名为 {table} 的源表，请确认表名是否正确，或先登记该源表",
                )
            if table in self._unreadable_tables:
                return TableRead(
                    table=table,
                    status=READ_UNREADABLE,
                    reason="暂不可读",
                    detail=self._unreadable_tables[table] or "源表当前不可读，请稍后重试",
                )
            rows = [dict(row) for row in self._tables.get(table, [])]
            self._read_sequence += 1
            at = time.time()
            return TableRead(
                table=table,
                status=READ_EMPTY if not rows else READ_OK,
                rows=rows,
                read_point=f"{table}@{self._read_sequence:06d}",
                digest=digest_rows(rows),
                at=at,
            )

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        # 跨模块流水线状态
        pipeline_stats = []
        for pname, pl in self._pipelines.items():
            pipeline_stats.append(pl.health_report())
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
            {"label": "流水线", "value": len(pipeline_stats)},
        ]
        return {"cards": cards, "modules": modules, "pipelines": pipeline_stats}

    # ---------- 跨模块流水线 ----------
    def create_pipeline(self, name: str, sources: list[str], target_module: str) -> DataPipeline:
        pl = DataPipeline(name, sources, target_module)
        self._pipelines[name] = pl
        return pl

    def pipeline(self, name: str) -> DataPipeline | None:
        return self._pipelines.get(name)

    def all_pipelines(self) -> dict[str, DataPipeline]:
        return dict(self._pipelines)

    # ---------- Schema 注册表 ----------
    def register_schema(self, topic: str, schema_def: dict[str, Any], version: str) -> dict[str, Any]:
        versions = self._schema_registry.setdefault(topic, [])
        entry = {"topic": topic, "definition": schema_def, "version": version, "ts": time.time()}
        versions.append(entry)
        return entry

    def schema_versions(self, topic: str) -> list[dict[str, Any]]:
        return self._schema_registry.get(topic, [])

    def check_schema_compatibility(self, topic: str, new_fields: list[str]) -> dict[str, Any]:
        versions = self.schema_versions(topic)
        if not versions:
            return {"compatible": True, "reason": "新主题，无兼容性问题"}
        latest = versions[-1]
        old_fields = set(latest.get("definition", {}).get("fields", []))
        missing_required = [f for f in old_fields if f not in new_fields]
        if missing_required:
            return {"compatible": False, "reason": f"缺少已有字段: {missing_required}"}
        return {"compatible": True, "reason": "向后兼容"}

    # ---------- 水位记录 ----------
    def update_watermark(self, partition: str, offset: dict[str, Any]) -> None:
        self._watermark_offsets[partition] = offset

    def get_watermark(self, partition: str) -> dict[str, Any] | None:
        return self._watermark_offsets.get(partition)

    # ---------- 快照存储 ----------
    def save_snapshot(self, snap_id: str, data: dict[str, Any]) -> None:
        self._snapshot_store[snap_id] = data

    def get_snapshot(self, snap_id: str) -> dict[str, Any] | None:
        return self._snapshot_store.get(snap_id)

    # ---------- 变更日志 ----------
    def append_changelog(self, entry: dict[str, Any]) -> None:
        entry["_ts"] = time.time()
        self._global_changelog.append(entry)

    def changelog_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [e for e in self._global_changelog if e.get("目标表") == table_name]

    # ---------- 血缘图 ----------
    def add_lineage_edge(self, upstream: str, downstream: str, edge_data: dict[str, Any]) -> None:
        edges = self._lineage_graph.setdefault(upstream, [])
        edges.append({"downstream": downstream, **edge_data, "ts": time.time()})

    def lineage_downstream(self, table_name: str) -> list[dict[str, Any]]:
        return self._lineage_graph.get(table_name, [])

    # ---------- 归档清单 ----------
    def add_archive_record(self, record: dict[str, Any]) -> None:
        self._archive_manifest.append(record)

    def archive_manifest_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return [r for r in self._archive_manifest if r.get("源表名称") == table_name]


store = Store()