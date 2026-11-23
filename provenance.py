"""Timing-source provenance: follow forwarded timing events upstream.

Two evidence tables are imported per batch:

* a *reading link* table (``lap``, ``source``, ``event_id``) that ties each
  primary/backup reading to the event it received;
* an *event registry* table (``event_id`` + exactly one of
  ``upstream_event_id`` / ``raw_device_id``) that records forwarding relations.

Following ``upstream_event_id`` from a linked event eventually reaches an event
that carries a ``raw_device_id``.  That event is the *original event*, and the
device is the physical timing source.  Two readings of one lap sharing the same
device are flagged as "same device"; two different devices as "two devices";
anything that cannot be traced is "insufficient evidence".

The tables are append-only supplements.  A supplement may add a missing row or
repeat an identical one, but changing an already registered relation is a
conflict and rejects the whole upload.  A supplement may also close a cycle in
the merged event graph; that is likewise rejected without touching the stored
evidence or the per-lap judgement.

This module has no database or Flask dependency; :mod:`app` loads the stored
evidence and persists the report.
"""

from __future__ import annotations

import csv
import io
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

MAX_CSV_BYTES = 2 * 1024 * 1024
MAX_ROWS = 20_000
MAX_ID_LENGTH = 200
MAX_LAP = 500

SAME_DEVICE = "SAME_DEVICE"
TWO_DEVICES = "TWO_DEVICES"
INSUFFICIENT = "INSUFFICIENT"

PROVENANCE_LABELS = {
    SAME_DEVICE: "同一台设备",
    TWO_DEVICES: "两台设备",
    INSUFFICIENT: "证据不全",
}

PROVENANCE_TONE = {
    SAME_DEVICE: "bad",
    TWO_DEVICES: "good",
    INSUFFICIENT: "warn",
}

PRIMARY_SOURCE = "主计时"
BACKUP_SOURCE = "备计时"

SOURCE_ALIASES = {
    "主": PRIMARY_SOURCE,
    "主表": PRIMARY_SOURCE,
    "主计时": PRIMARY_SOURCE,
    "primary": PRIMARY_SOURCE,
    "main": PRIMARY_SOURCE,
    "备": BACKUP_SOURCE,
    "备表": BACKUP_SOURCE,
    "备计时": BACKUP_SOURCE,
    "backup": BACKUP_SOURCE,
    "secondary": BACKUP_SOURCE,
}


class ProvenanceError(ValueError):
    """Raised when an upload cannot be parsed or does not fit the batch."""


class ProvenanceConflict(ProvenanceError):
    """Raised when a supplement must reject the whole upload."""


def normalise_source(raw: str) -> str:
    cleaned = " ".join(raw.strip().split())
    if not cleaned:
        raise ProvenanceError("计时来源不能为空")
    return SOURCE_ALIASES.get(cleaned.casefold(), cleaned)


LINK_HEADER_ALIASES = {
    "lap": {"lap", "lap_number", "lapno", "圈号", "圈"},
    "source": {"source", "timer", "计时来源", "来源"},
    "event_id": {
        "event_id",
        "event",
        "event_no",
        "event_number",
        "事件号",
        "事件编号",
        "计时事件号",
    },
}

EVENT_HEADER_ALIASES = {
    "event_id": {
        "event_id",
        "event",
        "event_no",
        "event_number",
        "事件号",
        "事件编号",
        "计时事件号",
    },
    "upstream_event_id": {
        "upstream_event_id",
        "upstream_event",
        "upstream",
        "parent_event_id",
        "上游事件号",
        "上游事件",
        "上游编号",
        "上游事件编号",
    },
    "raw_device_id": {
        "raw_device_id",
        "raw_device",
        "device_id",
        "device",
        "原始设备号",
        "原始设备",
        "原始设备编号",
        "设备号",
        "设备编号",
    },
}


def _decode(raw: bytes) -> str:
    if len(raw) > MAX_CSV_BYTES:
        raise ProvenanceError("CSV 文件过大（上限 2 MB）")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise ProvenanceError("CSV 必须使用 UTF-8 编码") from None
    if not text.strip():
        raise ProvenanceError("CSV 文件为空")
    return text


def _find_header_mapping(
    fieldnames: Sequence[str] | None,
    aliases: Mapping[str, set[str]],
    required: Iterable[str],
) -> dict[str, str]:
    if not fieldnames:
        raise ProvenanceError("CSV 缺少表头")
    cleaned = {name: (name or "").strip().casefold() for name in fieldnames}
    mapping: dict[str, str] = {}
    for canonical, candidates in aliases.items():
        matches = [name for name, lowered in cleaned.items() if lowered in candidates]
        if len(matches) > 1:
            raise ProvenanceError(f"CSV 表头重复：{canonical} 对应多个列")
        if matches:
            mapping[canonical] = matches[0]
    missing = [key for key in required if key not in mapping]
    if missing:
        raise ProvenanceError("CSV 缺少必需列：" + "、".join(missing))
    return mapping


def _open_csv(
    text: str,
    aliases: Mapping[str, set[str]],
    required: Iterable[str],
) -> tuple[csv.DictReader, dict[str, str]]:
    last_error: ProvenanceError | None = None
    for delimiter in (",", ";", "\t"):
        candidate = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        try:
            mapping = _find_header_mapping(candidate.fieldnames, aliases, required)
        except ProvenanceError as exc:
            last_error = exc
            continue
        return candidate, mapping
    if last_error is not None:
        raise last_error
    raise ProvenanceError("无法识别 CSV 表头")


def _clean_id(raw: str, label: str, row_number: int) -> str:
    cleaned = raw.strip()
    if not cleaned:
        raise ProvenanceError(f"第 {row_number} 行{label}不能为空")
    if len(cleaned) > MAX_ID_LENGTH:
        raise ProvenanceError(f"第 {row_number} 行{label}过长（上限 {MAX_ID_LENGTH} 字符）")
    return cleaned


def _cell(row: Mapping[str, str | None], column: str | None) -> str:
    if column is None:
        return ""
    return (row.get(column) or "").strip()


def parse_link_csv(raw: bytes) -> list[dict]:
    """Parse the reading-link table into ``{lap, source, event_id}`` rows."""

    text = _decode(raw)
    reader, mapping = _open_csv(
        text, LINK_HEADER_ALIASES, ("lap", "source", "event_id")
    )

    entries: dict[tuple[int, str], dict] = {}
    for row_number, row in enumerate(reader, start=2):
        if row.get(None):
            raise ProvenanceError(f"第 {row_number} 行多出无法识别的列")
        lap_text = _cell(row, mapping.get("lap"))
        source_text = _cell(row, mapping.get("source"))
        event_text = _cell(row, mapping.get("event_id"))
        if not lap_text and not source_text and not event_text:
            continue

        try:
            lap = int(lap_text)
        except ValueError:
            raise ProvenanceError(
                f"第 {row_number} 行圈号不是整数：{lap_text!r}"
            ) from None
        if lap < 1 or lap > MAX_LAP:
            raise ProvenanceError(f"第 {row_number} 行圈号超出范围（1-{MAX_LAP}）")

        source = normalise_source(source_text)
        event_id = _clean_id(event_text, "事件号", row_number)

        key = (lap, source)
        if key in entries:
            if entries[key]["event_id"] == event_id:
                continue
            raise ProvenanceConflict(
                f"第 {row_number} 行与前面的第 {lap} 圈 {source} 关联冲突："
                f"已写事件 {entries[key]['event_id']}，本行事件 {event_id}"
            )
        entries[key] = {"lap": lap, "source": source, "event_id": event_id}
        if len(entries) > MAX_ROWS:
            raise ProvenanceError(f"关联记录超过 {MAX_ROWS} 条上限")

    if not entries:
        raise ProvenanceError("关联表没有有效数据行")
    return list(entries.values())


def parse_event_csv(raw: bytes) -> list[dict]:
    """Parse the event registry into ``{event_id, upstream_event_id, raw_device_id}``."""

    text = _decode(raw)
    reader, mapping = _open_csv(text, EVENT_HEADER_ALIASES, ("event_id",))

    entries: dict[str, dict] = {}
    for row_number, row in enumerate(reader, start=2):
        if row.get(None):
            raise ProvenanceError(f"第 {row_number} 行多出无法识别的列")
        event_text = _cell(row, mapping.get("event_id"))
        upstream_text = _cell(row, mapping.get("upstream_event_id"))
        device_text = _cell(row, mapping.get("raw_device_id"))
        if not event_text and not upstream_text and not device_text:
            continue

        event_id = _clean_id(event_text, "事件号", row_number)
        upstream = (
            _clean_id(upstream_text, "上游事件号", row_number) if upstream_text else None
        )
        device = (
            _clean_id(device_text, "原始设备号", row_number) if device_text else None
        )
        if (upstream is None) == (device is None):
            raise ProvenanceError(
                f"第 {row_number} 行必须且只能填写上游事件号或原始设备号之一"
            )

        entry = {
            "event_id": event_id,
            "upstream_event_id": upstream,
            "raw_device_id": device,
        }
        if event_id in entries:
            previous = entries[event_id]
            if (
                previous["upstream_event_id"] == upstream
                and previous["raw_device_id"] == device
            ):
                continue
            raise ProvenanceConflict(
                f"第 {row_number} 行与前面的记录冲突：事件 {event_id} "
                "在同一个文件中登记了两个不同的上游关系"
            )
        entries[event_id] = entry
        if len(entries) > MAX_ROWS:
            raise ProvenanceError(f"事件记录超过 {MAX_ROWS} 条上限")

    if not entries:
        raise ProvenanceError("事件登记表没有有效数据行")
    return list(entries.values())


def _relation_text(upstream: str | None, device: str | None) -> str:
    if device is not None:
        return f"原始设备 {device}"
    return f"上游事件 {upstream}"


def _entry_relation_text(entry: Mapping[str, str | None]) -> str:
    return _relation_text(entry.get("upstream_event_id"), entry.get("raw_device_id"))


def find_event_cycle(events: Mapping[str, Mapping[str, str | None]]) -> list[str] | None:
    """Return one cycle (with the start repeated at the end), or ``None``."""

    state: dict[str, int] = {}  # 0 = on the current path, 1 = finished
    for start in events:
        if start in state:
            continue
        path: list[str] = []
        positions: dict[str, int] = {}
        current: str | None = start
        while (
            current is not None
            and current in events
            and current not in state
        ):
            positions[current] = len(path)
            path.append(current)
            state[current] = 0
            current = events[current].get("upstream_event_id")
        if current is not None and current in events and state.get(current) == 0:
            return path[positions[current]:] + [current]
        for node in path:
            state[node] = 1
    return None


def merge_event_entries(
    existing: Mapping[str, Mapping[str, str | None]],
    incoming: Iterable[Mapping[str, str | None]],
) -> tuple[dict[str, dict], list[dict], int]:
    """Merge registry supplements, rejecting conflicts and cycles.

    Returns ``(merged_graph, added_rows, unchanged_count)``.  Nothing is
    written to the database here, so a rejection leaves stored evidence
    untouched.
    """

    merged: dict[str, dict] = {
        event_id: {
            "event_id": event_id,
            "upstream_event_id": row.get("upstream_event_id"),
            "raw_device_id": row.get("raw_device_id"),
        }
        for event_id, row in existing.items()
    }
    added: list[dict] = []
    unchanged = 0

    for entry in incoming:
        event_id = entry["event_id"]
        if event_id in merged:
            previous = merged[event_id]
            if (
                previous["upstream_event_id"] == entry.get("upstream_event_id")
                and previous["raw_device_id"] == entry.get("raw_device_id")
            ):
                unchanged += 1
                continue
            raise ProvenanceConflict(
                f"事件 {event_id} 已登记为{_entry_relation_text(previous)}，"
                f"补交为{_entry_relation_text(entry)}，与已登记关系冲突。"
                "整次补交已拒收，原证据与判定保持不变。"
            )
        row = {
            "event_id": event_id,
            "upstream_event_id": entry.get("upstream_event_id"),
            "raw_device_id": entry.get("raw_device_id"),
        }
        merged[event_id] = row
        added.append(row)

    cycle = find_event_cycle(merged)
    if cycle is not None:
        raise ProvenanceConflict(
            "合并后事件链成环：" + " → ".join(cycle) + "。"
            "整次补交已拒收，原证据与判定保持不变。"
        )

    return merged, added, unchanged


def merge_link_entries(
    existing: Mapping[tuple[int, str], str],
    incoming: Iterable[Mapping[str, object]],
) -> tuple[dict[tuple[int, str], str], list[dict], int]:
    """Merge reading-link supplements, rejecting conflicting links."""

    merged = dict(existing)
    added: list[dict] = []
    unchanged = 0
    for entry in incoming:
        key = (int(entry["lap"]), str(entry["source"]))
        event_id = str(entry["event_id"])
        if key in merged:
            if merged[key] == event_id:
                unchanged += 1
                continue
            raise ProvenanceConflict(
                f"第 {key[0]} 圈 {key[1]} 已关联事件 {merged[key]}，"
                f"补交事件 {event_id} 与已登记关系冲突。"
                "整次补交已拒收，原证据与判定保持不变。"
            )
        merged[key] = event_id
        added.append(dict(entry))
    return merged, added, unchanged


@dataclass(frozen=True)
class ResolvedChain:
    """The upstream path from a linked event to its original event."""

    chain: tuple[dict, ...]
    original_event_id: str | None = None
    raw_device_id: str | None = None
    problem: str | None = None

    @property
    def resolved(self) -> bool:
        return self.original_event_id is not None

    def describe(self) -> str:
        if not self.chain:
            return self.problem or "事件未登记"
        path = " → ".join(link["event_id"] for link in self.chain)
        if self.resolved:
            return (
                f"{path}（原始事件 {self.original_event_id} · "
                f"设备 {self.raw_device_id}）"
            )
        return f"{path}（{self.problem}）"


def resolve_chain(
    event_id: str, events: Mapping[str, Mapping[str, str | None]]
) -> ResolvedChain:
    chain: list[dict] = []
    seen: set[str] = set()
    current: str | None = event_id

    while current is not None:
        if current in seen:
            return ResolvedChain(
                tuple(chain), problem=f"事件链成环（{current}）"
            )
        row = events.get(current)
        if row is None:
            return ResolvedChain(tuple(chain), problem=f"事件 {current} 未登记")
        seen.add(current)
        chain.append(
            {
                "event_id": current,
                "upstream_event_id": row.get("upstream_event_id"),
                "raw_device_id": row.get("raw_device_id"),
            }
        )
        if row.get("raw_device_id") is not None:
            return ResolvedChain(
                tuple(chain),
                original_event_id=current,
                raw_device_id=row["raw_device_id"],
            )
        current = row.get("upstream_event_id")

    # The CHECK constraint and parser make this unreachable for stored data.
    return ResolvedChain(tuple(chain), problem="事件未登记原始设备")


@dataclass(frozen=True)
class ReadingChain:
    reading_id: int
    source: str
    cumulative_ms: int
    event_id: str | None
    chain: ResolvedChain | None

    @property
    def resolved(self) -> bool:
        return self.chain is not None and self.chain.resolved

    @property
    def raw_device_id(self) -> str | None:
        return self.chain.raw_device_id if self.chain is not None else None

    @property
    def problem(self) -> str:
        if self.chain is None:
            return "未关联事件"
        return self.chain.problem or ""

    @property
    def chain_text(self) -> str:
        if self.event_id is None:
            return "未关联事件"
        assert self.chain is not None
        return self.chain.describe()


@dataclass(frozen=True)
class LapProvenance:
    lap_number: int
    status: str
    detail: str
    primary: ReadingChain | None
    backup: ReadingChain | None
    readings: tuple[ReadingChain, ...]

    @property
    def extra_readings(self) -> tuple[ReadingChain, ...]:
        return tuple(
            reading
            for reading in self.readings
            if reading.source not in (PRIMARY_SOURCE, BACKUP_SOURCE)
        )


@dataclass(frozen=True)
class CrossLapReference:
    original_event_id: str
    raw_device_id: str | None
    entries: tuple[dict, ...]


@dataclass(frozen=True)
class ProvenanceReport:
    laps: tuple[LapProvenance, ...]
    cross_lap: tuple[CrossLapReference, ...]
    events: tuple[dict, ...]
    linked_readings: int
    total_readings: int


def _assess_lap(
    primary: ReadingChain | None, backup: ReadingChain | None
) -> tuple[str, str]:
    if primary is None and backup is None:
        return INSUFFICIENT, "缺少主计时和备计时读数"
    if primary is None:
        return INSUFFICIENT, "缺少主计时读数"
    if backup is None:
        return INSUFFICIENT, "缺少备计时读数"

    problems: list[str] = []
    if not primary.resolved:
        problems.append(f"主计时链未追到原始事件（{primary.problem}）")
    if not backup.resolved:
        problems.append(f"备计时链未追到原始事件（{backup.problem}）")
    if problems:
        return INSUFFICIENT, "；".join(problems)

    if primary.raw_device_id == backup.raw_device_id:
        return (
            SAME_DEVICE,
            f"主备读数都追溯到设备 {primary.raw_device_id}",
        )
    return (
        TWO_DEVICES,
        f"主计时设备 {primary.raw_device_id}；备计时设备 {backup.raw_device_id}",
    )


def build_report(
    laps: Sequence[Mapping[str, object]],
    links: Mapping[int, str],
    events: Mapping[str, Mapping[str, str | None]],
) -> ProvenanceReport:
    """Resolve every reading and summarise per-lap and cross-lap evidence.

    ``laps`` uses the same shape as ``app._readings_by_lap``: each item has
    ``lap_number`` and ``readings`` (``id``, ``source``, ``cumulative_ms``).
    """

    lap_reports: list[LapProvenance] = []
    by_original: dict[str, list[dict]] = defaultdict(list)
    linked_readings = 0
    total_readings = 0

    for lap in laps:
        lap_number = int(lap["lap_number"])
        chains: list[ReadingChain] = []
        for reading in lap["readings"]:
            total_readings += 1
            reading_id = int(reading["id"])
            event_id = links.get(reading_id)
            if event_id is not None:
                linked_readings += 1
                chain = resolve_chain(event_id, events)
            else:
                chain = None
            reading_chain = ReadingChain(
                reading_id=reading_id,
                source=str(reading["source"]),
                cumulative_ms=int(reading["cumulative_ms"]),
                event_id=event_id,
                chain=chain,
            )
            chains.append(reading_chain)
            if reading_chain.resolved:
                by_original[reading_chain.chain.original_event_id].append(  # type: ignore[index]
                    {
                        "lap_number": lap_number,
                        "source": reading_chain.source,
                        "cumulative_ms": reading_chain.cumulative_ms,
                        "event_id": reading_chain.event_id,
                        "raw_device_id": reading_chain.raw_device_id,
                        "chain_text": reading_chain.chain_text,
                    }
                )

        primary = next(
            (chain for chain in chains if chain.source == PRIMARY_SOURCE), None
        )
        backup = next(
            (chain for chain in chains if chain.source == BACKUP_SOURCE), None
        )
        status, detail = _assess_lap(primary, backup)
        lap_reports.append(
            LapProvenance(
                lap_number=lap_number,
                status=status,
                detail=detail,
                primary=primary,
                backup=backup,
                readings=tuple(chains),
            )
        )

    cross_lap: list[CrossLapReference] = []
    for original_event_id, entries in sorted(by_original.items()):
        lap_numbers = {entry["lap_number"] for entry in entries}
        if len(lap_numbers) < 2:
            continue
        entries.sort(key=lambda item: (item["lap_number"], item["source"]))
        cross_lap.append(
            CrossLapReference(
                original_event_id=original_event_id,
                raw_device_id=entries[0].get("raw_device_id"),
                entries=tuple(entries),
            )
        )
    event_rows = tuple(
        {
            "event_id": event_id,
            "upstream_event_id": row.get("upstream_event_id"),
            "raw_device_id": row.get("raw_device_id"),
            "relation_text": _entry_relation_text(row),
        }
        for event_id, row in sorted(events.items())
    )

    return ProvenanceReport(
        laps=tuple(lap_reports),
        cross_lap=tuple(cross_lap),
        events=event_rows,
        linked_readings=linked_readings,
        total_readings=total_readings,
    )
