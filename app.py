"""Lap-record review application."""

from __future__ import annotations

import csv
import io
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from flask import (
    Flask,
    flash,
    g,
    redirect,
    render_template,
    request,
    url_for,
)

from provenance import (
    PROVENANCE_LABELS,
    PROVENANCE_TONE,
    ProvenanceConflict,
    ProvenanceError,
    build_report,
    merge_event_entries,
    merge_link_entries,
    parse_event_csv,
    parse_link_csv,
)
from solver import (
    BREAKS,
    INFEASIBLE,
    NOT_BREAK,
    REVIEW,
    STATUS_LABELS,
    solve,
)

MAX_CSV_BYTES = 2 * 1024 * 1024
# The import form carries one CSV; the provenance form may carry two.
MAX_REQUEST_BYTES = 2 * MAX_CSV_BYTES + 512 * 1024  # multipart overhead
MAX_READINGS = 20_000

HEADER_ALIASES = {
    "lap": {"lap", "lap_number", "lapno", "圈号", "圈"},
    "source": {"source", "timer", "计时来源", "来源"},
    "cumulative_ms": {
        "cumulative_ms",
        "cumulative",
        "elapsed_ms",
        "发车后累计毫秒数",
        "累计毫秒",
        "累计毫秒数",
    },
}

SOURCE_ALIASES = {
    "主": "主计时",
    "主表": "主计时",
    "主计时": "主计时",
    "primary": "主计时",
    "main": "主计时",
    "备": "备计时",
    "备表": "备计时",
    "备计时": "备计时",
    "backup": "备计时",
    "secondary": "备计时",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    min_lap_ms INTEGER NOT NULL CHECK (min_lap_ms > 0),
    record_ms INTEGER NOT NULL CHECK (record_ms >= 0),
    revision INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS laps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES sessions(id),
    lap_number INTEGER NOT NULL,
    pinned_reading_id INTEGER REFERENCES readings(id),
    UNIQUE (session_id, lap_number)
);

CREATE TABLE IF NOT EXISTS readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lap_id INTEGER NOT NULL REFERENCES laps(id),
    source TEXT NOT NULL,
    cumulative_ms INTEGER NOT NULL CHECK (cumulative_ms >= 0),
    UNIQUE (lap_id, source)
);

CREATE TABLE IF NOT EXISTS conclusions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES sessions(id),
    revision INTEGER NOT NULL,
    lower_ms INTEGER,
    upper_ms INTEGER,
    status TEXT NOT NULL,
    min_lap_ms INTEGER NOT NULL,
    record_ms INTEGER NOT NULL,
    reason TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS approvals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES sessions(id),
    lap_number INTEGER NOT NULL,
    reading_id INTEGER,
    source TEXT,
    cumulative_ms INTEGER,
    revision INTEGER NOT NULL,
    lower_ms INTEGER,
    upper_ms INTEGER,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- Evidence for the timing-source audit.  Both tables are append-only: a
-- supplement may add rows but never rewrite an existing relation.
CREATE TABLE IF NOT EXISTS timing_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES sessions(id),
    event_id TEXT NOT NULL,
    upstream_event_id TEXT,
    raw_device_id TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (session_id, event_id),
    CHECK ((upstream_event_id IS NULL) <> (raw_device_id IS NULL))
);

CREATE TABLE IF NOT EXISTS reading_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reading_id INTEGER NOT NULL REFERENCES readings(id),
    event_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (reading_id)
);

CREATE TABLE IF NOT EXISTS provenance_uploads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES sessions(id),
    kind TEXT NOT NULL,
    added_links INTEGER NOT NULL DEFAULT 0,
    added_events INTEGER NOT NULL DEFAULT 0,
    unchanged_rows INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS provenance_judgements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL REFERENCES sessions(id),
    upload_id INTEGER NOT NULL REFERENCES provenance_uploads(id),
    lap_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


class CsvError(ValueError):
    """Raised when an uploaded CSV cannot be accepted."""


class ReviewConflict(Exception):
    """Raised when a review submission must not change stored state."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect(path: str, *, initialise: bool = False) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    if initialise:
        # WAL is persistent; setting it on every request would take an extra
        # write lock for no benefit.
        connection.execute("PRAGMA journal_mode = WAL")
    return connection


@contextmanager
def _transaction(connection: sqlite3.Connection):
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    else:
        connection.execute("COMMIT")


def _normalise_source(raw: str) -> str:
    cleaned = " ".join(raw.strip().split())
    if not cleaned:
        raise CsvError("计时来源不能为空")
    return SOURCE_ALIASES.get(cleaned.casefold(), cleaned)


def _find_header_mapping(fieldnames: list[str] | None) -> dict[str, str]:
    if not fieldnames:
        raise CsvError("CSV 缺少表头")
    cleaned = {name: (name or "").strip().casefold() for name in fieldnames}
    mapping: dict[str, str] = {}
    for canonical, aliases in HEADER_ALIASES.items():
        matches = [name for name, lowered in cleaned.items() if lowered in aliases]
        if len(matches) > 1:
            raise CsvError(f"CSV 表头重复：{canonical} 对应多个列")
        if matches:
            mapping[canonical] = matches[0]
    missing = [key for key in HEADER_ALIASES if key not in mapping]
    if missing:
        raise CsvError(
            "CSV 缺少必需列：" + "、".join(missing)
            + "（支持 lap/圈号、source/计时来源、cumulative_ms/发车后累计毫秒数）"
        )
    return mapping


def _parse_rows(text: str):
    if len(text) > MAX_CSV_BYTES:
        raise CsvError("CSV 文件过大")
    if not text.strip():
        raise CsvError("CSV 文件为空")

    last_error: Exception | None = None
    reader = None
    mapping = None
    for delimiter in (",", ";", "\t"):
        try:
            candidate = csv.DictReader(io.StringIO(text), delimiter=delimiter)
            candidate_mapping = _find_header_mapping(candidate.fieldnames)
        except CsvError as exc:
            last_error = exc
            continue
        reader = candidate
        mapping = candidate_mapping
        break

    if reader is None or mapping is None:
        if isinstance(last_error, CsvError):
            raise last_error
        raise CsvError("无法识别 CSV 表头")

    laps: dict[int, dict[str, int]] = {}
    seen_pairs: set[tuple[int, str]] = set()
    total = 0

    for row_number, row in enumerate(reader, start=2):
        if row.get(None):
            raise CsvError(f"第 {row_number} 行多出无法识别的列")
        values = [
            (row.get(mapping[key]) or "").strip()
            for key in ("lap", "source", "cumulative_ms")
        ]
        if not any(values):
            continue

        lap_text, source_text, cumulative_text = values
        try:
            lap = int(lap_text)
        except ValueError:
            raise CsvError(f"第 {row_number} 行圈号不是整数：{lap_text!r}") from None
        if lap < 1:
            raise CsvError(f"第 {row_number} 行圈号必须从 1 开始")
        if lap > 500:
            raise CsvError(f"第 {row_number} 行圈号超出允许范围（1-500）")

        try:
            cumulative = int(cumulative_text)
        except ValueError:
            raise CsvError(
                f"第 {row_number} 行累计毫秒数不是整数：{cumulative_text!r}"
            ) from None
        if cumulative < 0:
            raise CsvError(f"第 {row_number} 行累计毫秒数不能为负")

        source = _normalise_source(source_text)
        pair = (lap, source)
        if pair in seen_pairs:
            raise CsvError(f"第 {row_number} 行重复：第 {lap} 圈来源 {source!r} 已有读数")
        seen_pairs.add(pair)

        laps.setdefault(lap, {})[source] = cumulative
        total += 1
        if total > MAX_READINGS:
            raise CsvError(f"读数超过 {MAX_READINGS} 条上限")

    if not laps:
        raise CsvError("CSV 没有有效数据行")

    missing_laps = [lap for lap in range(1, max(laps) + 1) if lap not in laps]
    if missing_laps:
        preview = "、".join(str(lap) for lap in missing_laps[:10])
        suffix = "…" if len(missing_laps) > 10 else ""
        raise CsvError(f"圈号必须从 1 连续编号，缺少第 {preview}{suffix} 圈")

    return [
        (lap, sorted(laps[lap].items(), key=lambda item: item[0]))
        for lap in sorted(laps)
    ]


def parse_csv(raw: bytes):
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise CsvError("CSV 必须使用 UTF-8 编码") from None
    return _parse_rows(text)


def _readings_by_lap(connection: sqlite3.Connection, session_id: int):
    rows = connection.execute(
        """
        SELECT laps.lap_number, laps.pinned_reading_id,
               readings.id AS reading_id, readings.source, readings.cumulative_ms
        FROM laps
        JOIN readings ON readings.lap_id = laps.id
        WHERE laps.session_id = ?
        ORDER BY laps.lap_number, readings.source
        """,
        (session_id,),
    ).fetchall()

    laps: list[dict] = []
    by_number: dict[int, dict] = {}
    for row in rows:
        lap = by_number.get(row["lap_number"])
        if lap is None:
            lap = {
                "lap_number": row["lap_number"],
                "pinned_reading_id": row["pinned_reading_id"],
                "readings": [],
            }
            by_number[row["lap_number"]] = lap
            laps.append(lap)
        lap["readings"].append(
            {
                "id": row["reading_id"],
                "source": row["source"],
                "cumulative_ms": row["cumulative_ms"],
            }
        )
    return sorted(laps, key=lambda item: item["lap_number"])


def _candidates(
    laps: list[dict],
    target_lap: int | None = None,
    target_values: list[int] | None = None,
):
    """Build per-lap candidate values.

    ``target_lap`` is the lap currently being decided.  Its candidates are
    replaced by ``target_values`` (a single selected value after approval, or
    every reading after clearing the pin).  Every other lap contributes its
    latest stored pin, or all of its readings when it is still unpinned.
    """
    result = []
    for lap in laps:
        if target_lap is not None and lap["lap_number"] == target_lap:
            if target_values is not None:
                result.append(list(target_values))
            else:
                # Clearing a pin: all readings of this lap become selectable.
                result.append([reading["cumulative_ms"] for reading in lap["readings"]])
        elif lap["pinned_reading_id"] is not None:
            pinned = [
                reading["cumulative_ms"]
                for reading in lap["readings"]
                if reading["id"] == lap["pinned_reading_id"]
            ]
            if not pinned:  # Defensive; foreign keys and app logic prevent this.
                raise ReviewConflict(f"第 {lap['lap_number']} 圈的核定读数不存在")
            result.append(pinned)
        else:
            result.append([reading["cumulative_ms"] for reading in lap["readings"]])
    return result


def _latest_conclusion(connection: sqlite3.Connection, session_id: int):
    return connection.execute(
        "SELECT * FROM conclusions WHERE session_id = ? ORDER BY revision DESC, id DESC LIMIT 1",
        (session_id,),
    ).fetchone()


STATUS_TONE = {
    BREAKS: "good",
    NOT_BREAK: "neutral",
    REVIEW: "warn",
    INFEASIBLE: "bad",
}


def _load_provenance_evidence(connection: sqlite3.Connection, session_id: int):
    """Return ``(links_by_reading_id, events_by_event_id)`` for a batch."""

    links = {
        row["reading_id"]: row["event_id"]
        for row in connection.execute(
            """
            SELECT reading_links.reading_id, reading_links.event_id
            FROM reading_links
            JOIN readings ON readings.id = reading_links.reading_id
            JOIN laps ON laps.id = readings.lap_id
            WHERE laps.session_id = ?
            """,
            (session_id,),
        )
    }
    events = {
        row["event_id"]: {
            "event_id": row["event_id"],
            "upstream_event_id": row["upstream_event_id"],
            "raw_device_id": row["raw_device_id"],
        }
        for row in connection.execute(
            "SELECT * FROM timing_events WHERE session_id = ?", (session_id,)
        )
    }
    return links, events


def _reading_choices(laps: list[dict]) -> dict[tuple[int, str], int]:
    return {
        (lap["lap_number"], reading["source"]): reading["id"]
        for lap in laps
        for reading in lap["readings"]
    }


def create_app(test_config: dict | None = None) -> Flask:
    app = Flask(__name__)
    app.config.from_mapping(
        SECRET_KEY=os.environ.get("SECRET_KEY", "dev-only-change-me"),
        DATABASE=os.environ.get(
            "DATABASE_PATH",
            os.path.join(app.instance_path, "race_review.sqlite"),
        ),
        MAX_CONTENT_LENGTH=MAX_REQUEST_BYTES,
    )
    if test_config:
        app.config.update(test_config)

    database = app.config["DATABASE"]
    os.makedirs(os.path.dirname(os.path.abspath(database)), exist_ok=True)

    def get_db() -> sqlite3.Connection:
        if "db" not in g:
            g.db = _connect(database)
        return g.db

    app.get_db = get_db  # type: ignore[attr-defined]

    @app.teardown_appcontext
    def close_db(_exception=None):
        connection = g.pop("db", None)
        if connection is not None:
            connection.close()

    connection = _connect(database, initialise=True)
    try:
        connection.executescript(SCHEMA)
    finally:
        connection.close()

    @app.template_filter("lap_time")
    def lap_time(value):
        if value is None:
            return "—"
        seconds, millis = divmod(int(value), 1000)
        return f"{seconds}.{millis:03d} s"

    @app.context_processor
    def inject_helpers():
        return {
            "status_labels": STATUS_LABELS,
            "status_tone": STATUS_TONE,
            "provenance_labels": PROVENANCE_LABELS,
            "provenance_tone": PROVENANCE_TONE,
        }

    @app.errorhandler(413)
    def payload_too_large(_error):
        return render_index("上传内容过大（单个 CSV 上限 2 MB）", 413)

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    def list_sessions():
        connection = get_db()
        return connection.execute(
            """
            SELECT sessions.*,
                   (SELECT status FROM conclusions
                     WHERE conclusions.session_id = sessions.id
                     ORDER BY revision DESC, id DESC LIMIT 1) AS status,
                   (SELECT lower_ms FROM conclusions
                     WHERE conclusions.session_id = sessions.id
                     ORDER BY revision DESC, id DESC LIMIT 1) AS lower_ms,
                   (SELECT upper_ms FROM conclusions
                     WHERE conclusions.session_id = sessions.id
                     ORDER BY revision DESC, id DESC LIMIT 1) AS upper_ms,
                   (SELECT COUNT(*) FROM laps
                     WHERE laps.session_id = sessions.id) AS lap_count
            FROM sessions
            ORDER BY sessions.created_at DESC, sessions.id DESC
            """
        ).fetchall()

    def render_index(error: str | None = None, status_code: int = 200):
        return (
            render_template(
                "index.html",
                sessions=list_sessions(),
                error=error,
            ),
            status_code,
        )

    @app.get("/")
    def index():
        return render_index(request.args.get("error"))

    @app.post("/import")
    def import_csv():
        name = (request.form.get("name") or "").strip() or "未命名批次"
        if len(name) > 200:
            return render_index("批次名称过长", 400)

        try:
            min_lap_ms = int(request.form.get("min_lap_ms", ""))
        except ValueError:
            return render_index("最短可信圈时必须为整数毫秒", 400)
        if min_lap_ms <= 0:
            return render_index("最短可信圈时必须大于 0", 400)

        try:
            record_ms = int(request.form.get("record_ms", ""))
        except ValueError:
            return render_index("现有纪录必须为整数毫秒", 400)
        if record_ms < 0:
            return render_index("现有纪录不能为负数", 400)

        upload = request.files.get("csv_file")
        if upload is None or not upload.filename:
            return render_index("请选择 CSV 文件", 400)
        raw = upload.read(MAX_CSV_BYTES + 1)
        if len(raw) > MAX_CSV_BYTES:
            return render_index("CSV 文件过大（上限 2 MB）", 400)

        try:
            parsed = parse_csv(raw)
        except CsvError as exc:
            return render_index(str(exc), 400)

        candidates = [[value for _source, value in readings] for _lap, readings in parsed]
        result = solve(candidates, min_lap_ms, record_ms)
        if not result.feasible:
            return render_index(result.reason or "没有完整可行序列", 400)

        connection = get_db()
        timestamp = _now()
        with _transaction(connection):
            cursor = connection.execute(
                """
                INSERT INTO sessions (name, min_lap_ms, record_ms, revision,
                                      created_at, updated_at)
                VALUES (?, ?, ?, 1, ?, ?)
                """,
                (name, min_lap_ms, record_ms, timestamp, timestamp),
            )
            session_id = cursor.lastrowid
            for lap_number, readings in parsed:
                lap_cursor = connection.execute(
                    "INSERT INTO laps (session_id, lap_number) VALUES (?, ?)",
                    (session_id, lap_number),
                )
                lap_id = lap_cursor.lastrowid
                for source, cumulative in readings:
                    connection.execute(
                        "INSERT INTO readings (lap_id, source, cumulative_ms) VALUES (?, ?, ?)",
                        (lap_id, source, cumulative),
                    )
            connection.execute(
                """
                INSERT INTO conclusions (session_id, revision, lower_ms, upper_ms,
                                         status, min_lap_ms, record_ms, reason, created_at)
                VALUES (?, 1, ?, ?, ?, ?, ?, NULL, ?)
                """,
                (
                    session_id,
                    result.lower_ms,
                    result.upper_ms,
                    result.status,
                    min_lap_ms,
                    record_ms,
                    timestamp,
                ),
            )

        return redirect(url_for("review_session", session_id=session_id))

    def render_review(session_id: int, error: str | None = None, status_code: int = 200):
        connection = get_db()
        session = connection.execute(
            "SELECT * FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if session is None:
            return render_template("not_found.html"), 404

        laps = _readings_by_lap(connection, session_id)
        conclusion = _latest_conclusion(connection, session_id)
        approvals = connection.execute(
            """
            SELECT * FROM approvals
            WHERE session_id = ?
            ORDER BY id DESC
            LIMIT 20
            """,
            (session_id,),
        ).fetchall()
        links, events = _load_provenance_evidence(connection, session_id)
        provenance = build_report(laps, links, events)
        uploads = connection.execute(
            """
            SELECT * FROM provenance_uploads
            WHERE session_id = ?
            ORDER BY id DESC
            LIMIT 10
            """,
            (session_id,),
        ).fetchall()
        return (
            render_template(
                "review.html",
                session=session,
                laps=laps,
                conclusion=conclusion,
                approvals=approvals,
                provenance=provenance,
                provenance_uploads=uploads,
                error=error,
            ),
            status_code,
        )

    @app.get("/sessions/<int:session_id>")
    def review_session(session_id: int):
        return render_review(session_id)

    @app.post("/sessions/<int:session_id>/approve")
    def approve_reading(session_id: int):
        expected_pin_raw = request.form.get("expected_pin", "")
        expected_pin: int | None
        if expected_pin_raw in ("", "none"):
            expected_pin = None
        else:
            try:
                expected_pin = int(expected_pin_raw)
            except ValueError:
                return render_review(session_id, "缺少有效的核定状态，请刷新后重试", 409)

        lap_number_text = request.form.get("lap_number", "")
        try:
            lap_number = int(lap_number_text)
        except ValueError:
            return render_review(session_id, "圈号无效", 409)

        clear = request.form.get("action") == "clear"
        reading_id_text = request.form.get("reading_id", "")
        reading_id = None
        if not clear:
            try:
                reading_id = int(reading_id_text)
            except ValueError:
                return render_review(session_id, "请选择一条要核定的读数", 409)

        connection = get_db()
        session = connection.execute(
            "SELECT * FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if session is None:
            return render_template("not_found.html"), 404

        try:
            with _transaction(connection):
                # Re-read inside the write transaction so the decision uses
                # the freshest revision and every lap's latest pin.
                session = connection.execute(
                    "SELECT * FROM sessions WHERE id = ?", (session_id,)
                ).fetchone()
                if session is None:
                    raise ReviewConflict("该复核批次已不存在")

                lap = connection.execute(
                    "SELECT * FROM laps WHERE session_id = ? AND lap_number = ?",
                    (session_id, lap_number),
                ).fetchone()
                if lap is None:
                    raise ReviewConflict(f"第 {lap_number} 圈不存在")

                # Per-lap optimistic concurrency.  Changes to *other* laps are
                # expected and deliberately included in the recalculation
                # below; only a concurrent change to this very lap makes the
                # submission stale, because it would silently overwrite a
                # colleague's decision for the same lap.
                if lap["pinned_reading_id"] != expected_pin:
                    raise ReviewConflict(
                        f"第 {lap_number} 圈的核定已被另一位裁判更新。"
                        "本次提交未保存，原状态保持不变，请刷新后重新提交。"
                    )

                reading = None
                if not clear:
                    reading = connection.execute(
                        "SELECT * FROM readings WHERE id = ? AND lap_id = ?",
                        (reading_id, lap["id"]),
                    ).fetchone()
                    if reading is None:
                        raise ReviewConflict("所选读数不属于该圈")

                # Read *latest* pins for all laps inside the write transaction,
                # replacing the target lap with the newly selected reading.
                laps = _readings_by_lap(connection, session_id)
                candidates = _candidates(
                    laps,
                    lap_number,
                    None if clear else [reading["cumulative_ms"]],
                )

                result = solve(candidates, session["min_lap_ms"], session["record_ms"])
                if not result.feasible:
                    reason = (result.reason or "没有完整可行序列").rstrip("。")
                    raise ReviewConflict(
                        reason + "。本次核定未保存，原状态保持不变。"
                    )

                new_revision = session["revision"] + 1
                timestamp = _now()
                connection.execute(
                    "UPDATE laps SET pinned_reading_id = ? WHERE id = ?",
                    (None if clear else reading_id, lap["id"]),
                )
                connection.execute(
                    "UPDATE sessions SET revision = ?, updated_at = ? WHERE id = ?",
                    (new_revision, timestamp, session_id),
                )
                connection.execute(
                    """
                    INSERT INTO conclusions (session_id, revision, lower_ms, upper_ms,
                                             status, min_lap_ms, record_ms, reason, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)
                    """,
                    (
                        session_id,
                        new_revision,
                        result.lower_ms,
                        result.upper_ms,
                        result.status,
                        session["min_lap_ms"],
                        session["record_ms"],
                        timestamp,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO approvals (session_id, lap_number, reading_id, source,
                                           cumulative_ms, revision, lower_ms, upper_ms,
                                           status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        session_id,
                        lap_number,
                        None if clear else reading_id,
                        None if clear else reading["source"],
                        None if clear else reading["cumulative_ms"],
                        new_revision,
                        result.lower_ms,
                        result.upper_ms,
                        result.status,
                        timestamp,
                    ),
                )
        except ReviewConflict as exc:
            return render_review(session_id, exc.message, 409)

        flash("核定已保存，结论已按最新核定重算。", "good")
        return redirect(url_for("review_session", session_id=session_id))

    @app.post("/sessions/<int:session_id>/provenance")
    def add_provenance(session_id: int):
        connection = get_db()
        session = connection.execute(
            "SELECT * FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if session is None:
            return render_template("not_found.html"), 404

        uploads = [
            field
            for field in ("link_csv", "event_csv")
            if request.files.get(field) is not None
            and request.files[field].filename
        ]
        if not uploads:
            return render_review(
                session_id, "请至少选择一份要补交的 CSV（关联表或事件登记表）", 400
            )

        raw_files: dict[str, bytes] = {}
        for field in uploads:
            raw = request.files[field].read(MAX_CSV_BYTES + 1)
            if len(raw) > MAX_CSV_BYTES:
                return render_review(session_id, "CSV 文件过大（上限 2 MB）", 413)
            raw_files[field] = raw

        try:
            link_entries = (
                parse_link_csv(raw_files["link_csv"])
                if "link_csv" in raw_files
                else None
            )
            event_entries = (
                parse_event_csv(raw_files["event_csv"])
                if "event_csv" in raw_files
                else None
            )
        except ProvenanceConflict as exc:
            return render_review(session_id, str(exc), 409)
        except ProvenanceError as exc:
            return render_review(session_id, str(exc), 400)

        try:
            with _transaction(connection):
                # Re-read evidence inside the write transaction so the merge
                # and every affected lap's judgement come from one snapshot.
                session = connection.execute(
                    "SELECT * FROM sessions WHERE id = ?", (session_id,)
                ).fetchone()
                if session is None:
                    raise ProvenanceConflict("该复核批次已不存在")

                laps = _readings_by_lap(connection, session_id)
                by_key = _reading_choices(laps)
                links, events = _load_provenance_evidence(connection, session_id)
                timestamp = _now()

                added_links: list[dict] = []
                added_events: list[dict] = []
                unchanged = 0
                upload_id: int | None = None

                if link_entries is not None:
                    for entry in link_entries:
                        key = (entry["lap"], entry["source"])
                        if key not in by_key:
                            raise ProvenanceError(
                                f"第 {entry['lap']} 圈 {entry['source']} "
                                "在该批次中不存在对应读数"
                            )
                    # Map the reading-id keyed store onto (lap, source) keys.
                    existing_keyed = {
                        key: links[reading_id]
                        for key, reading_id in by_key.items()
                        if reading_id in links
                    }
                    _merged, new_keyed, same_keyed = merge_link_entries(
                        existing_keyed, link_entries
                    )
                    added_links = new_keyed
                    unchanged += same_keyed
                    upload_id = connection.execute(
                        """
                        INSERT INTO provenance_uploads
                            (session_id, kind, added_links, added_events,
                             unchanged_rows, created_at)
                        VALUES (?, 'link', ?, 0, ?, ?)
                        """,
                        (session_id, len(new_keyed), same_keyed, timestamp),
                    ).lastrowid
                    for entry in new_keyed:
                        connection.execute(
                            """
                            INSERT INTO reading_links (reading_id, event_id, created_at)
                            VALUES (?, ?, ?)
                            """,
                            (
                                by_key[(entry["lap"], entry["source"])],
                                entry["event_id"],
                                timestamp,
                            ),
                        )

                if event_entries is not None:
                    _merged_events, new_events, same_events = merge_event_entries(
                        events, event_entries
                    )
                    added_events = new_events
                    unchanged += same_events
                    upload_id = connection.execute(
                        """
                        INSERT INTO provenance_uploads
                            (session_id, kind, added_links, added_events,
                             unchanged_rows, created_at)
                        VALUES (?, 'event', 0, ?, ?, ?)
                        """,
                        (session_id, len(new_events), same_events, timestamp),
                    ).lastrowid
                    for entry in new_events:
                        connection.execute(
                            """
                            INSERT INTO timing_events
                                (session_id, event_id, upstream_event_id,
                                 raw_device_id, created_at)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                session_id,
                                entry["event_id"],
                                entry.get("upstream_event_id"),
                                entry.get("raw_device_id"),
                                timestamp,
                            ),
                        )

                # Recompute the whole provenance picture, so a supplement that
                # resolves an upstream event also updates every lap whose
                # chain runs through it.
                links_after, events_after = _load_provenance_evidence(
                    connection, session_id
                )
                report = build_report(laps, links_after, events_after)
                for lap in report.laps:
                    connection.execute(
                        """
                        INSERT INTO provenance_judgements
                            (session_id, upload_id, lap_number, status, detail,
                             created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            session_id,
                            upload_id,
                            lap.lap_number,
                            lap.status,
                            lap.detail,
                            timestamp,
                        ),
                    )
        except ProvenanceConflict as exc:
            return render_review(session_id, str(exc), 409)
        except ProvenanceError as exc:
            return render_review(session_id, str(exc), 400)

        parts = []
        if link_entries is not None:
            parts.append(f"关联表新增 {len(added_links)} 条")
        if event_entries is not None:
            parts.append(f"事件登记新增 {len(added_events)} 条")
        if unchanged:
            parts.append(f"重复且一致 {unchanged} 条未改动")
        flash(
            "计时来源证据已补交：" + "，".join(parts) + "。相关各圈判定已一起更新。",
            "good",
        )
        return redirect(url_for("review_session", session_id=session_id))

    return app
