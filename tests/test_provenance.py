"""Tests for timing-source provenance (cross-source audit)."""

from __future__ import annotations

import io
import os
import tempfile
import unittest

from app import create_app
from provenance import (
    INSUFFICIENT,
    SAME_DEVICE,
    TWO_DEVICES,
    ProvenanceConflict,
    ProvenanceError,
    build_report,
    find_event_cycle,
    merge_event_entries,
    parse_event_csv,
    parse_link_csv,
)

LAP_CSV = """lap,source,cumulative_ms
1,主计时,74210
1,备计时,74230
2,主计时,148900
2,备计时,148870
3,主计时,223600
3,备计时,223640
"""

LINK_CSV = """lap,source,event_id
1,主计时,P1
1,备计时,B1
2,主计时,P2
2,备计时,B2
3,主计时,P3
3,备计时,B3
"""

# P1/B1 were forwarded through UP1; P2/B2/P3/B3 all through UP2.
EVENTS_INITIAL = """event_id,upstream_event_id,raw_device_id
P1,UP1,
B1,UP1,
UP1,,DEV-A
P2,UP2,
B2,UP2,
P3,UP2,
B3,UP2,
"""

EVENTS_RESOLVE_UP2 = """event_id,raw_device_id
UP2,DEV-A
"""

EVENTS_ALL = """event_id,upstream_event_id,raw_device_id
P1,UP1,
B1,UP1,
UP1,,DEV-A
P2,UP2,
B2,UP2,
P3,UP2,
B3,UP2,
UP2,,DEV-A
"""


class ProvenanceUnitTests(unittest.TestCase):
    def test_parse_link_aliases(self):
        rows = parse_link_csv(
            "圈号,计时来源,事件号\n1,主,ev-1\n1,备,ev-2\n".encode("utf-8")
        )
        self.assertEqual(
            rows,
            [
                {"lap": 1, "source": "主计时", "event_id": "ev-1"},
                {"lap": 1, "source": "备计时", "event_id": "ev-2"},
            ],
        )

    def test_link_duplicate_with_different_event_rejected(self):
        text = "lap,source,event_id\n1,主计时,A\n1,主计时,B\n"
        with self.assertRaises(ProvenanceError):
            parse_link_csv(text.encode("utf-8"))

    def test_event_row_needs_exactly_one_relation(self):
        both = "event_id,upstream_event_id,raw_device_id\nE1,U1,D1\n"
        neither = "event_id,upstream_event_id,raw_device_id\nE1,,\n"
        for text in (both, neither):
            with self.assertRaises(ProvenanceError):
                parse_event_csv(text.encode("utf-8"))

    def test_find_event_cycle(self):
        graph = {
            "A": {"event_id": "A", "upstream_event_id": "B", "raw_device_id": None},
            "B": {"event_id": "B", "upstream_event_id": "A", "raw_device_id": None},
        }
        cycle = find_event_cycle(graph)
        self.assertIsNotNone(cycle)
        self.assertEqual(cycle[0], cycle[-1])

    def test_merge_rejects_conflicting_relation(self):
        existing = {
            "E1": {
                "event_id": "E1",
                "upstream_event_id": None,
                "raw_device_id": "D1",
            }
        }
        with self.assertRaises(ProvenanceConflict):
            merge_event_entries(
                existing,
                [{"event_id": "E1", "upstream_event_id": "U1", "raw_device_id": None}],
            )

    def test_merge_rejects_new_cycle(self):
        with self.assertRaises(ProvenanceConflict):
            merge_event_entries(
                {},
                [
                    {"event_id": "X", "upstream_event_id": "Y", "raw_device_id": None},
                    {"event_id": "Y", "upstream_event_id": "X", "raw_device_id": None},
                ],
            )

    def test_build_report_same_device_and_cross_lap(self):
        laps = [
            {
                "lap_number": lap,
                "readings": [
                    {"id": lap * 10 + 1, "source": "主计时", "cumulative_ms": 1000},
                    {"id": lap * 10 + 2, "source": "备计时", "cumulative_ms": 1010},
                ],
            }
            for lap in (1, 2)
        ]
        links = {11: "P1", 12: "B1", 21: "P2", 22: "B2"}
        events = {
            "P1": {"upstream_event_id": "UP", "raw_device_id": None},
            "B1": {"upstream_event_id": "UP", "raw_device_id": None},
            "P2": {"upstream_event_id": "UP", "raw_device_id": None},
            "B2": {"upstream_event_id": "UP", "raw_device_id": None},
            "UP": {"upstream_event_id": None, "raw_device_id": "DEV-A"},
        }
        report = build_report(laps, links, events)
        self.assertEqual([lap.status for lap in report.laps], [SAME_DEVICE, SAME_DEVICE])
        self.assertEqual(len(report.cross_lap), 1)
        reference = report.cross_lap[0]
        self.assertEqual(reference.original_event_id, "UP")
        self.assertEqual(reference.raw_device_id, "DEV-A")
        self.assertEqual(
            [(entry["lap_number"], entry["source"]) for entry in reference.entries],
            [(1, "主计时"), (1, "备计时"), (2, "主计时"), (2, "备计时")],
        )

    def test_build_report_two_devices_and_insufficient(self):
        laps = [
            {
                "lap_number": 1,
                "readings": [
                    {"id": 1, "source": "主计时", "cumulative_ms": 1000},
                    {"id": 2, "source": "备计时", "cumulative_ms": 1010},
                ],
            },
            {
                "lap_number": 2,
                "readings": [
                    {"id": 3, "source": "主计时", "cumulative_ms": 2000},
                    {"id": 4, "source": "备计时", "cumulative_ms": 2010},
                ],
            },
        ]
        links = {1: "P1", 2: "B1", 3: "P2", 4: "B2"}
        events = {
            "P1": {"upstream_event_id": None, "raw_device_id": "DEV-A"},
            "B1": {"upstream_event_id": None, "raw_device_id": "DEV-B"},
            "P2": {"upstream_event_id": "MISSING", "raw_device_id": None},
            "B2": {"upstream_event_id": "MISSING", "raw_device_id": None},
        }
        report = build_report(laps, links, events)
        self.assertEqual(report.laps[0].status, TWO_DEVICES)
        self.assertEqual(report.laps[1].status, INSUFFICIENT)
        self.assertIn("未登记", report.laps[1].detail)


class ProvenanceAppTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        self.app = create_app(
            {"TESTING": True, "DATABASE": self.db_path, "SECRET_KEY": "test"}
        )
        self.client = self.app.test_client()

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        for suffix in ("-wal", "-shm"):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.unlink(path)

    def _import_batch(self):
        data = {
            "name": "来源核查批次",
            "min_lap_ms": "60000",
            "record_ms": "78000",
            "csv_file": (io.BytesIO(LAP_CSV.encode("utf-8")), "laps.csv"),
        }
        response = self.client.post("/import", data=data, content_type="multipart/form-data")
        self.assertEqual(response.status_code, 302)
        return self.session_id()

    def session_id(self):
        with self.app.app_context():
            return self.app.get_db().execute("SELECT id FROM sessions").fetchone()[0]

    def upload_provenance(self, *, link=None, event=None):
        data = {}
        if link is not None:
            data["link_csv"] = (io.BytesIO(link.encode("utf-8")), "links.csv")
        if event is not None:
            data["event_csv"] = (io.BytesIO(event.encode("utf-8")), "events.csv")
        return self.client.post(
            f"/sessions/{self.session_id()}/provenance",
            data=data,
            content_type="multipart/form-data",
        )

    def page(self):
        return self.client.get(f"/sessions/{self.session_id()}").get_data(as_text=True)

    def count(self, table):
        with self.app.app_context():
            return self.app.get_db().execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]

    def judgements(self):
        with self.app.app_context():
            return self.app.get_db().execute(
                "SELECT * FROM provenance_judgements ORDER BY id"
            ).fetchall()

    def test_initial_page_marks_evidence_insufficient(self):
        self._import_batch()
        body = self.page()
        self.assertIn("证据不全", body)
        self.assertIn("计时来源核查", body)

    def test_supplement_resolves_all_affected_laps_and_cross_lap(self):
        self._import_batch()
        self.assertEqual(self.upload_provenance(link=LINK_CSV).status_code, 302)

        body = self.page()
        self.assertIn("证据不全", body)
        # Laps 2 and 3 share event UP2, which is not registered yet.
        self.assertIn("未登记", body)

        self.assertEqual(self.upload_provenance(event=EVENTS_INITIAL).status_code, 302)
        body = self.page()
        # UP1 resolves; UP2 still missing, so laps 2 and 3 stay insufficient.
        self.assertEqual(body.count("同一台设备"), 2)  # 1 pill + 1 summary dt
        self.assertIn("未登记", body)

        response = self.upload_provenance(event=EVENTS_RESOLVE_UP2)
        self.assertEqual(response.status_code, 302)
        body = self.page()
        self.assertNotIn("未登记", body)
        # One supplement resolved both lap 2 and lap 3.
        self.assertEqual(body.count("同一台设备"), 4)  # 3 pills + 1 summary dt
        self.assertIn("跨圈引用同一原始事件", body)
        self.assertIn("原始事件 UP2", body)
        # Both laps 2 and 3 list their own source chains under the reference.
        self.assertIn("P2 → UP2", body)
        self.assertIn("P3 → UP2", body)

        rows = self.judgements()
        self.assertEqual(len(rows), 3 + 3 + 3)
        self.assertTrue(all(row["status"] == SAME_DEVICE for row in rows[-3:]))

    def test_missing_upstream_supplement_then_resolved(self):
        self._import_batch()
        self.upload_provenance(link=LINK_CSV)

        rows = self.judgements()
        self.assertEqual(len(rows), 3)  # link upload only; no events yet
        self.assertTrue(all(row["status"] == INSUFFICIENT for row in rows))

        response = self.upload_provenance(event=EVENTS_ALL)
        self.assertEqual(response.status_code, 302)
        rows = self.judgements()
        # All three laps are re-judged by the one resolving supplement.
        self.assertEqual(len(rows), 3 + 3)
        self.assertTrue(all(row["status"] == SAME_DEVICE for row in rows[3:]))
        upload_ids = {row["upload_id"] for row in rows[3:]}
        self.assertEqual(len(upload_ids), 1)
        self.assertIn("同一台设备", self.page())

    def test_two_devices_are_reported_per_lap(self):
        self._import_batch()
        links = """lap,source,event_id
1,主计时,P1
1,备计时,B1
"""
        events = """event_id,raw_device_id
P1,DEV-A
B1,DEV-B
"""
        self.assertEqual(self.upload_provenance(link=links).status_code, 302)
        self.assertEqual(self.upload_provenance(event=events).status_code, 302)
        body = self.page()
        self.assertIn("两台设备", body)
        self.assertIn("主计时设备 DEV-A；备计时设备 DEV-B", body)

    def test_conflicting_event_relation_rejects_whole_upload(self):
        self._import_batch()
        self.upload_provenance(link=LINK_CSV)
        self.upload_provenance(event=EVENTS_INITIAL)
        before_events = self.count("timing_events")
        before_judgements = self.count("provenance_judgements")

        conflict = """event_id,upstream_event_id
UP1,U9
"""
        response = self.upload_provenance(event=conflict)
        self.assertEqual(response.status_code, 409)
        self.assertIn("冲突", response.get_data(as_text=True))
        self.assertIn("整次补交已拒收", response.get_data(as_text=True))
        self.assertEqual(self.count("timing_events"), before_events)
        self.assertEqual(self.count("provenance_judgements"), before_judgements)

        with self.app.app_context():
            device = self.app.get_db().execute(
                "SELECT raw_device_id FROM timing_events WHERE event_id = 'UP1'"
            ).fetchone()[0]
            self.assertEqual(device, "DEV-A")

    def test_conflicting_link_rejects_whole_upload(self):
        self._import_batch()
        self.upload_provenance(link=LINK_CSV)
        before = self.count("reading_links")

        # One conflicting row plus one legitimate new row: both are rejected.
        mixed = """lap,source,event_id
1,主计时,P9
1,主计时,P1
"""
        response = self.upload_provenance(link=mixed)
        self.assertEqual(response.status_code, 409)
        self.assertIn("冲突", response.get_data(as_text=True))
        self.assertEqual(self.count("reading_links"), before)
        with self.app.app_context():
            event_id = self.app.get_db().execute(
                """
                SELECT reading_links.event_id FROM reading_links
                JOIN readings ON readings.id = reading_links.reading_id
                JOIN laps ON laps.id = readings.lap_id
                WHERE laps.lap_number = 1 AND readings.source = '主计时'
                """
            ).fetchone()[0]
            self.assertEqual(event_id, "P1")

    def test_cycle_from_supplement_rejected_without_changes(self):
        self._import_batch()
        self.upload_provenance(link=LINK_CSV)
        self.upload_provenance(event=EVENTS_INITIAL)
        before_events = self.count("timing_events")
        before_judgements = self.count("provenance_judgements")
        before_statuses = [row["status"] for row in self.judgements()]

        cycle = """event_id,upstream_event_id
X,Y
Y,X
"""
        response = self.upload_provenance(event=cycle)
        self.assertEqual(response.status_code, 409)
        body = response.get_data(as_text=True)
        self.assertIn("成环", body)
        self.assertIn("整次补交已拒收", body)
        self.assertEqual(self.count("timing_events"), before_events)
        self.assertEqual(self.count("provenance_judgements"), before_judgements)
        self.assertEqual([row["status"] for row in self.judgements()], before_statuses)
        self.assertEqual(self.count("provenance_uploads"), 2)

    def test_link_for_unknown_reading_rejected(self):
        self._import_batch()
        bad = """lap,source,event_id
9,主计时,E1
"""
        response = self.upload_provenance(link=bad)
        self.assertEqual(response.status_code, 400)
        self.assertIn("不存在对应读数", response.get_data(as_text=True))
        self.assertEqual(self.count("reading_links"), 0)

    def test_intra_file_event_conflict_rejected(self):
        self._import_batch()
        before = self.count("timing_events")
        conflicting = """event_id,upstream_event_id,raw_device_id
E1,U1,
E1,U2,
"""
        response = self.upload_provenance(event=conflicting)
        self.assertEqual(response.status_code, 409)
        self.assertIn("冲突", response.get_data(as_text=True))
        self.assertEqual(self.count("timing_events"), before)

    def test_cycle_closing_on_existing_chain_is_rejected_atomically(self):
        self._import_batch()
        self.assertEqual(
            self.upload_provenance(
                event="event_id,upstream_event_id,raw_device_id\nA,B,\nB,C,\nC,,DEV-X\n"
            ).status_code,
            302,
        )
        before_events = self.count("timing_events")
        before_judgements = self.count("provenance_judgements")
        # C is already registered with an original device; recasting it as an
        # upstream of A would close A -> B -> C -> A.  The stored relation may
        # not be rewritten, so the whole supplement is rejected.
        response = self.upload_provenance(event="event_id,upstream_event_id\nC,A\n")
        self.assertEqual(response.status_code, 409)
        self.assertIn("整次补交已拒收", response.get_data(as_text=True))
        self.assertEqual(self.count("timing_events"), before_events)
        self.assertEqual(self.count("provenance_judgements"), before_judgements)
        with self.app.app_context():
            relation = self.app.get_db().execute(
                "SELECT raw_device_id FROM timing_events WHERE event_id = 'C'"
            ).fetchone()[0]
            self.assertEqual(relation, "DEV-X")

    def test_duplicate_identical_rows_are_idempotent(self):
        self._import_batch()
        self.assertEqual(self.upload_provenance(link=LINK_CSV).status_code, 302)
        response = self.upload_provenance(link=LINK_CSV)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.count("reading_links"), 6)
        upload = None
        with self.app.app_context():
            upload = self.app.get_db().execute(
                "SELECT * FROM provenance_uploads ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(upload["added_links"], 0)
        self.assertEqual(upload["unchanged_rows"], 6)

    def test_combined_upload_of_both_tables(self):
        self._import_batch()
        response = self.upload_provenance(link=LINK_CSV, event=EVENTS_ALL)
        self.assertEqual(response.status_code, 302)
        body = self.page()
        self.assertEqual(body.count("同一台设备"), 4)
        self.assertIn("原始事件 UP2", body)

    def test_no_file_selected(self):
        self._import_batch()
        response = self.upload_provenance()
        self.assertEqual(response.status_code, 400)
        self.assertIn("至少选择一份", response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
