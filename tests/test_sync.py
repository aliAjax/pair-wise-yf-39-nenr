import tempfile
import unittest
from pathlib import Path

from src.domain import Actor
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService
from src.sync import SyncService


def _observation_data(event_id, observed_at, lat=40.0, lon=116.0, location="North"):
    return {
        "event_id": event_id,
        "species": "deer",
        "location": location,
        "observed_at": observed_at,
        "lat": lat,
        "lon": lon,
    }


class SyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())
        self.sync = SyncService(self.service)
        self.terminal = Actor("terminal-1", "field")
        self.admin = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def _batch(self, items, batch_id="batch-1"):
        return self.sync.process_batch(
            self.terminal, {"batch_id": batch_id, "items": items}
        )

    def _create_observation(self, event_id, observed_at, lat=40.0, lon=116.0):
        entity = self.service.create(
            self.admin, "observation", _observation_data(event_id, observed_at, lat, lon)
        )
        return self.service.transition(
            self.admin, entity["id"], "submit",
            {"location": "North", "observed_at": observed_at},
        )

    def _confirmed_cluster(self, members):
        cluster = self.service.create(self.admin, "cluster", {"region": "North"})
        return self.service.transition(
            self.admin, cluster["id"], "confirm_cluster",
            {"observation_ids": [m["id"] for m in members], "centroid": [40.01, 116.01]},
        )

    def test_batch_create_reports_each_item(self):
        result = self._batch([
            {"op": "create", "kind": "observation", "client_id": "o-1",
             "data": _observation_data("E-1", "2026-04-01")},
            {"op": "create", "kind": "observation", "client_id": "o-2",
             "data": {"event_id": "E-2", "observed_at": "2026-04-02"}},
            {"op": "explode", "client_id": "o-3"},
        ])
        self.assertEqual(result["total"], 3)
        self.assertEqual(result["stored"], 1)
        first, second, third = result["results"]
        self.assertEqual(first["status"], "stored")
        self.assertEqual(first["client_id"], "o-1")
        self.assertEqual(second["status"], "rejected")  # 缺 species
        self.assertEqual(third["status"], "rejected")  # 未知 op
        self.assertEqual(len(self.service.list("observation")), 1)

    def test_replay_same_batch_is_idempotent(self):
        items = [
            {"op": "create", "kind": "observation", "client_id": "o-1",
             "data": _observation_data("E-1", "2026-04-01")},
            {"op": "create", "kind": "observation", "client_id": "o-2",
             "data": _observation_data("E-2", "2026-04-02")},
        ]
        first = self._batch(items)
        second = self._batch(items)
        self.assertEqual(
            [r["entity_id"] for r in first["results"]],
            [r["entity_id"] for r in second["results"]],
        )
        self.assertTrue(all(r.get("replayed") for r in second["results"]))
        self.assertEqual(len(self.service.list("observation")), 2)

    def test_resume_completes_remaining_items_after_interruption(self):
        done = self.service.create(
            self.terminal, "observation", _observation_data("E-1", "2026-04-01"),
            idempotency_key="o-1",
        )
        self.repo.save_sync_item(self.terminal.user_id, "batch-9", "o-1", {
            "index": 0, "op": "create", "client_id": "o-1",
            "status": "stored", "entity_id": done["id"], "version": 1,
        })
        result = self._batch([
            {"op": "create", "kind": "observation", "client_id": "o-1",
             "data": _observation_data("E-1", "2026-04-01")},
            {"op": "create", "kind": "observation", "client_id": "o-2",
             "data": _observation_data("E-2", "2026-04-02")},
        ], batch_id="batch-9")
        first, second = result["results"]
        self.assertTrue(first["replayed"])
        self.assertEqual(first["entity_id"], done["id"])
        self.assertEqual(second["status"], "stored")
        self.assertNotIn("replayed", second)
        self.assertEqual(len(self.service.list("observation")), 2)

    def test_observation_merge_keeps_station_fields(self):
        entity = self.service.create(
            self.admin, "observation", _observation_data("E-1", "2026-04-01")
        )
        station_data = dict(entity["data"])
        station_data.update({
            "species": "fox",  # 站内也改过物种，但以终端最后提交为准
            "identification": "canine distemper",
            "sample_status": "in_lab",
        })
        self.repo.update_entity(entity["id"], 1, entity["status"], station_data)
        result = self._batch([{
            "op": "update", "id": entity["id"], "client_id": "u-1", "base_version": 1,
            "data": {
                "species": "elk",
                "location": "South",
                "lat": 39.9,
                "identification": "terminal guess",
                "sample_status": "terminal claim",
            },
        }])
        item = result["results"][0]
        self.assertEqual(item["status"], "stored")
        self.assertTrue(item["merged"])
        self.assertEqual(sorted(item["station_kept"]), ["identification", "sample_status"])
        merged = self.service.get(entity["id"])
        self.assertEqual(merged["data"]["species"], "elk")  # 终端为准
        self.assertEqual(merged["data"]["location"], "South")
        self.assertEqual(merged["data"]["lat"], 39.9)
        self.assertEqual(merged["data"]["identification"], "canine distemper")  # 站内为准
        self.assertEqual(merged["data"]["sample_status"], "in_lab")
        self.assertEqual(merged["version"], 3)

    def test_non_observation_update_conflict_is_reported(self):
        observation = self._create_observation("E-1", "2026-04-01")
        sample = self.service.create(
            self.admin, "sample",
            {"observation_id": observation["id"], "sample_code": "W-1"},
        )
        result = self._batch([{
            "op": "update", "id": sample["id"], "client_id": "u-9",
            "base_version": 999, "data": {"sample_code": "W-2"},
        }])
        item = result["results"][0]
        self.assertEqual(item["status"], "conflict")
        self.assertEqual(item["entity_id"], sample["id"])
        self.assertEqual(item["current_version"], 1)
        self.assertEqual(self.service.get(sample["id"])["data"]["sample_code"], "W-1")

    def test_action_with_stale_version_is_conflict(self):
        observation = self.service.create(
            self.admin, "observation", _observation_data("E-1", "2026-04-01")
        )
        result = self._batch([{
            "op": "action", "id": observation["id"], "client_id": "a-1",
            "action": "submit",
            "data": {"location": "North", "observed_at": "2026-04-01"},
            "expected_version": 999,
        }])
        item = result["results"][0]
        self.assertEqual(item["status"], "conflict")
        self.assertEqual(item["current_version"], observation["version"])
        self.assertEqual(self.service.get(observation["id"])["status"], "captured")

    def test_late_observation_reverts_confirmed_cluster(self):
        members = [
            self._create_observation("E-1", "2026-04-01", 40.0, 116.0),
            self._create_observation("E-2", "2026-04-03", 40.01, 116.01),
            self._create_observation("E-3", "2026-04-05", 40.02, 116.02),
        ]
        cluster = self._confirmed_cluster(members)
        result = self._batch([{
            "op": "create", "kind": "observation", "client_id": "late-1",
            "data": _observation_data("E-4", "2026-04-10", 40.015, 116.015),
        }])
        item = result["results"][0]
        self.assertEqual(item["status"], "stored")
        self.assertEqual(item["clusters_reset"], [cluster["id"]])
        updated = self.service.get(cluster["id"])
        self.assertEqual(updated["status"], "draft")  # 退回待确认
        self.assertEqual(len(updated["data"]["observation_ids"]), 4)  # 重算后含迟到观察
        self.assertIn(item["entity_id"], updated["data"]["observation_ids"])
        self.assertEqual(updated["data"]["centroid"], [40.01125, 116.01125])
        log = self.service.audit_log(cluster["id"])
        self.assertEqual(log[-1]["action"], "recalculate")
        self.assertEqual(log[-1]["from_status"], "confirmed")
        self.assertEqual(log[-1]["to_status"], "draft")

    def test_timed_out_observation_is_not_pulled_in(self):
        members = [
            self._create_observation("E-1", "2026-04-01", 40.0, 116.0),
            self._create_observation("E-2", "2026-04-03", 40.01, 116.01),
            self._create_observation("E-3", "2026-04-05", 40.02, 116.02),
        ]
        cluster = self._confirmed_cluster(members)
        result = self._batch([{
            "op": "create", "kind": "observation", "client_id": "late-2",
            "data": _observation_data("E-5", "2026-05-20", 40.015, 116.015),
        }])
        item = result["results"][0]
        self.assertEqual(item["status"], "stored")
        self.assertNotIn("clusters_reset", item)
        updated = self.service.get(cluster["id"])
        self.assertEqual(updated["status"], "confirmed")
        self.assertEqual(len(updated["data"]["observation_ids"]), 3)

    def test_cluster_below_three_points_reverts_to_pending(self):
        members = [
            self._create_observation("E-1", "2026-04-01", 40.0, 116.0),
            self._create_observation("E-2", "2026-04-03", 40.01, 116.01),
            self._create_observation("E-3", "2026-04-05", 40.02, 116.02),
        ]
        cluster = self._confirmed_cluster(members)
        target = members[2]
        result = self._batch([{
            "op": "update", "id": target["id"], "client_id": "u-late",
            "base_version": target["version"],
            "data": {"observed_at": "2026-05-20"},  # 改出 14 天窗口
        }])
        item = result["results"][0]
        self.assertEqual(item["status"], "stored")
        self.assertEqual(item["clusters_reset"], [cluster["id"]])
        updated = self.service.get(cluster["id"])
        self.assertEqual(updated["status"], "draft")
        self.assertEqual(updated["data"]["observation_ids"], [])  # 不足三点不拉入
        self.assertNotIn("centroid", updated["data"])

    def test_batch_results_can_be_fetched_for_recovery(self):
        self._batch([{
            "op": "create", "kind": "observation", "client_id": "o-1",
            "data": _observation_data("E-1", "2026-04-01"),
        }], batch_id="batch-7")
        recovered = self.sync.batch_results(self.terminal, "batch-7")
        self.assertEqual(recovered["batch_id"], "batch-7")
        self.assertEqual(len(recovered["results"]), 1)
        self.assertEqual(recovered["results"][0]["status"], "stored")


if __name__ == "__main__":
    unittest.main()
