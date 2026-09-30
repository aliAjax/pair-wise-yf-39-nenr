import tempfile
import unittest
from pathlib import Path

from src.domain import Actor
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class OfflineSyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())
        self.admin = Actor("admin", "admin")
        self.field = Actor("field1", "field")

    def tearDown(self):
        self.tmp.cleanup()

    def _obs(self, client_id, event_id, observed_at, lat, lon, species="deer"):
        return {
            "client_id": client_id,
            "kind": "observation",
            "data": {
                "event_id": event_id,
                "species": species,
                "location": "North",
                "observed_at": observed_at,
                "lat": lat,
                "lon": lon,
            },
        }

    def test_batch_create_returns_per_item_results(self):
        items = [
            self._obs("c1", "E-1", "2026-04-01", 40.0, 116.0),
            self._obs("c2", "E-2", "2026-04-03", 40.01, 116.01),
        ]
        result = self.service.sync_batch(self.field, items, batch_id="b1")
        self.assertEqual(result["batch_id"], "b1")
        self.assertEqual(len(result["results"]), 2)
        for item in result["results"]:
            self.assertEqual(item["status"], "stored")
            self.assertIsNotNone(item["entity_id"])

    def test_replay_same_batch_is_idempotent(self):
        items = [self._obs("c1", "E-1", "2026-04-01", 40.0, 116.0)]
        first = self.service.sync_batch(self.field, items, batch_id="b1")
        second = self.service.sync_batch(self.field, items, batch_id="b1")
        self.assertEqual(
            first["results"][0]["entity_id"], second["results"][0]["entity_id"]
        )
        self.assertEqual(first["results"][0]["status"], second["results"][0]["status"])

    def test_field_level_merge_terminal_wins_species_station_wins_lab(self):
        items = [self._obs("c1", "E-1", "2026-04-01", 40.0, 116.0)]
        created = self.service.sync_batch(self.field, items, batch_id="b1")
        entity_id = created["results"][0]["entity_id"]
        entity = self.service.get(entity_id)
        entity["data"]["lab_result"] = "positive"
        entity["data"]["sample_status"] = "in_lab"
        self.repo.update_entity(entity_id, entity["version"], "submitted", entity["data"])

        update = [
            {
                "client_id": "c1-upd",
                "entity_id": entity_id,
                "base_version": 1,
                "data": {
                    "event_id": "E-1",
                    "species": "elk",
                    "location": "South",
                    "observed_at": "2026-04-01",
                    "lat": 40.0,
                    "lon": 116.0,
                    "lab_result": "negative",
                    "sample_status": "collected",
                },
            }
        ]
        result = self.service.sync_batch(self.field, update, batch_id="b2")["results"][0]
        merged = self.service.get(entity_id)["data"]
        self.assertEqual(result["status"], "stored")
        self.assertTrue(result["conflict"])
        self.assertEqual(merged["species"], "elk")
        self.assertEqual(merged["location"], "South")
        self.assertEqual(merged["lab_result"], "positive")
        self.assertEqual(merged["sample_status"], "in_lab")
        self.assertIn("lab_result", result["rejected_fields"])
        self.assertIn("sample_status", result["rejected_fields"])

    def test_late_observation_reverts_confirmed_cluster_to_draft(self):
        items = [
            self._obs("c1", "E-1", "2026-04-01", 40.0, 116.0),
            self._obs("c2", "E-2", "2026-04-03", 40.01, 116.01),
            self._obs("c3", "E-3", "2026-04-05", 40.02, 116.02),
        ]
        created = self.service.sync_batch(self.field, items, batch_id="b1")
        ids = [r["entity_id"] for r in created["results"]]
        cluster = self.service.create(
            self.admin,
            "cluster",
            {"region": "North", "observation_ids": ids, "centroid": [40.01, 116.01]},
        )
        self.service.transition(
            self.admin,
            cluster["id"],
            "confirm_cluster",
            {"observation_ids": ids, "centroid": [40.01, 116.01]},
        )
        self.assertEqual(self.service.get(cluster["id"])["status"], "confirmed")

        late = [self._obs("c4", "E-4", "2026-04-08", 40.03, 116.03)]
        self.service.sync_batch(self.field, late, batch_id="b2")
        after = self.service.get(cluster["id"])
        self.assertEqual(after["status"], "draft")
        self.assertEqual(len(after["data"]["observation_ids"]), 4)

    def test_timed_out_observation_not_pulled_into_cluster(self):
        items = [
            self._obs("f1", "F-1", "2026-05-01", 30.0, 120.0),
            self._obs("f2", "F-2", "2026-05-03", 30.01, 120.01),
            self._obs("f3", "F-3", "2026-05-05", 30.02, 120.02),
        ]
        created = self.service.sync_batch(self.field, items, batch_id="bf1")
        ids = [r["entity_id"] for r in created["results"]]
        cluster = self.service.create(
            self.admin,
            "cluster",
            {"region": "South", "observation_ids": ids, "centroid": [30.01, 120.01]},
        )
        self.service.transition(
            self.admin,
            cluster["id"],
            "confirm_cluster",
            {"observation_ids": ids, "centroid": [30.01, 120.01]},
        )

        old = [self._obs("f4", "F-4", "2026-07-01", 30.03, 120.03)]
        result = self.service.sync_batch(self.field, old, batch_id="bf2")["results"][0]
        after = self.service.get(cluster["id"])
        self.assertEqual(after["status"], "confirmed")
        self.assertNotIn(result["entity_id"], after["data"]["observation_ids"])

    def test_duplicate_event_id_reported_as_conflict(self):
        items = [self._obs("c1", "E-1", "2026-04-01", 40.0, 116.0)]
        self.service.sync_batch(self.field, items, batch_id="b1")
        dup = [self._obs("c2", "E-1", "2026-04-01", 40.0, 116.0)]
        result = self.service.sync_batch(self.field, dup, batch_id="b2")["results"][0]
        self.assertEqual(result["status"], "conflict")
        self.assertIn("duplicate", result["reason"])


if __name__ == "__main__":
    unittest.main()
