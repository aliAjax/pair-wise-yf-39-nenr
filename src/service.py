from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, DomainError, NotFoundError
from .rules import (
    CLUSTER_MAX_DAYS,
    CLUSTER_RADIUS_KM,
    STATION_AUTHORITATIVE_FIELDS,
    _date_ordinal,
    _haversine_km,
    merge_observation,
    recompute_cluster,
)
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    # ------------------------------------------------------------------
    # 离线批次同步
    # ------------------------------------------------------------------

    def sync_batch(self, actor, items, batch_id=None):
        """按批次同步离线记录，逐条回传入库/冲突结果。

        重放同一批次时，已处理的条目直接回传上次结果，不重复入库。
        """
        batch_id = batch_id or str(uuid4())
        results = []
        for item in items:
            result = self._sync_item(actor, item or {}, batch_id)
            results.append(result)
        return {"batch_id": batch_id, "results": results}

    def _sync_item(self, actor, item, batch_id):
        client_id = item.get("client_id")
        if not client_id:
            return {"client_id": None, "status": "error", "reason": "client_id is required"}
        stored = self.repository.get_sync_result(actor.user_id, client_id)
        if stored is not None:
            return stored
        kind = self.rules.normalize_kind(item.get("kind", "observation"))
        entity_id = item.get("entity_id")
        base_version = item.get("base_version")
        data = item.get("data", {}) or {}
        try:
            if entity_id:
                result = self._sync_update(actor, client_id, entity_id, base_version, data, batch_id)
            else:
                result = self._sync_create(actor, client_id, kind, data, batch_id)
        except DomainError as exc:
            result = {
                "client_id": client_id,
                "status": "error",
                "reason": str(exc),
                "type": type(exc).__name__,
            }
        self.repository.save_sync_result(
            actor.user_id, client_id, batch_id, result.get("entity_id"), result
        )
        return result

    def _sync_create(self, actor, client_id, kind, data, batch_id):
        cleaned = dict(data)
        rejected = []
        for field in STATION_AUTHORITATIVE_FIELDS:
            if field in cleaned:
                rejected.append(field)
                del cleaned[field]
        event_id = cleaned.get("event_id")
        if event_id:
            existing = self.repository.find_entities(kind, "event_id", event_id)
            if existing:
                return {
                    "client_id": client_id,
                    "status": "conflict",
                    "reason": "duplicate event_id: " + str(event_id),
                    "entity_id": existing[0]["id"],
                    "rejected_fields": rejected,
                }
        entity = self.create(actor, kind, cleaned)
        self._revert_affected_clusters(actor, entity)
        return {
            "client_id": client_id,
            "status": "stored",
            "entity_id": entity["id"],
            "version": entity["version"],
            "conflict": False,
            "terminal_fields": sorted(cleaned.keys()),
            "station_fields": [],
            "rejected_fields": rejected,
            "reason": None,
        }

    def _sync_update(self, actor, client_id, entity_id, base_version, data, batch_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            return {
                "client_id": client_id,
                "status": "error",
                "reason": "entity not found: " + str(entity_id),
            }
        current_version = int(entity["version"])
        merged, terminal_applied, station_preserved, rejected = merge_observation(data, entity["data"])
        version_conflict = base_version is not None and int(base_version) != current_version
        if version_conflict and not terminal_applied:
            return {
                "client_id": client_id,
                "status": "conflict",
                "reason": "version conflict: terminal-authoritative fields unchanged; station-authoritative fields not received",
                "entity_id": entity_id,
                "version": current_version,
                "conflict": True,
                "terminal_fields": [],
                "station_fields": station_preserved,
                "rejected_fields": rejected,
            }
        updated = self.repository.update_entity(
            entity_id, current_version, entity["status"], merged
        )
        self.audit.record(
            entity_id,
            actor,
            "sync_merge" if version_conflict else "sync_update",
            entity["status"],
            updated["status"],
            {
                "client_id": client_id,
                "batch_id": batch_id,
                "base_version": base_version,
                "terminal_applied": terminal_applied,
                "rejected": rejected,
            },
        )
        self._revert_affected_clusters(actor, updated)
        return {
            "client_id": client_id,
            "status": "stored",
            "entity_id": entity_id,
            "version": updated["version"],
            "conflict": version_conflict,
            "terminal_fields": terminal_applied,
            "station_fields": station_preserved,
            "rejected_fields": rejected,
            "reason": None,
        }

    def _revert_affected_clusters(self, actor, observation):
        """迟到观察让已确认聚集事件发生变化时，退回待确认并重算成员。

        超时（超出窗口）或拉入后不足三点的观察不能被拉入，事件不发生变化。
        """
        if observation.get("kind") != "observation":
            return
        obs_data = observation.get("data", {})
        if "lat" not in obs_data or "lon" not in obs_data or "observed_at" not in obs_data:
            return
        clusters = self.repository.list_entities(kind="cluster", status="confirmed")
        for cluster in clusters:
            cdata = cluster.get("data", {})
            centroid = cdata.get("centroid")
            if not centroid or len(centroid) < 2:
                continue
            if _haversine_km(centroid[0], centroid[1], obs_data["lat"], obs_data["lon"]) > CLUSTER_RADIUS_KM:
                continue  # 不在聚集范围内，不影响事件
            member_ids, new_centroid, viable = recompute_cluster(
                cluster, self.repository.list_entities(kind="observation")
            )
            current_members = cdata.get("observation_ids", [])
            if set(member_ids) == set(current_members) and new_centroid == centroid:
                continue  # 成员未变化，事件不退回
            new_data = dict(cdata)
            new_data["observation_ids"] = member_ids
            new_data["centroid"] = new_centroid
            self.repository.update_entity(cluster["id"], int(cluster["version"]), "draft", new_data)
            self.audit.record(
                cluster["id"],
                actor,
                "revert_to_draft",
                "confirmed",
                "draft",
                {
                    "reason": "late observation",
                    "observation_id": observation["id"],
                    "viable": viable,
                    "member_count": len(member_ids),
                },
            )
