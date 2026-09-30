"""离线批次同步：幂等重放、断点续传、字段级合并与聚集事件重算。

- 终端按批次上报条目（create / update / action），每条带 client_id。
- 每条结果即时落库（sync_items）：重放同一批次直接返回已存结果，
  断网恢复后只继续处理剩余条目。
- 观察在终端与站内都改过时按字段合并：物种、位置以终端最后一次提交
  为准，鉴定结果、样本状态以站内为准，不整条互相覆盖。
- 迟到观察改变已确认聚集事件时，事件退回待确认并重算成员；超时或
  不足三点的观察不会被拉入。
- 每条结果标明 stored（入库）/ conflict（版本冲突未收）/ rejected。
"""

from .domain import ConflictError, DomainError, NotFoundError, ValidationError
from .rules import merge_observation_data, recalculate_members


class SyncService:
    def __init__(self, service):
        self.service = service
        self.repository = service.repository
        self.rules = service.rules

    def process_batch(self, actor, batch):
        if not isinstance(batch, dict):
            raise ValidationError("batch must be a JSON object")
        batch_id = batch.get("batch_id")
        if not batch_id:
            raise ValidationError("batch_id is required")
        items = batch.get("items")
        if not isinstance(items, list):
            raise ValidationError("items must be a list")
        results = []
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                result = {"index": index, "op": None, "status": "rejected",
                          "error": "item must be a JSON object"}
                results.append(result)
                continue
            key = self._item_key(item, index)
            stored = self.repository.get_sync_item(actor.user_id, batch_id, key)
            if stored is not None:
                replayed = dict(stored)
                replayed["replayed"] = True
                results.append(replayed)
                continue
            result = self._process_item(actor, item, index)
            self.repository.save_sync_item(actor.user_id, batch_id, key, result)
            results.append(result)
        return {
            "batch_id": batch_id,
            "total": len(results),
            "stored": sum(1 for r in results if r["status"] == "stored"),
            "results": results,
        }

    def batch_results(self, actor, batch_id):
        return {
            "batch_id": batch_id,
            "results": self.repository.list_sync_items(actor.user_id, batch_id),
        }

    @staticmethod
    def _item_key(item, index):
        client_id = item.get("client_id")
        return str(client_id) if client_id else "#%d" % index

    @staticmethod
    def _base(item, index):
        result = {"index": index, "op": item.get("op")}
        if item.get("client_id") is not None:
            result["client_id"] = item["client_id"]
        return result

    def _process_item(self, actor, item, index):
        op = item.get("op")
        try:
            if op == "create":
                return self._process_create(actor, item, index)
            if op == "update":
                return self._process_update(actor, item, index)
            if op == "action":
                return self._process_action(actor, item, index)
            raise ValidationError("unknown op: %s" % op)
        except ConflictError as exc:
            result = self._base(item, index)
            result.update({"status": "conflict", "error": str(exc)})
            entity_id = item.get("id") or (item.get("data") or {}).get("id")
            if entity_id:
                result["entity_id"] = entity_id
                current = self.repository.get_entity(entity_id)
                if current:
                    result["current_version"] = current["version"]
            return result
        except DomainError as exc:
            result = self._base(item, index)
            result.update({
                "status": "rejected",
                "error": str(exc),
                "type": type(exc).__name__,
            })
            return result

    def _process_create(self, actor, item, index):
        kind = item.get("kind")
        data = item.get("data")
        if not isinstance(data, dict):
            raise ValidationError("create requires a data object")
        idem_key = item.get("client_id") or self._item_key(item, index)
        entity = self.service.create(actor, kind, dict(data), idempotency_key=str(idem_key))
        result = self._base(item, index)
        result.update({
            "status": "stored",
            "entity_id": entity["id"],
            "kind": entity["kind"],
            "version": entity["version"],
        })
        if entity["kind"] == "observation":
            self._attach_cluster_resets(result, self._recalculate_clusters(actor, entity))
        return result

    def _process_update(self, actor, item, index):
        entity_id = item.get("id")
        if not entity_id:
            raise ValidationError("update requires id")
        data = item.get("data")
        if not isinstance(data, dict) or not data:
            raise ValidationError("update requires a data object")
        base_version = item.get("base_version")
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + str(entity_id))
        result = self._base(item, index)
        if entity["kind"] == "observation":
            merged, applied, station_kept = merge_observation_data(entity["data"], data)
            mismatch = base_version is not None and int(base_version) != entity["version"]
            updated = self._update_observation(entity, data, merged)
            self.service.audit.record(
                entity_id, actor, "sync_update", entity["status"], updated["status"],
                {"applied": applied, "station_kept": station_kept,
                 "base_version": base_version},
            )
            result.update({
                "status": "stored",
                "entity_id": entity_id,
                "kind": "observation",
                "version": updated["version"],
                "merged": mismatch,
            })
            if station_kept:
                result["station_kept"] = station_kept
            self._attach_cluster_resets(
                result,
                self._recalculate_clusters(
                    actor, updated, previous_location=entity["data"].get("location")
                ),
            )
            return result
        if base_version is None:
            raise ValidationError("base_version is required for " + entity["kind"])
        merged = dict(entity["data"])
        merged.update(data)
        updated = self.repository.update_entity(
            entity_id, int(base_version), entity["status"], merged
        )
        self.service.audit.record(
            entity_id, actor, "sync_update", entity["status"], updated["status"],
            {"applied": sorted(data), "base_version": base_version},
        )
        result.update({
            "status": "stored",
            "entity_id": entity_id,
            "kind": entity["kind"],
            "version": updated["version"],
        })
        return result

    def _update_observation(self, entity, incoming, merged):
        try:
            return self.repository.update_entity(
                entity["id"], entity["version"], entity["status"], merged
            )
        except ConflictError:
            current = self.repository.get_entity(entity["id"])
            remerged, _, _ = merge_observation_data(current["data"], incoming)
            return self.repository.update_entity(
                current["id"], current["version"], current["status"], remerged
            )

    def _process_action(self, actor, item, index):
        entity_id = item.get("id")
        action = item.get("action")
        if not entity_id or not action:
            raise ValidationError("action requires id and action")
        data = item.get("data")
        updated = self.service.transition(
            actor, entity_id, action,
            dict(data) if isinstance(data, dict) else {},
            item.get("expected_version"),
        )
        result = self._base(item, index)
        result.update({
            "status": "stored",
            "entity_id": entity_id,
            "kind": updated["kind"],
            "version": updated["version"],
            "entity_status": updated["status"],
        })
        return result

    @staticmethod
    def _attach_cluster_resets(result, clusters):
        if clusters:
            result["clusters_reset"] = [cluster["id"] for cluster in clusters]

    def _recalculate_clusters(self, actor, observation, previous_location=None):
        locations = {observation["data"].get("location"), previous_location}
        locations.discard(None)
        if not locations:
            return []
        affected = []
        confirmed = self.service.list("cluster", status="confirmed")
        for cluster in confirmed:
            region = cluster["data"].get("region")
            if region not in locations:
                continue
            candidates = [
                item for item in self.service.list("observation")
                if item["data"].get("location") == region and item["status"] != "rejected"
            ]
            member_ids, centroid = recalculate_members(candidates)
            current_ids = list(cluster["data"].get("observation_ids", []))
            if set(member_ids) == set(current_ids):
                continue
            data = dict(cluster["data"])
            data["observation_ids"] = member_ids
            if centroid is not None:
                data["centroid"] = centroid
            else:
                data.pop("centroid", None)
            data["recalc_trigger"] = observation["id"]
            updated = self.repository.update_entity(cluster["id"], None, "draft", data)
            self.service.audit.record(
                cluster["id"], actor, "recalculate", "confirmed", "draft",
                {
                    "added": sorted(set(member_ids) - set(current_ids)),
                    "removed": sorted(set(current_ids) - set(member_ids)),
                    "trigger_observation": observation["id"],
                },
            )
            affected.append(updated)
        return affected
