from datetime import datetime, timedelta

from .domain import (
    ConflictError,
    InvalidTransition,
    PermissionDenied,
    ValidationError,
)


def _validate_observation(actor, data, lookup):
    rows = lookup("observation", "event_id", data.get("event_id")) or [] if lookup else []
    for row in rows:
        if row["data"].get("observed_at") == data.get("observed_at"):
            raise ConflictError("duplicate observation event")
    if not data.get("species"):
        raise ValidationError("species is required")


def _validate_sample(actor, data, lookup):
    observation = _find_one(lookup, "observation", "id", data.get("observation_id"))
    if not observation or observation["status"] not in ("submitted", "sampled"):
        raise ValidationError("sample requires a submitted observation")


def _validate_lab_result(actor, entity, data, lookup):
    if data.get("result", "").lower() not in ("positive", "negative"):
        raise ValidationError("lab result must be positive or negative")


def _haversine_km(lat1, lon1, lat2, lon2):
    from math import asin, cos, radians, sin, sqrt
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 6371.0 * 2 * asin(sqrt(a))


def is_cluster(observations, max_days=14, radius_km=10):
    if len(observations) < 3:
        return False
    points = observations[:3]
    same_window = all(
        abs(_date_ordinal(points[0].get("observed_at")) - _date_ordinal(item.get("observed_at"))) <= max_days
        for item in points[1:]
    )
    close = all(
        _haversine_km(points[0]["lat"], points[0]["lon"], item["lat"], item["lon"]) <= radius_km
        for item in points[1:]
    )
    return same_window and close


CUSTOM_CREATE = {'observation': _validate_observation, 'sample': _validate_sample}
CUSTOM_TRANSITIONS = {('sample', 'lab_result'): _validate_lab_result}

# 站内权威字段：鉴定结果与样本状态以站内为准，终端提交的这些字段一律丢弃。
# 其余字段（物种、位置、时间等）以终端最后一次提交为准。
OBSERVATION_STATION_FIELDS = frozenset({
    "identification",
    "identified_by",
    "identified_at",
    "sample_id",
    "sample_status",
    "reason",
    "reviewed_by",
    "review_note",
})

# 聚集事件成员资格：时间窗（天）、空间半径（公里）、最少点数。
CLUSTER_MAX_DAYS = 14
CLUSTER_RADIUS_KM = 10
CLUSTER_MIN_POINTS = 3


def merge_observation_data(current, incoming):
    """字段级合并终端与站内都改过的观察，不整条互相覆盖。

    站内权威字段（鉴定结果、样本状态）保留站内现值；其余字段以终端
    最后一次提交为准。返回 (合并后数据, 采用终端值的字段, 保留站内值的字段)。
    """
    merged = dict(current)
    applied = []
    station_kept = []
    for field, value in incoming.items():
        if field in OBSERVATION_STATION_FIELDS:
            station_kept.append(field)
            continue
        if merged.get(field) != value:
            merged[field] = value
            applied.append(field)
    return merged, applied, station_kept


def recalculate_members(observations, max_days=CLUSTER_MAX_DAYS,
                        radius_km=CLUSTER_RADIUS_KM, min_points=CLUSTER_MIN_POINTS):
    """重算聚集事件成员。

    候选观察按观测日期滑动时间窗、以锚点限制空间半径，取规模最大的
    一组；规模相同取结束日期更新的一组。超窗（超时）的观察不会被拉入，
    不足 min_points 个点时返回空成员，事件不得维持确认状态。
    返回 (成员 id 列表, 质心 [lat, lon] 或 None)。
    """
    points = []
    for entity in observations:
        data = entity["data"]
        observed_at = data.get("observed_at")
        lat, lon = data.get("lat"), data.get("lon")
        if observed_at and lat is not None and lon is not None:
            points.append({
                "id": entity["id"],
                "observed_at": observed_at,
                "lat": float(lat),
                "lon": float(lon),
            })
    points.sort(key=lambda p: (_date_ordinal(p["observed_at"]), p["id"]))
    best = []
    for index, anchor in enumerate(points):
        start = _date_ordinal(anchor["observed_at"])
        group = [
            point for point in points[index:]
            if _date_ordinal(point["observed_at"]) - start <= max_days
            and _haversine_km(anchor["lat"], anchor["lon"], point["lat"], point["lon"]) <= radius_km
        ]
        if len(group) > len(best) or (
            len(group) == len(best) and group and best
            and _date_ordinal(group[-1]["observed_at"]) > _date_ordinal(best[-1]["observed_at"])
        ):
            best = group
    if len(best) < min_points:
        return [], None
    centroid = [
        round(sum(p["lat"] for p in best) / len(best), 6),
        round(sum(p["lon"] for p in best) / len(best), 6),
    ]
    return [p["id"] for p in best], centroid


class RuleEngine:
    ALIASES = {'observations': 'observation', 'samples': 'sample', 'clusters': 'cluster'}
    INITIAL_STATUS = {'observation': 'captured', 'sample': 'collected', 'cluster': 'draft'}
    TRANSITIONS = {'observation': {'submit': (('captured',), 'submitted'), 'reject': (('submitted',), 'rejected'), 'link_sample': (('submitted',), 'sampled')}, 'sample': {'send_lab': (('collected',), 'in_lab'), 'lab_result': (('in_lab',), 'resulted'), 'retest': (('resulted',), 'in_lab'), 'close': (('resulted',), 'closed')}, 'cluster': {'confirm_cluster': (('draft',), 'confirmed'), 'dismiss': (('draft',), 'dismissed')}}
    CREATE_REQUIRED = {'observation': ('event_id', 'species', 'location', 'observed_at', 'lat', 'lon'), 'sample': ('observation_id', 'sample_code'), 'cluster': ('region',)}
    ACTION_REQUIRED = {('observation', 'submit'): ('location', 'observed_at'), ('observation', 'reject'): ('reason',), ('observation', 'link_sample'): ('sample_id',), ('sample', 'send_lab'): ('lab_id',), ('sample', 'lab_result'): ('result', 'result_at'), ('sample', 'retest'): ('reason',), ('sample', 'close'): ('outcome',), ('cluster', 'confirm_cluster'): ('observation_ids', 'centroid'), ('cluster', 'dismiss'): ('reason',)}
    CREATE_ROLES = {'observation': ('admin', 'field'), 'sample': ('admin', 'field'), 'cluster': ('admin', 'epidemiologist')}
    ROLE_ACTIONS = {'submit': ('admin', 'field'), 'reject': ('admin', 'epidemiologist'), 'link_sample': ('admin', 'field'), 'send_lab': ('admin', 'field'), 'lab_result': ('admin', 'lab'), 'retest': ('admin', 'lab'), 'close': ('admin', 'epidemiologist'), 'confirm_cluster': ('admin', 'epidemiologist'), 'dismiss': ('admin', 'epidemiologist')}

    def normalize_kind(self, kind):
        return self.ALIASES.get(kind, kind)

    def initial_status(self, kind):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        return self.INITIAL_STATUS[kind]

    @staticmethod
    def _ensure_role(actor, allowed):
        if "*" not in allowed and actor.role not in allowed:
            raise PermissionDenied("role %s is not allowed here" % actor.role)

    @staticmethod
    def _require(data, fields):
        for field in fields:
            value = data.get(field)
            if value is None or value == "" or value == [] or value == {}:
                raise ValidationError("missing required field: " + field)

    def validate_create(self, actor, kind, data, lookup=None):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        self._ensure_role(actor, self.CREATE_ROLES.get(kind, ("admin",)))
        self._require(data, self.CREATE_REQUIRED.get(kind, ()))
        custom = CUSTOM_CREATE.get(kind)
        if custom:
            custom(actor, data, lookup)
        return dict(data)

    def validate_transition(self, actor, entity, action, data, lookup=None):
        kind = self.normalize_kind(entity["kind"])
        transition = self.TRANSITIONS.get(kind, {}).get(action)
        if not transition:
            raise InvalidTransition("unknown action %s for %s" % (action, kind))
        allowed_statuses, next_status = transition
        if entity["status"] not in allowed_statuses:
            raise InvalidTransition(
                "cannot %s from status %s" % (action, entity["status"])
            )
        allowed_roles = self.ROLE_ACTIONS.get(
            (kind, action), self.ROLE_ACTIONS.get(action, ("admin",))
        )
        self._ensure_role(actor, allowed_roles)
        self._require(data, self.ACTION_REQUIRED.get((kind, action), ()))
        custom = CUSTOM_TRANSITIONS.get((kind, action))
        extra = custom(actor, entity, data, lookup) if custom else {}
        patch = dict(data)
        if extra:
            patch.update(extra)
        return next_status, patch


def _find_one(lookup, kind, field, value):
    if lookup is None:
        return None
    rows = lookup(kind, field, value) or []
    return rows[0] if rows else None


def _date_ordinal(value):
    return datetime.fromisoformat(str(value)[:10]).date().toordinal()
