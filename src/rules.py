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


CLUSTER_MAX_DAYS = 14
CLUSTER_RADIUS_KM = 10

# 终端权威字段：野外现场采集的物种与位置，冲突时以终端最后一次提交为准。
TERMINAL_AUTHORITATIVE_FIELDS = ("event_id", "species", "location", "observed_at", "lat", "lon")
# 站内权威字段：鉴定结果与样本状态，冲突时以站内为准，终端不得整条覆盖。
STATION_AUTHORITATIVE_FIELDS = ("lab_result", "sample_status")


def merge_observation(terminal_data, station_data):
    """终端与站内都修改过同一观察时的字段级合并。

    物种、位置、观察时间等野外字段以终端最后一次提交为准；
    鉴定结果、样本状态等站内字段以站内为准。不允许整条互相覆盖。

    返回 (merged, terminal_applied, station_preserved, rejected)：
    - merged：合并后的完整数据
    - terminal_applied：本次采纳的终端字段
    - station_preserved：保留的站内字段
    - rejected：终端试图写入但被拒绝的站内字段
    """
    merged = dict(station_data)
    terminal_applied = []
    station_preserved = []
    rejected = []
    for field in TERMINAL_AUTHORITATIVE_FIELDS:
        if field in terminal_data:
            if terminal_data[field] != station_data.get(field):
                terminal_applied.append(field)
            merged[field] = terminal_data[field]
    for field in STATION_AUTHORITATIVE_FIELDS:
        if field in terminal_data and terminal_data[field] != station_data.get(field):
            rejected.append(field)
        if field in station_data:
            merged[field] = station_data[field]
            station_preserved.append(field)
    return merged, terminal_applied, station_preserved, rejected


def recompute_cluster(cluster, observations, max_days=CLUSTER_MAX_DAYS, radius_km=CLUSTER_RADIUS_KM):
    """根据全部观察重算聚集事件成员。

    以事件已有成员的最早观察时间为窗口基准，拉入半径内且未超时的观察。
    超时（超出窗口）的观察不能被拉入；有效成员不足三点时事件不可恢复确认。

    返回 (member_ids, centroid, viable)。
    """
    cdata = cluster.get("data", {})
    centroid = cdata.get("centroid")
    if not centroid or len(centroid) < 2:
        return [], None, False
    existing_ids = set(cdata.get("observation_ids", []))
    existing_dates = [
        item["data"].get("observed_at")
        for item in observations
        if item.get("id") in existing_ids and item.get("data", {}).get("observed_at")
    ]
    if existing_dates:
        ref_ord = _date_ordinal(min(existing_dates))
    else:
        nearby = [
            item for item in observations
            if item.get("kind") == "observation"
            and "lat" in item.get("data", {})
            and _haversine_km(centroid[0], centroid[1], item["data"]["lat"], item["data"]["lon"]) <= radius_km
        ]
        if not nearby:
            return [], centroid, False
        ref_ord = _date_ordinal(min(item["data"]["observed_at"] for item in nearby))
    valid = []
    for item in observations:
        if item.get("kind") != "observation":
            continue
        data = item.get("data", {})
        if "lat" not in data or "lon" not in data or "observed_at" not in data:
            continue
        if _haversine_km(centroid[0], centroid[1], data["lat"], data["lon"]) > radius_km:
            continue
        if abs(_date_ordinal(data["observed_at"]) - ref_ord) > max_days:
            continue  # 超时观察不能被拉入
        valid.append(item)
    if len(valid) < 3:
        return [item["id"] for item in valid], centroid, False
    clat = round(sum(item["data"]["lat"] for item in valid) / len(valid), 6)
    clon = round(sum(item["data"]["lon"] for item in valid) / len(valid), 6)
    return [item["id"] for item in valid], [clat, clon], True


CUSTOM_CREATE = {'observation': _validate_observation, 'sample': _validate_sample}
CUSTOM_TRANSITIONS = {('sample', 'lab_result'): _validate_lab_result}


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
