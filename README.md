# 野生动物疫病监测与离线同步

这是一个只使用Python标准库和SQLite的模块化项目，默认端口为`8305`。所有业务规则集中在`src/rules.py`，`app.py`只负责组装依赖和启动服务。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、数据结构、领域异常和基础校验。
- `src/rules.py`：状态机、权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务和乐观锁。
- `src/service.py`：用例编排、幂等处理、版本控制和审计写入。
- `src/sync.py`：离线批次同步、幂等重放、字段级合并和聚集重算。
- `src/http_api.py`：HTTP路由、请求解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则和失败场景测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8305
```

服务启动时会自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。

## 核心对象

- `observation`：现场观察；`sample`：样本与实验室结果；`cluster`：异常聚集事件。

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `POST /api/sync/batch`：离线批次同步，请求体见下文。
- `GET /api/sync/batch/<batch_id>`：按批次号取回已处理的逐条结果（断网恢复用）。
- `GET /api/audit`：读取审计记录。

请求身份通过`X-User-Id`和`X-Role`请求头传入。创建和动作的可执行角色由规则引擎控制。

## 离线批次同步

野外终端离线记录，回网后按批次上报：

```json
{
  "batch_id": "terminal-1-2026-09-30-001",
  "items": [
    {"op": "create", "kind": "observation", "client_id": "obs-1", "data": {"event_id": "E-1", "species": "deer", "location": "North", "observed_at": "2026-04-01", "lat": 40.0, "lon": 116.0}},
    {"op": "update", "id": "<entity_id>", "client_id": "u-1", "base_version": 2, "data": {"species": "elk"}},
    {"op": "action", "id": "<entity_id>", "client_id": "a-1", "action": "submit", "data": {"location": "North", "observed_at": "2026-04-01"}, "expected_version": 1}
  ]
}
```

- **幂等重放与断点续传**：每条结果处理完即落库（按`batch_id`+`client_id`）。重放同一批次直接返回已存结果（带`replayed: true`），断网恢复后只继续处理剩余条目，不会重复入库。
- **逐条回执**：每条结果标明`stored`（入库）、`conflict`（版本冲突未收，附`current_version`供终端变基）或`rejected`（校验/权限失败，附原因）。
- **字段级合并**：观察在终端和站内都改过时不整条互相覆盖——物种、位置等以终端最后一次提交为准；鉴定结果、样本状态等站内权威字段（见`rules.OBSERVATION_STATION_FIELDS`）保留站内值，回执的`station_kept`列出被站内保留的字段。非观察类对象的更新版本不一致时按版本冲突拒收。
- **聚集事件重算**：迟到观察（新建或改位置/时间）使已确认聚集事件的成员发生变化时，事件退回`draft`待确认并重算成员与质心；超出14天时间窗（超时）或重算后不足3个点的观察不会被拉入。回执的`clusters_reset`列出被退回的事件。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

离线同步使用批次和幂等键演示，不包含真实野外通信协议、地图底图或完整空间索引。聚集重算按区域（cluster.region 与 observation.location 文本一致）圈定候选，不区分物种。
