# Media Pulse 架构设计

## 目标

- 后端负责采集、过滤、分类、存储、API 输出。
- 前端只负责展示和交互，不直接访问数据库。
- 支持一体模式（本地调试）与分离模式（可部署）。

## 分层

- 对外项目名与命令入口统一为 `media_pulse`
- 内部保留 `nt_cam_pulse/` 目录结构，以兼容现有模块导入与历史脚本
- Data Pipeline：`nt_cam_pulse/pipeline.py`
- Domain/Storage：`nt_cam_pulse/models.py`、`nt_cam_pulse/storage.py`
- Source Profiling：`nt_cam_pulse/source_profile.py`
- Local AI Enrichment：`nt_cam_pulse/ai_enricher.py`
- Backend API：`nt_cam_pulse/backend/`
- Frontend Server：`nt_cam_pulse/frontend/server.py`
- Frontend App：`nt_cam_pulse/web/`

## API 边界

- `GET /api/health`：服务健康状态
- `GET /api/dates`：可选报告日期
- `GET /api/summary?date=YYYY-MM-DD&scope=all|camera`：核心看板聚合
- `GET /api/status?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`：本地 AI 可用性 + 任务运行状态
- `GET /api/trend?days=14`：趋势数据
- `GET /api/video/candidates?date=YYYY-MM-DD&limit=50`：视频候选列表
- `GET /api/video/item?id=ROW_ID`：视频二级页详情
- `POST /api/video/tasks/export`：导出视频任务文件
- `POST /api/video/process`：触发手动视频分析（单条或批量）

后端统一返回 JSON，前端只通过 HTTP 获取数据。

## 运行模式

- 分离部署（推荐）
  - Backend：`python -m media_pulse.cli --config config.yaml backend --port 8788`
  - Frontend：`python -m media_pulse.cli --config config.yaml frontend --port 8787 --api-base-url http://127.0.0.1:8788`
- 一体模式（兼容）
  - `python -m media_pulse.cli --config config.yaml dashboard --port 8787`

## 关键设计点

- 前端通过 `/runtime-config.js` 注入 `apiBaseUrl`，实现同一套页面适配不同环境。
- Backend API 默认开启 CORS（`Access-Control-Allow-Origin: *`），支持跨端口前后端联调。
- 统计聚合逻辑统一在 `nt_cam_pulse/backend/service.py`，避免 UI 和 API 重复实现。
- Dashboard 任务状态按当前时间范围聚合，而不是只看单日，避免与主筛选范围脱节。
- CLI 作为统一入口，采集任务和服务启动职责清晰分离。
