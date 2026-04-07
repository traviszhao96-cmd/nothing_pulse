# Nothing Camera Pulse (camera-only)

Nothing Camera Pulse 是一个面向 `camera-only` 反馈的日更流水线，按我们确认的 4 步执行：

1. 相机主题定义（关键词 + 分类字典）
2. 采集与强过滤（仅保留 camera 相关 + 去重）
3. 自动分类（问题类型 + 情绪 + 严重级别）
4. 飞书多维表格同步 + 每日报告输出

## 功能概览

- 多源采集：Nothing Community、Google News、自定义 RSS、YouTube（Data API / yt-dlp）、X（twscrape / snscrape）、Instagram（instaloader）、Reddit（OAuth / snscrape）
- camera-only 过滤：命中相机关键词才入库
- 去重：链接去重 + 文本近似去重（Jaccard）
- 自动分类：`画质/对焦/曝光/夜景/人像/视频/防抖/性能发热/功能建议`
- 来源身份识别：`真实购买用户 / 官方KOL媒体 / 核心KOC自媒体 / 待确认`
- 本地 AI 结构化总结：好评/中性/差评要点、情绪依据、领域与二级标签
- 本地 AI 状态看板：显式展示可用性、最近错误与视频处理进度
- 视频二级详情页：支持原视频跳转、详情页入口与手动单条分析
- 视频去重：基于平台视频 ID + URL + 标题签名，避免重复转写同一内容
- 每日报告：输出 Markdown（新增量、Top 类别、高风险案例、趋势）
- 飞书同步：可选写入 Lark Bitable
- 飞书观点级同步：一条内容可拆成多条观点记录（每条含独立情绪、时间点、观点ID）
- 飞书观点级标签：严重级别/情绪/一级标签/二级标签按“单条观点”计算，而非整条内容复用
- 前后端解耦：Backend API（数据访问/聚合）+ Frontend（可视化展示）

## 目录

- `nt_cam_pulse/cli.py`：命令行入口
- `nt_cam_pulse/pipeline.py`：采集、过滤、分类、入库、报告编排
- `nt_cam_pulse/filtering.py`：camera 过滤与去重
- `nt_cam_pulse/classifier.py`：分类与情绪/严重级别
- `nt_cam_pulse/backend/`：后端 API 路由与服务
- `nt_cam_pulse/frontend/`：前端静态服务
- `nt_cam_pulse/lark.py`：飞书多维表格同步
- `nt_cam_pulse/report.py`：日报生成
- `nt_cam_pulse/web/`：前端静态页面资源
- `config.example.yaml`：完整配置模板
- `.env.example`：密钥环境变量模板

## 安装

```bash
cd /Users/travis.zhao/nt_cam_pulse
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 配置

```bash
cp config.example.yaml config.yaml
cp .env.example .env
```

程序会自动读取 `config.yaml` 同目录下的 `.env`（以及当前工作目录 `.env`），无需每次手动 `source .env`。

按需修改：

- `camera_keywords` / `camera_categories`
- `camera_filter_mode` (`strict` / `review` / `off`)
- `sources.*.enabled`
- `sources.google_news/custom_rss.fetch_article_body`（抓正文给 AI 分析）
- `sources.youtube.*`（YouTube Data API，需 `YOUTUBE_API_KEY`）
- `sources.youtube_yt_dlp.*`（YouTube 无 API 抓取；支持 `queries` 多关键词列表）
- `sources.x_twscrape.*` / `sources.x_snscrape.*`（X 抓取，两种方式可二选一）
- `sources.instagram_instaloader.*`（Instagram 抓取）
- `sources.reddit_snscrape.*`（Reddit 无 OAuth 抓取）
- `ingest-video`（无 API 手动导入视频链接并进入分析流程）
- `local_ai.*`（启用 DeepSeek/OpenAI 兼容 API；建议使用 `./prompts/deepseek_tagging_v2.md`）
- `video_processing.*`（对视频候选执行 videosummary 手动/夜间自动处理）
- `video_processing.comment_*`（YouTube 评论：`newest + top` 双通道抓取、规则优先级筛选、仅高价值评论走 AI）
- `lark.enabled` 与飞书密钥
- `lark.auto_create_fields`（建议开启；空表首次同步会自动补齐缺失列）

示例（本地模型）：

```yaml
local_ai:
  enabled: true
  base_url: http://127.0.0.1:11434/v1
  api_key: local-key
  model: qwen2.5:14b-instruct
  prompt_path: ./prompts/deepseek_tagging_v2.md
```

如果运行输出里出现 `ai_failed>0`，通常是本地 API 未启动或地址不通，请先确认 `base_url` 可访问。

海外平台优先（推荐最小配置）：

```yaml
sources:
  youtube:
    enabled: false
  youtube_yt_dlp:
    enabled: true
  x_twscrape:
    enabled: true
  x_snscrape:
    enabled: false
  instagram_instaloader:
    enabled: true
  reddit:
    enabled: false
  reddit_snscrape:
    enabled: true
```

注意：
- `x_twscrape` 需要先完成账号池登录（`db_path` 指向 twscrape 账号库）。
- 如果 `x_twscrape` 尚未准备好，可先用 `x_snscrape` 快速验证。
- `instagram_instaloader` 在未登录时可抓公开内容；登录后稳定性更高。
- `reddit` 已支持 `OAuth 优先 + PullPush 降级`。若要拿到“最新实时”Reddit 数据，建议配置 `REDDIT_CLIENT_ID/REDDIT_CLIENT_SECRET`；PullPush 更适合历史回填。
- `sources.reddit.lookback_days` 可为 Reddit 单独设置回溯天数（例如 `730`），用于先拉历史高量数据，不影响其他平台窗口。

`twscrape` 账号池初始化示例：

```bash
# 1) 准备账号文件（每行: username,password,email,email_password）
cat > /tmp/x_accounts.txt <<'EOF'
your_x_username,your_x_password,your_email,your_email_password
EOF

# 2) 写入账号池并登录（db 路径与 config 保持一致）
.venv/bin/twscrape --db ./data/twscrape_accounts.db add_accounts /tmp/x_accounts.txt username,password,email,email_password
.venv/bin/twscrape --db ./data/twscrape_accounts.db login_accounts
```

## 运行

### 一次性执行全链路

```bash
python -m nt_cam_pulse.cli --config config.yaml run
```

### 只跑采集/分类（不落库）

```bash
python -m nt_cam_pulse.cli --config config.yaml run --dry-run
```

### 只生成日报

```bash
python -m nt_cam_pulse.cli --config config.yaml report --date 2026-03-31
```

### 只同步飞书

```bash
python -m nt_cam_pulse.cli --config config.yaml sync-lark --date 2026-03-31
```

支持更新已同步记录（upsert），如果分析结果有变更会自动重推到同一条 Lark 记录。

说明：

- Lark 默认按“观点粒度”同步：`1 条视频/文章` 可能会拆成 `N 条观点记录`。
- 每条记录会自动带 `反馈ID`（内容级唯一短 ID）与 `观点ID`（观点级唯一短 ID），便于筛选和追踪。
- 如需“只同步新增，不回刷历史”，可在 `config.yaml` 设置：`lark.only_sync_new_records: true`。

### 循环同步飞书（近实时）

```bash
python -m nt_cam_pulse.cli --config config.yaml sync-lark-loop --interval 60 --limit 200
```

可选：

- `--date 2026-03-31`：只同步指定入库日期
- `--max-rounds 10`：仅跑固定轮数，便于调试

### 对历史数据回填来源/领域/AI 标签

```bash
python -m nt_cam_pulse.cli --config config.yaml backfill --date 2026-03-31 --limit 500
```

### 标签重分析工具（按新 Prompt 全量回填）

先小样本验证：

```bash
python -m nt_cam_pulse.cli --config config.yaml retag --limit 5
```

全量重分析并强制回写 Lark（包含已同步旧记录）：

```bash
python -m nt_cam_pulse.cli --config config.yaml retag --all --sync-lark --sync-batch-limit 200
```

### 导出待补视频转写任务（给 videosummary 用）

```bash
python -m nt_cam_pulse.cli --config config.yaml video-tasks --date 2026-03-31 --output-dir ./reports
```

### 手动执行视频分析（调用 videosummary）

```bash
python -m nt_cam_pulse.cli --config config.yaml video-process --date 2026-03-31 --limit 5
```

单条处理（用于详情页调试）：

```bash
python -m nt_cam_pulse.cli --config config.yaml video-process --id 123
```

说明：

- `video-process` 会自动跳过已处理或重复签名的视频，结果里会返回 `skipped_duplicates`。
- 若观点文本包含时间戳（如 `[01:24] ...`），详情页会自动渲染为可点击的时间点跳转链接。

### 无 API 手动导入视频链接（推荐）

先把链接放到固定文件（已创建）：

- `/Users/travis.zhao/nt_cam_pulse/reports/video-links.txt`

单条导入：

```bash
python -m nt_cam_pulse.cli --config config.yaml ingest-video --url 'https://www.youtube.com/watch?v=D4QyStJWgCc'
```

批量导入（文本文件每行一个链接，支持 `#` 注释）：

```bash
python -m nt_cam_pulse.cli --config config.yaml ingest-video --file ./reports/video-links.txt
```

导入后立即触发视频分析：

```bash
python -m nt_cam_pulse.cli --config config.yaml ingest-video --file ./reports/video-links.txt --analyze --limit 5
```

最简批处理（默认读取 `./reports/video-links.txt`）：

```bash
python -m nt_cam_pulse.cli --config config.yaml ingest-video --analyze --limit 5
```

### 启动每日调度（默认每天 09:00 Asia/Shanghai）

```bash
python -m nt_cam_pulse.cli --config config.yaml schedule
```

如果 `video_processing.nightly_enabled=true`，`schedule` 会额外注册夜间视频自动处理任务（时间由 `video_processing.nightly_hour/minute` 配置决定）。

### 启动后端 API（推荐与前端分离部署）

```bash
python -m nt_cam_pulse.cli --config config.yaml backend --host 127.0.0.1 --port 8788
```

默认地址：`http://127.0.0.1:8788`

### 启动前端页面服务（仅负责展示）

```bash
python -m nt_cam_pulse.cli --config config.yaml frontend --host 127.0.0.1 --port 8787 --api-base-url http://127.0.0.1:8788
```

默认地址：`http://127.0.0.1:8787`

### 本地一体模式（兼容命令）

```bash
python -m nt_cam_pulse.cli --config config.yaml dashboard --host 127.0.0.1 --port 8787
```

说明：`dashboard` 会同时提供 UI 和 `/api/*`，用于单机快速验证；生产建议前后端分离部署。

## 报告输出

日报默认生成到：

- `reports/camera-pulse-YYYY-MM-DD.md`

内容包含：

- 今日概览
- 问题类型 Top
- 情绪分布
- 高频关键词
- 高风险案例
- 跟进建议
