# Nothing Camera Pulse

Nothing Camera Pulse 是一套面向 `Nothing / CMF` 以及竞品机型的社媒与媒体反馈分析流水线，当前已经不只是 `camera-only` 抓取，而是覆盖了：

1. 多源内容采集与去重
2. camera 相关过滤与 AI 结构化标签
3. 视频转写 / 评论区挖掘 / 竞品视频归类
4. Lark 多维表格同步、Dashboard 辅助字段、日报与 HTML 周报邮件

适合放在 GitHub 仓库简介的一句话：

> Multi-source Nothing / CMF media feedback pipeline with camera tagging, video analysis, Lark sync, and HTML weekly reporting.

## 当前能力范围

- 面向 Nothing / CMF 的媒体评测、社媒讨论、评论区反馈和竞品视频检索
- 支持 `camera-only` 强过滤，也支持更宽松的 review 模式保留非相机内容
- 支持 YouTube / Bilibili / X / Google News / RSS / Instagram / Reddit / Nothing Community
- 支持视频分析结果回写数据库、Lark、多端前端页面和 HTML 邮件
- 支持从历史数据回填 AI 标签、重刷 Lark、增量生成周报

## 功能概览

- 多源采集：Nothing Community、Google News、自定义 RSS、YouTube（Data API / yt-dlp）、Bilibili、X（官方 API / twscrape / snscrape）、Instagram、Reddit
- camera-only 过滤：命中相机关键词才入库；也支持 `review` 模式先保留再打标
- 去重：链接去重 + 文本近似去重（Jaccard）+ 视频签名去重
- 自动分类：`画质 / 对焦 / 曝光 / 夜景 / 人像 / 视频 / 防抖 / 性能发热 / 功能建议`
- 来源身份识别：`真实购买用户 / 官方KOL媒体 / 核心KOC自媒体 / 待确认`
- AI 结构化总结：好评 / 中性 / 差评要点、情绪依据、领域标签、二级标签、评论区高价值信息
- 视频处理：调用 `videosummary` 做转写、观点抽取、评论区优先级筛选、重复视频复用
- 竞品视频检索：自动生成 query，支持 `compare-to`、竞品品牌/机型标记、视频类型归类
- 报告输出：Markdown 日报 + HTML 周报预览 + SMTP 群发邮件
- 飞书同步：支持观点粒度拆分、Lark Dashboard 辅助字段、历史数据回刷
- 前后端解耦：Backend API（数据访问/聚合）+ Frontend（可视化展示）

## 常用场景

- 日常增量抓取 Nothing / CMF 相机反馈
- 周度汇总媒体评测和评论区洞察并发邮件
- 拉竞品视频做相机对比洞察
- 把 AI 结构化结果同步到 Lark 做问题看板
- 对历史视频 backlog 批量补跑分析

## 目录

- `nt_cam_pulse/cli.py`：命令行入口
- `nt_cam_pulse/pipeline.py`：采集、过滤、分类、入库、报告编排
- `nt_cam_pulse/competitor_video.py`：竞品视频检索、归类、入库
- `nt_cam_pulse/filtering.py`：camera 过滤与去重
- `nt_cam_pulse/classifier.py`：分类与情绪/严重级别
- `nt_cam_pulse/video_analysis.py`：videosummary 视频分析编排
- `nt_cam_pulse/youtube_comments.py`：YouTube 评论区抓取与 AI 观点筛选
- `nt_cam_pulse/email_summary.py`：SMTP 每日汇报邮件
- `nt_cam_pulse/weekly_email.py`：HTML 周报构建与发送
- `nt_cam_pulse/backend/`：后端 API 路由与服务
- `nt_cam_pulse/frontend/`：前端静态服务
- `nt_cam_pulse/lark.py`：飞书多维表格同步
- `nt_cam_pulse/process_log.py`：过程日志与处理 run 记录
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
- `sources.bilibili.*`（Bilibili 视频搜索）
- `sources.x_api.*` / `sources.x_twscrape.*` / `sources.x_snscrape.*`（X 抓取，优先官方 API）
- `sources.instagram_instaloader.*`（Instagram 抓取）
- `sources.reddit_snscrape.*`（Reddit 无 OAuth 抓取）
- `competitor_video.*`（竞品视频检索默认配置）
- `ingest-video`（无 API 手动导入视频链接并进入分析流程）
- `competitor-video`（竞品视频检索、分析、归类）
- `local_ai.*`（启用 DeepSeek/OpenAI 兼容 API；建议使用 `./prompts/deepseek_tagging_v2.md`）
- `video_processing.*`（对视频候选执行 videosummary 手动/夜间自动处理）
- `video_processing.comment_*`（YouTube 评论：`newest + top` 双通道抓取、规则优先级筛选、仅高价值评论走 AI）
- `lark.enabled` 与飞书密钥
- `lark.auto_create_fields`（建议开启；空表首次同步会自动补齐缺失列）
- `email_summary.*`（可选：通过 SMTP 发送每日处理汇报邮件）

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
  bilibili:
    enabled: true
  x_api:
    enabled: true
  x_twscrape:
    enabled: false
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
- `bilibili` 当前走公开 web 搜索接口，不依赖 token，适合先补齐 B 站视频搜索；如果后续接口策略变化，再切换实现。
- `x_api` 走官方 X API，需要 `X_BEARER_TOKEN`。默认用 `recent search` 抓近 7 天公开帖子，并可继续抓每条帖子的 replies（作为评论区）。
- `x_twscrape` 需要先完成账号池登录（`db_path` 指向 twscrape 账号库）。
- 如果官方 `x_api` 还没配好，可先用 `x_twscrape` 或 `x_snscrape` 快速验证。
- `instagram_instaloader` 在未登录时可抓公开内容；登录后稳定性更高。
- `reddit` 已支持 `OAuth 优先 + PullPush 降级`。若要拿到“最新实时”Reddit 数据，建议配置 `REDDIT_CLIENT_ID/REDDIT_CLIENT_SECRET`；PullPush 更适合历史回填。
- `sources.reddit.lookback_days` 可为 Reddit 单独设置回溯天数（例如 `730`），用于先拉历史高量数据，不影响其他平台窗口。
- `nothing_community` 如果也想保留 OS 建议、系统体验帖，`include_keywords` 不要只写 `camera/photo/video`，否则很多帖子会在入库前就被过滤掉，即使 `camera_filter_mode: review` 也看不到。

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

### 最常用命令

```bash
# 1) 跑一次全链路抓取
python -m nt_cam_pulse.cli --config config.yaml run

# 2) 处理待分析视频
python -m nt_cam_pulse.cli --config config.yaml video-process --limit 20

# 3) 同步到 Lark
python -m nt_cam_pulse.cli --config config.yaml sync-lark --limit 200

# 4) 生成或发送周报
python -m nt_cam_pulse.cli --config config.yaml weekly-email --scope camera --send
```

### 一次性执行全链路

```bash
python -m nt_cam_pulse.cli --config config.yaml run
```

### 只跑采集/分类（不落库）

```bash
python -m nt_cam_pulse.cli --config config.yaml run --dry-run
```

### 检索竞品视频并自动归类

```bash
python -m nt_cam_pulse.cli --config config.yaml competitor-video \
  --target "iPhone 17 Pro" \
  --target "Galaxy S26 Ultra" \
  --compare-to "Nothing Phone 4a Pro" \
  --platform youtube \
  --platform bilibili \
  --analyze \
  --sync-lark
```

说明：

- 会自动生成 `camera review / camera test / vs Nothing` 类查询词
- 检索结果会复用现有数据库、AI enrich、视频分析和 Lark 同步链路
- 每条结果会额外写入竞品结构化信息：`竞品品牌`、`竞品机型`、`对比对象`、`视频类型`、`相机焦点标签`

### 周报邮件工作流

先本地生成 HTML 预览：

```bash
python -m nt_cam_pulse.cli --config config.yaml weekly-email \
  --start-date 2026-03-19 \
  --end-date 2026-04-16 \
  --scope camera \
  --top-limit 12 \
  --output ./reports/nothing-camera-social-summary-2026-03-19-to-2026-04-16.html
```

确认预览没问题后直接发出：

```bash
python -m nt_cam_pulse.cli --config config.yaml weekly-email \
  --start-date 2026-03-19 \
  --end-date 2026-04-16 \
  --scope camera \
  --top-limit 12 \
  --send
```

当前模板已支持：

- 热门内容精选
- 核心好评 / 核心差评聚类
- 评论区中文摘要
- 近 7 天反馈量与总时长趋势
- 复用 `email_summary` 的 SMTP 群发配置

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

### 初始化 Lark 问题看板视图

```bash
python -m nt_cam_pulse.cli --config config.yaml prepare-lark-dashboard
```

这一步会在当前多维表格里补齐几张适合做仪表盘的视图：

- `全量观点`
- `负向问题`
- `高严重负向`
- `评论区观点`
- `原视频观点`
- `原帖子观点`

同时会检查关键字段的类型是否适合做看板，并输出仍建议手动调整的字段。

如果刚升级了代码里的 Lark 字段定义，建议先强制回刷一次历史记录，把新的看板辅助字段推到飞书：

```bash
python -m nt_cam_pulse.cli --config config.yaml mark-lark-dirty
python -m nt_cam_pulse.cli --config config.yaml sync-lark --limit 500 --force-all-updates
```

当前会额外补这些更适合仪表盘的辅助字段：

- `主产品`
- `平台大类`
- `发布时间日期`
- `是否负向`
- `是否高严重`

建议优先在 Lark 里把这些字段改成更适合图表的类型：

- 单选：`观点情绪`、`观点严重级别`、`一级标签`、`来源标签`、`平台`、`来源身份`
- 单选：`主产品`、`平台大类`
- 多选：`二级标签`、`产品标签`
- 复选框：`是否负向`、`是否高严重`
- 数字：`观点时间秒`
- 复选框：`相机关联`、`待补视频转写`

说明：

- 当前同步表是“观点粒度”，适合做问题分类、差评聚类、评论区洞察。
- “本周热度变化”和“热门内容精选”更适合继续放在 HTML 周报里，不建议硬塞进同一张静态看板。
- 飞书开放 API 目前更适合管理数据表、字段、视图和记录；仪表盘容器本身建议在 Lark UI 中手工搭建。

### 发送每日邮件汇报

先在 `config.yaml` 填好 `email_summary` 的 SMTP 和收件人配置，然后执行：

```bash
python -m nt_cam_pulse.cli --config config.yaml send-email-summary --date 2026-04-07
```

如果你的日常流程主要靠 `run` 一次走完，可以设置：

```yaml
email_summary:
  enabled: true
  auto_send_after_run: true
```

如果你的流程是“先跑视频处理，再同步 Lark，最后发汇报”，建议保持 `auto_send_after_run: false`，并在最后一步单独调用 `send-email-summary`，这样邮件内容更接近最终状态。

只做 SMTP 连接和登录检查，不真正发信：

```bash
python -m nt_cam_pulse.cli --config config.yaml email-check
```

发送一封测试邮件：

```bash
python -m nt_cam_pulse.cli --config config.yaml send-test-email
```

如果要把本地 HTML 预览文件作为测试邮件正文发出去：

```bash
python -m nt_cam_pulse.cli --config config.yaml send-test-email --html-file ./reports/weekly-media-email-2026-03-27-to-2026-04-02.html
```

### 生成 HTML 周报邮件预览

先生成本地 HTML 文件做调试：

```bash
python -m nt_cam_pulse.cli --config config.yaml weekly-email --end-date 2026-04-02
```

可选参数：

- `--start-date 2026-03-27`：自定义周报起始日期
- `--scope all|camera`：默认 `all`，适合媒体周报；如只看相机内容可改成 `camera`
- `--top-limit 8`：控制“热门内容精选”条数
- `--output ./reports/weekly.html`：指定输出 HTML 文件
- `--send`：在生成 HTML 的同时，复用 `email_summary` 的 SMTP 配置直接发送

这份周报会优先参考 `view_count / like_count / comment_count / score` 等互动数据做“热门内容”排序，缺失时回退到发布时间。

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
