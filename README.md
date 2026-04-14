# xtrain

Windows 本地部署的网络安全日志分析工作流平台。

## 当前状态
- `plan.md`：总体方案
- `milestones.md`：交付里程碑
- `backend/`：FastAPI 后端骨架
- `frontend/`：React 前端骨架
- `scripts/`：Windows 启动与检查脚本
- 当前已进入 `M4` 的第一段，可完成项目下文件导入、字段探测、字段映射确认、预处理执行、特征工程执行、监督/无监督训练和结果预览

## 快速启动
### 后端
1. 创建虚拟环境并安装依赖
2. 复制 `backend/.env.example` 为 `backend/.env`
3. 推荐运行 `python -m uvicorn app.main:app --reload --app-dir backend`
4. 当前 M1 已针对本机 `Python 3.14` 验证基础骨架安装；后续进入 Parquet 重度处理阶段时，优先推荐 `Python 3.12/3.13`

如果你不使用 `.venv`，至少先执行：
- `python -m pip install -r backend\\requirements.txt`

推荐使用 `python -m uvicorn` 而不是直接执行 `uvicorn`，这样能确保启动服务的解释器就是你刚安装依赖的那个 Python。

当前默认数据库是本地 `SQLite`，后端已为 Windows 单机场景切换到 `NullPool`，避免默认连接池在并发访问时被打满。

### 前端
1. 进入 `frontend`
2. 执行 `npm install`
3. 运行 `npm run dev`

### 脚本
- `scripts/start-backend.ps1`
- `scripts/start-frontend.ps1`
- `scripts/check-ollama.ps1`

## 当前已交付
- Windows 友好的本地技术骨架
- SQLite 元数据初始化
- 系统健康检查
- 项目列表和基础任务接口
- 工作流总览前端页面
- M1 依赖安装已避开 `pyarrow` 在 Python 3.14 Windows 下的编译阻塞
- `.log`、`.csv`、`.xlsx` 导入接口
- 导入工作台首版：支持先创建 `ImportSession`、查看解析模板建议和字段预览，确认后再生成正式 `DatasetVersion`
- 导入阶段交互清洗首版：支持保留字段、剔除字段、字段重命名并刷新导入预览
- `DataSource` 与 `DatasetVersion` 元数据登记
- 字段候选识别与样本预览接口
- 前端导入面板、数据集列表和字段探测展示
- 字段映射建议、确认与保存接口
- 预处理 Pipeline 创建、执行、预览接口
- 前端字段映射面板与预处理步骤链工作台
- 预处理支持多字段批量处理、同字段多次链式处理，以及输出到新字段
- 预处理新增字段重命名、条件过滤、时间标准化步骤，便于适配不同日志结构
- 预处理步骤支持“步骤级预览”，可查看单步执行前后字段与样本变化
- 特征工程 Pipeline 创建、执行、预览接口
- 特征工程已升级为“双模式工作台”：支持日志类型模板快速模式和高级步骤链模式
- 特征工程支持项目内模板保存/复用，以及步骤级预览
- 首批内置 `nginx_access`、`program_runtime`、`nta_flow` 特征模板
- 前端特征工程编排面板与结果预览
- 监督/无监督训练接口、模型版本与预测预览接口
- 前端训练编排面板、模型版本列表和结果预览
- 数据集工作区聚合接口 `/api/v1/datasets/{id}/workspace`
- 前端已优化为仅轮询任务状态，数据集切换时通过单次聚合请求加载详情
- 无监督结果可视化：异常分数散点图、分布图、二维投影视图
- M5 首版大模型分析：支持项目级配置本地 `Ollama`、`MiniMax` 或在线 `OpenAI 兼容接口`，可配置接口地址、模型名和 API Key，并对当前模型的 Top 异常样本生成解释
