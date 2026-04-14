# Windows 本地部署版网络安全日志分析平台 v1

## Summary
平台仍按“前后端分离 + 固定阶段工作流”设计，但组件改为更适合 Windows 原生、尽量全离线、单机运行的组合：

- 前端：`React + TypeScript + Vite + Ant Design + React Flow + ECharts`
- 后端：`FastAPI + SQLAlchemy + Pydantic`
- 元数据存储：`SQLite`
- 日志中间数据与分析数据：`Parquet + DuckDB`
- 任务执行：后端内置任务队列，使用 `ThreadPoolExecutor / ProcessPoolExecutor`
- 模型与数据处理：`pandas / scikit-learn / pyod / openpyxl`
- 本地大模型：`Ollama HTTP API`
- 文件产物：本地目录存储，按项目与版本分层

不推荐 v1 在 Windows 原生里引入 `Celery + Redis + PostgreSQL`，因为安装、服务管理和故障排查成本偏高，不利于先跑通单机闭环。

## Key Changes
### 1. Windows 友好的系统架构
- 采用 3 个本地进程即可运行：
  - 前端开发/发布服务
  - FastAPI 后端服务
  - Ollama 服务
- 后端统一负责：
  - 文件上传
  - 数据解析
  - 预处理与特征工程执行
  - 模型训练任务调度
  - 评估结果生成
  - 异常样本调用 Ollama 分析
- 长任务不走外部队列，改为后端内置任务管理器：
  - 轻量任务用线程池
  - 训练和重计算优先用进程池
  - 任务状态写入 SQLite，前端轮询或 WebSocket 查看进度

### 2. 数据与存储调整
- `SQLite` 只存元数据、任务状态、配置快照、评估摘要，不存大体量日志明细。
- 日志原始文件保存在本地，例如：
  - `storage/raw/<project_id>/...`
  - `storage/processed/<dataset_version>/...`
  - `storage/models/<model_version>/...`
- 标准化后的日志数据默认落成 `Parquet`，便于复用和版本化。
- 数据预览、筛选、统计、评估查询优先通过 `DuckDB` 直接读 `Parquet`，避免把百万级日志塞进 SQLite。
- 这样可以兼顾：
  - Windows 安装简单
  - 百万级数据可接受
  - 后续迁移到 PostgreSQL/对象存储时边界清晰

### 3. 工作流与能力边界
- 仍保留固定阶段：
  - 数据导入
  - 字段识别与映射确认
  - 预处理编排
  - 特征编排
  - 监督/无监督训练
  - 追加样本后重训练
  - 评估与异常分析
- `.log/.csv/.xlsx` 都统一转成标准化表结构，再进入后续步骤。
- 预处理与特征工程继续用“有序步骤链”表达，不做自由 DAG。
- v1 的“增量训练”继续定义为：
  - 导入新增数据
  - 形成新数据版本
  - 复用已有 pipeline 配置重新训练
- Ollama 只做异常解释，不参与模型训练与推理主链路。

### 4. Windows 部署与运维约束
- 默认按单机目录安装，提供一个启动脚本集合：
  - `start-backend`
  - `start-frontend`
  - `start-ollama-check`
- 配置文件集中在 `.env` 或 `config.yaml`：
  - `STORAGE_ROOT`
  - `SQLITE_PATH`
  - `OLLAMA_BASE_URL`
  - `MAX_CONCURRENT_JOBS`
  - `MODEL_ARTIFACT_DIR`
- 后续若需要长期驻留，可再补：
  - `NSSM` 或 `WinSW` 把后端注册为 Windows 服务
- v1 不要求先做服务注册，先保证命令式启动可用。

## Public Interfaces / Types
核心对象不变，但存储语义调整如下：

- `Project`
  - 项目本身与默认配置
- `DataSource`
  - 原始文件记录与解析模板
- `DatasetVersion`
  - 标准化 schema、行数、字段映射、Parquet 路径
- `PreprocessPipeline`
  - 预处理步骤与参数快照
- `FeaturePipeline`
  - 特征步骤与输出字段定义
- `TrainingJob`
  - 算法、参数、任务状态、进度、日志
- `ModelVersion`
  - 模型产物路径、输入数据版本、训练摘要
- `EvaluationReport`
  - 指标、图表数据、混淆矩阵/分布结果
- `AnomalyCase`
  - 样本引用、异常分数、预测结果、聚合标签
- `OllamaAnalysis`
  - 模型名、提示词版本、解释结果、调用状态

建议新增 2 个接口约束：
- `/jobs/{id}/logs`
  - 返回训练或处理任务的阶段日志，方便 Windows 本地排障
- `/system/health`
  - 检查 SQLite、存储目录、Ollama 连通性、磁盘空间

## Test Plan
- Windows 11 原生环境可完成：
  - Python 后端启动
  - 前端启动
  - SQLite 初始化
  - Ollama 连通性检查
- 文件导入测试：
  - `.log`、`.csv`、`.xlsx` 导入后都能产出 Parquet 数据版本
- 数据处理测试：
  - 百万级日志下，字段探测、预处理、特征生成能完成且不依赖外部数据库
- 任务执行测试：
  - 长任务可异步执行
  - 后端重启后任务状态可恢复为“失败”或“中断”，不能卡死为“运行中”
- 训练评估测试：
  - 有监督、无监督任务都能生成指标与可视化数据
  - 历史模型版本可对比
- Ollama 测试：
  - 未安装或未启动时给出明确提示但不影响主流程
  - 已配置时可对异常样本生成解释文本

## Assumptions
- v1 默认运行在 Windows 10/11 单机环境，不考虑 Linux 首发兼容性。
- v1 数据规模按百万级日志设计，靠 `Parquet + DuckDB` 支撑本地分析。
- v1 元数据数据库先用 `SQLite`，后续若扩展多用户或多机，再升级 PostgreSQL。
- v1 不引入 Redis、Celery、Kafka 等额外基础设施。
- v1 先提供浏览器式前后端分离平台，不做 Electron 桌面壳。
