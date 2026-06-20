# 实验十四 分任务执行Prompt集

> 说明：本文档包含实验十四5个任务的独立执行Prompt，可分别复制对应任务的Prompt，驱动AI完成对应开发工作。

---

## 任务1：全链路系统联调与启动脚本设计

### 任务目标
编写一个仅使用Python标准库的跨平台一键启动脚本 `run_app.py`，实现启动前环境自检、异步拉起FastAPI服务、自动唤起浏览器打开看板、优雅终止子进程四大功能，打通"数据存储 → 后端API → 前端渲染"的全链路。

### 项目上下文
- **技术栈**：Python 3.x + FastAPI + Uvicorn + DuckDB + ECharts静态前端
- **项目根目录**：`d:\MyProjects\DataAnalysis`
- **后端服务文件**：`lab13/dashboard/server.py`（FastAPI应用实例名为 `app`，监听端口8000，已配置CORS和静态文件挂载）
- **前端页面**：`lab13/dashboard/frontend/index.html`（ECharts数据看板，系统数据流拓扑参见下文）
- **数据源**：后端从 `lab10/batch_1000_features.csv`（LLM增强宽表）或 `lab09/online_shopping_10_cats.csv`（原始数据集）加载数据
- **系统数据流拓扑（必须对齐）**：
  ```
  数据模拟器Producer → 内存队列/消息管道 → 流式消费处理器Worker → DuckDB/Parquet存储 → FastAPI服务端 → ECharts交互前端
  ```
  本次联调聚焦后半段（存储→API→前端），前置流式组件已在lab08中完成，暂不纳入本次启动脚本。
- **前置条件**：后端服务文件 `lab13/dashboard/server.py` 已存在且可正常运行；前端 `index.html` 已存在；数据源CSV文件已就位

### 强制操作要求

1. **启动前环境自检**（必须最先执行）：
   - 检查 `lab13/dashboard/server.py` 文件是否存在，不存在则打印明确错误信息并退出（exit code=1）
   - 检查 `lab13/dashboard/frontend/index.html` 文件是否存在，不存在则打印明确错误信息并退出
   - 检查数据源文件（`lab10/batch_1000_features.csv` 和 `lab09/online_shopping_10_cats.csv`）是否至少存在一个，两者均缺失则打印警告（不退出，交给后端server.py自行降级处理）
   - 检测本地8000端口是否已被占用（使用socket尝试绑定），若被占用则打印明确警告信息并退出

2. **异步子进程管理**：
   - 使用 `subprocess.Popen` 后台启动Uvicorn服务，工作目录必须设置为 `lab13/dashboard/`，命令等价于：`uvicorn server:app --host 0.0.0.0 --port 8000`
   - 子进程的标准输出（stdout）和标准错误（stderr）必须实时打印到主进程控制台，带 `[uvicorn]` 前缀标识
   - 子进程启动失败时必须捕获异常并打印错误信息后退出

3. **服务就绪轮询**：
   - 子进程启动后，以1秒为间隔循环向 `http://localhost:8000/api/health` 发送HTTP GET请求
   - 收到HTTP 200响应即视为服务就绪，打印成功提示
   - 设置最大等待超时时间30秒，超时则打印错误信息并终止子进程后退出
   - 服务就绪后，使用 `webbrowser` 模块自动在系统默认浏览器中打开 `http://localhost:8000/`（或 `http://localhost:8000/index.html`）

4. **优雅终止机制**：
   - 使用 `signal.signal(signal.SIGINT, handler)` 或 `try/except KeyboardInterrupt` 捕获Ctrl+C
   - 收到终止信号后，必须主动调用 `subprocess.terminate()` 终止Uvicorn子进程
   - 调用 `subprocess.wait(timeout=5)` 等待子进程退出，超时则调用 `subprocess.kill()` 强制终止
   - 打印"服务已停止"确认信息后退出
   - 必须确保无孤儿进程残留（子进程被终止后端口8000必须释放）

5. **技术约束**：
   - 仅使用Python标准库（`subprocess`、`socket`、`http.client`、`webbrowser`、`signal`、`sys`、`os`、`time`、`pathlib`），不得依赖任何第三方包
   - 必须兼容Windows、Linux、macOS三平台（路径使用 `os.path` 或 `pathlib`，避免硬编码平台特定路径分隔符；`signal.SIGINT`在Windows上兼容处理）
   - 脚本入口使用 `if __name__ == "__main__":` 保护

### 输出与交付要求
- 在项目根目录（`d:\MyProjects\DataAnalysis`）下生成 `run_app.py` 文件
- 脚本可直接通过 `python run_app.py` 运行
- 验收标准：
  1. 执行 `python run_app.py` 后，控制台依次输出环境自检通过、Uvicorn启动日志、服务就绪提示、浏览器自动打开
  2. 浏览器中能正常看到ECharts数据看板页面，API数据正常加载
  3. 按下Ctrl+C后，控制台输出"服务已停止"，Uvicorn进程终止，端口8000释放（可用 `netstat -ano | findstr 8000` 验证）
  4. 脚本在Windows/Linux/macOS均可正常运行

---

## 任务2：环境依赖规范化与可移植性配置

### 任务目标
为项目生成一份最小化、无冗余、版本约束准确的 `requirements.txt`，并通过干净虚拟环境重建验证，确保项目在其他机器上可实现零阻碍依赖安装。

### 项目上下文
- **技术栈**：Python 3.x + FastAPI + Uvicorn + DuckDB + Polars + Pandas + scikit-learn + tenacity + httpx + openai（异步SDK）
- **项目根目录**：`d:\MyProjects\DataAnalysis`
- **核心代码目录分布**：
  - 后端服务：`lab13/dashboard/server.py`（FastAPI，依赖 fastapi、uvicorn、pandas）
  - 数据管道：`lab04/m1_pipeline.py`（Polars ELT），`lab08/run_pipeline.py`（流处理入口，依赖 scikit-learn、joblib）
  - LLM集成：`lab09/`、`lab10/`（异步OpenAI调用，依赖 openai、httpx、tenacity）
  - 可解释性分析：`lab11/`（依赖 scikit-learn、shap）
- **当前全局虚拟环境**：`d:\MyProjects\DataAnalysis\.venv`（包含大量无关全局包，必须忽略）
- **前置条件**：项目Python代码均已完成，所有 `import` 语句可直接扫描

### 强制操作要求

1. **禁止使用全局 `pip freeze`**：
   - 绝对不得执行 `pip freeze > requirements.txt` 或将全局环境的 `pip freeze` 输出直接作为依赖列表
   - 必须通过代码扫描（推荐使用 `pipreqs` 工具或手动遍历所有 `.py` 文件的 `import` 语句）生成最小依赖集

2. **依赖扫描范围**：
   - 必须覆盖以下目录中所有 `.py` 文件的 `import` 语句：`lab01/`、`lab02/`、`lab03/`、`lab04/`、`lab05/`、`lab06/`、`lab07/`、`lab08/`、`lab09/`、`lab10/`、`lab11/`、`lab12/dashboard/`、`lab13/dashboard/`、`H_M/`
   - 仅保留项目代码中直接 `import` 的第三方库，Python标准库（如 `os`、`sys`、`json`、`re`、`pathlib`、`subprocess`、`signal`、`argparse`、`logging`、`queue`、`threading`、`asyncio`、`dataclasses`、`collections`、`webbrowser` 等）不得列入 `requirements.txt`

3. **版本约束规则**：
   - 所有依赖版本必须使用 `>=` 大版本范围约束，例如 `fastapi>=0.100.0`、`uvicorn>=0.22.0`、`duckdb>=0.9.0`、`pandas>=2.0.0`、`polars>=0.19.0`、`httpx>=0.24.0`、`scikit-learn>=1.2.0`
   - 禁止使用 `==` 精确版本号锁定（防止在新系统或新CPU架构上安装失败）
   - 禁止使用 `<=` 或 `~=` 等限制性约束

4. **输出文件格式**：
   - 文件路径：`d:\MyProjects\DataAnalysis\requirements.txt`
   - 每行一个依赖，格式为 `package>=major.minor.0`
   - 包名使用小写（如 `scikit-learn` 而非 `Scikit-Learn`）
   - 第一行必须添加注释 `# 实验十四 系统联调与工程规范 - 最小依赖清单`
   - 同一包不得重复出现

5. **虚拟环境重建验证**（必须执行并记录结果）：
   - 在项目根目录外创建临时干净虚拟环境：`python -m venv test_env`
   - 激活该环境并执行 `pip install -r requirements.txt`
   - 验证所有包无报错、无冲突地完成安装
   - 在临时环境中运行 `python -c "import fastapi; import uvicorn; import duckdb; import polars; import pandas; import sklearn; print('所有依赖导入成功')"` 确认导入无报错
   - 验证完毕后删除 `test_env` 目录

### 输出与交付要求
- 输出文件：`d:\MyProjects\DataAnalysis\requirements.txt`
- 验收标准：
  1. `requirements.txt` 中每行均为项目代码中直接 `import` 的第三方库，无标准库混入
  2. 所有版本约束均使用 `>=` 格式，无 `==` 精确锁定
  3. 干净虚拟环境中执行 `pip install -r requirements.txt` 一次性成功，无依赖冲突报错
  4. 所有核心依赖（fastapi、uvicorn、duckdb、polars、pandas、scikit-learn、openai、httpx、tenacity、shap、joblib）均能成功导入

---

## 任务3：撰写标准项目文档（README.md）

### 任务目标
在项目根目录撰写一份具备工业级品质的 `README.md` 文档，使外部工程师能在3分钟内理解项目业务价值、5分钟内完成本地部署，文档必须包含5大强制板块并使用Mermaid语法绘制端到端系统架构图。

### 项目上下文
- **项目名称**：大数据分析课程综合实验项目（实验一至实验十四）
- **项目根目录**：`d:\MyProjects\DataAnalysis`
- **技术栈**：Python 3.x + FastAPI + Uvicorn + DuckDB + Polars + ECharts静态前端 + SiliconFlow大语言模型API（Qwen3.5-4B）
- **核心交付物**：
  - M1：千万级数据ELT管道（lab04/，Polars Lazy API + Parquet列式存储）
  - M2：流批一体实时预测管道（lab08/，Producer-Consumer + sklearn Pipeline）
  - M3：LLM非结构化特征抽取（lab09-11/，AsyncOpenAI + SHAP可解释性）
  - M4：前后端分离数据看板（lab13/dashboard/，FastAPI + ECharts联动下钻）
- **系统数据流拓扑**（以下为完整端到端链路，必须体现在Mermaid图中）：
  ```
  数据模拟器Producer → 内存队列/消息管道 → 流式消费处理器Worker → DuckDB/Parquet存储 → FastAPI服务端 → ECharts交互前端
  上游补充：原始CSV → Polars Lazy ETL → 分区Parquet → LLM API特征抽取 → 增强宽表
  ```
- **前置条件**：项目所有代码已开发完毕，`requirements.txt` 和 `run_app.py` 已就位

### 强制操作要求

1. **板块一：项目简介与特色（Project Overview & Features）**：
   - 必须明确指出：这是基于"轻量级高性能数据栈 + 大语言模型API"的高校课程实验交付项目
   - 必须简要罗列4个核心技术特色：
     1. 千万级脱敏日志极速ETL（Polars Lazy API流式处理，Snappy压缩Parquet列式存储）
     2. 流式背压管道与ML/LLM实时特征预测（生产者-消费者模式，有界队列背压调控，sklearn Pipeline在线推理）
     3. 高并发大模型调用的容错设计（asyncio异步并发 + Semaphore并发控制 + tenacity指数退避重试）
     4. 前后端解耦的动态可视化看板（FastAPI RESTful接口 + ECharts多维联动、维度下钻、区域刷选、正则搜索防抖）

2. **板块二：系统架构与数据流拓扑（System Architecture）**：
   - 必须使用Markdown内置的Mermaid语法（` ```mermaid ` 代码块）绘制一**幅**端到端架构图
   - Mermaid图必须覆盖以下完整链条中的所有节点和交互关系：
     - 数据采集/模拟层：原始CSV数据源 / Producer模拟器
     - 流式处理层：queue.Queue消息管道 / Consumer-Worker
     - 数据处理与建模层：Polars ETL清洗 / LLM API特征抽取（SiliconFlow Qwen3.5-4B）/ sklearn模型推理
     - 持久化存储层：Parquet列式存储 / DuckDB数据库
     - Web服务层：FastAPI REST API（6个接口：health、category-distribution、sentiment-overview、sub-category-stats、word-cloud、reviews）
     - 前端可视化层：ECharts交互看板（柱状图、堆叠图、词云、评论列表、联动下钻、刷选搜索）
   - Mermaid图必须使用 `flowchart LR`（从左到右）或 `flowchart TB`（从上到下）布局，节点间用带标签的箭头连接，标注数据流向

3. **板块三：快速开始与部署指南（Quick Start）**：
   - 步骤1：克隆/下载项目后，创建虚拟环境（完整命令：`python -m venv .venv`，Windows激活命令 `.\venv\Scripts\activate`，Linux/macOS激活命令 `source .venv/bin/activate`）
   - 步骤2：安装依赖（完整命令：`pip install -r requirements.txt`）
   - 步骤3：一键启动（完整命令：`python run_app.py`），说明脚本会自动启动后端服务并打开浏览器
   - 步骤4：手动启动备选方案（完整命令：`cd lab13/dashboard && uvicorn server:app --host 0.0.0.0 --port 8000`，然后手动打开 `http://localhost:8000`）
   - 每个步骤必须有对应的可执行的完整命令，不得省略参数

4. **板块四：配置说明（Configurations）**：
   - LLM API Key环境变量配置：
     - 说明本项目使用SiliconFlow作为大模型服务提供商，模型为Qwen3.5-4B
     - 环境变量名：`SILICONFLOW_API_KEY`（或项目中实际使用的变量名）
     - 配置方法：Windows使用 `set SILICONFLOW_API_KEY=your_key`，Linux/macOS使用 `export SILICONFLOW_API_KEY=your_key`，或在项目根目录创建 `.env` 文件写入 `SILICONFLOW_API_KEY=your_key`
     - 说明未配置时系统的降级行为：后端自动切换为内置规则库，前端顶部显示降级横幅提示
   - 监听端口修改方法：说明默认端口为8000，如需修改，编辑 `run_app.py` 中的 `PORT` 变量（或 `server.py` 中uvicorn的 `--port` 参数）

5. **板块五：项目目录树说明（Directory Tree）**：
   - 必须提供项目根目录的树状结构图（可用 `tree` 命令生成或手动整理，使用代码块包裹）
   - 对以下关键目录和文件各附一句简要解释：
     - `lab01/` ~ `lab04/`：M1数据清洗与ELT管道
     - `lab05/` ~ `lab08/`：M2流式数据处理与ML推理
     - `lab09/` ~ `lab11/`：M3 LLM集成与可解释性分析
     - `lab12/dashboard/` ~ `lab13/dashboard/`：M4数据看板前后端
     - `lab14/`：系统联调与工程规范
     - `H_M/`：H&M推荐系统独立项目
     - `run_app.py`：一键启动脚本
     - `requirements.txt`：项目依赖清单

### 输出与交付要求
- 输出文件：`d:\MyProjects\DataAnalysis\README.md`
- 验收标准：
  1. 文档使用标准Markdown语法，排版清晰，层级正确
  2. 5大板块全部包含，无遗漏
  3. Mermaid架构图可正常渲染，覆盖从数据源到前端展示的完整链路，节点数和连线数与实际系统一致
  4. 部署步骤中每条命令均可直接复制执行，无语法错误
  5. 配置说明涵盖LLM API Key和端口修改两项
  6. 目录树包含所有实验目录及关键文件，解释准确

---

## 任务4：防御性编程与系统健壮性优化

### 任务目标
对现有后端服务代码和启动逻辑进行防御性重构，实现DuckDB只读并发安全、数据文件缺失自动降级、API Key缺失显式通知三大健壮性优化，确保系统在任何异常场景下均不静默崩溃。

### 项目上下文
- **技术栈**：Python 3.x + FastAPI + Uvicorn + DuckDB + Pandas + ECharts静态前端 + SiliconFlow大语言模型API
- **当前后端文件**：`d:\MyProjects\DataAnalysis\lab13\dashboard\server.py`
- **当前前端文件**：`d:\MyProjects\DataAnalysis\lab13\dashboard\frontend\index.html`
- **当前启动脚本**：`d:\MyProjects\DataAnalysis\run_app.py`（由任务1编写）
- **数据源路径**（server.py中硬编码）：
  - LLM增强数据：`../../lab10/batch_1000_features.csv`
  - 原始数据集：`../../lab09/online_shopping_10_cats.csv`
- **LLM API环境变量名**：`SILICONFLOW_API_KEY`（或项目实际使用的变量名）
- **系统中可能使用DuckDB的场景**：若前序实验中存在DuckDB数据库文件（如 `data/analytics.db`），FastAPI查询时必须使用只读连接
- **前置条件**：`server.py` 和 `index.html` 已存在且功能正常，`run_app.py` 已编写完成

### 强制操作要求

1. **DuckDB并发锁处理**（如果系统中存在DuckDB连接）：
   - 在FastAPI的任何数据库查询接口中，连接DuckDB必须显式使用 `read_only=True` 参数：
     ```python
     conn = duckdb.connect(database="data/analytics.db", read_only=True)
     ```
   - 禁止以默认读写模式连接DuckDB进行查询操作，防止与流式写入Worker进程发生写锁冲突
   - 如果项目中当前不存在DuckDB使用场景，则跳过此条，但必须在代码中保留注释说明此设计原则

2. **数据文件缺失容错**（核心要求，必须在 `server.py` 中实现）：
   - 在数据加载入口处（`server.py` 的全局数据加载代码段），增加对 `FEATURES_PATH` 和 `RAW_PATH` 两个路径的显式存在性检测
   - 当两个数据源均缺失时：
     - 不得抛出未捕获的Traceback直接崩溃
     - 必须自动生成样本数据（至少包含 `cat`、`review`、`sentiment`、`label` 字段，最少生成3个品类各10条示例记录），使系统能正常启动并提供有意义的看板展示
     - 控制台必须以亮黄色/警告级别输出清晰的警示信息，格式为：`⚠️ [数据降级] 所有数据源文件缺失，已自动生成样本数据以维持系统运行。请检查 lab10/batch_1000_features.csv 和 lab09/online_shopping_10_cats.csv 路径。`
   - 当仅LLM增强数据缺失但原始数据存在时：
     - 自动回退加载原始数据集（当前已有此逻辑，增强为显式打印降级原因）
     - 控制台输出：`⚠️ [数据降级] LLM增强数据缺失，已回退至原始数据集。`
   - 所有降级行为必须在控制台显式告知，禁止静默回退

3. **API Key缺失显式降级**（三大子项必须全部实现）：

   a. **后端启动检测与控制台告警**：
      - 在 `server.py` 启动时（模块加载阶段），检测 `SILICONFLOW_API_KEY` 环境变量是否已配置
      - 若未配置，必须使用 `logging.warning()` 输出醒目的警告信息：`⚠️ [LLM降级] SILICONFLOW_API_KEY 环境变量未配置，大模型功能已降级为内置规则库。请设置环境变量以启用完整LLM功能。`
      - 系统必须设置一个全局状态变量（如 `LLM_ACTIVE = False`），标记当前LLM功能是否可用
      - 若未配置，系统自动降级为内置规则库/轻量词典方案（不依赖LLM API的功能模块仍可正常工作），不允许直接抛出 `KeyError` 崩溃

   b. **新增 `/api/system-status` 接口**：
      - 在 `server.py` 中新增一个GET接口，路径为 `/api/system-status`，路由函数名为 `get_system_status`
      - 返回JSON格式的系统运行状态，必须包含以下字段：
        ```json
        {
          "status": "running",
          "llm_active": true/false,
          "reason": "API Key已配置，LLM功能正常" 或 "API_KEY_MISSING",
          "data_source": "llm_enhanced" / "raw_fallback" / "sample_generated",
          "timestamp": "当前ISO格式时间戳"
        }
        ```
      - `llm_active` 为 `false` 时，`reason` 必须明确填写降级原因
      - `data_source` 字段反映当前数据加载状态（LLM增强数据 / 原始数据回退 / 自动生成样本数据）

   c. **前端降级提示横幅**：
      - 在 `index.html` 页面加载时（DOMContentLoaded或ECharts初始化之前），必须先调用 `/api/system-status` 接口
      - 当返回的 `llm_active` 为 `false` 时，必须在**看板页面的顶部显著位置**（header区域下方、图表区域上方）渲染一条醒目的降级提示横幅
      - 横幅样式要求：背景色为警告黄/橙色系（如 `#fff3cd` 背景 + `#856404` 文字 + 橙色左边框），字体清晰可读
      - 横幅文案必须包含：`"当前大模型功能已降级为内置规则库计算，请配置 API Key 以启用完整功能"`
      - 当 `llm_active` 为 `true` 时，不显示横幅
      - 当 `data_source` 为 `"sample_generated"` 时，必须额外显示一条数据降级横幅，文案包含：`"数据文件缺失，当前展示为自动生成的样本数据，请检查数据源配置"`
   - **严禁静默失败**：所有降级行为必须在控制台和前端界面上显式告知用户

4. **通用防御性原则**：
   - 所有文件I/O操作必须包裹 try/except 并输出明确的错误信息
   - 所有外部API调用必须包含超时设置和异常捕获
   - 所有降级路径不得导致系统崩溃或白屏

### 输出与交付要求
- 输出文件：
  1. `d:\MyProjects\DataAnalysis\lab13\dashboard\server.py`（重构后的后端文件）
  2. `d:\MyProjects\DataAnalysis\lab13\dashboard\frontend\index.html`（重构后的前端文件，增加状态检测和降级横幅）
- 验收标准：
  1. DuckDB查询连接使用 `read_only=True`（如项目中有DuckDB场景），或保留注释说明
  2. 删除/移走 `lab10/batch_1000_features.csv` 和 `lab09/online_shopping_10_cats.csv` 后启动服务，系统不崩溃，自动生成样本数据，控制台输出降级警告
  3. 调用 `GET /api/system-status` 返回正确JSON，`llm_active` 字段反映实际API Key配置状态
  4. 未配置API Key时启动服务，控制台有醒目警告输出，前端顶部显示黄色/橙色降级横幅
  5. 配置API Key后重新启动，横幅消失，`/api/system-status` 返回 `llm_active: true`
  6. 任何异常场景下系统均不出现未捕获的Traceback崩溃

---

## 任务5：基于AI协同的Git规范管理与仓库同步

### 任务目标
为项目生成一份精细化的 `.gitignore` 规则文件，防止大体积数据文件和敏感信息被误提交；整理当前所有变更并生成符合 Conventional Commits 规范的提交信息；完成本地提交并推送至远程仓库，验证无大文件误提交。

### 项目上下文
- **项目根目录**：`d:\MyProjects\DataAnalysis`
- **技术栈**：Python 3.x + FastAPI + Uvicorn + DuckDB + ECharts静态前端
- **项目中有大量需要排除的文件**：
  - 大体积数据文件：`lab02/UserBehavior.csv`（约1亿行，数百MB）、各实验目录下的 `*.csv`、`*.parquet`、`*.jsonl`
  - 数据库文件：DuckDB/SQLite的 `.db`、`.db.tmp`、`.db.wal` 文件
  - Python虚拟环境：`.venv/`、`lab01/data_env/`、`test_env/`
  - IDE缓存：`.vscode/`、`.idea/`、`__pycache__/`
  - 敏感文件：`lab09/lab09_api_key.env`（包含API Key）
- **远程仓库**：GitHub/Gitee仓库（已在Milestone 1中创建）
- **本次涉及的变更文件**：
  - 新增：`run_app.py`（一键启动脚本）
  - 新增：`requirements.txt`（项目依赖清单）
  - 新增：`README.md`（项目说明文档）
  - 修改：`lab13/dashboard/server.py`（健壮性优化）
  - 修改：`lab13/dashboard/frontend/index.html`（前端降级横幅）
  - 新增：`lab14/task_prompts.md`（本文档）
- **前置条件**：任务1-4的所有代码变更已完成，本地Git仓库已初始化，远程仓库地址已配置

### 强制操作要求

1. **生成精细化 `.gitignore` 规则**：
   - 文件路径：`d:\MyProjects\DataAnalysis\.gitignore`
   - 必须忽略的内容（按类别分，每类必须添加注释行说明）：
     - **大体积数据文件**（必须包含）：`*.csv`、`*.tsv`、`*.parquet`、`*.jsonl`、`*.xlsx`、`*.zip`、`*.gz`、`*.tar`，以及 `data/`、`OrginalData/` 目录
     - **数据库文件及临时锁文件**（必须包含）：`*.db`、`*.db.tmp`、`*.db.wal`、`*.sqlite`、`*.sqlite3`
     - **Python虚拟环境**（必须包含）：`.venv/`、`venv/`、`env/`、`test_env/`、`lab01/data_env/`、`**/data_env/`
     - **Python缓存**（必须包含）：`__pycache__/`、`*.pyc`、`*.pyo`、`*.pyd`
     - **IDE缓存与配置**（必须包含）：`.vscode/`、`.idea/`、`.DS_Store`、`Thumbs.db`
     - **敏感配置文件**（必须包含）：`*.env`（排除所有环境变量文件，但保留 `*.env.example` 模板文件）
     - **模型文件**（如有必须包含）：`*.pkl`、`*.joblib`、`*.h5`、`*.onnx`
   - 每类规则前必须添加一行 `#` 注释说明该类别用途
   - 不得忽略的例外（必须在 `.gitignore` 中用 `!` 前缀声明保留）：
     - `!lab10/batch_1000_features.csv`（LLM增强宽表，体积小，是看板核心数据源，且不属于原始大文件）
     - `!lab09/lab09_api_key.env.example`（API Key模板文件，如有）
     - 注意：如果 `batch_1000_features.csv` 体积不超过1MB，应在 `.gitignore` 中添加例外规则保留；如果超过1MB，则仍需忽略并在README中说明如何生成

2. **生成 Conventional Commits 规范的提交信息**：
   - 必须遵循 Conventional Commits 1.0.0 规范格式：`<type>(<scope>): <description>`
   - type必须为 `feat`（本次为实验十四的工程交付，包含新功能脚本和配置）
   - scope必须为 `m4`（对应M4里程碑：数据看板与产品级交付）
   - description必须为英文，概括本次所有变更内容
   - 提交信息参考格式：
     ```
     feat(m4): integrate e2e startup script, cleanup dependencies and implement explicit degradation for missing API key

     - Add run_app.py one-click startup script with env check, async subprocess management, and graceful shutdown
     - Generate minimal requirements.txt via code scanning with >= version constraints
     - Add README.md with Mermaid architecture diagram, quick start guide, and configuration docs
     - Implement defensive programming: DuckDB read-only mode, data file fallback with sample generation, /api/system-status endpoint
     - Add degradation banner in frontend for missing API key and missing data
     - Add precise .gitignore rules covering large data files, databases, and IDE caches
     ```

3. **Git操作执行**：
   - 使用 `git add .gitignore requirements.txt README.md run_app.py lab13/dashboard/server.py lab13/dashboard/frontend/index.html lab14/task_prompts.md` 精确添加变更文件（避免 `git add .` 或 `git add -A` 误添加被 `.gitignore` 排除的文件）
   - 使用上述Conventional Commits格式的提交信息执行 `git commit`
   - 执行 `git push` 推送到远程仓库

4. **推送后验证**：
   - 推送成功后，打开GitHub/Gitee网页端查看仓库文件目录
   - 必须验证以下事项：
     - `.gitignore` 已生效，大体积CSV/Parquet/DB文件未出现在远程仓库中
     - 虚拟环境目录（`.venv/`、`lab01/data_env/`）未出现在远程仓库中
     - 敏感配置文件（`.env`）未出现在远程仓库中
     - 远程仓库目录树结构清晰，核心代码文件均可正常浏览
   - 如果发现误提交大文件，必须立即处理：使用 `git rm --cached <file>` 从Git追踪中移除，修改 `.gitignore` 补充规则，执行 `git commit --amend` 修正提交，重新推送

### 输出与交付要求
- 输出文件：`d:\MyProjects\DataAnalysis\.gitignore`
- 验收标准：
  1. `.gitignore` 文件包含所有上述强制忽略类别，每类有注释说明
  2. 执行 `git add .` 后 `git status` 不显示大体积CSV/Parquet/DB文件、虚拟环境目录、IDE缓存文件
  3. 首次 `git commit` 的提交信息符合 Conventional Commits 格式（type、scope、英文description完整）
  4. `git push` 成功，远程仓库页面可正常访问
  5. 远程仓库中不包含任何大体积数据文件（`.csv`原始数据、`.parquet`、`.db`）、虚拟环境目录（`.venv/`）、敏感文件（`.env`）
  6. 远程仓库目录树结构与本地项目一致（排除 `.gitignore` 规则之外的文件）
