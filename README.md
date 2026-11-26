# 解构工坊 · 异构文件解析与字段抽取编排平台

面向异构文件投递、格式探测、编码归一、解析器路由、抽取模板版本、字段解析、坏行隔离、结构嗅探、嵌套展平、取值转型、单位归一、输出契约、结果投递、解析水位与重解析编排的数据采集接入平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/migration.py 规则迁移专用接口（预览/启用/回滚/领取/证据/看板）
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/pipeline/rule_migration.py  规则版本化与迁移状态机（管线层）
│   ├── app/pipeline/csv_split.py       引号/转义兼容的统一切分
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8004/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5177/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8004`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 文件投递 | `file_intake` | 投递批次 | 批次编号、来源渠道、原始文件名 |
| 格式探测 | `format_probe` | 探测结论 | 探测编号、关联批次、魔数特征 |
| 编码归一 | `encoding_norm` | 转码记录 | 转码编号、关联批次、原始编码 |
| 解析路由 | `parser_route` | 路由决策 | 决策编号、关联批次、推断格式 |
| 抽取模板 | `extract_tmpl` | 模板版本 | 模板编号、模板名称、适用格式 |
| 字段解析 | `field_parse` | 解析字段 | 解析编号、关联批次、命中模板 |
| 坏行隔离 | `row_quarantine` | 隔离行 | 隔离编号、关联批次、原始行号 |
| 解析批次 | `parse_batch` | 批次记录 | 批次记录编号、关联投递批次、解析器标识 |
| 结构嗅探 | `schema_sniff` | 结构推断 | 嗅探编号、关联批次、样本行数 |
| 分隔规则 | `delimiter_rule` | 规则条目 | 规则编号、适用格式、分隔符 |
| 批次规则版本 | `batch_rule_version` | 批次规则版本记录 | 版本记录编号、关联批次、规则版本号 |
| 规则迁移单 | `rule_migration` | 规则迁移单 | 迁移单编号、旧规则编号、新规则编号 |
| 嵌套展平 | `nested_flatten` | 展平路径 | 展平编号、关联模板、原始路径 |
| 取值转型 | `value_cast` | 转型规则 | 转型编号、目标类型、来源类型 |
| 单位归一 | `unit_normalize` | 归一系数 | 归一编号、字段名称、原始单位 |
| 输出契约 | `output_contract` | 契约条目 | 契约编号、关联批次、输出列集合 |
| 结果投递 | `result_deliver` | 投递记录 | 投递记录编号、关联契约、目标位置 |
| 解析水位 | `parse_watermark` | 水位读数 | 水位编号、来源渠道、已解析至位点 |
| 重解析任务 | `reparse_task` | 重解析单 | 重解析编号、关联批次、重解析原因 |
| 重复指纹 | `dup_fingerprint` | 指纹记录 | 指纹编号、关联批次、内容指纹 |
| 抽样复核 | `sample_review` | 复核样本 | 样本编号、关联批次、抽样比例 |
| 解析指标 | `parse_metric` | 指标读数 | 指标读数编号、统计窗口、来源渠道 |
| 模板差异 | `tmpl_diff` | 差异条目 | 差异编号、旧模板版本、新模板版本 |
| 投递审计 | `intake_audit` | 审计条目 | 审计编号、关联对象、操作类型 |

## 分隔规则版本化迁移

存量分隔规则迁移成可预览、可启用、可回滚的版本化流程，专用接口挂在
`/api/rule-migration/*`（见 `app/routers/migration.py`）：

- **预览**：建迁移单时用同一份样本分别按新旧规则切分，两份分列结果随单永久保留，回滚后仍可对照；旧逗号规则的引号与转义行为由 `app/pipeline/csv_split.py` 统一保持历史兼容。
- **批次规则版本记录**：每个批次每次采用记一条采用轮次（已领取样本/字段在途/已交付/已回滚）。字段解析、首行表头结论与列位置必须来自同一次结构嗅探，嗅探命中规则与轮次采用规则不一致即拒绝混用。
- **启用/中断/回滚/重试**：启用只切换规则状态不删除旧规则；中断后重试强制先回滚；回滚关闭在途轮次但保留已确认列与两份预览，并按服务端资格把操作员导向旧批继续或重新选择合格规则。
- **恢复逗号**：按迁移单辨别旧动作结果，旧列已确认则续接原列，绝不再次替换或清空已认可列；字段解析按有效采用版本的起始行输出，并保留对已确认旧轮的回查。
- **发布页与证据**：`publish-board` 区分已领取样本与待处理新批；`evidence` 供结构嗅探页与分隔规则页打开同一份采用证据。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA
