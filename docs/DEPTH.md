# 深度登记 cdcwater（CDC 乱序去重与水位线推进）

- 规模：中型（7 个功能模块 / 7 个页面 / 96 条测试）
- 核心规则一：**版本序折叠 + 墓碑**——每个 `(table, pk)` 按 `version` 升序应用，`delete` 写墓碑；
  同键同版本出现两种内容整组不应用（`C_VERSION_CONFLICT`），删除之后到达的低版本 upsert
  不复活（`C_TOMBSTONE_RESURRECT`），删除之前到达的低版本照常应用后被覆盖。
- 核心规则二：**水位线与半开窗口**——水位线 = 已应用事件最大 `event_time` − 10 分钟，单调
  不回退；`event_time < 水位线` 记迟到但**仍然应用**；快照取 `event_time ≤ as_of`，
  待定窗口 `(as_of, +inf)` 半开，两窗口不重叠也不留缝。
- 核心规则三：**截止位置与只追加续跑**——上游文件还在续写时，每次运行只消费到
  「截止位置」（最后一个完整行末尾），未换行的尾行挂起为待续尾行，不解析、不隔离；
  批次键 = 来源 + 规则版本 + 参数（不含输入摘要），追加后重跑命中同一批次、结果按完整
  前缀累计替换；上次截止位置之前的字节必须一字不差（sha256 前缀校验），改写 / 截短 /
  文件消失一律 409 `C_STREAM_REWRITTEN`。完整记录只进一次，失败重试整体回滚不留半批。
- 使用者与工作流：订单库 CDC 管道负责人，每天收 3 个导出文件（主表、旧列名补数、迟到补丁），
  原先靠 grep 日志判断「这条到底应用了没有」；现在一次采信即可得到水位、迟到清单、冲突与
  隔离明细，以及可导出复核的快照与运行清单。早班封板看 `/batches` 的截止位置（每文件
  上次 → 本次行号与字节），追加后点「采信」即续跑，不用等文件写完。

## 功能模块

| 模块 | 使用者动作 | 入口 | 核心文件 | 测试 |
|---|---|---|---|---|
| M1 事件采信 | 选来源跑一次采信，看逐文件诊断与行号 | POST /api/events/import、页面 /events | `app/modules/m1_events/reader.py` `app/modules/m1_events/decode.py` `app/modules/m1_events/format.py` `app/modules/m1_events/tail.py` `app/modules/m1_events/service.py` | `tests/test_m1_events.py` `tests/test_cutoff_continuation.py` |
| M2 契约兼容 | 看字段契约与别名，试归一一条事件 | GET /api/schema、POST /api/schema/preview、页面 /schema | `app/modules/m2_schema/contract.py` `app/modules/m2_schema/coerce.py` `app/modules/m2_schema/alias.py` `app/modules/m2_schema/service.py` | `tests/test_m2_schema.py` |
| M3 去重冲突 | 看重复、冲突、隔离与优先级 | GET /api/conflicts、页面 /conflicts | `app/modules/m3_dedup/dedup.py` `app/modules/m3_dedup/conflict.py` `app/modules/m3_dedup/service.py` | `tests/test_m3_dedup.py` |
| M4 水位迟到 | 看水位时间轴与迟到明细 | GET /api/watermark、页面 / | `app/modules/m4_watermark/watermark.py` `app/modules/m4_watermark/late.py` `app/modules/m4_watermark/service.py` | `tests/test_m4_watermark.py` |
| M5 快照物化 | 按水位物化快照，看待定键与三条对账 | POST /api/snapshot、页面 /snapshot | `app/modules/m5_snapshot/apply.py` `app/modules/m5_snapshot/materialize.py` `app/modules/m5_snapshot/service.py` | `tests/test_m5_snapshot.py` |
| M6 批次续跑 | 查历史、看运行清单、续跑与重跑、看截止位置前后对照 | GET /api/batches、POST /api/batches/{id}/resume、页面 /batches | `app/modules/m6_batches/state.py` `app/modules/m6_batches/resume.py` `app/modules/m6_batches/continuation.py` `app/modules/m6_batches/history.py` `app/modules/m6_batches/service.py` | `tests/test_m6_batches.py` `tests/test_cutoff_continuation.py` |
| M7 审计导出 | 下载证据行、隔离、冲突、快照与清单 | GET /api/export/* | `app/modules/m7_export/csv_out.py` `app/modules/m7_export/json_out.py` `app/modules/m7_export/manifest_out.py` `app/modules/m7_export/service.py` | `tests/test_m7_export.py` |

端到端：`tests/test_e2e.py` 覆盖「样例 → 采信 → 归一 → 去重冲突 → 水位 → 快照 → 导出 → 页面」，
含 4xx 失败输入与幂等重跑；`tests/test_invariants.py` 覆盖行账、键账、金额、幂等、迟到互补。

## 陷阱登记

| TRAP | 现象 | 朴素实现会怎样 | 本项目处理 | 处理位置 | 测试 |
|---|---|---|---|---|---|
| TRAP-01 | 旧列名导出文件声明 `utf-8`，字节其实是 GB18030 | 按声明解码就崩，或整列乱码 | 声明只作首个候选，失败即回退 gb18030，并记 `E_CHARSET_MISMATCH` | `app/modules/m1_events/decode.py` | `tests/test_m1_events.py::test_gb18030_fallback_and_mismatch_code` |
| TRAP-05 | CSV 行尾多一个分隔符（1102 行多出 EXTRA） | 静默丢弃多余列或错位 | 严格列数校验，整行隔离并保留原值 | `app/modules/m1_events/reader.py` | `tests/test_m1_events.py::test_csv_column_count_mismatch_quarantined` |
| TRAP-09 | 主键 `9007199254740993` 超过 2^53，另有前导零 `00123` | `int()` 化后丢精度或丢零 | 主键全程字符串；JSON 导出超过 2^53 的整数也转字符串 | `app/modules/m7_export/json_out.py`、`app/modules/m2_schema/service.py` | `tests/test_m7_export.py::test_csv_keeps_bigint_and_leading_zero`、`tests/test_m2_schema.py::test_bigint_pk_kept_as_text` |
| TRAP-11 | 同一时刻写成 `08:10Z` 与 `16:10+08:00`，另有无偏移时间 | 落进两个时刻、去重失效 | 先归一 UTC；无偏移按 +08:00 解释并记 `W_TIME_NO_OFFSET` | `app/modules/m2_schema/coerce.py` | `tests/test_m2_schema.py::test_timezone_offsets_collapse_to_same_utc`、`tests/test_m3_dedup.py::test_timezone_written_duplicate_merged` |
| TRAP-13 | 补数文件整批比当前水位早（主文件 08:40 那条迟到 12 分钟） | 水位一推进就丢迟到事件 | 迟到窗口 10 分钟，`event_time < 水位线` 记迟到但仍应用，快照标 `late` | `app/modules/m4_watermark/watermark.py` | `tests/test_m4_watermark.py::test_late_event_still_applied`、`tests/test_m4_watermark.py::test_late_by_exactly_twelve_minutes` |
| TRAP-14 | 同键同版本两种内容（1005 的 PAID / CANCELLED），删除后飘来低版本（1012 的 v2） | 后写覆盖，或整批丢弃，看不出保留了哪条 | 两条都进冲突表、都不应用，并给出证据行与差异字段 | `app/modules/m3_dedup/conflict.py` | `tests/test_m3_dedup.py::test_same_version_two_payloads_both_conflict`、`tests/test_m3_dedup.py::test_tombstone_after_delete_conflicts` |
| TRAP-16 | 主文件声明 34 行实际 33 行，末行只剩半个 JSON | 半行被当有效事件，凭空多一条 | 声明条数校验 + 末行解析失败 → `E_TAIL_TRUNCATED` 隔离 | `app/modules/m1_events/reader.py` | `tests/test_m1_events.py::test_declared_rows_mismatch_marks_truncation`、`tests/test_m1_events.py::test_tail_unparsable_line_isolated_as_truncated` |
| TRAP-17 | v1 改列名（`ref_no`/`amt`/`st`）后新旧混发 | `KeyError` 或整行丢弃 | 别名映射到规范列名；新旧同时出现且取值不同判 `E_ALIAS_AMBIGUOUS` | `app/modules/m2_schema/alias.py` | `tests/test_m2_schema.py::test_v1_alias_maps_to_canonical`、`tests/test_m2_schema.py::test_alias_ambiguous_quarantined` |
| TRAP-19 | 一条事件同时像「重复」又像「冲突」，坏行还互相重复 | 按代码书写顺序覆盖，结果随遍历顺序变 | 优先级 `quarantine > conflict > duplicate > apply`，各桶由集合运算得出 | `app/modules/m3_dedup/service.py` | `tests/test_m3_dedup.py::test_conflict_beats_duplicate`、`tests/test_m3_dedup.py::test_priority_independent_of_input_order` |
| TRAP-23 | 事件 `event_time` 恰好等于水位线 / 恰好等于 `as_of` | 两端都算，重复计入或漏计 | 迟到用半开 `[水位线, +inf)`；快照含 `as_of`、待定窗口 `(as_of, +inf)` 半开 | `app/modules/m4_watermark/watermark.py`、`app/modules/m5_snapshot/materialize.py` | `tests/test_m4_watermark.py::test_boundary_equal_watermark_is_not_late`、`tests/test_m5_snapshot.py::test_boundary_event_at_as_of_included_once` |
| TRAP-26 | 拿上一轮旧导出再跑一次（最大事件 06:00 < 已持久化水位 08:52） | 无条件覆盖，水位回退、快照变旧 | 陈旧批次直接 409 `C_STALE_BATCH`，已落库结果不动 | `app/modules/m6_batches/resume.py`、`app/modules/m6_batches/state.py` | `tests/test_m6_batches.py::test_stale_input_rejected` |
| TRAP-28 | 上游还在写，末行只有半个 JSON、没有换行 | 把半行当坏行隔离，或当有效事件应用 | 未换行尾行挂起为待续尾行（`S_PENDING_TAIL`），不解析、不隔离、不计入数据行；截止位置停在它之前，续写完成后下一次采信 | `app/modules/m1_events/tail.py`、`app/modules/m1_events/reader.py` | `tests/test_cutoff_continuation.py::test_pending_tail_held_not_quarantined`、`tests/test_cutoff_continuation.py::test_whole_file_pending_is_not_empty_input` |
| TRAP-29 | GB18030 文件在多字节字符中间被切断 | 按声明解码整文件直接失败，整批 500 | 先在字节层切完整前缀再解码，半截字符留在待续尾行 | `app/modules/m1_events/tail.py`、`app/modules/m1_events/reader.py` | `tests/test_cutoff_continuation.py::test_half_multibyte_char_at_cut_is_pending` |
| TRAP-30 | CSV 引号字段内有换行，文件恰好在引号里被切断 | 朴素按最后换行切，半截记录被当成「列数不对」的坏行隔离 | CSV 截止位置取最后一个「引号外换行」，引号内切断整体挂起，补完后按一条完整记录解析 | `app/modules/m1_events/tail.py` | `tests/test_cutoff_continuation.py::test_csv_cut_inside_quoted_field_is_pending` |
| TRAP-31 | 追加与重写混在一起：同一文件被截短、换序、改写前几行 | 静默继续，旧记录丢了或算两次，谁也发现不了 | 批次键不含输入摘要，追加命中同一批次续跑；上次截止位置前的字节做 sha256 前缀校验，改写 / 截短 / 文件消失一律 409 `C_STREAM_REWRITTEN`，已落库结果不动 | `app/modules/m6_batches/continuation.py`、`app/modules/m6_batches/service.py` | `tests/test_cutoff_continuation.py::test_prefix_rewrite_rejected`、`tests/test_cutoff_continuation.py::test_shrunk_input_rejected`、`tests/test_cutoff_continuation.py::test_missing_input_file_rejected` |
| TRAP-32 | 落库中途失败，早班重试 | 留下半批数据，重试后记录翻倍 | 一次运行一个事务，失败整体回滚；重试按完整前缀重建，事件行不重不丢 | `app/store.py`、`app/modules/m6_batches/service.py` | `tests/test_cutoff_continuation.py::test_retry_after_failure_leaves_no_duplicates` |
| TRAP-33 | 事件流翻页用「第 N 页」或偏移，追加后页码漂移 | 两页之间漏行或重复，早班对不上 | keyset 翻页：页边界是到达序号 seq（追加只往后排，序号不动），响应带 `total/next_after/has_more`，拼页恰好是完整有序集合 | `app/store.py`、`app/modules/m1_events/handler.py` | `tests/test_cutoff_continuation.py::test_events_api_keyset_pagination_covers_all` |

## 对账规则

- **行账**：原始数据行 = 已应用 + 重复 + 冲突 + 隔离（空行与注释行单独计数，
  待续尾行不是数据行、不进该账）；
- **键账**：已应用键 = 快照键 + 只有待定事件的键；快照键 = 活跃键 + 墓碑键；
- **金额账**：快照金额 + 待定金额 + 变化金额 = 已应用金额（Decimal，单位元，2 位）；
- **迟到账**：迟到 + 准时 = 已应用（迟到只打标记，不改变应用结果）；
- **字节账**：文件字节 = 本次截止位移 + 待续尾行字节（逐文件成立，`cutoff_ok` 汇总，
  页面 `/batches` 与 `cutoffs.csv` 给出每文件的上次 → 本次截止行 / 字节）。
- 前四条账在首页、`/snapshot` 与 `manifest.json` 的 `reconcile` 中同时出现，任一条不平
  会显式为「不平」；字节账在 `manifest.json` 的 `cutoff` 节与 `/batches` 页面给出。

样例数据的四账：行 42 = 27 + 2 + 3 + 10；键 19 = 18 + 1；快照 18 = 16 活跃 + 2 墓碑；
金额 1552.51 + 200.00 + 0.00 = 1752.51。

## 反例与不变量

- 反例（朴素实现会挂）：
  - `tests/test_m1_events.py::test_naive_utf8_decode_would_raise`（按声明编码直接解码抛
    `UnicodeDecodeError`，回退链不抛）；
  - `tests/test_m3_dedup.py::test_conflict_beats_duplicate`（顺序覆盖会把它判成 1 冲突 + 1 重复）；
  - `tests/test_m3_dedup.py::test_low_version_before_delete_still_applies`（把「删除前的低版本」
    一律当复活，会丢掉 1001 的 v1/v2，快照金额少 120.50）；
  - `tests/test_m6_batches.py::test_stale_input_rejected`（后写覆盖会让水位从 08:52 退回 06:00）。
- 不变量：
  - `tests/test_invariants.py::test_row_accounting_identity`（行账守恒）；
  - `tests/test_invariants.py::test_key_accounting_identity`（键账守恒）；
  - `tests/test_invariants.py::test_amount_conservation_is_exact`（金额守恒，Decimal 精确）；
  - `tests/test_invariants.py::test_idempotent_rerun_keeps_rows_and_totals`（幂等重跑）；
  - `tests/test_m4_watermark.py::test_watermark_monotonic`（水位单调）；
  - `tests/test_m5_snapshot.py::test_rebuild_is_idempotent`（物化可重复）；
  - `tests/test_cutoff_continuation.py::test_append_continues_same_batch_exactly_once`
    （续跑后完整记录只进一次、已有记录序号不动）；
  - `tests/test_cutoff_continuation.py::test_events_api_keyset_pagination_covers_all`
    （翻页拼回完整有序集合，不重不漏）。

## 已知不覆盖

- 只处理文件型导出，不接 Kafka / Debezium 在线流；水位线是「批内推进」，不是全局流进度。
- 不做跨批次键级合并：每个批次独立折叠，快照只反映当前批次；跨批次的复活检测需要把历史
  状态一起纳入，属于下一期。
- 事务边界未建模：同一事务的多条变更不保证原子可见。
- 中文数字、货币符号（`￥`）不在金额归一范围内，遇到直接按类型错误隔离。
