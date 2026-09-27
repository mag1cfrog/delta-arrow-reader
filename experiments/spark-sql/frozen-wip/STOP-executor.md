# Executor 停止记录

停止时间: 2026-09-27 06:19 UTC. 已按协调者转达的用户决定, 将 Spark SQL 工作冻结为未完成的工程实验. 协调者已告知原生任务 delta-arrow-reader-linux 暂停. 本线程不再推进, 除非用户明确重新授权.

## 仓库状态

- 仓库: `/home/hanbo/repo/delta-arrow-reader`
- 分支: `fix/local-projection-error-order`
- HEAD: `bde063e527547f20e687548cd8cda098c1b0fb88`
- Tree: `d6f15fdf5880675f11574f576b75be7d3042af05`
- 工作树干净, 没有暂存或未提交的仓库文件.
- STOP 前已合并 PR 292. 独立 PASS 对应 head `0c7e63cf45b551e56eefd984a1990546a5d79ed2`, required checks 成功, 合并 tree 与批准 tree 相同. 未修改 main 或发布包.

## 保留的未完成工作

当前任务是 C04 多列局部 CAST 首错修复 #293. STOP 前已指派给 mag1cfrog, 使用原生关系挂到 #172 下并阻塞 #149. C04 当前为 24 个已关闭叶子, 2 个开放叶子. #149/#172/#293 均未关闭.

未提交的 Rust 候选代码和全部私有证据保留在:

`/home/hanbo/.cache/delta-reader-spark-planning/local-projection-order-2a82385/`

- 候选: `after-crate/src/decimal_null.rs`, SHA-256 `654bb7e8e7316ab6277e87720d9e264b6084171bcece919abca3e70588bcac1b`. 成对源码、构建命令、依赖身份、probe、日志均保留. 没有覆盖共享源码或可执行文件槽位.
- 聚焦证据: `cases.jsonl`, `spark.json`, `accepted-before.json`, `before.json`, `after.json`, `focused-summary.json` 等. 52 条 SQL / 每引擎 104 个观察. 77 个成功值/类型匹配, 候选的 26 个 Decimal 首值匹配. 字符串控制的原因类别匹配, 专门的首值检查仍为 unobserved. 5 个 logical、6 个 physical nullability 差异保留, 27 个失败的 physical schema 未观察.
- 原生回归: `native-test*.rs`, `test-results.json` 和成对构建/运行日志. Before 失败, after 通过. 这是执行者验证, 尚无独立批准.
- 已启动的 `replay.py` 在停止清理时已经完成. `replay/`, `replay.log`, `replay-summary.json` 保留 67 组 / 31,890 个重叠观察, 旧比较器前后均匹配 30,513, 无丢失或新增匹配. 尚未完成候选的原始变化审计及最终验收.
- 成本准备: `predicate-bench.rs`, `bench-cases.py`, `bench-expressions.json`, `before-bench` 及构建记录. 仅 baseline benchmark 完成链接. 没有链接 candidate benchmark, 没有运行计时, 没有接受新增成本. 成本仍归 #159.

候选仅在确定性局部多列投影失败后按行复核, 复用 CAST/local batch 入口, 并处理更早的混合算术检查入口. 尚未生成仓库 patch、commit、PR 或独立审查. 不能根据聚焦检查宣称修复已验收.

已接受的 PR 292 证据及收尾文件保留在:
`/home/hanbo/.cache/delta-reader-spark-planning/arithmetic-error-coverage-2a82385/`

独立审查记录保留在:
`/home/hanbo/.local/share/delta-arrow-reader-review/reviews/`

## 进程与停止边界

待收取的两个自有会话为 replay 44297 和 baseline benchmark linker 64169, 均已自行完成, exit code 0. /proc 检查未发现仍使用当前私有实验目录的进程, 无需终止进程.

STOP 后只核对状态、收取已完成进程和写本记录. 没有新实验、切换分支、reset、stash、删除、提交、合并、issue 修改或审查/继续通知. Schema/诊断契约、历史参考问题、候选审查与成本验证、默认构建采用 #165 等仍未完成. Issue/文档收尾由协调者负责.
