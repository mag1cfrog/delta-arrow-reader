# Planning attribution (#1390, PR1)

Canonical walls use untouched production source. Observed stages use the committed disposable overlay. No query was executed.

Source baseline: `9fa5b4cd98fecc7a2e95debc67128c394b25fb9a`. Protocol: `d2dffd3793b20dc1dc5803c64b90f9a4418335be`.

All 120 paired/stress configuration cases and 558 applicable Spark sweep plans matched their canonical public plan displays, properties and exact schemas. The frozen #1379 execution checks provide historical result evidence; this run did not repeat result validation.

[Wall times](planning-1390-walls.csv) retain both native controls and observer inflation. [Stage medians](planning-1390-stages.csv) include every measured case, rule application count and pass indices. [A/B noise](planning-1390-noise.csv) gives each observed stage's same-binary difference. [Large parts](planning-1390-large-parts.csv) lists each component larger than its whole canonical native-configured phase.

Parent intervals include their child intervals. Residuals contain framework work, teardown and observation overhead. DefaultPhysicalPlanner includes physical optimizer passes and their validation. Small parts near their A/B difference remain unresolved; the observer wall difference is not subtracted from their values.

| Component | Cases above native phase | Largest median (ms) | Code |
| --- | ---: | ---: | --- |
| `logical.parsing` | 56 | 10.058283 | `sail-sql-analyzer/src/parser.rs::parse_one_statement` |
| `logical.parsing.lexing` | 2 | 0.803042 | `sail-sql-analyzer/src/parser.rs::parse! lexer.parse` |
| `logical.parsing.parse` | 40 | 9.229173 | `sail-sql-analyzer/src/parser.rs::parse! parser.parse` |
| `logical.residual` | 9 | 1.692324 | `src/lib.rs::plan_query; residual includes framework/teardown and observer overhead` |
| `logical.resolution` | 14 | 3.012967 | `sail-plan/src/resolver/plan.rs::resolve_named_plan` |
| `physical.analyzer` | 2 | 4.050514 | `vendor/dependencies/datafusion-optimizer/src/analyzer/mod.rs::execute_and_check` |
| `physical.frame` | 60 | 15.602612 | `datafusion-54.1.0/src/execution/session_state.rs::create_physical_plan` |
| `physical.optimizer` | 12 | 8.366170 | `vendor/dependencies/datafusion-optimizer/src/optimizer.rs::optimize` |
| `physical.optimizer.rule/optimize_projections/body` | 2 | 4.848326 | `vendor/dependencies/datafusion-optimizer/src/optimize_projections/mod.rs` |
| `physical.planner` | 4 | 3.064572 | `sail-plan/src/physical_plan/mod.rs::SparkQueryPlanner::create_physical_plan` |

Raw samples, full plan traces, assembly hashes and build receipts accompany these tables. No parser or planning optimization was made. Exact public snapshots do not compare opaque execution-node internals.
