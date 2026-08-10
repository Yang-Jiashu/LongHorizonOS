# Semantic Repair Comparison

| Metric | Result |
|---|---:|
| Valid deterministic trials | 24 / 24 |
| Mean full-restart reruns | 17.5 |
| Mean task-DAG checkpoint reruns | 9.416667 |
| Mean LongHorizonOS observed reruns | 9.416667 |
| Mean weighted saving vs full restart | 0.486427 |
| Mean weighted saving vs checkpoint | 0.0 |
| False VERIFIED after invalidation | 0 |
| Ownership interval conflicts | 0 |
| State-only false-closure trials | 24 |
| Real-workspace repair attempts | 3 |
| Real-workspace Goal reclosed | True |

The task-DAG checkpoint baseline is intentionally oracle-informed. Parity against it is an honest result for these task-level mutation workloads.
