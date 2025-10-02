# Generated Run 4

Auto-generated DAG with complex dependencies and shuffle operations.

## DAG Structure

The following Mermaid graph shows the dependency structure between stages:

```mermaid
graph TD
    Stage0["Stage 0<br/>Partitions: 3<br/>Task Avg: 15.2s<br/>Shuffle Avg: 8.1s"] --> Stage1["Stage 1<br/>Partitions: 9<br/>Task Avg: 16.0s<br/>Shuffle Avg: 10.4s"]
    Stage0 --> Stage2["Stage 2<br/>Partitions: 6<br/>Task Avg: 13.2s<br/>Shuffle Avg: 9.4s"]
    Stage1 --> Stage2
    Stage0 --> Stage3["Stage 3<br/>Partitions: 2<br/>Task Avg: 21.9s<br/>Shuffle Avg: 10.8s"]
    Stage1 --> Stage3
    Stage0 --> Stage4["Stage 4<br/>Partitions: 1<br/>Task Avg: 5.0s"]
    Stage1 --> Stage4
    Stage2 --> Stage4
    Stage3 --> Stage4

    classDef stageBox fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    class Stage0,Stage1,Stage2,Stage3,Stage4 stageBox
```

This generated DAG demonstrates:
- Single root stage (0) feeding into multiple branches
- High fan-out from Stage 0 to all other stages
- Complex convergence at final Stage 4 from all intermediate stages
- Highest partition count in Stage 1 (9 partitions)
