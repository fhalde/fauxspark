# Generated Run 2

Auto-generated DAG with complex dependencies and shuffle operations.

## DAG Structure

The following Mermaid graph shows the dependency structure between stages:

```mermaid
graph TD
    Stage0["Stage 0<br/>Partitions: 6<br/>Task Avg: 13.2s<br/>Shuffle Avg: 13.8s"] --> Stage1["Stage 1<br/>Partitions: 1<br/>Task Avg: 20.2s<br/>Shuffle Avg: 11.7s"]
    Stage2["Stage 2<br/>Partitions: 10<br/>Task Avg: 15.7s<br/>Shuffle Avg: 12.2s"] --> Stage3["Stage 3<br/>Partitions: 6<br/>Task Avg: 14.9s<br/>Shuffle Avg: 12.7s"]
    Stage0 --> Stage3
    Stage1 --> Stage4["Stage 4<br/>Partitions: 1<br/>Task Avg: 5.0s"]
    Stage2 --> Stage4
    Stage3 --> Stage4

    classDef stageBox fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    class Stage0,Stage1,Stage2,Stage3,Stage4 stageBox
```

This generated DAG demonstrates:
- Two independent root stages (0 and 2)
- Convergence at Stage 3 from multiple sources
- Final aggregation at Stage 4 from three different paths
- High partition count in Stage 2 (10 partitions)
