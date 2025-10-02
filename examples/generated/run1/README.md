# Generated Run 1

Auto-generated DAG with complex dependencies and shuffle operations.

## DAG Structure

The following Mermaid graph shows the dependency structure between stages:

```mermaid
graph TD
    Stage0["Stage 0<br/>Partitions: 2<br/>Task Avg: 18.4s"] --> Stage1["Stage 1<br/>Partitions: 3<br/>Task Avg: 28.2s<br/>Shuffle Avg: 10.6s"]
    Stage1 --> Stage2["Stage 2<br/>Partitions: 4<br/>Task Avg: 21.3s<br/>Shuffle Avg: 9.2s"]
    Stage1 --> Stage3["Stage 3<br/>Partitions: 1<br/>Task Avg: 22.9s<br/>Shuffle Avg: 11.6s"]
    Stage2 --> Stage3
    Stage0 --> Stage4["Stage 4<br/>Partitions: 1<br/>Task Avg: 5.0s"]
    Stage3 --> Stage4

    classDef stageBox fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    class Stage0,Stage1,Stage2,Stage3,Stage4 stageBox
```

