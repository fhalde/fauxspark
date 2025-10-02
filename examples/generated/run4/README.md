# Generated Run 4

Auto-generated DAG with complex dependencies and shuffle operations.

## DAG Structure

The following Mermaid graph shows the dependency structure between stages:

```mermaid
graph TD
    Stage0["Stage 0<br/>Partitions: 8<br/>Task Avg: 22.5s"] --> Stage1["Stage 1<br/>Partitions: 8<br/>Task Avg: 10.6s<br/>Shuffle Avg: 15.0s"]
    Stage0 --> Stage2["Stage 2<br/>Partitions: 4<br/>Task Avg: 25.8s<br/>Shuffle Avg: 10.8s"]
    Stage2 --> Stage3["Stage 3<br/>Partitions: 10<br/>Task Avg: 26.9s<br/>Shuffle Avg: 8.0s"]
    Stage0 --> Stage4["Stage 4<br/>Partitions: 1<br/>Task Avg: 14.2s<br/>Shuffle Avg: 33.7s"]
    Stage1 --> Stage4
    Stage2 --> Stage4
    Stage3 --> Stage4

    classDef stageBox fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    class Stage0,Stage1,Stage2,Stage3,Stage4 stageBox
```