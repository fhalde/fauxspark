# Generated Run 3

Auto-generated DAG with complex dependencies and shuffle operations.

## DAG Structure

The following Mermaid graph shows the dependency structure between stages:

```mermaid
graph TD
    Stage0["Stage 0<br/>Partitions: 4<br/>Task Avg: 14.8s"] --> Stage1["Stage 1<br/>Partitions: 6<br/>Task Avg: 23.7s<br/>Shuffle Avg: 6.9s"]
    Stage0 --> Stage2["Stage 2<br/>Partitions: 1<br/>Task Avg: 17.0s<br/>Shuffle Avg: 6.6s"]
    Stage0 --> Stage3["Stage 3<br/>Partitions: 2<br/>Task Avg: 26.2s<br/>Shuffle Avg: 12.9s"]
    Stage2 --> Stage3
    Stage0 --> Stage4["Stage 4<br/>Partitions: 1<br/>Task Avg: 15.7s<br/>Shuffle Avg: 27.4s"]
    Stage1 --> Stage4
    Stage3 --> Stage4

    classDef stageBox fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    class Stage0,Stage1,Stage2,Stage3,Stage4 stageBox
```