# Generated Run 2

Auto-generated DAG with complex dependencies and shuffle operations.

## DAG Structure

The following Mermaid graph shows the dependency structure between stages:

```mermaid
graph TD
    Stage0["Stage 0<br/>Partitions: 6<br/>Task Avg: 28s"] --> Stage2["Stage 2<br/>Partitions: 1<br/>Task Avg: 16s<br/>Shuffle Avg: 7s"]
    Stage1["Stage 1<br/>Partitions: 7<br/>Task Avg: 14s"] --> Stage3["Stage 3<br/>Partitions: 9<br/>Task Avg: 20s<br/>Shuffle Avg: 14s"]
    Stage0 --> Stage3
    Stage2 --> Stage3
    Stage0 --> Stage4["Stage 4<br/>Partitions: 1<br/>Task Avg: 15s<br/>Shuffle Avg: 29s"]
    Stage3 --> Stage4

    classDef stageBox fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    class Stage0,Stage1,Stage2,Stage3,Stage4 stageBox
```
