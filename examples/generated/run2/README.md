# Generated Run 2

Auto-generated DAG with complex dependencies and shuffle operations.

## DAG Structure

The following Mermaid graph shows the dependency structure between stages:

```mermaid
graph TD
    Stage0["Stage 0<br/>Partitions: 6<br/>Task Avg: 28.4s"] --> Stage2["Stage 2<br/>Partitions: 1<br/>Task Avg: 16.8s<br/>Shuffle Avg: 7.7s"]
    Stage1["Stage 1<br/>Partitions: 7<br/>Task Avg: 14.3s"] --> Stage3["Stage 3<br/>Partitions: 9<br/>Task Avg: 20.9s<br/>Shuffle Avg: 14.3s"]
    Stage0 --> Stage3
    Stage2 --> Stage3
    Stage0 --> Stage4["Stage 4<br/>Partitions: 1<br/>Task Avg: 15.2s<br/>Shuffle Avg: 29.1s"]
    Stage3 --> Stage4

    classDef stageBox fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    class Stage0,Stage1,Stage2,Stage3,Stage4 stageBox
```