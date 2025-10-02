# Generated Run 5

Auto-generated DAG with complex dependencies and shuffle operations.

## DAG Structure

The following Mermaid graph shows the dependency structure between stages:

```mermaid
graph TD
    Stage0["Stage 0<br/>Partitions: 4<br/>Task Avg: 24.3s"] --> Stage1["Stage 1<br/>Partitions: 7<br/>Task Avg: 29.9s<br/>Shuffle Avg: 11.9s"]
    Stage1 --> Stage2["Stage 2<br/>Partitions: 4<br/>Task Avg: 18.1s<br/>Shuffle Avg: 10.1s"]
    Stage2 --> Stage3["Stage 3<br/>Partitions: 2<br/>Task Avg: 20.9s<br/>Shuffle Avg: 5.3s"]
    Stage1 --> Stage4["Stage 4<br/>Partitions: 1<br/>Task Avg: 16.6s<br/>Shuffle Avg: 34.1s"]
    Stage2 --> Stage4
    Stage3 --> Stage4

    classDef stageBox fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    class Stage0,Stage1,Stage2,Stage3,Stage4 stageBox
```