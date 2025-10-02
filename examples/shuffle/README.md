# Shuffle Example DAG

This example demonstrates a shuffle operation with stage dependencies.

## DAG Structure

The following Mermaid graph shows the dependency structure between stages:

```mermaid
graph TD
    Stage0["Stage 0<br/>Tasks: 2<br/>Avg: 5.0s"] --> Stage1["Stage 1<br/>Tasks: 5<br/>Avg: 5.0s<br/>Shuffle Avg: 0.0s"]

    classDef stageBox fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    class Stage0,Stage1 stageBox
```

## Stage Details

- **Stage 0**: Initial stage with 2 tasks (no dependencies)
  - Average execution time: 5.0 seconds
- **Stage 1**: Shuffle stage with 5 tasks (depends on Stage 0)
  - Average execution time: 5.0 seconds
  - Average shuffle time: 0.0 seconds

The shuffle operation increases the number of tasks from 2 to 5, requiring data redistribution between stages.
