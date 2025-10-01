# FauxSpark

A discrete event simulation of Apache Spark, built with SimPy.


> The implementation reflects my understanding of Apache Spark internals.
> Contributions, feedback, and discussions are welcome!

## Purpose

If you're running Apache Spark at large scale, experimentation can be costly and sometimes impractical. While data analysis can offer insights, I found simulations more intuitive/approachable. Surprisingly, they work just fine!

This simulator intends to fills that gap by allowing you to experiment and observe runtime characteristics such as performance & reliability under different job schedules, cluster configurations, and failure rate distributions.

Like any simulator, the numbers produced here are approximate & may differ from real-world behavior, and are only as accurate as the model. **The plan of course is to make the model better** 😀

## Getting Started

```bash
git clone https://github.com/fhalde/fauxspark;
cd fauxspark;
uv sync;
uv run sim --executors 1 --cores 1 -f examples/dag.json;
```

## ✅ Current Features

FauxSpark currently simulates a simplified version of Apache Spark, including:

- DAG execution: stages, tasks, and dependencies
- Executor failure handling with automatic retries
- Shuffle fetch failures and stage resubmission
- Basic shuffle read
- Supports single-job execution

## 🚀 Future Ideas

Planned enhancements:

- Speculative task execution
- Spark's Caching mechanism
- Support for multiple concurrent jobs with fair resource sharing
- Modeling Cluster topology (e.g., for inter-AZ traffic and cost)
- Enhanced reporting
- Accepting RDD graphs / SparkPlans as input

Some stretch goals:
- Modeling Network & Disk IO (e.g., network bandwidth to observe changes in shuffle performance, spills)
- Adaptive Query Execution (AQE) behavior
