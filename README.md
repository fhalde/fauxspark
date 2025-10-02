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
uv run sim -f examples/dag.json;
```

## ✅ Current Features

FauxSpark currently implements a simplified model of Apache Spark, which includes:

- A DAG execution engine with Stages and Tasks
- Automatic retries when Executors fail
- Handling of shuffle-fetch failures with stage resubmission
- Naive shuffle reads
- Only supports running a single job

## 🚀 Future Ideas

Planned enhancements:

- Speculative Task Execution
- Caching in Spark
- Support for multiple concurrent jobs & fair resource sharing
- Modeling different cluster topologies (e.g., for inter-AZ traffic and cost)
- Enhanced reporting
- Accepting RDD graphs / SparkPlans as input

Some stretch goals:
- Modeling Network & Disk IO (e.g., network bandwidth to observe changes in shuffle performance, spills)
- Adaptive Query Execution (AQE) behavior
