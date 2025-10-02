# FauxSpark

A discrete event simulation of Apache Spark, built with SimPy.


> The implementation reflects my understanding of Apache Spark internals.
> Contributions, feedback, and discussions are welcome!

## Purpose

If you're running Apache Spark at large scale, experimentation can be costly and sometimes impractical. While data analysis can offer insights, I found simulations to be more approachable in understanding system behavior. Surprisingly, they work just fine!

This simulator intends to fills that gap by allowing you to experiment and observe runtime characteristics such as performance & reliability under different job schedules, cluster configurations, and failure rate distributions.

Like any simulator, the numbers produced here are approximate & may differ from real-world behavior, and are only as accurate as the model. **The plan of course is to make the model better** 😀

## Getting Started

```bash
git clone https://github.com/fhalde/fauxspark
cd fauxspark
uv sync
uv run sim -f examples/simple/dag.json

uv run sim -f examples/shuffle/dag.json -a true -d 2 --sf 0,7 1,13 -e 2 -c 2
# 1. auto-replace (-a) executor with a delay (-d) of 2 seconds.
# 2. simulate a executor failure (--sf) for executor 0 at t=7 and executor 1 at t=13.
# 3. initial cluster is comprised of 2 executors (-e) and 2 cores (-c).
```

## Help
```bash
options:
  -h, --help            show this help message and exit
  -e EXECUTORS, --executors EXECUTORS
                        Set the number of executors to use (default: 1).
  -c CORES, --cores CORES
                        Specify how many cores each executor will have (default: 1).
  -f FILE, --file FILE  Path to DAG JSON file
  --sf SF [SF ...]      Specify list of failure events as pairs of (executor_id,time) to simulate executor failures.
  --sa SA [SA ...]      Specify times (t) at which autoscaling (adding a new executor) should take place.
  -a AUTO_REPLACE, --auto-replace AUTO_REPLACE
                        Turn on/off auto-replacement of executors on failure.
  -d AUTO_REPLACE_DELAY, --auto-replace-delay AUTO_REPLACE_DELAY
                        Set the delay (in seconds) it takes to replace an executor on failure.
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
