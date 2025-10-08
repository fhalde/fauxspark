# FauxSpark

A discrete event simulation of Apache Spark, built with SimPy.


> The implementation reflects my understanding of Apache Spark internals.
> Contributions, feedback, and discussions are welcome!

## Purpose

If you're running Apache Spark at large scale, experimentation can be costly and sometimes impractical. While data analysis can offer insights, I found simulations to be more approachable in understanding system behavior. Surprisingly, they work just fine!

This simulator intends to fill that gap by allowing users to experiment and observe Apache Spark's runtime characteristics such as performance and reliability for different job schedules, cluster configurations, and failure modes.

Like any simulator, the numbers produced here are approximate & may differ from real-world behavior, and are only as accurate as the model. **The plan of course is to make the model better** 😀

A [walkthrough](https://github.com/fhalde/fauxspark/edit/main/README.md#walkthrough) demonstrating how to use this tool is provided below.
## Getting Started

```bash
git clone https://github.com/fhalde/fauxspark
cd fauxspark
uv sync
uv run sim -f examples/simple/dag.json

uv run sim -f examples/shuffle/dag.json -a true -d 2 --sf 0,7 1,13 -e 2 -c 2
# 1. auto-replace (-a) executor on failure with a delay (-d) of 2 seconds
# 2. simulate a failure (--sf) for executor 0 at t=7 and executor 1 at t=13
# 3. bootstrap the cluster with 2 executors (-e) and 2 cores (-c) each
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

- DAG scheduling with stages, tasks, and dependencies
- Automatic retries on executor or shuffle-fetch failures
- Single-job execution with configurable cluster parameters
- Simple CLI to tweak cluster size, simulate failures, and scaling up executors

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

## Walkthrough

**Consider** a straightforward SQL query.
```sql
SELECT * FROM foo;
```
which could be represented using this json [examples/simple/dag.json](https://github.com/fhalde/fauxspark/blob/main/examples/simple/dag.json).
```json
[
  {
    "id": 0,
    "deps": [],
    "status": "pending",
    "input": {
      "size": "1024 MB",
      "partitions": 10,
      "distribution": {
        "kind": "uniform"
      }
    }
    "throughput": "102.4 MB",
    "tasks": []
  }
]

```
This is a single stage query (no shuffle) reading an input of 1024 MB uniformly distributed across 10 partitions. Let's assume a single core can process 102.4 MB/s for now.

Let's run the simulation:
```bash
(fauxspark) ➜  fauxspark git:(main) uv run sim -f examples/simple/dag.json # 1 executor, 1 core (default parameters)
00:00:10: [main        ] job completed successfully
00:00:10: [report      ] {"utilization": 1.0, "runtime": 10.0}
```
Since the simulator is currently idealized (no network/scheduling delays etc.,) the utilization is 1.0 (100%).

A few more runs:
```
# double the cores
(fauxspark) ➜  fauxspark git:(main) uv run sim -f examples/simple/dag.json -c 2
00:00:05: [main        ] job completed successfully
00:00:05: [report      ] {"utilization": 1.0, "runtime": 5.0}
```

```
# and again
(fauxspark) ➜  fauxspark git:(main) uv run sim -f examples/simple/dag.json -c 4
00:00:03: [main        ] job completed successfullyutilization: 0.8333333333333334
00:00:03: [report      ] {"utilization": 0.8333333333333334, "runtime": 3.0}
```

Two observations:

1. The execution time didn't shrink by half unlike before (5.0 ➜ 3.0)
2. The utilization dropped by ~16%

Observation #1 is not surprising. The total number of tasks wasn't divisible by the number of cores. Looking at the schedule step by step:

- First batch: 4 tasks run in 1s
- Second batch: 4 tasks run in 1s
- Final batch: 2 tasks run in 1s

This adds up to a total runtime of 3s.

To understand utilization, we first need to define it. In the simulator, utilization is defined as:
> Σ task.runtime / Σ executor.uptime * executor.cores

In our example, the last batch kept 2 cores idle hence the drop in utilization.

----

**The** example above didn't really justify a simulator – but neither was the example either!

In practice, jobs:

- process non-uniformly distributed data – often skewed,
- are often quite complex,
- may encounter failures during execution.

Analyzing such situations & planning for it can quickly become challenging.

Time to get real: **skews**.
```json
"input": {
  "size": "1024 MB",
  "partitions": 10,
  "distribution": {
    "kind": "pareto",
    "alpha": 1.5
  }
}
```
This will generate 10 randomly sized partitions that together sum to 1024 MB and are heavily skewed. Far more realistic!
<img width="593" height="442" alt="Screenshot 2025-10-08 at 18 50 36" src="https://github.com/user-attachments/assets/ec1128a0-95af-4401-bb1c-bd2140880034" />
