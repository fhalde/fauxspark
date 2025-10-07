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

Consider a straightforward SQL query.
```sql
SELECT * FROM foo;
```
which may be represented using the [examples/simple/dag.json](https://github.com/fhalde/fauxspark/blob/main/examples/simple/dag.json).
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
This is a single stage query (no shuffle) reading an input of 1024 MB, uniformly distributed across 10 equal partitions. Based on historical analysis, our executor throughput for such an RDD is 102.4 MB/s.

Let's run the simulation:
```bash
(fauxspark) ➜  fauxspark git:(main) uv run sim -f examples/simple/dag.json
 10.00: [main        ] utilization: 1.0
 10.00: [main        ] job completed successfully
```
Execution time 10s. In Spark each task acquires a single core (by default). With 1 executor and 1 core, all tasks execute sequentially. Each task processes a partition of size 102.4 MB. So with an executor throughput of 102.4 MB/s, each task takes 1 second. Executed sequentially, the total runtime is 10 seconds.

Since the simulator is currently idealized, it reports 100% utilization.

You don't need a simulator to predict this! Well, almost... hang on.

A few more runs:
```
# double the cores
(fauxspark) ➜  fauxspark git:(main) uv run sim -f examples/simple/dag.json -c 2
  5.00: [main        ] utilization: 1.0
  5.00: [main        ] job completed successfully
```

```
# and again
(fauxspark) ➜  fauxspark git:(main) uv run sim -f examples/simple/dag.json -c 4
  3.00: [main        ] utilization: 0.8333333333333334
  3.00: [main        ] job completed successfully
```

Two observations:

1. The execution time didn't shrink by half unlike before (5.0 ➜ 3.0)
2. The utilization dropped by ~16%

There's nothing surprising about #1, it's just the number of tasks was not divisble by the cores. Looking at the schedule, we have 4 tasks scheduled in the first batch (1s), then another batch of 4 tasks (1s), and finally 2 tasks (1s) totaling to 3s.

To understand utilization, we first need to define it. In the simulator, utilization is defined as:
> Σ task.runtime / Σ executor.uptime * executor.cores

In our example, the last batch consisted of only 2 tasks, each using 1 core, leaving the remaining 2 cores underutilized hence the drop.

Alright, this was fun, but it still doesn't justify building a simulator.
