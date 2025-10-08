# FauxSpark

A discrete event simulation of Apache Spark, built with SimPy.


> The implementation reflects my understanding of Apache Spark internals.
> Contributions, feedback, and discussions are welcome!

## Purpose

If you're running Apache Spark at large scale, experimentation can be costly and sometimes impractical. While data analysis can offer insights, I found simulations to be more approachable in understanding system behavior. Surprisingly, they work just fine!

This simulator intends to fill that gap by allowing users to experiment and observe Apache Spark's runtime characteristics such as performance and reliability for different job schedules, cluster configurations, and failure modes.

Like any simulator, the numbers produced here are approximate & may differ from real-world behavior, and are only as accurate as the model. **The plan of course is to make the model better** 😀

A [walkthrough](https://github.com/fhalde/fauxspark?tab=readme-ov-file#walkthrough) demonstrating how to use this tool is provided below.
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
00:00:03: [main        ] job completed successfully
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

Analyzing such situations & planning for it can quickly become challenging. For example, let's consider **skews**.
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
This will split our 1024 MB input into 10 randomly sized partitions that follow a [Pareto distribution](https://en.wikipedia.org/wiki/Pareto_distribution), resulting in a heavily skewed dataset. Far more realistic!

<img width="593" height="442" alt="Screenshot 2025-10-08 at 18 50 36" src="https://github.com/user-attachments/assets/ec1128a0-95af-4401-bb1c-bd2140880034" />

Running the sim:
```
(fauxspark) ➜  fauxspark git:(main) ✗ uv run sim -f examples/simple/dag.json -c 5 | grep 'input bytes'
00:00:00: [executor-0  ] [0-0] input bytes=2.63 MB
00:00:00: [executor-0  ] [0-1] input bytes=18.5 MB
00:00:00: [executor-0  ] [0-2] input bytes=17.51 MB
00:00:00: [executor-0  ] [0-3] input bytes=81.51 MB
00:00:00: [executor-0  ] [0-4] input bytes=504.2 MB
00:00:00: [executor-0  ] [0-5] input bytes=11.3 MB
00:00:00: [executor-0  ] [0-6] input bytes=3.91 MB
00:00:00: [executor-0  ] [0-7] input bytes=310.48 MB
00:00:00: [executor-0  ] [0-8] input bytes=45.93 MB
00:00:00: [executor-0  ] [0-9] input bytes=28.02 MB
```
It's clear that some tasks are handling more data than others. Running the sim several times, it's not clear how to interpret the numbers.
```
(fauxspark) ➜  fauxspark git:(main) ✗ uv run sim -f examples/simple/dag.json -c 5
00:00:03: [main        ] job completed successfully
00:00:03: [report      ] {"utilization": 0.631879572897418, "runtime": 3.165160080787559}

(fauxspark) ➜  fauxspark git:(main) ✗ uv run sim -f examples/simple/dag.json -c 5
00:00:04: [main        ] job completed successfully
00:00:04: [report      ] {"utilization": 0.42678031467385535, "runtime": 4.686251758187104}

(fauxspark) ➜  fauxspark git:(main) ✗ uv run sim -f examples/simple/dag.json -c 5
00:00:02: [main        ] job completed successfully
00:00:02: [report      ] {"utilization": 0.7940439222567488, "runtime": 2.5187523560608693}
```
Since our simulation is now stochastic (using random inputs), the outputs will constantly vary. You can't rely on any single run to draw conclusions. You run thousands of simulations to form a statistically significant result. 

> Randomness is seemingly chaotic, yet inherently consistent

Suppose we anticipate that our dataset will skew over the coming months, and your team wants to plan capacity to minimize wasted cost (1 − utilization) while maintaining the target SLA. You could write a optimization function such as [this](https://github.com/fhalde/fauxspark/blob/main/fauxspark/main.py#L203).

This function takes two inputs

- utilization
- runtime
  
and performs 10k simulations for each cluster configuration and selects the ones where the p90 of 10k sim runtimes is below the target SLA and the p10 utilization of 10k sims exceeds the required utilization. I chose p90 and p10 arbitrarily for this example.

Let's be ambitious

```
>>> m.optimizer(utilization=1, runtime=1)
```
| Status | Cores | Utilization (p10) | Runtime (p90) |
|:------:|------:|----------------:|------------:|
| 👎     | 1     | 1.0000          | 10.0000     |
| 👎     | 2     | 0.6491          | 7.7028      |
| 👎     | 3     | 0.4586          | 7.2678      |
| 👎     | 4     | 0.3518          | 7.1073      |
| 👎     | 5     | 0.2847          | 7.0252      |
| 👎     | 6     | 0.2393          | 6.9648      |
| 👎     | 7     | 0.2057          | 6.9449      |
| 👎     | 8     | 0.1805          | 6.9240      |
| 👎     | 9     | 0.1607          | 6.9129      |
| 👎     | 10    | 0.1447          | 6.9116      |

_pretty printed markdown table from console logs_


It's apparent that under skewed conditions, utilization declines quickly. We might have to sacrifice some $$ for the projected skew or simply mitigate skew altogether.
```
>>> m.optimizer(utilization=0.7, runtime=8)
```
| Status | Cores | Utilization (p10) | Runtime (p90) |
|:------:|------:|----------------:|------------:|
| 👎     | 1     | 1.0000          | 10.0000     |
| 👎     | 2     | 0.6562          | 7.6200      |
| 👎     | 3     | 0.4634          | 7.1939      |
| 👎     | 4     | 0.3558          | 7.0263      |
| 👎     | 5     | 0.2882          | 6.9407      |
| 👎     | 6     | 0.2422          | 6.8826      |
| 👎     | 7     | 0.2080          | 6.8680      |
| 👎     | 8     | 0.1825          | 6.8484      |
| 👎     | 9     | 0.1624          | 6.8417      |
| 👎     | 10    | 0.1462          | 6.8400      |



```
>>> m.optimizer(utilization=0.6, runtime=8)
```
| Status | Cores | Utilization (p10) | Runtime (p90) |
|:------:|------:|----------------:|------------:|
| 👎     | 1     | 1.0000          | 10.0000     |
| ✅     | 2     | 0.6536          | 7.6503      |
| 👎     | 3     | 0.4631          | 7.1980      |
| 👎     | 4     | 0.3553          | 7.0370      |
| 👎     | 5     | 0.2875          | 6.9559      |
| 👎     | 6     | 0.2409          | 6.9198      |
| 👎     | 7     | 0.2071          | 6.8974      |
| 👎     | 8     | 0.1816          | 6.8832      |
| 👎     | 9     | 0.1615          | 6.8816      |
| 👎     | 10    | 0.1453          | 6.8804      |


Finally! The simulation is suggesting us that we are better off with just 2 cores that provides us with 65% utilization and a runtime of 7.6s.

By the way, did you notice that despite all the randomness in our simulations, the p10 and p90 percentiles consistently converge?! And that's simulation theory.
