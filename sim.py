import simpy
import random
from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple
import heapq


class DAGNode:
    def __init__(self, node_id: str, num_tasks: int, task_duration: float):
        self.node_id = node_id
        self.num_tasks = num_tasks
        self.task_duration = task_duration
        self.dependencies = []  # List of node_ids this node depends on
        self.dependents = []  # List of node_ids that depend on this node
        self.completed_tasks = 0
        self.running_tasks = 0
        self.is_ready = False  # True when all dependencies are complete

    def add_dependency(self, dep_node_id: str):
        if dep_node_id not in self.dependencies:
            self.dependencies.append(dep_node_id)

    def add_dependent(self, dep_node_id: str):
        if dep_node_id not in self.dependents:
            self.dependents.append(dep_node_id)

    def is_complete(self):
        return self.completed_tasks >= self.num_tasks

    def has_pending_tasks(self):
        return (self.completed_tasks + self.running_tasks) < self.num_tasks

    def __repr__(self):
        return f"DAGNode({self.node_id}, tasks: {self.completed_tasks}/{self.num_tasks})"


class ExecutorNode:
    def __init__(self, node_id: str, num_slots: int):
        self.node_id = node_id
        self.num_slots = num_slots
        self.available_slots = num_slots
        self.running_tasks = []  # List of (task_info, completion_event)

    def has_available_slots(self):
        return self.available_slots > 0

    def allocate_slot(self):
        if self.available_slots > 0:
            self.available_slots -= 1
            return True
        return False

    def release_slot(self):
        self.available_slots += 1

    def __repr__(self):
        return f"ExecutorNode({self.node_id}, slots: {self.available_slots}/{self.num_slots})"


class SparkScheduler:
    def __init__(self, env: simpy.Environment):
        self.env = env
        self.dag_nodes: Dict[str, DAGNode] = {}
        self.executor_nodes: Dict[str, ExecutorNode] = {}
        self.ready_queue = deque()  # Queue of ready DAG nodes
        self.completed_nodes: Set[str] = set()
        self.total_tasks_completed = 0
        self.total_tasks = 0

        # Events for coordination
        self.task_completion_event = env.event()
        self.scheduling_event = env.event()

    def add_dag_node(self, node: DAGNode):
        self.dag_nodes[node.node_id] = node
        self.total_tasks += node.num_tasks

        # Check if node is initially ready (no dependencies)
        if not node.dependencies:
            node.is_ready = True
            self.ready_queue.append(node.node_id)

    def add_executor_node(self, node: ExecutorNode):
        self.executor_nodes[node.node_id] = node

    def add_dependency(self, from_node_id: str, to_node_id: str):
        """Add dependency: from_node must complete before to_node can start"""
        if from_node_id in self.dag_nodes and to_node_id in self.dag_nodes:
            self.dag_nodes[to_node_id].add_dependency(from_node_id)
            self.dag_nodes[from_node_id].add_dependent(to_node_id)

    def check_node_ready(self, node_id: str):
        """Check if a node's dependencies are satisfied"""
        node = self.dag_nodes[node_id]
        if node.is_ready:
            return True

        # Check if all dependencies are complete
        for dep_id in node.dependencies:
            if not self.dag_nodes[dep_id].is_complete():
                return False

        node.is_ready = True
        return True

    def schedule_task(self, dag_node_id: str, executor_node_id: str):
        """Schedule a single task from a DAG node on an executor"""
        dag_node = self.dag_nodes[dag_node_id]
        executor_node = self.executor_nodes[executor_node_id]

        if not executor_node.allocate_slot():
            return None

        dag_node.running_tasks += 1

        # Create task completion event
        task_completion = self.env.event()

        # Schedule task completion
        def complete_task():
            yield self.env.timeout(dag_node.task_duration)
            task_completion.succeed()

        self.env.process(complete_task())

        # Handle task completion
        def handle_completion():
            yield task_completion

            # Update state
            dag_node.running_tasks -= 1
            dag_node.completed_tasks += 1
            executor_node.release_slot()
            self.total_tasks_completed += 1

            print(
                f"Time {self.env.now:.2f}: Task completed on {executor_node_id} "
                f"for DAG node {dag_node_id} ({dag_node.completed_tasks}/{dag_node.num_tasks})"
            )

            # Check if DAG node is complete
            if dag_node.is_complete() and dag_node_id not in self.completed_nodes:
                self.completed_nodes.add(dag_node_id)
                print(f"Time {self.env.now:.2f}: DAG node {dag_node_id} completed")

                # Enable dependent nodes
                for dependent_id in dag_node.dependents:
                    if self.check_node_ready(dependent_id) and dependent_id not in self.ready_queue:
                        self.ready_queue.append(dependent_id)
                        print(f"Time {self.env.now:.2f}: DAG node {dependent_id} now ready")

            # Trigger scheduling event
            if not self.scheduling_event.triggered:
                self.scheduling_event.succeed()

        self.env.process(handle_completion())
        return task_completion

    def scheduler_process(self):
        """Main scheduler process"""
        while self.total_tasks_completed < self.total_tasks:
            scheduled_any = False

            # Try to schedule tasks from ready queue
            ready_nodes = list(self.ready_queue)
            for dag_node_id in ready_nodes:
                dag_node = self.dag_nodes[dag_node_id]

                if not dag_node.has_pending_tasks():
                    # Remove from ready queue if no more tasks
                    if dag_node_id in self.ready_queue:
                        self.ready_queue.remove(dag_node_id)
                    continue

                # Find available executor
                for executor_node_id, executor_node in self.executor_nodes.items():
                    if executor_node.has_available_slots():
                        self.schedule_task(dag_node_id, executor_node_id)
                        scheduled_any = True
                        print(
                            f"Time {self.env.now:.2f}: Scheduled task from {dag_node_id} on {executor_node_id}"
                        )
                        break

            # If we couldn't schedule anything, wait for task completion
            if not scheduled_any:
                self.scheduling_event = self.env.event()
                yield self.scheduling_event
            else:
                # Small delay to allow for task completions
                yield self.env.timeout(0.1)

        print(f"Time {self.env.now:.2f}: All tasks completed!")

    def run_simulation(self):
        """Run the complete simulation"""
        print("Starting Spark Scheduler simulation...")
        print(f"Total tasks to process: {self.total_tasks}")
        print(f"Available executors: {list(self.executor_nodes.keys())}")
        print()

        # Start scheduler process
        scheduler_proc = self.env.process(self.scheduler_process())

        # Run simulation
        self.env.run(until=scheduler_proc)

        print(f"\nSimulation completed in {self.env.now:.2f} time units")
        return self.env.now


def create_sample_dag():
    """Create a sample DAG for testing"""
    # Create DAG nodes
    # Stage 1: Data loading (2 tasks, 3 time units each)
    load_data = DAGNode("load_data", 2, 3.0)

    # Stage 2: Map operations (4 tasks, 2 time units each) - depends on load_data
    map_stage = DAGNode("map_stage", 4, 2.0)

    # Stage 3: Reduce operations (2 tasks, 4 time units each) - depends on map_stage
    reduce_stage = DAGNode("reduce_stage", 2, 4.0)

    # Stage 4: Final output (1 task, 1 time unit) - depends on reduce_stage
    output_stage = DAGNode("output_stage", 1, 1.0)

    return [load_data, map_stage, reduce_stage, output_stage]


def create_sample_executors():
    """Create sample executor nodes"""
    return [ExecutorNode("executor #1", 2)]


# Example usage
if __name__ == "__main__":
    # Create SimPy environment
    env = simpy.Environment()

    # Create scheduler
    scheduler = SparkScheduler(env)

    # Add DAG nodes
    dag_nodes = create_sample_dag()
    for node in dag_nodes:
        scheduler.add_dag_node(node)

    # Add dependencies (linear pipeline)
    scheduler.add_dependency("load_data", "map_stage")
    scheduler.add_dependency("map_stage", "reduce_stage")
    scheduler.add_dependency("reduce_stage", "output_stage")

    # Add executor nodes
    executor_nodes = create_sample_executors()
    for node in executor_nodes:
        scheduler.add_executor_node(node)

    # Run simulation
    completion_time = scheduler.run_simulation()

    print(f"\nJob completed in {completion_time:.2f} time units")
    print(f"Total tasks processed: {scheduler.total_tasks_completed}/{scheduler.total_tasks}")
