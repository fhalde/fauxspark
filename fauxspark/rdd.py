import simpy
import itertools


class RDD:
    def __init__(
        self, env: simpy.Environment, name: str, num_partitions: int, parent_rdds: list["RDD"] = []
    ):
        self.env = env
        self.name = name
        self.num_partitions = num_partitions
        self.parent_rdds = parent_rdds


def fetch_shuffle_blocks(rdd: RDD, partition: int):
    return rdd.env.timeout(1)


def fetch_coalesced_blocks(rdd: RDD, partition: int):
    return rdd.env.timeout(1)


def iter(rdd: RDD, partition: int) -> list[simpy.Timeout]:
    match rdd.name:
        case "MapPartitionsRDD":
            return iter(rdd.parent_rdds[0], partition)
        case "ShuffledRDD":
            shuffle_blocks = fetch_shuffle_blocks(rdd, partition)
            return shuffle_blocks
        case "CoalescedRDD":
            coalesced_blocks = fetch_coalesced_blocks(rdd, partition)
            return coalesced_blocks
        case "CoGroupedRDD":
            return list(
                itertools.chain.from_iterable(
                    [iter(parent, partition) for parent in rdd.parent_rdds]
                )
            )
        case "ZipPartitionsRDD":
            return list(itertools.chain.from_iterable(
                [iter(parent, partition) for parent in rdd.parent_rdds]
            ))
        case "UnionRDD":
            base = 0
            for _, r in enumerate(rdd.parent_rdds):
                if partition < base + r.num_partitions:
                    return iter(r, partition - base)
                base += r.num_partitions
        case _:
            raise ValueError(f"Unknown RDD type: {rdd.name}")
