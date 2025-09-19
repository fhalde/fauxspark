import simpy

MiB = 1024 * 1024

# Adjustable parameters
UPLOAD_BW_MBPS = 100  # A's upload bandwidth in MiB/s
DOWNLOAD_BW_MBPS = 80  # B's download bandwidth in MiB/s
CHUNK_SIZE_MIB = 48  # Size of each chunk Machine B reads
RTT = 0.0005  # AWS intra-AZ round-trip latency in seconds


class TokenBucket:
    def __init__(self, env, rate_mib_s, burst_seconds=1.0, name=""):
        self.env = env
        self.rate = rate_mib_s * MiB  # bytes per second
        self.capacity = burst_seconds * self.rate
        self.bucket = simpy.Container(env, init=self.capacity, capacity=self.capacity)
        self.name = name
        self.action = env.process(self.refill())

    def refill(self):
        """Refill tokens continuously at the given rate."""
        while True:
            yield self.env.timeout(0.01)  # Refill every 10 ms
            tokens_to_add = self.rate * 0.01
            available_space = self.capacity - self.bucket.level
            add_amount = min(tokens_to_add, available_space)
            if add_amount > 0:  # avoid zero-put error
                yield self.bucket.put(add_amount)


def transfer_chunk(env, chunk_size_mib, up_bucket, down_bucket, rtt, total_bytes_mib):
    total_bytes = total_bytes_mib * MiB
    while total_bytes > 0:
        if total_bytes < chunk_size_mib:
            chunk_size_mib = total_bytes
        chunk_bytes = chunk_size_mib * MiB
        start = env.now

        print(f"[{env.now:.6f}s] Starting transfer of {chunk_size_mib} MiB")

        # Wait for RTT before data transfer starts
        yield env.timeout(rtt)

        # Need tokens from both upload and download buckets
        print(f"[{env.now:.6f}s] up={up_bucket.bucket.level / MiB}")
        yield up_bucket.bucket.get(chunk_bytes)
        print(f"[{env.now:.6f}s] down={down_bucket.bucket.level / MiB}")
        yield down_bucket.bucket.get(chunk_bytes)
        print(f"[{env.now:.6f}s] gotdown={down_bucket.bucket.level / MiB}")

        finish = env.now
        print(
            f"[{finish:.6f}s] Finished transfer of {chunk_size_mib} MiB "
            f"(duration {finish - start:.6f} s)"
        )
        total_bytes -= chunk_bytes


def main():
    env = simpy.Environment()
    import os

    os.environ["PYTHONUNBUFFERED"] = "1"

    up_bucket = TokenBucket(env, UPLOAD_BW_MBPS, name="A:Upload")
    down_bucket = TokenBucket(env, DOWNLOAD_BW_MBPS, name="B:Download")

    # Schedule Machine B to read a 48 MiB chunk
    env.process(transfer_chunk(env, CHUNK_SIZE_MIB, up_bucket, down_bucket, RTT, 1024))

    # Run simulation long enough to finish transfer
    env.run()


if __name__ == "__main__":
    main()
