import simpy
import time
from typing import Dict, Optional
from dataclasses import dataclass
from enum import Enum


class TransferStatus(Enum):
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Transfer:
    """Represents a file transfer between two nodes"""

    id: str
    src_node: str
    dest_node: str
    file_size_gb: float
    status: TransferStatus
    progress_gb: float = 0.0
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    @property
    def progress_percent(self) -> float:
        return (self.progress_gb / self.file_size_gb) * 100 if self.file_size_gb > 0 else 0


class Node:
    """Network node with upload and download bandwidth limits"""

    def __init__(self, node_id: str, upload_bw_gbps: float, download_bw_gbps: float):
        self.node_id = node_id
        self.upload_bw_gbps = upload_bw_gbps  # Gigabits per second
        self.download_bw_gbps = download_bw_gbps  # Gigabits per second

        # Track active transfers
        self.active_uploads: Dict[str, Transfer] = {}
        self.active_downloads: Dict[str, Transfer] = {}

    def __repr__(self):
        return f"Node(id={self.node_id}, upload={self.upload_bw_gbps}Gbps, download={self.download_bw_gbps}Gbps)"

    @property
    def available_upload_bw(self) -> float:
        """Calculate available upload bandwidth considering active transfers"""
        if not self.active_uploads:
            return self.upload_bw_gbps

        # Simple equal sharing among active uploads
        used_bw = len(self.active_uploads) * (self.upload_bw_gbps / len(self.active_uploads))
        return max(
            0, self.upload_bw_gbps - used_bw + (self.upload_bw_gbps / len(self.active_uploads))
        )

    @property
    def available_download_bw(self) -> float:
        """Calculate available download bandwidth considering active transfers"""
        if not self.active_downloads:
            return self.download_bw_gbps

        # Simple equal sharing among active downloads
        used_bw = len(self.active_downloads) * (self.download_bw_gbps / len(self.active_downloads))
        return max(
            0,
            self.download_bw_gbps - used_bw + (self.download_bw_gbps / len(self.active_downloads)),
        )


class NetworkSimulator:
    """Network bandwidth simulator for file transfers between nodes"""

    def __init__(self):
        self.env = simpy.Environment()
        self.nodes: Dict[str, Node] = {}
        self.transfers: Dict[str, Transfer] = {}
        self.transfer_counter = 0

    def add_node(self, node_id: str, upload_bw_gbps: float, download_bw_gbps: float) -> Node:
        """Add a node to the network"""
        node = Node(node_id, upload_bw_gbps, download_bw_gbps)
        self.nodes[node_id] = node
        return node

    def start_transfer(self, src_node_id: str, dest_node_id: str, file_size_gb: float) -> str:
        """Start a file transfer from source to destination node"""
        if src_node_id not in self.nodes:
            raise ValueError(f"Source node {src_node_id} not found")
        if dest_node_id not in self.nodes:
            raise ValueError(f"Destination node {dest_node_id} not found")

        self.transfer_counter += 1
        transfer_id = f"transfer_{self.transfer_counter}"

        transfer = Transfer(
            id=transfer_id,
            src_node=src_node_id,
            dest_node=dest_node_id,
            file_size_gb=file_size_gb,
            status=TransferStatus.PENDING,
        )

        self.transfers[transfer_id] = transfer

        # Start the transfer process
        self.env.process(self._transfer_process(transfer))

        return transfer_id

    def _transfer_process(self, transfer: Transfer):
        """SimPy process for handling file transfer"""
        src_node = self.nodes[transfer.src_node]
        dest_node = self.nodes[transfer.dest_node]

        # Mark transfer as active
        transfer.status = TransferStatus.ACTIVE
        transfer.start_time = self.env.now

        # Add to active transfers
        src_node.active_uploads[transfer.id] = transfer
        dest_node.active_downloads[transfer.id] = transfer

        print(
            f"[{self.env.now:.2f}s] Starting transfer {transfer.id}: {transfer.file_size_gb}GB from {transfer.src_node} to {transfer.dest_node}"
        )

        try:
            while transfer.progress_gb < transfer.file_size_gb:
                # Calculate effective bandwidth (limited by both upload and download)
                src_bw = (
                    src_node.available_upload_bw / len(src_node.active_uploads)
                    if src_node.active_uploads
                    else src_node.upload_bw_gbps
                )
                dest_bw = (
                    dest_node.available_download_bw / len(dest_node.active_downloads)
                    if dest_node.active_downloads
                    else dest_node.download_bw_gbps
                )
                effective_bw = min(src_bw, dest_bw)

                # Calculate how much we can transfer in the next time step (1 second intervals)
                time_step = 1.0  # 1 second
                transfer_amount = min(
                    effective_bw * time_step, transfer.file_size_gb - transfer.progress_gb
                )

                # Wait for the time step
                yield self.env.timeout(time_step)

                # Update progress
                transfer.progress_gb += transfer_amount

                print(
                    f"[{self.env.now:.2f}s] Transfer {transfer.id}: {transfer.progress_gb:.2f}/{transfer.file_size_gb}GB ({transfer.progress_percent:.1f}%) - {effective_bw:.2f}Gbps"
                )

            # Transfer completed
            transfer.status = TransferStatus.COMPLETED
            transfer.end_time = self.env.now

            print(
                f"[{self.env.now:.2f}s] Transfer {transfer.id} completed! Duration: {transfer.end_time - transfer.start_time:.2f}s"
            )

        except Exception as e:
            transfer.status = TransferStatus.FAILED
            transfer.end_time = self.env.now
            print(f"[{self.env.now:.2f}s] Transfer {transfer.id} failed: {e}")

        finally:
            # Remove from active transfers
            src_node.active_uploads.pop(transfer.id, None)
            dest_node.active_downloads.pop(transfer.id, None)

    def get_transfer_status(self, transfer_id: str) -> Optional[Transfer]:
        """Get the status of a transfer"""
        return self.transfers.get(transfer_id)

    def list_transfers(self) -> Dict[str, Transfer]:
        """List all transfers"""
        return self.transfers.copy()

    def run_simulation(self, until: float = None):
        """Run the simulation"""
        if until:
            self.env.run(until=until)
        else:
            self.env.run()


# Demo function
def demo_network_simulator():
    """Demo with 2 nodes transferring a 1GiB file"""
    print("=== Network Bandwidth Simulator Demo ===\n")

    # Create simulator
    sim = NetworkSimulator()

    # Add two nodes
    node1 = sim.add_node("node1", upload_bw_gbps=10, download_bw_gbps=10)  # 10 Gbps up/down
    node2 = sim.add_node("node2", upload_bw_gbps=5, download_bw_gbps=8)  # 5 Gbps up, 8 Gbps down

    print(f"Created {node1}")
    print(f"Created {node2}\n")

    # Start a 1 GiB transfer (8 Gigabits)
    file_size_gb = 100.0  # 1 GiB = 8 Gb
    transfer_id = sim.start_transfer("node1", "node2", file_size_gb)

    print(f"Started transfer: {transfer_id}\n")

    # Run simulation for 30 seconds max
    sim.run_simulation(until=30)

    # Print final results
    print("\n=== Final Results ===")
    for tid, transfer in sim.list_transfers().items():
        print(f"Transfer {tid}:")
        print(f"  Status: {transfer.status.value}")
        print(
            f"  Progress: {transfer.progress_gb:.2f}/{transfer.file_size_gb}GB ({transfer.progress_percent:.1f}%)"
        )
        if transfer.start_time and transfer.end_time:
            duration = transfer.end_time - transfer.start_time
            avg_speed = transfer.progress_gb / duration if duration > 0 else 0
            print(f"  Duration: {duration:.2f}s")
            print(f"  Average Speed: {avg_speed:.2f}Gbps")


if __name__ == "__main__":
    demo_network_simulator()
