import subprocess
import hazelcast
import os
from pathlib import Path


class Hazelcaster:
    def __init__(
        self,
        cluster_name: str,
        num: int,
        hz_ip: str,
        logs_dir: str = "logs",
        autostart_podman_hz: bool = False,
    ):
        self.cluster_name = cluster_name
        self.map = None
        self.num = num
        self.hazelcast_process = None
        self.client = None
        self.logs_dir = Path(logs_dir)
        self.autostart_podman_hz = autostart_podman_hz
        self.hz_ip = hz_ip

        os.makedirs(self.logs_dir, exist_ok=True)

    @property
    def port(self) -> int:
        return self.num + 5701

    @property
    def log_file_path(self) -> Path:
        return self.logs_dir / f"hazelcast_{self.num}.log"

    def __enter__(self):
        log_file = open(self.log_file_path, "w")

        if self.autostart_podman_hz:
            self.hazelcast_process = subprocess.Popen(
                [
                    "podman",
                    "run",
                    "--rm",
                    f"--name=hazelcast-{self.num}",
                    "--network=hazelcast-network",
                    "-e",
                    f"HZ_NETWORK_PUBLICADDRESS={self.hz_ip}:{self.port}",
                    "-e",
                    f"HZ_CLUSTERNAME={self.cluster_name}",
                    "-p",
                    f"{self.port}:5701",
                    "hazelcast/hazelcast:5.3.8",
                ],
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
        else:
            self.hazelcast_process = None

        self.client = hazelcast.HazelcastClient(
            cluster_name=self.cluster_name,
            cluster_members=[f"{self.hz_ip}:{self.port}"],
        )
        self.map = self.client.get_map("logs").blocking()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self.client is not None:
            self.client.shutdown()

        if self.hazelcast_process is not None:
            self.hazelcast_process.kill()

            if hasattr(self.hazelcast_process.stdout, "close"):
                self.hazelcast_process.stdout.close()
