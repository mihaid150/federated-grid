import json
import re
import subprocess
import os
import subprocess
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict, Any
import re
from shared.logging_config import logger


class FederatedNodeType(Enum):
    CLOUD_NODE = 0
    FOG_NODE = 1
    EDGE_NODE = 2


def find_topology_file(folder: str, name_substring: str) -> str:
    """
    Search for the newest JSON file in `folder` whose name contains `name_substring`.
    """
    folder_path = Path(folder)
    if not folder_path.is_dir():
        raise ValueError(f"{folder} is not a valid folder")

    matches = [f for f in folder_path.iterdir()
               if f.is_file() and name_substring in f.name and f.suffix.lower() == ".json"]

    if not matches:
        raise FileNotFoundError(f"No file found in {folder} containing '{name_substring}'")

    # sort by modification time (descending) and take newest
    newest_file = max(matches, key=lambda f: f.stat().st_mtime)
    return str(newest_file)

def load_topology(folder: str, name_substring: str) -> Dict[str, Any]:
    """
    Load topology JSON from the newest file matching the substring in the given folder.
    """
    file_path = find_topology_file(folder, name_substring)
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def node_info(node: Dict[str, Any]) -> Dict[str, Any]:
    d = node["data"]
    return {
        "id": node["id"],
        "label": d.get("label"),
        "ip_address": d.get("ip_address"),
        "port": d.get("port"),
        "node_type": d.get("node_type"),
    }

def parse_topology_for_port(folder: str, name_substring: str, current_port: int) -> Dict[str, Any]:
    topo = load_topology(folder, name_substring)

    nodes: List[Dict[str, Any]] = topo.get("nodes", [])
    edges: List[Dict[str, Any]] = topo.get("edges", [])

    id_to_node = {n["id"]: n for n in nodes}
    port_to_node = {n["data"].get("port"): n for n in nodes}

    current = port_to_node.get(current_port)
    if current is None:
        raise ValueError(f"No node found with port {current_port}")

    current_id = current["id"]

    parent_ids = [e["source"] for e in edges if e.get("target") == current_id]
    child_ids = [e["target"] for e in edges if e.get("source") == current_id]

    parents = [node_info(id_to_node[p]) for p in parent_ids if p in id_to_node]
    children = [node_info(id_to_node[c]) for c in child_ids if c in id_to_node]

    return {
        "current_node": node_info(current),
        "parent": parents[0] if parents else None,
        "parents": parents,
        "children": children
    }

class FederatedNode:
    def __init__(self, name: str, federated_node_type: FederatedNodeType, ip_address: str, port: int,
                 device_mac: str = None) -> None:
        self.name = name
        self.federated_node_type = federated_node_type
        self.ip_address = ip_address
        self.port = port
        self.parent_node: Optional['ParentFederatedNode'] = None
        self.child_nodes: List[Optional['ChildFederatedNode']] = []
        self.device_mac = MacFetcher.get_mac_address() if device_mac is None else device_mac

    def set_parent_node(self, parent_node: Optional['ParentFederatedNode']):
        self.parent_node = parent_node

    def add_child_node(self, child_node: Optional['ChildFederatedNode']):
        self.child_nodes.append(child_node)

    def add_child_nodes(self, child_nodes: List[Optional['ChildFederatedNode']]):
        self.child_nodes.extend(child_nodes)


class ParentFederatedNode(FederatedNode):
    def __init__(self, name: str, federated_node_type: FederatedNodeType, ip_address: str, port: int, device_mac: str) -> None:
        super().__init__(name, federated_node_type, ip_address, port, device_mac)


class ChildFederatedNode(FederatedNode):
    def __init__(self, name: str, federated_node_type: FederatedNodeType, ip_address: str, port: int, device_mac: str) -> None:
        super().__init__(name, federated_node_type, ip_address, port, device_mac)

class MacFetcher:
    HOST_SYS_PATH = "/host_sys/class/net"

    @classmethod
    def get_mac_address(cls) -> str:
        # First try inside the container (default behavior)
        interface = cls.__get_default_interface()
        if interface:
            mac = cls.__read_mac_from_interface(interface)
            if mac:
                # If MAC is in container namespace, check if it's a veth (Docker)
                if cls.__is_docker_mac(mac):
                    # Try to fetch from host
                    host_mac = cls.__read_host_mac(interface)
                    if host_mac:
                        return host_mac
                return mac

        # If default interface not found, try known interfaces
        for iface in ["eth0", "enp2s0", "ens33", "wlan0"]:
            mac = cls.__read_mac_from_interface(iface)
            if mac:
                if cls.__is_docker_mac(mac):
                    host_mac = cls.__read_host_mac(iface)
                    if host_mac:
                        return host_mac
                return mac

        return "00:00:00:00:00:00"

    @staticmethod
    def __get_default_interface() -> Optional[str]:
        try:
            result = subprocess.check_output(["ip", "route", "show", "default"]).decode("utf-8")
            match = re.search(r"dev (\S+)", result)
            if match:
                return match.group(1)
        except Exception as e:
            print(f"Error retrieving default interface: {e}")
        return None

    @staticmethod
    def __read_mac_from_interface(interface: str) -> Optional[str]:
        try:
            result = subprocess.check_output(["ip", "link", "show", interface]).decode("utf-8")
            mac_match = re.search(r"link/ether ([0-9a-f:]{17})", result)
            if mac_match:
                return mac_match.group(1)
        except subprocess.CalledProcessError:
            pass
        return None

    def __read_host_mac(self, interface: str) -> Optional[str]:
        """Reads the MAC from the host via bind-mounted /sys/class/net."""
        if os.path.exists(self.HOST_SYS_PATH):
            host_iface_path = os.path.join(self.HOST_SYS_PATH, interface, "address")
            if os.path.exists(host_iface_path):
                try:
                    with open(host_iface_path, "r") as f:
                        mac = f.read().strip()
                        if re.match(r"^[0-9a-f:]{17}$", mac):
                            return mac
                except Exception:
                    pass
        return None

    @staticmethod
    def __is_docker_mac(mac: str) -> bool:
        """Rough check: Docker MACs often start with 02:42 or are in locally administered range."""
        return mac.lower().startswith("02:42") or (int(mac.split(":")[0], 16) & 2) != 0
