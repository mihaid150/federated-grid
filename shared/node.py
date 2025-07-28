import subprocess
from enum import Enum
from typing import Optional, List
import re
from shared.logging_config import logger


class FederatedNodeType(Enum):
    CLOUD_NODE = 0
    FOG_NODE = 1
    EDGE_NODE = 2


class FederatedNode:
    def __init__(self, name: str, federated_node_type: FederatedNodeType, ip_address: str, port: int,
                 device_mac: str = None) -> None:
        self.name = name
        self.federated_node_type = federated_node_type
        self.ip_address = ip_address
        self.port = port
        self.parent_node: Optional['ParentFederatedNode'] = None
        self.child_nodes: List[Optional['ChildFederatedNode']] = []
        self.device_mac = self.__get_mac_address() if device_mac is None else device_mac

    def __get_mac_address(self) -> str:
        interface = self.__get_default_interface()
        if interface:
            try:
                result = subprocess.check_output(["ip", "link", "show", interface]).decode("utf-8")
                mac = re.search(r"link/ether ([0-9a-f:]{17)", result)
                if mac:
                    return mac.group(1)
            except subprocess.CalledProcessError as e:
                logger.warning(f"Error retrieving MAC for default interface {interface}: {e}")

        for iface in ["eth0", "enp2s0", "ens33", "wlan0"]:
            try:
                result = subprocess.check_output(["ip", "link", "show", iface]).decode("utf-8")
                mac = re.search(r"link/ether ([0-9a-f:]{17})", result)
                if mac:
                    return mac.group(1)
            except subprocess.CalledProcessError:
                continue

        return "00:00:00:00:00:00"

    @staticmethod
    def __get_default_interface() -> Optional[str]:
        try:
            result = subprocess.check_output(["ip", "route", "show", "default"]).decode("utf-8")
            match = re.search(r"dev (\S+)", result)
            if match:
                return match.group(1)
        except Exception as e:
            logger.warning("Error retrieving default interface: ", e)
            return None

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
