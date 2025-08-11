from __future__ import annotations
import os
from typing import Dict, Any, Union
from fastapi import WebSocket, WebSocketDisconnect, APIRouter
from shared.node import FederatedNode, FederatedNodeType, ParentFederatedNode, ChildFederatedNode, parse_topology_for_port
from shared.node_state import FederatedNodeState
from shared.logging_config import logger
from shared.commands import Command

shared_router = APIRouter()


class InitializeNodeCommand(Command):
    def execute(self, data: Dict[str, any]) -> Dict[str, Any]:
        return initialize_node(data)


class GetNodeInfo(Command):
    def execute(self, data: Dict[str, any]) -> Dict[str, Any]:
        return get_node_info()


class SetParentNode(Command):
    def execute(self, data: Dict[str, any]) -> Dict[str, Any]:
        return set_parent_node(data)


class SetChildNode(Command):
    def execute(self, data: Dict[str, any]) -> Dict[str, Any]:
        return set_child_node(data)


class SetChildrenNodes(Command):
    def execute(self, data: Dict[str, any]) -> Dict[str, Any]:
        return set_children_nodes(data)

# class AutoInitialization(Command):
#     def execute(self, data: Dict[str, any]) -> Dict[str, Any]:

command_map = {
    0: InitializeNodeCommand(),
    1: GetNodeInfo(),
    2: SetParentNode(),
    3: SetChildNode(),
    4: SetChildrenNodes(),
   # 5: AutoInitialization(),
}


@shared_router.websocket('/ws')
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        while True:
            message: Dict[str, Any] = await websocket.receive_json()
            operation: int = message.get('operation')
            data: Dict[str, any] = message.get('data', {})

            try:
                command = command_map.get(operation)
                if command:
                    response = command.execute(data)
                else:
                    response = {"Error": f"Invalid Operation {command}."}
            except Exception as e:
                response = {"error": str(e)}

            await websocket.send_json(response)
    except WebSocketDisconnect:
        logger.warning("Websocket disconnected...")


def initialize_node(data: Dict[str, any]) -> Dict[str, any]:
    if FederatedNodeState.get_current_node() is not None:
        raise ValueError("Current node is already initialized!")

    node = FederatedNode(data.get("name"), data.get("federated_node_type"), data.get("ip_address"), data.get("port"))
    FederatedNodeState.initialize_node(node)

    logger.info(f"Successfully created a new node with name {node.name}, type {node.federated_node_type}, "
                f"ip {node.ip_address}, port {node.port} and device mac {node.device_mac}.")

    return {
        "message": "Node initialized successfully.",
        "node": {
            "name": node.name,
            "federated_node_type": node.federated_node_type,
            "ip_address": node.ip_address,
            "port": node.port,
            "device_address": node.device_mac
        }
    }


def get_node_info() -> Dict[str, Union[str, Dict[str, Union[str, FederatedNodeType]]]]:
    node = FederatedNodeState.get_current_node()
    if node is None:
        raise ValueError("Current node is not initialized!")

    return {
        "message": "Node is present.",
        "node": {
            "name": node.name,
            "federated_node_type": node.federated_node_type,
            "ip_address": node.ip_address,
            "port": node.port,
            "device_address": node.device_mac
        }
    }


def set_parent_node(data: Dict[str, Any]) -> Dict[str, Union[str, Dict[str, Union[str, FederatedNodeType]]]]:
    current_node = FederatedNodeState.get_current_node()

    if current_node is None:
        raise ValueError("Current node was not initialized yet!")

    parent_node = ParentFederatedNode(data.get("name"), data.get("federated_node_type"), data.get("ip_address"),
                                      data.get("port"), data.get("device_mac"))
    current_node.set_parent_node(parent_node)

    return {
        "message": "Parent node of the current working node set successfully.",
        "node": {
            "name": parent_node.name,
            "federated_node_type": parent_node.federated_node_type,
            "ip_address": parent_node.ip_address,
            "port": parent_node.port,
            "device_address": parent_node.device_mac
        }
    }


def set_child_node(data: Dict[str, Any]) -> Dict[str, Union[str, Dict[str, Union[str, FederatedNodeType]]]]:
    current_node = FederatedNodeState.get_current_node()

    if current_node is None:
        raise ValueError("Current node was not initialized!")
    child_node = ChildFederatedNode(data.get("name"), data.get("federated_node_type"), data.get("ip_address"),
                                    data.get("port"), data.get("device_port"))
    current_node.add_child_node(child_node)

    return {
        "message": "A child node of the current working node set successfully.",
        "node": {
            "name": child_node.name,
            "federated_node_type": child_node.federated_node_type,
            "ip_address": child_node.ip_address,
            "port": child_node.port,
            "device_address": child_node.device_mac
        }
    }


def set_children_nodes(data) -> dict[str, str | list[dict[str, str | FederatedNodeType]]]:
    current_node = FederatedNodeState.get_current_node()

    if current_node is None:
        raise ValueError("Current node was not initialized yet!")

    child_nodes = [
        ChildFederatedNode(ind.get("name"), ind.get("federated_node_type"), ind.get("ip_address"),
                           ind.get("port"), ind.get("device_port"))
        for ind in data
    ]

    current_node.add_child_nodes(child_nodes)

    added_children = [
        {
            "name": child.name,
            "federated_node_type": child.federated_node_type,
            "ip_address": child.ip_address,
            "port": child.port,
            "device_mac": child.device_mac
        }
        for child in child_nodes
    ]

    return {
        "message": f"Added {len(added_children)} child nodes to the current working node.",
        "children": added_children,
    }

def auto_initialization():
    current_running_port = os.getenv("APP_HOST_PORT", "unknown")
    if current_running_port == "unknown":
        pass
    else:
        node_info = parse_topology_for_port("/app", "federated_topology", int(current_running_port))
        current_node_info = node_info["current_node"]
        current_node = FederatedNode(current_node_info["name"], current_node_info["federated_node_type"], current_node_info["ip_address"],)
