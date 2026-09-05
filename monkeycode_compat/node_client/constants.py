"""Copied node wire constants used by the data service."""
NODE_STATUS_PENDING = "pending"
NODE_STATUS_APPROVED = "approved"
NODE_STATUS_REVOKED = "revoked"
NODE_ROLE_EXECUTION = "execution"
NODE_ROLE_MANAGEMENT = "management"
NODE_ROLE_PASSIVE_MANAGEMENT = "passive_management"
NODE_ROLE_IOS_HOST = "ios_host"
NODE_STARTUP_STANDALONE = "standalone"
NODE_STARTUP_SYSTEMD = "systemd"
NODE_STARTUP_DOCKER = "docker"
NODE_STARTUP_DOCKER_COMPOSE = "docker-compose"


def normalize_node_role(role: str) -> str:
    value = (role or "").strip().lower().replace("-", "_")
    if value in {NODE_ROLE_MANAGEMENT, NODE_ROLE_PASSIVE_MANAGEMENT, NODE_ROLE_IOS_HOST}:
        return value
    return NODE_ROLE_EXECUTION
