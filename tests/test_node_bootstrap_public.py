"""Tests for the public node bootstrap endpoints (no auth).

These endpoints are hit by the one-click install scripts on a fresh node machine
that has no administrator session yet. The docker artifacts (Dockerfile +
entrypoint.sh) let that machine ``docker build`` the shared node image locally
instead of pulling from a registry.
"""
from __future__ import annotations

from fastapi import FastAPI
from starlette.testclient import TestClient

from user_platform import routes_node_bootstrap


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(routes_node_bootstrap.router)
    return TestClient(app)


def test_docker_artifacts_are_served():
    """Both whitelisted build artifacts are served with real content."""
    client = _client()

    dockerfile = client.get("/api/v1/public/nodes/docker/Dockerfile")
    assert dockerfile.status_code == 200
    # Must carry both role binaries so one image serves management + execution.
    assert "node-execution" in dockerfile.text
    assert "agent-compose-node-management" in dockerfile.text

    entrypoint = client.get("/api/v1/public/nodes/docker/entrypoint.sh")
    assert entrypoint.status_code == 200
    # The entrypoint dispatches on the role env the launcher/installer sets.
    assert "AGENT_COMPOSE_NODE_ROLE" in entrypoint.text


def test_dockerfile_carries_runtime_toolchain():
    """The served Dockerfile must bake in the host CLIs a node execs at runtime.

    The image is built locally on a fresh host from this very Dockerfile, so a
    missing tool breaks a real path with no recovery until the image is rebuilt:
      git            — provisionGit / fetchResource / fileservice git diff+changes
      nodejs + npm   — run the server-pushed agent-compose-runtime; npm i -g editors
      openssh-client — git over SSH clones
    Locking the package list here keeps a future "slim the image" refactor from
    silently dropping one of these and re-introducing the failure mode.
    """
    client = _client()
    dockerfile = client.get("/api/v1/public/nodes/docker/Dockerfile")
    assert dockerfile.status_code == 200
    text = dockerfile.text
    for package in ("git", "nodejs", "npm", "openssh-client", "ca-certificates", "docker-cli", "tini"):
        assert package in text, f"Dockerfile missing runtime toolchain package: {package}"
    # The bind-mounted workspace is owned by the node process user, not the
    # container's root — without this git refuses to operate on it.
    assert "safe.directory" in text



def test_non_whitelisted_docker_artifact_is_rejected():
    client = _client()
    resp = client.get("/api/v1/public/nodes/docker/config.json")
    assert resp.status_code == 404
    assert "not found" in resp.text.lower()


def test_docker_artifact_path_traversal_is_rejected():
    """A crafted name must not escape the fixed nodes/docker directory."""
    client = _client()
    for name in ("../../.env", "%2e%2e%2f%2e%2e%2f.env", "sub/Dockerfile"):
        resp = client.get(f"/api/v1/public/nodes/docker/{name}")
        # Either the whitelist rejects it (404) or the route never matches (404).
        assert resp.status_code == 404, name
        assert ".env" not in resp.text


def test_docker_artifact_directory_is_pinned_to_nodes_docker():
    """Guard the served directory so a refactor cannot silently widen it."""
    assert routes_node_bootstrap._DOCKER_FILES == frozenset(
        {"Dockerfile", "entrypoint.sh"}
    )
    assert routes_node_bootstrap._DOCKER_DIR.name == "docker"
    assert routes_node_bootstrap._DOCKER_DIR.parent.name == "nodes"


def test_install_script_method_override_renders_container_for_standalone_node():
    """An execution node onboarded as standalone can be installed as a container.

    The Docker tab requests ``?method=docker`` on the same credential; the render
    must switch to the local-build container shape without touching the record.
    """
    from user_platform.nodes_service import render_install_script

    standalone_bootstrap = {
        "node_id": "node-abc",
        "secret": "JBSWY3DPEHPK3PXP",
        "server_url": "https://nodes.example.test",
        "role": "execution",
        "startup_method": "standalone",
    }
    # Baseline: standalone renders the host-daemon installer, not a container.
    base = render_install_script(
        standalone_bootstrap, public_base_url="https://x.test", agent_image=""
    )
    assert "docker build" not in base

    # Override to docker (what the route does with ?method=docker).
    overridden = render_install_script(
        {**standalone_bootstrap, "startup_method": "docker"},
        public_base_url="https://x.test",
        agent_image="",
    )
    assert "docker build" in overridden
    assert "AGENT_COMPOSE_AGENT_IMAGE=" in overridden

