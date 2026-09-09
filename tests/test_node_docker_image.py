from pathlib import Path


def test_dockerfile_bakes_runtime_and_editors():
    dockerfile = Path(__file__).parents[1].joinpath("nodes", "docker", "Dockerfile").read_text(encoding="utf-8")
    assert "COPY node-runtime.tar.gz /tmp/node-runtime.tar.gz" in dockerfile
    assert "test -f /opt/agent-compose/state/runtime/dist/cli.js" in dockerfile
    assert "ENV AGENT_COMPOSE_NODE_STATE_DIR=/opt/agent-compose/state" in dockerfile
    for package in (
        "@anthropic-ai/claude-code",
        "@openai/codex",
        "@google/gemini-cli",
        "opencode-ai",
    ):
        assert package in dockerfile
    assert "AGENT_COMPOSE_NODE_ROLE" not in dockerfile or "node-management" in dockerfile
