from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOP_DIR = ROOT / "agent" / "sop"


REQUIRED_SOPS = {
    "memory_management_sop.md": [
        "# Memory Management SOP",
        "file_read",
        "start_long_term_update",
        "update_working_checkpoint",
        "memory/",          # GA-style real subtree path
        "L1",
        "L2",
        "L3",
        "L4",
        "No Execution, No Memory",
        "Limitations",
    ],
    "scheduled_task_sop.md": [
        "# Scheduled Task SOP",
        "AgentScheduler",
        "capability_call",
        "scheduler.create",
        "scheduler.list",
        "scheduler.cancel",
        "scheduler.run_now",
        "AgentTaskRunner",
        "APScheduler",
        "Limitations",
    ],
    "browser_sop.md": [
        "# Browser SOP",
        "web",
        "operation",
        "scan",
        "execute",
        "tabs",
        "screenshot",
        "browser_mcp_not_configured",
        "Limitations",
    ],
    "mcp_usage_sop.md": [
        "# MCP Usage SOP",
        "capability_call",
        "[Available Capabilities]",
        "start_long_term_update",
    ],
}


def test_required_agent_sop_files_exist_with_key_content():
    for filename, expected_terms in REQUIRED_SOPS.items():
        path = SOP_DIR / filename
        assert path.is_file(), f"missing SOP file: {path}"
        text = path.read_text(encoding="utf-8")
        missing = [term for term in expected_terms if term not in text]
        assert not missing, f"{filename} is missing expected terms: {missing}"
