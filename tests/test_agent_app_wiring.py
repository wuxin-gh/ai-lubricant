import importlib
import os
import sys


_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)


def _route_signatures(app):
    return {
        (route.path, tuple(sorted(route.methods or [])))
        for route in app.routes
        if hasattr(route, "methods")
    }


def test_main_app_includes_agent_task_routes():
    main = importlib.import_module("main")

    routes = _route_signatures(main.app)

    assert ("/agent/task", ("POST",)) in routes
    assert ("/agent/task/{task_id}", ("GET",)) in routes


def test_main_app_includes_cdp_chat_routes():
    main = importlib.import_module("main")

    routes = _route_signatures(main.app)

    assert ("/agent/client/messages", ("POST",)) in routes
    assert ("/agent/client/conversations", ("GET",)) in routes
    assert ("/agent/client/conversations", ("POST",)) in routes
    assert ("/agent/client/conversations/{conv_id}", ("GET",)) in routes
    assert ("/agent/client/conversations/{conv_id}/abort", ("POST",)) in routes
    assert ("/agent/client/conversations/{conv_id}/approvals/{confirmation_id}", ("POST",)) in routes


def test_agent_package_exports_phase1_public_symbols():
    import agent
    from agent.agent_main import GenericAgent
    from agent.api import router
    from agent.config import AgentConfig

    assert agent.router is router
    assert agent.GenericAgent is GenericAgent
    assert agent.AgentConfig is AgentConfig
    assert set(agent.__all__) == {"router", "GenericAgent", "AgentConfig"}


def test_main_module_import_exposes_fastapi_app():
    main = importlib.import_module("main")

    assert main.app is not None
    assert main.app.title == "Multi-Model Proxy"
