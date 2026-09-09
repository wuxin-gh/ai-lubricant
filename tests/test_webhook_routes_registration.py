from types import SimpleNamespace

from fastapi import FastAPI

import user_platform


def test_webhook_receive_route_is_mounted(monkeypatch):
    monkeypatch.setattr(user_platform, "settings", SimpleNamespace(
        enabled=True,
        agent_compose_base_url="",
        node_control_token="",
        agent_compose_timeout=30,
    ))
    app = FastAPI()
    assert user_platform.mount_routes(app) is True
    matches = [
        route for route in app.routes
        if getattr(route, "path", "") == "/api/v1/webhooks/projects/{project_id}"
    ]
    assert len(matches) == 1
    assert "POST" in matches[0].methods
    management = [
        route for route in app.routes
        if getattr(route, "path", "") == "/api/v1/users/projects/{project_id}/webhook"
    ]
    assert {method for route in management for method in route.methods} >= {
        "GET", "POST", "PATCH", "DELETE"
    }
    resync = [
        route for route in app.routes
        if getattr(route, "path", "")
        == "/api/v1/users/projects/{project_id}/webhook/resync-callback"
    ]
    assert len(resync) == 1
    assert "POST" in resync[0].methods
