from pathlib import Path


def test_model_routing_endpoint_uses_lightweight_model_options():
    source = (Path(__file__).resolve().parents[1] / "admin.py").read_text(encoding="utf-8")
    helper_body = source[source.index("async def _routing_models_metadata"):source.index("async def _real_model_ids")]

    assert "get_admin_model_route_options" in helper_body
    assert "get_admin_models_response" not in helper_body
