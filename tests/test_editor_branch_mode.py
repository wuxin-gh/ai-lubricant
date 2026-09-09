from user_platform.routes_editors import (
    CreateEditorReq,
    _editor_git_payload,
    _validated_editor_create_payload,
)


def test_editor_branch_mode_default_follows_repository_head():
    payload = _validated_editor_create_payload(
        CreateEditorReq(provider="claude", branch_mode="default", branch="ignored")
    )
    assert payload["branch_mode"] == "default"
    assert "branch" not in payload
    assert _editor_git_payload({"id": "ed_123", **payload}) == {"branch": ""}


def test_editor_branch_mode_existing_requires_and_preserves_branch():
    payload = _validated_editor_create_payload(
        CreateEditorReq(provider="claude", branch_mode="existing", branch=" feature/demo ")
    )
    assert payload["branch"] == "feature/demo"
    assert _editor_git_payload({"id": "ed_123", **payload}) == {"branch": "feature/demo"}


def test_editor_branch_mode_existing_rejects_empty_branch():
    try:
        _validated_editor_create_payload(
            CreateEditorReq(provider="claude", branch_mode="existing", branch="  ")
        )
    except ValueError as exc:
        assert "requires branch" in str(exc)
    else:
        raise AssertionError("existing branch mode should require a branch")


def test_editor_branch_mode_auto_creates_stable_git_safe_branch():
    payload = _validated_editor_create_payload(
        CreateEditorReq(provider="claude", branch_mode="auto", branch="ignored")
    )
    assert "branch" not in payload
    assert _editor_git_payload({"id": "ed_abc:123", **payload}) == {
        "branch": "",
        "createBranch": True,
        "newBranch": "editor/ed_abc-123",
    }
