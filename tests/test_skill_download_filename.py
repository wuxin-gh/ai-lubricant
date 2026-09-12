"""Skill URL import filename handling across redirects."""
from __future__ import annotations

from types import SimpleNamespace

from user_platform.routes_skill import _download_filename


def test_download_filename_prefers_content_disposition():
    response = SimpleNamespace(
        headers={
            "Content-Disposition": 'attachment; filename="anthropics-skills-main.zip"',
            "Content-Type": "application/zip",
        }
    )

    assert _download_filename(
        "https://github.com/anthropics/skills/archive/refs/heads/main.zip",
        "https://codeload.github.com/anthropics/skills/zip/refs/heads/main",
        response,
    ) == "anthropics-skills-main.zip"


def test_download_filename_uses_original_url_after_github_redirect():
    response = SimpleNamespace(headers={"Content-Type": "application/zip"})

    assert _download_filename(
        "https://github.com/acme/skill/archive/refs/heads/main.zip",
        "https://codeload.github.com/acme/skill/zip/refs/heads/main",
        response,
    ) == "main.zip"


def test_download_filename_adds_zip_for_extensionless_zip_response():
    response = SimpleNamespace(headers={"Content-Type": "application/zip"})

    assert _download_filename(
        "https://example.com/download/skill-42",
        "https://example.com/download/skill-42",
        response,
    ) == "skill-42.zip"
