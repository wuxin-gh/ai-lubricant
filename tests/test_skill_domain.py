"""Tests for Skill package parsing and SOP experience path safety."""
import io
import zipfile

import pytest

from monkeycode_compat.skill_domain import SkillDomainService, _script_manifest, parse_skill_source


def _zip(entries: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        for name, content in entries.items():
            archive.writestr(name, content)
    return buf.getvalue()


def test_parse_skill_source_keeps_frontmatter_metadata():
    data = _zip({
        "demo/SKILL.md": "---\nname: demo\ndescription: Demo\ntags: [one, two]\n---\n# Demo",
        "demo/experience/run.md": "# Run",
        "demo/scripts/run.py": "print(1)",
    })

    parsed = parse_skill_source(data, "demo.zip")

    assert parsed.name == "demo"
    assert parsed.description == "Demo"
    assert parsed.tags == ["one", "two"]
    assert parsed.skill_md_path == "demo/SKILL.md"


def test_parse_skill_source_rejects_package_without_skill_md():
    with pytest.raises(ValueError, match="SKILL.md"):
        parse_skill_source(_zip({"experience/run.md": "# Run"}), "demo.zip")


@pytest.mark.parametrize("ref", ["../x.md", "/x.md", "a/../../x.md"])
def test_experience_ref_rejects_traversal(ref: str):
    with pytest.raises(ValueError):
        SkillDomainService()._safe_experience_ref(ref)


def test_experience_ref_allows_nested_markdown():
    assert SkillDomainService()._safe_experience_ref("browser/login.md") == "browser/login.md"


def test_script_manifest_hashes_and_flags_process_execution():
    data = _zip({"SKILL.md": "# demo", "scripts/run.py": "import subprocess\nsubprocess.run(['x'])"})

    manifest, risk = _script_manifest(data, "demo.zip")

    assert risk == "high"
    assert manifest[0]["path"] == "scripts/run.py"
    assert len(manifest[0]["sha256"]) == 64
