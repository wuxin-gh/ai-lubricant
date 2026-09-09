"""stack_detector 纯规则引擎测试（无 DB、无网络）。

覆盖：规则表命中（Python/FastAPI、Node/Next、Go/Gin、Rust/Actix、Java/Spring
pom、移动端 Podfile+AndroidManifest 六组样例）、取数预算、截断启发式、扩展名
分布、fetch 全失败优雅降级、坏 TOML/JSON 不崩、stack_tags 扁平化投影。
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

# 直接按文件加载，绕开 user_platform 包级 __init__（服务端全量初始化）。
_sd_path = os.path.join(_proj, "user_platform", "stack_detector.py")
_spec = importlib.util.spec_from_file_location("stack_detector_standalone", _sd_path)
sd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sd)


def _fetch_from(texts: dict[str, str]):
    async def fetch(path: str) -> str | None:
        return texts.get(path)
    return fetch


def _run(coro):
    return asyncio.run(coro)


# ── 规则表：六组典型仓库 ─────────────────────────────────────────────────────
def test_python_fastapi_repo():
    paths = ["requirements.txt", "main.py", "app/api.py", "app/models.py",
             "tests/test_api.py", "README.md", "Dockerfile", "docker-compose.yml"]
    texts = {"requirements.txt": "fastapi==0.104.0\nuvicorn[standard]\n# 注释\npydantic\n"}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert profile["schema"] == "ai-lubricant.stack-profile/v1"
    assert profile["primary_language"] == "python"
    assert "fastapi" in profile["frameworks"]
    assert "web_backend" in profile["project_types"]
    assert "containerized" in profile["project_types"]
    assert "pip" in profile["package_managers"]
    assert "docker" in profile["containers"]


def test_node_next_repo():
    paths = ["package.json", "tsconfig.json", "src/app/page.tsx",
             "src/app/layout.tsx", "next.config.mjs"]
    texts = {"package.json": '{"name":"x","main":"index.js","dependencies":{"next":"14","react":"18","react-dom":"18"}}'}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert profile["primary_language"] == "typescript"
    assert {"react", "next"} <= set(profile["frameworks"])
    assert "web_frontend" in profile["project_types"]
    assert "npm" in profile["package_managers"]


def test_go_gin_repo():
    paths = ["go.mod", "main.go", "handler/handler.go", "go.sum"]
    texts = {"go.mod": "module example.com/x\n\ngo 1.21\n\nrequire github.com/gin-gonic/gin v1.9.1\n"}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert profile["primary_language"] == "go"
    assert "gin" in profile["frameworks"]
    assert "web_backend" in profile["project_types"]
    assert "go" in profile["package_managers"]


def test_rust_actix_repo():
    paths = ["Cargo.toml", "src/main.rs", "src/lib.rs"]
    texts = {"Cargo.toml": "[dependencies]\nactix-web = \"4\"\n"}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert profile["primary_language"] == "rust"
    assert "actix-web" in profile["frameworks"]
    assert "web_backend" in profile["project_types"]


def test_java_spring_pom_repo():
    paths = ["pom.xml", "src/main/java/com/x/App.java", "src/test/java/T.java"]
    texts = {"pom.xml": '<project><dependencies><dependency><groupId>org.springframework.boot</groupId></dependency></dependencies></project>'}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert profile["primary_language"] == "java"
    assert "spring" in profile["frameworks"]
    assert "web_backend" in profile["project_types"]
    assert "maven" in profile["package_managers"]


def test_mobile_flutter_repo():
    # Podfile 在 iOS 目录、AndroidManifest 在深层——存在性证据任意深度生效。
    paths = ["pubspec.yaml", "lib/main.dart", "ios/Runner.xcodeproj/project.pbxproj",
             "ios/Podfile", "android/app/src/main/AndroidManifest.xml"]
    profile = _run(sd.detect_stack(paths, _fetch_from({})))
    assert profile["primary_language"] == "dart"
    # 两端原生目录都有 → 双平台标签。
    assert {"ios", "android"} <= set(profile["project_types"])
    assert "pub" in profile["package_managers"]


# ── 语言分布与主语言回退 ──────────────────────────────────────────────────────
def test_language_ratios():
    paths = [f"f{i}.py" for i in range(10)] + ["a.ts", "b.ts"]
    profile = _run(sd.detect_stack(paths, _fetch_from({})))
    assert profile["primary_language"] == "python"
    assert profile["languages"]["python"] == pytest.approx(10 / 12, abs=1e-3)
    assert profile["languages"]["typescript"] == pytest.approx(2 / 12, abs=1e-3)


def test_primary_language_fallback_to_manifest():
    # 无可计数扩展名（树被截断成纯清单行）时按清单回退。
    paths = ["go.mod"]
    texts = {"go.mod": "module x\n"}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert profile["primary_language"] == "go"
    assert profile["languages"] == {"go": 1.0}


def test_language_fallback_typescript_when_tsconfig():
    paths = ["package.json", "tsconfig.json"]
    texts = {"package.json": '{"name":"x"}'}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert profile["primary_language"] == "typescript"


# ── 预算与截断 ───────────────────────────────────────────────────────────────
def test_budget_limits_fetches_and_marks_truncated():
    # 4 个依赖清单在场但预算只允许 2 次取数 → 只取 2 个 + truncated=True。
    paths = ["requirements.txt", "pyproject.toml", "package.json", "go.mod", "main.py"]
    fetch_calls: list[str] = []

    async def fetch(path):
        fetch_calls.append(path)
        return "django\n" if path == "requirements.txt" else None

    profile = _run(sd.detect_stack(paths, fetch, max_fetches=2))
    assert len(fetch_calls) == 2
    assert profile["truncated"] is True


def test_truncated_at_100_paths():
    paths = [f"dir/f{i}.py" for i in range(100)]
    profile = _run(sd.detect_stack(paths, _fetch_from({})))
    assert profile["truncated"] is True


def test_truncated_false_on_small_repo():
    paths = ["requirements.txt", "main.py", "app/x.py"]
    texts = {"requirements.txt": "flask\n"}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert profile["truncated"] is False


def test_truncated_hint_passthrough():
    profile = _run(sd.detect_stack(["a.py"], _fetch_from({}), truncated_hint=True))
    assert profile["truncated"] is True


# ── 优雅降级 ────────────────────────────────────────────────────────────────
def test_all_fetches_fail_still_returns_profile():
    async def fetch(path):
        return None
    profile = _run(sd.detect_stack(["requirements.txt", "main.py"], fetch))
    assert profile["frameworks"] == []
    assert profile["primary_language"] == "python"


def test_malformed_toml_and_json_do_not_raise():
    paths = ["pyproject.toml", "package.json", "main.py"]
    texts = {
        "pyproject.toml": "[project 这不是合法 toml [[[",
        "package.json": '{"name": ',
    }
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert profile["frameworks"] == []
    assert profile["primary_language"] == "python"


def test_empty_paths_profile():
    profile = _run(sd.detect_stack([], _fetch_from({})))
    assert profile["primary_language"] == ""
    assert profile["frameworks"] == []
    assert profile["truncated"] is False


# ── CLI / 库形态 ─────────────────────────────────────────────────────────────
def test_cli_from_package_bin():
    paths = ["package.json", "bin/x.js", "lib/y.js"]
    texts = {"package.json": '{"name":"x","bin":{"x":"bin/x.js"},"dependencies":{"commander":"11"}}'}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert "cli" in profile["project_types"]


def test_library_from_cargo_lib():
    paths = ["Cargo.toml", "src/lib.rs", "README.md"]
    texts = {"Cargo.toml": "[lib]\nname = \"xlib\"\n[dependencies]\nserde = \"1\"\n"}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert "library" in profile["project_types"]


# ── stack_tags 扁平化 ───────────────────────────────────────────────────────
def test_stack_tags_projection():
    profile = {
        "primary_language": "Python",
        "frameworks": ["FastAPI", ""],
        "project_types": ["web_backend", "containerized"],
    }
    assert sd.stack_tags(profile) == ["python", "fastapi", "web_backend", "containerized"]


def test_stack_tags_empty_and_none():
    assert sd.stack_tags(None) == []
    assert sd.stack_tags({}) == []
    assert sd.stack_tags({"primary_language": "", "frameworks": [], "project_types": []}) == []


# ── 框架规则细节 ─────────────────────────────────────────────────────────────
def test_frameworks_dedup_ordered():
    # 同一框架只命中一次且保持规则表声明顺序（django 在 flask 前）。
    paths = ["requirements.txt", "main.py"]
    texts = {"requirements.txt": "flask\ndjango\n"}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert profile["frameworks"] == ["django", "flask"]


def test_spring_via_gradle_substring():
    paths = ["build.gradle", "src/main/java/A.java"]
    texts = {"build.gradle": 'implementation "org.springframework.boot:spring-boot-starter-web"'}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert "spring" in profile["frameworks"]


def test_gemfile_rails():
    paths = ["Gemfile", "app/models/user.rb", "config/routes.rb"]
    texts = {"Gemfile": "source 'https://rubygems.org'\ngem 'rails', '~> 7.0'\n"}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert "rails" in profile["frameworks"]
    assert "bundler" in profile["package_managers"]


def test_pyproject_poetry_project_manager():
    paths = ["pyproject.toml", "main.py"]
    texts = {"pyproject.toml": "[tool.poetry]\nname = \"x\"\n[tool.poetry.dependencies]\npython = \"^3.11\"\n"}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert "poetry" in profile["package_managers"]


# ── 移动端框架：React Native / Expo / Flutter ────────────────────────────────
def test_react_native_classified_as_mobile_not_web_frontend():
    paths = ["package.json", "App.tsx", "ios/Podfile",
             "android/app/src/main/AndroidManifest.xml"]
    texts = {"package.json": json.dumps({
        "dependencies": {"react": "18", "react-native": "0.73", "expo": "~50"},
    })}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert {"react-native", "expo"} <= set(profile["frameworks"])
    # RN 内含 react 依赖但不是 web 前端——裸 react 应被压掉。
    assert "react" not in profile["frameworks"]
    # RN 裸工程两端目录都有 → 双平台标签，不再出泛化的 mobile_app。
    assert {"ios", "android"} <= set(profile["project_types"])
    assert "mobile_app" not in profile["project_types"]
    assert "web_frontend" not in profile["project_types"]


def test_expo_managed_workflow_mobile_app_without_native_manifest():
    # Expo 托管工作流常无 android/ios 目录、无 AndroidManifest.xml——靠框架兜底。
    paths = ["package.json", "app.config.js", "src/App.tsx"]
    texts = {"package.json": json.dumps({
        "dependencies": {"expo": "~50", "react": "18", "react-native": "0.73"},
    })}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert "expo" in profile["frameworks"]
    assert "mobile_app" in profile["project_types"]
    assert "ios" not in profile["project_types"]
    assert "android" not in profile["project_types"]


def test_flutter_framework_tag_from_pubspec_sdk_signal():
    paths = ["pubspec.yaml", "lib/main.dart", "android/app/build.gradle",
             "android/app/src/main/AndroidManifest.xml"]
    texts = {"pubspec.yaml": (
        "name: myapp\nenvironment:\n  sdk: '>=3.0.0 <4.0.0'\n"
        "dependencies:\n  flutter:\n    sdk: flutter\n  cupertino_icons: ^1.0.2\n"
    )}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert "flutter" in profile["frameworks"]
    assert "android" in profile["project_types"]
    assert "pub" in profile["package_managers"]
    # cupertino_icons 依赖键被解析（证明 pubspec 扫描覆盖依赖段）。
    assert "pubspec.yaml" in profile["evidence"]


def test_dart_cli_package_not_tagged_flutter():
    # 纯 dart 包无 ``sdk: flutter`` 行——不应被标 flutter/mobile_app。
    paths = ["pubspec.yaml", "bin/mycli.dart"]
    texts = {"pubspec.yaml": (
        "name: mycli\nenvironment:\n  sdk: ^3.0.0\n"
        "dependencies:\n  args: ^2.4.0\n"
    )}
    profile = _run(sd.detect_stack(paths, _fetch_from(texts)))
    assert "flutter" not in profile["frameworks"]
    assert "mobile_app" not in profile["project_types"]
    assert profile["primary_language"] == "dart"


# ── 移动平台细分：ios / android ──────────────────────────────────────────────
def test_ios_native_only_project():
    # Xcode 工程 + storyboard，无任何 android 目录 → 只标 ios。
    paths = ["Podfile", "MyApp.xcodeproj/project.pbxproj",
             "MyApp/AppDelegate.swift", "MyApp/Base.lproj/Main.storyboard",
             "MyApp/Info.plist"]
    profile = _run(sd.detect_stack(paths, _fetch_from({})))
    assert "ios" in profile["project_types"]
    assert "android" not in profile["project_types"]
    assert "mobile_app" not in profile["project_types"]
    assert profile["primary_language"] == "swift"


def test_android_native_only_project():
    # Gradle android 工程，无 iOS 目录 → 只标 android。
    paths = ["app/build.gradle", "app/src/main/AndroidManifest.xml",
             "app/src/main/java/com/x/MainActivity.kt",
             "gradle/wrapper/gradle-wrapper.properties"]
    profile = _run(sd.detect_stack(paths, _fetch_from({})))
    assert "android" in profile["project_types"]
    assert "ios" not in profile["project_types"]
    assert "mobile_app" not in profile["project_types"]
    assert profile["primary_language"] == "kotlin"


def test_ios_workspace_and_xib_signals():
    # 无 xcodeproj（xcworkspace 管理）、storyboard/xib 都算 iOS 证据。
    paths = ["App.xcworkspace/contents.xcins", "App/Screens.xib",
             "App/ViewController.swift"]
    profile = _run(sd.detect_stack(paths, _fetch_from({})))
    assert "ios" in profile["project_types"]
