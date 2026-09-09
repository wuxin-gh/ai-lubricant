"""项目技术栈识别（规则驱动，无 LLM）。

输入是仓库的文件路径清单 + 一个按需取文件文本的异步回调；输出结构化 profile：

- 语言：扩展名计数分布 → 占比 → 主语言（树被截断时的清单回退）
- 框架：根清单文件内容里的依赖指纹（requirements/pyproject/package.json/go.mod…）
- 项目形态：web 前后端 / 移动（ios/android/mobile_app）/ 桌面 / 库 / CLI / 容器化
- truncated：路径数超阈值（上游树分页静默截断的边界）或取数预算耗尽时提示
  「结果可能不完整」，宁可警告不静默漏判

纯函数：不触网（取文件全部经调用方注入的 ``fetch_text``）、不读 DB、绝不抛
——单个 manifest 取不到/解析失败只影响对应规则，整体仍返回 profile。

两处消费方共用本引擎：项目扫描（project_service.scan_stack_profile）与市场
探针（leaderboard_probe，喂它已抓的 files，零新增网络）。探针的
``_ROOT_MANIFESTS`` 保持为本引擎规则路径的子集，市场侧扩抓清单即自动受益。
"""
from __future__ import annotations

import datetime as _dt
import json
import re
import tomllib
from fnmatch import fnmatch
from pathlib import PurePosixPath
from typing import Any, Awaitable, Callable

PROFILE_SCHEMA = "ai-lubricant.stack-profile/v1"

# ---- 输入边界 -------------------------------------------------------------
# gitlab/gitea 递归树有服务端分页（git_clients 已翻页至 _MAX_FETCH_ALL_PAGES）；
# 路径数达到该阈值仍提示「结果可能不完整」——超巨仓库翻页有上限，语言分布
# 可用但嵌套清单可能漏检。
_TRUNCATION_PATH_HINT = 100
# 取内容预算上限（与项目扫描侧 fetch_blob 的 HTTP 次数一致；超出即 truncated）。
_MAX_FETCH_BUDGET_DEFAULT = 12
# 扩展名计数上限，防超巨仓库把 profile 撑爆。
_MAX_PATHS_FOR_EXT_COUNT = 20_000

# 依赖清单：根目录（或多层）出现即取内容解析。「根清单优先」指取数顺序，
# 匹配一律按 basename 任意深度（AndroidManifest.xml 几乎从不放根目录）。
_DEPENDENCY_MANIFESTS: tuple[str, ...] = (
    "requirements.txt", "pyproject.toml", "package.json", "go.mod", "Cargo.toml",
    "pom.xml", "build.gradle", "build.gradle.kts", "Gemfile", "composer.json",
    "pubspec.yaml",
)
# 只看存在性、不取内容的证据文件（节省取数预算）。
_PRESENCE_MANIFESTS: tuple[str, ...] = (
    "Dockerfile", "Podfile", "AndroidManifest.xml", "tsconfig.json",
    "package-lock.json", "yarn.lock", "pnpm-lock.yaml", "uv.lock", "poetry.lock",
    "composer.lock", "Cargo.lock", "packages.config",
)
# glob 形态的额外证据（basename 或全路径 fnmatch）。
_PRESENCE_GLOBS: tuple[str, ...] = (
    "*.csproj", "docker-compose*.yml", "docker-compose*.yaml", "*.xcodeproj",
)

# 扩展名 → 语言标识。语言分布对全部路径计数（含嵌套目录）。
_EXT_LANGUAGE_MAP: dict[str, str] = {
    "py": "python", "js": "javascript", "mjs": "javascript", "cjs": "javascript",
    "jsx": "javascript", "ts": "typescript", "tsx": "typescript",
    "go": "go", "rs": "rust", "java": "java", "kt": "kotlin", "kts": "kotlin",
    "swift": "swift", "c": "c", "h": "c", "cpp": "cpp", "cc": "cpp", "hpp": "cpp",
    "cs": "csharp", "rb": "ruby", "php": "php", "dart": "dart",
    "vue": "vue", "svelte": "svelte",
}
# 语言占比并列时的优先序（值小者优先；未列出者排最后并列按字典序）。
_LANG_PRIORITY: dict[str, int] = {
    "python": 0, "typescript": 1, "javascript": 2, "go": 3, "rust": 4,
    "java": 5, "kotlin": 6, "swift": 7, "csharp": 8, "cpp": 9,
}

# ---- 框架指纹 -------------------------------------------------------------
# (框架名, 匹配的清单 basename 集合, 依赖标识集合)。
# 结构化/行式清单（requirements/pyproject/package.json/Cargo/go.mod/Gemfile/
# composer）对依赖标识做「精确匹配」；pom/gradle 是 XML/Groovy 文本，做「子串
# 包含」——springframework 一个词同时覆盖 org.springframework 与 spring-boot-*。
_FRAMEWORK_RULES: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...] = (
    # Python
    ("fastapi", ("requirements.txt", "pyproject.toml"), ("fastapi",)),
    ("django", ("requirements.txt", "pyproject.toml"), ("django",)),
    ("flask", ("requirements.txt", "pyproject.toml"), ("flask",)),
    # JavaScript / TypeScript
    ("react", ("package.json",), ("react",)),
    ("vue", ("package.json",), ("vue",)),
    ("next", ("package.json",), ("next",)),
    ("nuxt", ("package.json",), ("nuxt",)),
    ("svelte", ("package.json",), ("svelte",)),
    ("astro", ("package.json",), ("astro",)),
    ("express", ("package.json",), ("express",)),
    ("nest", ("package.json",), ("@nestjs/core",)),
    ("electron", ("package.json",), ("electron",)),
    # Go
    ("gin", ("go.mod",), ("github.com/gin-gonic/gin",)),
    ("echo", ("go.mod",), ("github.com/labstack/echo",)),
    ("fiber", ("go.mod",), ("github.com/gofiber/fiber",)),
    # Rust
    ("actix-web", ("Cargo.toml",), ("actix-web",)),
    ("rocket", ("Cargo.toml",), ("rocket",)),
    ("tauri", ("Cargo.toml",), ("tauri",)),
    # Java / Kotlin
    ("spring", ("pom.xml", "build.gradle", "build.gradle.kts"), ("springframework", "spring-boot")),
    # Ruby / PHP
    ("rails", ("Gemfile",), ("rails",)),
    ("laravel", ("composer.json",), ("laravel",)),
    ("symfony", ("composer.json",), ("symfony",)),
    # 移动端（React Native / Expo / Flutter）
    ("react-native", ("package.json",), ("react-native",)),
    ("expo", ("package.json",), ("expo",)),
    ("flutter", ("pubspec.yaml",), ("flutter",)),
)

# 框架 → 项目形态的归类（形态推断用，不直接展示）。
_BACKEND_FRAMEWORKS = {
    "fastapi", "django", "flask", "spring", "gin", "echo", "fiber",
    "actix-web", "rocket", "nest", "express", "laravel", "symfony", "rails",
}
_FRONTEND_FRAMEWORKS = {"react", "vue", "next", "nuxt", "svelte", "astro"}
_DESKTOP_FRAMEWORKS = {"electron", "tauri"}
_MOBILE_FRAMEWORKS = {"react-native", "expo", "flutter"}

# 文件名常量避免散落魔法字符串。
_NAME_GEMFILE = "Gemfile"


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _lower_paths(paths: list[str]) -> dict[str, str]:
    """小写 basename → 原始路径（保序去重，先见者优先）。"""
    out: dict[str, str] = {}
    for p in paths:
        base = PurePosixPath(p).name.lower()
        if base and base not in out:
            out[base] = p
    return out


def _match_presence(paths: list[str]) -> set[str]:
    """收集全部「存在性证据」basename 集合（含 glob）。"""
    found: set[str] = set()
    for p in paths:
        base = PurePosixPath(p).name.lower()
        found.add(base)
        for pattern in _PRESENCE_GLOBS:
            if fnmatch(base, pattern) or fnmatch(p.lower(), pattern):
                found.add(base)
    return found


def _extract_dep_tokens(basename: str, text: str) -> tuple[set[str], set[str]]:
    """把清单内容归一成 (精确依赖名集合, 原文子串集合)。

    行式/结构化清单走精确集合；pom/gradle 留空精确集合、原文本身即子串域。
    解析失败返回空集合——对应规则不命中，整体不抛。
    """
    exact: set[str] = set()
    if basename == "requirements.txt":
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith(("#", "-", ";")):
                continue
            # 去掉环境标记与版本约束：fastapi[all]>=0.1 → fastapi
            token = re.split(r"[\s\[;=<>~!]", line, 1)[0].strip().lower()
            if token:
                exact.add(token)
    elif basename == "pyproject.toml":
        try:
            data = tomllib.loads(text)
        except (tomllib.TOMLDecodeError, ValueError):
            return exact, set()
        project = data.get("project") or {}
        if isinstance(project, dict):
            for dep in project.get("dependencies") or []:
                token = re.split(r"[\s\[;=<>~!]", str(dep), 1)[0].strip().lower()
                if token:
                    exact.add(token)
            # 可选依赖组 [project.optional-dependencies.*]
            optional = project.get("optional-dependencies") or {}
            if isinstance(optional, dict):
                for deps in optional.values():
                    for dep in deps or []:
                        token = re.split(r"[\s\[;=<>~!]", str(dep), 1)[0].strip().lower()
                        if token:
                            exact.add(token)
        poetry = data.get("tool") or {}
        if isinstance(poetry, dict) and isinstance(poetry.get("poetry"), dict):
            for key in (poetry["poetry"].get("dependencies") or {}):
                exact.add(str(key).strip().lower())
    elif basename == "package.json":
        try:
            data = json.loads(text)
        except ValueError:
            return exact, set()
        for section in ("dependencies", "devDependencies", "peerDependencies"):
            deps = data.get(section)
            if isinstance(deps, dict):
                exact.update(str(k).strip().lower() for k in deps)
        # bin/main 是 CLI/库证据而非依赖，放子串域给形态规则用。
        markers: set[str] = set()
        if data.get("bin"):
            markers.add("bin")
        if data.get("main"):
            markers.add("main")
        return exact, markers
    elif basename == "composer.json":
        try:
            data = json.loads(text)
        except ValueError:
            return exact, set()
        for section in ("require", "require-dev"):
            deps = data.get(section)
            if isinstance(deps, dict):
                exact.update(str(k).strip().lower() for k in deps)
    elif basename == "Cargo.toml":
        try:
            data = tomllib.loads(text)
        except (tomllib.TOMLDecodeError, ValueError):
            return exact, set()
        deps = data.get("dependencies") or {}
        if isinstance(deps, dict):
            exact.update(str(k).strip().lower() for k in deps)
        # [lib] 段是库证据，放进子串域给形态规则用。
        markers = []
        if isinstance(data.get("lib"), dict):
            markers.append("lib")
        if isinstance(data.get("bin"), (list, dict)) and data.get("bin"):
            markers.append("bin")
        return exact, set(markers)
    elif basename == "go.mod":
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if line.startswith(("#",)) or not line:
                continue
            for token in line.split():
                if "/" in token or "." in token:
                    exact.add(token.strip().lower())
    elif basename == "Gemfile":
        for match in re.finditer(r"^\s*gem\s+['\"]([\w\-.]+)['\"]", text, re.IGNORECASE | re.MULTILINE):
            exact.add(match.group(1).lower())
    elif basename == "pubspec.yaml":
        # stdlib 无 YAML 解析器：按缩进扫 dependencies/dev_dependencies 段的
        # 依赖键（「段名:」顶格起段、两空格起键）；``sdk: flutter`` 只出现在
        # Flutter 项目里，命中即注入 flutter 依赖 token（框架规则据此归
        # mobile_app，纯 dart CLI 包无此行不会误中）。
        section = ""
        for raw in text.splitlines():
            line = raw.rstrip()
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if not line[:1].isspace():
                section = stripped.rstrip(":").lower()
                continue
            if section in ("dependencies", "dev_dependencies"):
                m = re.match(r"^[\t ]+([\w\-.]+)\s*:", line)
                if m:
                    exact.add(m.group(1).lower())
        if re.search(r"sdk:\s*['\"]?flutter\b", text, re.IGNORECASE):
            exact.add("flutter")
    # pom.xml / build.gradle(.kts)：不归一，原文小写即子串域。
    return exact, set()


def _cli_library_evidence(parsed: dict[str, tuple[set[str], set[str]]]) -> tuple[bool, bool]:
    """从已解析清单判断 (是 CLI, 是库) 的原始证据。

    CLI：package.json bin / Cargo [bin] / pyproject [project.scripts] / clap。
    库：Cargo [lib] / package.json main（无 bin）。pyproject 的 scripts 与 clap
    证据在调用方直接查 toml 原文，不在这里重复解析。
    """
    pkg = parsed.get("package.json")
    if pkg is not None:
        exact, subs = pkg
        if "bin" in subs:
            return True, False
        if "main" in subs:
            return False, True
    cargo = parsed.get("Cargo.toml")
    if cargo is not None:
        _, subs = cargo
        if "bin" in subs:
            return True, False
        if "lib" in subs:
            return False, True
    return False, False


async def detect_stack(
    paths: list[str],
    fetch_text: Callable[[str], Awaitable[str | None]],
    *,
    max_fetches: int = _MAX_FETCH_BUDGET_DEFAULT,
    truncated_hint: bool = False,
) -> dict:
    """规则驱动的技术栈识别（见模块 docstring）。**绝不抛**。"""
    paths = [str(p) for p in (paths or []) if p]
    lowered = _lower_paths(paths)
    presence = _match_presence(paths)

    # ---- 取数：依赖清单按声明顺序消耗预算（basename 任意深度匹配，浅路径
    # 自然先见）。lowered 的键是小写 basename，清单名统一小写后查找。
    texts: dict[str, str] = {}
    budget_used = 0
    wanted = 0
    for basename in _DEPENDENCY_MANIFESTS:
        original_path = lowered.get(basename.lower())
        if original_path is None:
            continue
        wanted += 1
        if budget_used >= max_fetches:
            continue
        text = await fetch_text(original_path)
        budget_used += 1
        if text is not None:
            texts[basename] = text
    truncated = bool(truncated_hint)
    if wanted > budget_used:
        truncated = True
    if len(paths) >= _TRUNCATION_PATH_HINT:
        truncated = True

    # ---- 依赖指纹归一
    parsed: dict[str, tuple[set[str], set[str]]] = {}
    for basename, text in texts.items():
        parsed[basename] = _extract_dep_tokens(basename, text)

    # ---- 框架命中
    frameworks: list[str] = []
    for name, manifests, terms in _FRAMEWORK_RULES:
        for manifest in manifests:
            if manifest not in parsed:
                continue
            exact, subs = parsed[manifest]
            if manifest in ("pom.xml", "build.gradle", "build.gradle.kts"):
                raw = texts.get(manifest, "")
                if any(t in raw.lower() for t in terms):
                    frameworks.append(name)
                    break
            elif any(t in exact for t in terms):
                frameworks.append(name)
                break
    framework_set = set(frameworks)
    # React Native/Expo 项目 package.json 里必带 react 依赖，但形态是移动
    # app 不是 web 前端——压掉裸 react，避免误标 web_frontend。
    if "react-native" in framework_set or "expo" in framework_set:
        framework_set.discard("react")
        frameworks = [f for f in frameworks if f != "react"]

    # ---- 语言分布
    counts: dict[str, int] = {}
    total = 0
    for p in paths[:_MAX_PATHS_FOR_EXT_COUNT]:
        ext = PurePosixPath(p).suffix.lstrip(".").lower()
        lang = _EXT_LANGUAGE_MAP.get(ext)
        if lang:
            counts[lang] = counts.get(lang, 0) + 1
            total += 1
    languages: dict[str, float] = {}
    if total:
        for lang, count in counts.items():
            languages[lang] = round(count / total, 4)
    primary_language = ""
    if counts:
        primary_language = min(
            counts,
            key=lambda lang: (-counts[lang], _LANG_PRIORITY.get(lang, 99), lang),
        )
    else:
        # 树被截断/空仓库时按清单回退。
        for basename, lang in (
            ("requirements.txt", "python"), ("pyproject.toml", "python"),
            ("package.json", "typescript" if "tsconfig.json" in presence else "javascript"),
            ("go.mod", "go"), ("Cargo.toml", "rust"), ("pom.xml", "java"),
            ("build.gradle", "java"), ("build.gradle.kts", "kotlin"),
            ("Gemfile", "ruby"), ("composer.json", "php"),
            ("pubspec.yaml", "dart"), ("Podfile", "swift"),
        ):
            if basename in presence:
                primary_language = lang
                languages = {lang: 1.0}
                break

    # ---- 项目形态
    project_types: list[str] = []
    if framework_set & _BACKEND_FRAMEWORKS:
        project_types.append("web_backend")
    if framework_set & _FRONTEND_FRAMEWORKS:
        project_types.append("web_frontend")
    # 移动端：原生工程证据进一步区分 ios / android 平台（RN/Flutter 两端
    # 目录都有 → 双标签）；Expo 托管工作流常无原生目录，靠框架兜底成泛化的
    # mobile_app。iOS：Podfile/.xcodeproj/.xcworkspace/.storyboard/.xib；
    # Android：AndroidManifest.xml（所有 android 工程必有）。
    # 注意 pubspec.yaml 不单独触发——纯 dart 包也有 pubspec。
    android = "androidmanifest.xml" in presence
    ios = ("podfile" in presence) or any(
        # .xcodeproj/.xcworkspace 是目录：调用方常只传文件路径，按路径段识别。
        ".xcodeproj/" in p.lower() or p.lower().endswith(".xcodeproj")
        or ".xcworkspace/" in p.lower() or p.lower().endswith(".xcworkspace")
        or p.lower().endswith((".storyboard", ".xib"))
        for p in paths[:_MAX_PATHS_FOR_EXT_COUNT]
    )
    if ios or android:
        if ios:
            project_types.append("ios")
        if android:
            project_types.append("android")
    elif framework_set & _MOBILE_FRAMEWORKS:
        project_types.append("mobile_app")
    if framework_set & _DESKTOP_FRAMEWORKS:
        project_types.append("desktop")
    is_cli, is_library = _cli_library_evidence(parsed)
    # pyproject [project.scripts] 与 clap 也算 CLI 证据。
    py_text = texts.get("pyproject.toml")
    if py_text and "[project.scripts]" in py_text:
        is_cli = True
    if py_text and re.search(r"^\s*clap\s*=", py_text, re.MULTILINE):
        is_cli = True
    if is_cli:
        project_types.append("cli")
    if "dockerfile" in presence or any(
        fnmatch(p.lower(), "docker-compose*.y*ml") for p in paths[:_MAX_PATHS_FOR_EXT_COUNT]
    ):
        project_types.append("containerized")
    # 库是「无任何应用形态」时的兜底归类。
    if not project_types and (is_library or py_text or "package.json" in presence):
        project_types.append("library")

    # ---- 包管理器 / 容器
    package_managers: list[str] = []
    # Python 系按实际管理器归类（互斥，避免 poetry 项目同时标 pip）。
    if py_text and "[tool.poetry]" in py_text:
        package_managers.append("poetry")
    elif "uv.lock" in presence or (py_text and "[tool.uv]" in py_text):
        package_managers.append("uv")
    elif "requirements.txt" in presence or "pyproject.toml" in presence:
        package_managers.append("pip")
    if "package.json" in presence:
        for lock, manager in (
            ("pnpm-lock.yaml", "pnpm"), ("yarn.lock", "yarn"), ("package-lock.json", "npm"),
        ):
            if lock in presence:
                package_managers.append(manager)
                break
        else:
            package_managers.append("npm")
    if "go.mod" in presence:
        package_managers.append("go")
    if "Cargo.toml" in presence:
        package_managers.append("cargo")
    if "pom.xml" in presence:
        package_managers.append("maven")
    if "build.gradle" in presence or "build.gradle.kts" in presence:
        package_managers.append("gradle")
    if any(fnmatch(p.lower(), "*.csproj") for p in paths[:_MAX_PATHS_FOR_EXT_COUNT]):
        package_managers.append("nuget")
    if _NAME_GEMFILE.lower() in presence:
        package_managers.append("bundler")
    if "composer.json" in presence:
        package_managers.append("composer")
    if "pubspec.yaml" in presence:
        package_managers.append("pub")
    if "podfile" in presence:
        package_managers.append("cocoapods")

    containers: list[str] = []
    if "dockerfile" in presence:
        containers.append("docker")
    if any(fnmatch(p.lower(), "docker-compose*.y*ml") for p in paths[:_MAX_PATHS_FOR_EXT_COUNT]):
        containers.append("docker-compose")

    # ---- evidence（只存路径→类别映射，控 JSONB 体积）
    evidence: dict[str, str] = {}
    for basename in _DEPENDENCY_MANIFESTS:
        original_path = lowered.get(basename.lower())
        if original_path is not None:
            evidence[original_path] = "dependency_manifest" if basename in texts else "present"
    for basename in _PRESENCE_MANIFESTS:
        if basename.lower() in presence:
            # presence 集合是 basename；反查原始路径展示。
            evidence[lowered.get(basename.lower(), basename)] = "present"

    return {
        "schema": PROFILE_SCHEMA,
        "primary_language": primary_language,
        "languages": languages,
        "frameworks": frameworks,
        "project_types": project_types,
        "package_managers": package_managers,
        "containers": containers,
        "evidence": evidence,
        "scanned_at": _now_iso(),
        "truncated": truncated,
    }


def stack_tags(profile: dict | None) -> list[str]:
    """把 profile 扁平化成去重小写 tag 列表（供 ``@>`` JSONB 过滤用）。

    主语言 + 全部框架 + 全部项目形态。marketplace 候选池的 stack_tags 列与
    项目扫描侧共用该投影。
    """
    if not isinstance(profile, dict):
        return []
    tags: list[str] = []
    for key in ("primary_language",):
        value = str(profile.get(key) or "").strip().lower()
        if value:
            tags.append(value)
    for key in ("frameworks", "project_types"):
        for item in profile.get(key) or []:
            value = str(item).strip().lower()
            if value:
                tags.append(value)
    return list(dict.fromkeys(tags))
