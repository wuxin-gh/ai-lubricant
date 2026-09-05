"""测试在子目录下，需要把仓库根目录加入 sys.path，保证 `import main` / `import providers` / `import admin` / `import message_utils` 可解析。"""
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
