"""Desktop build package.

Bundles the ai-lubricant server + its React SPA into a Windows double-click exe
via PyInstaller + pywebview. All desktop-specific glue lives here; the main
service (main.py / db.py / node_server / tunnel_server) is used unmodified.
"""
