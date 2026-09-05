"""Standalone configuration wizard for the desktop build.

A minimal FastAPI app with NO dependency on the main service or a live DB. It
collects PostgreSQL / Redis / (optional) ClickHouse connection details, tests
connectivity, and writes them to the user ``.env``. Shown only when the user
``.env`` is missing required keys or a connectivity probe fails; otherwise the
desktop launcher goes straight to the main UI.

Notes baked into the UI:
- Redis must be >= 6.0: coredis 6.6.1 forces a RESP3 ``HELLO`` handshake with no
  RESP2 fallback, and ``HELLO`` did not exist before 6.0.
- If Redis/PG are absent, the wizard links to Memurai (Windows-native Redis 7)
  and to ``docker compose up -d`` using the repo's docker-compose.yml.
"""
from __future__ import annotations

import asyncio
import contextlib

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from desktop import paths

app = FastAPI(title="Ai Lubricant Setup", docs_url=None, redoc_url=None)

# Set to True once the user has saved a working configuration; the launcher
# polls this to know when to tear the wizard down and start the real service.
saved_ok: bool = False


class PgConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 5432
    user: str = "ai_lubricant"
    password: str = "ai_lubricant"
    database: str = "ai-lubricant"


class RedisConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 6379
    db: int = 0
    prefix_key: str = "ai_lubricant"


class ClickHouseConfig(BaseModel):
    enabled: bool = False
    addr: str = ""
    database: str = "ai_lubricant_logs"
    username: str = ""
    password: str = ""


class SaveRequest(BaseModel):
    pg: PgConfig
    redis: RedisConfig
    clickhouse: ClickHouseConfig


async def _probe_pg(cfg: PgConfig) -> dict:
    try:
        import asyncpg

        conn = await asyncio.wait_for(
            asyncpg.connect(
                host=cfg.host, port=cfg.port, user=cfg.user,
                password=cfg.password, database=cfg.database,
            ),
            timeout=8,
        )
        try:
            version = await conn.fetchval("SELECT version()")
        finally:
            await conn.close()
        return {"ok": True, "detail": str(version)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "detail": f"{type(exc).__name__}: {exc}"}


async def _probe_redis(cfg: RedisConfig) -> dict:
    try:
        from redis.asyncio import Redis

        client = Redis(host=cfg.host, port=cfg.port, db=cfg.db)
        try:
            await asyncio.wait_for(client.ping(), timeout=8)
            info = await client.info("server")
            version = info.get("redis_version") or info.get(b"redis_version")
            if isinstance(version, bytes):
                version = version.decode()
        finally:
            with contextlib.suppress(Exception):
                await client.aclose()
        # redis-py reaches this point after a successful RESP handshake.
        return {"ok": True, "detail": f"redis_version={version}"}
        return {"ok": True, "detail": f"redis_version={version}"}
    except Exception as exc:  # noqa: BLE001
        detail = f"{type(exc).__name__}: {exc}"
        hint = ""
        lowered = str(exc).lower()
        if "proto" in lowered or "hello" in lowered:
            hint = " —— 可能是 Redis < 6.0（本程序要求 Redis ≥ 6.0）"
        return {"ok": False, "detail": detail + hint}


async def _probe_clickhouse(cfg: ClickHouseConfig) -> dict:
    if not cfg.enabled or not cfg.addr:
        return {"ok": True, "detail": "disabled (skipped)"}
    try:
        from integrations.clickhouse import ClickHousePayloadClient

        client = ClickHousePayloadClient(
            addr=cfg.addr, database=cfg.database,
            username=cfg.username, password=cfg.password,
        )
        await asyncio.wait_for(client.connect(), timeout=8)
        rows = await client.query("SELECT version() AS v")
        await client.close()
        version = rows[0].get("v") if rows else "?"
        return {"ok": True, "detail": f"clickhouse {version}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "detail": f"{type(exc).__name__}: {exc}"}


def _write_env(req: SaveRequest) -> None:
    """Persist connection settings to the user .env via dotenv_loader."""
    from dotenv_loader import update_env_vars

    updates = {
        "POSTGRES_HOST": req.pg.host,
        "POSTGRES_PORT": str(req.pg.port),
        "POSTGRES_USER": req.pg.user,
        "POSTGRES_PASSWORD": req.pg.password,
        "POSTGRES_DATABASE": req.pg.database,
        "REDIS_HOST": req.redis.host,
        "REDIS_PORT": str(req.redis.port),
        "REDIS_DB": str(req.redis.db),
        "REDIS_PREFIX_KEY": req.redis.prefix_key,
        "REDIS_DECODE_RESPONSES": "false",
        "REDIS_MAX_CONNECTIONS": "50",
        "CLICKHOUSE_REQUEST_PAYLOAD_ENABLED": "true" if req.clickhouse.enabled else "false",
        "CLICKHOUSE_ADDR": req.clickhouse.addr,
        "CLICKHOUSE_DATABASE": req.clickhouse.database,
        "CLICKHOUSE_USERNAME": req.clickhouse.username,
        "CLICKHOUSE_PASSWORD": req.clickhouse.password,
    }
    update_env_vars(updates, path=paths.env_file_path())


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return _WIZARD_HTML


@app.post("/api/test-pg")
async def test_pg(cfg: PgConfig) -> JSONResponse:
    return JSONResponse(await _probe_pg(cfg))


@app.post("/api/test-redis")
async def test_redis(cfg: RedisConfig) -> JSONResponse:
    return JSONResponse(await _probe_redis(cfg))


@app.post("/api/test-clickhouse")
async def test_clickhouse(cfg: ClickHouseConfig) -> JSONResponse:
    return JSONResponse(await _probe_clickhouse(cfg))


@app.post("/api/save")
async def save(req: SaveRequest) -> JSONResponse:
    """Test PG + Redis (both mandatory), then persist. ClickHouse is optional."""
    pg = await _probe_pg(req.pg)
    if not pg["ok"]:
        return JSONResponse({"ok": False, "stage": "postgres", "detail": pg["detail"]}, status_code=400)
    redis = await _probe_redis(req.redis)
    if not redis["ok"]:
        return JSONResponse({"ok": False, "stage": "redis", "detail": redis["detail"]}, status_code=400)
    if req.clickhouse.enabled:
        ch = await _probe_clickhouse(req.clickhouse)
        if not ch["ok"]:
            return JSONResponse({"ok": False, "stage": "clickhouse", "detail": ch["detail"]}, status_code=400)
    _write_env(req)
    global saved_ok
    saved_ok = True
    return JSONResponse({"ok": True})


@app.get("/__wizard_done", response_class=HTMLResponse)
async def wizard_done() -> str:
    """Landing page shown right after a successful save."""
    return (
        "<html><head><meta charset='utf-8'><title>启动中</title>"
        "<style>body{font:15px/1.6 'Segoe UI','Microsoft YaHei',sans-serif;"
        "display:flex;align-items:center;justify-content:center;height:100vh;margin:0;"
        "background:#f6f7f9;color:#1a1d21}@media(prefers-color-scheme:dark){"
        "body{background:#16181d;color:#e5e7eb}}</style></head>"
        "<body><div>配置已保存，正在启动服务，请稍候…</div></body></html>"
    )


async def env_is_ready() -> bool:
    """True when the user .env has working PG + Redis already configured.

    Used by the launcher to decide whether to show the wizard. Loads the user
    .env (without touching os.environ semantics of the main process).
    """
    import os

    from dotenv import dotenv_values

    values = {}
    env_file = paths.env_file_path()
    if env_file.exists():
        values = dict(dotenv_values(env_file))
    # env vars already in the process win over file (matches load semantics).

    def _get(key: str, default: str = "") -> str:
        return os.environ.get(key) or values.get(key) or default

    if not _get("POSTGRES_HOST") or not _get("REDIS_HOST"):
        return False
    pg = await _probe_pg(PgConfig(
        host=_get("POSTGRES_HOST", "127.0.0.1"),
        port=int(_get("POSTGRES_PORT", "5432")),
        user=_get("POSTGRES_USER", "ai_lubricant"),
        password=_get("POSTGRES_PASSWORD", ""),
        database=_get("POSTGRES_DATABASE", _get("POSTGRES_DB", "ai-lubricant")),
    ))
    if not pg["ok"]:
        return False
    redis = await _probe_redis(RedisConfig(
        host=_get("REDIS_HOST", "127.0.0.1"),
        port=int(_get("REDIS_PORT", "6379")),
        db=int(_get("REDIS_DB", "0")),
        prefix_key=_get("REDIS_PREFIX_KEY", "ai_lubricant"),
    ))
    return bool(redis["ok"])


_WIZARD_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>Ai Lubricant 首次配置</title>
<style>
  :root { color-scheme: light dark; }
  * { box-sizing: border-box; }
  body { margin:0; font:14px/1.6 "Segoe UI","Microsoft YaHei",sans-serif;
         background:#f6f7f9; color:#1a1d21; padding:28px 32px; }
  h1 { font-size:20px; margin:0 0 4px; }
  .sub { color:#6b7280; margin:0 0 22px; font-size:13px; }
  section { background:#fff; border:1px solid #e4e7eb; border-radius:10px;
            padding:18px 20px; margin-bottom:16px; }
  section h2 { font-size:15px; margin:0 0 2px; display:flex; align-items:center; gap:8px; }
  .req { font-size:11px; font-weight:600; padding:1px 7px; border-radius:20px;
         background:#fee2e2; color:#b91c1c; }
  .opt { font-size:11px; font-weight:600; padding:1px 7px; border-radius:20px;
         background:#e0f2fe; color:#0369a1; }
  .hint { color:#6b7280; font-size:12px; margin:2px 0 14px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; }
  label { display:block; font-size:12px; color:#4b5563; margin-bottom:4px; }
  input[type=text],input[type=password],input[type=number] {
    width:100%; padding:7px 9px; border:1px solid #d1d5db; border-radius:6px;
    font:inherit; background:#fff; }
  input:focus { outline:2px solid #2563eb33; border-color:#2563eb; }
  .row { display:flex; align-items:center; gap:10px; margin-top:14px; }
  button { font:inherit; padding:7px 15px; border-radius:6px; cursor:pointer; border:1px solid #d1d5db;
           background:#fff; }
  button:hover { background:#f3f4f6; }
  button.primary { background:#2563eb; border-color:#2563eb; color:#fff; font-weight:600; padding:9px 22px; }
  button.primary:hover { background:#1d4ed8; }
  button:disabled { opacity:.55; cursor:not-allowed; }
  .status { font-size:12px; font-family:Consolas,monospace; word-break:break-all; flex:1; }
  .ok { color:#15803d; } .bad { color:#b91c1c; }
  details { margin-top:10px; font-size:12px; }
  summary { cursor:pointer; color:#2563eb; }
  pre { background:#f3f4f6; padding:9px 11px; border-radius:6px; overflow-x:auto; font-size:12px; }
  a { color:#2563eb; }
  .bar { position:sticky; bottom:0; background:#f6f7f9; padding:14px 0 4px;
         display:flex; align-items:center; gap:14px; border-top:1px solid #e4e7eb; }
  @media (prefers-color-scheme: dark) {
    body { background:#16181d; color:#e5e7eb; }
    section { background:#1e2127; border-color:#2d3138; }
    input[type=text],input[type=password],input[type=number] { background:#16181d; border-color:#3a3f47; color:#e5e7eb; }
    button { background:#282c34; border-color:#3a3f47; color:#e5e7eb; }
    button:hover { background:#31363f; }
    pre { background:#16181d; } .bar { background:#16181d; border-color:#2d3138; }
    .hint,.sub,label { color:#9ca3af; }
  }
</style>
</head>
<body>
<h1>Ai Lubricant 首次配置</h1>
<p class="sub">本程序需要外部 PostgreSQL 与 Redis。填好连接信息后点“测试并启动”。</p>
<!--WIZARD_BODY-->
</body>
</html>
"""

_WIZARD_BODY = r"""
<section>
  <h2>PostgreSQL <span class="req">必填</span></h2>
  <p class="hint">运行时状态的唯一存储（渠道、账号、Key、路由、日志、任务…）。</p>
  <div class="grid">
    <div><label>主机 Host</label><input id="pg_host" type="text" value="127.0.0.1"></div>
    <div><label>端口 Port</label><input id="pg_port" type="number" value="5432"></div>
    <div><label>用户 User</label><input id="pg_user" type="text" value="ai_lubricant"></div>
    <div><label>密码 Password</label><input id="pg_password" type="password" value="ai_lubricant"></div>
    <div><label>数据库 Database</label><input id="pg_database" type="text" value="ai-lubricant"></div>
  </div>
  <div class="row">
    <button onclick="testPg()">测试连接</button>
    <span id="pg_status" class="status"></span>
  </div>
</section>

<section>
  <h2>Redis <span class="req">必填</span> <span class="opt">需 ≥ 6.0</span></h2>
  <p class="hint">限流、配额、冷却、会话、验证码、跨实例同步。要求 Redis ≥ 6.0（RESP3）。</p>
  <div class="grid">
    <div><label>主机 Host</label><input id="rd_host" type="text" value="127.0.0.1"></div>
    <div><label>端口 Port</label><input id="rd_port" type="number" value="6379"></div>
    <div><label>DB 序号</label><input id="rd_db" type="number" value="0"></div>
    <div><label>Key 前缀</label><input id="rd_prefix" type="text" value="ai_lubricant"></div>
  </div>
  <div class="row">
    <button onclick="testRedis()">测试连接</button>
    <span id="rd_status" class="status"></span>
  </div>
  <details>
    <summary>没有 PostgreSQL / Redis？点这里获取搭建方式</summary>
    <p><b>方式一：Docker（推荐，一条命令起全套）</b></p>
    <pre>docker compose up -d postgres redis</pre>
    <p>仓库已附带 docker-compose.yml，起好后按其中的用户名/密码/端口填上方表单。</p>
    <p><b>方式二：Windows 原生 Redis</b> — 安装
       <a href="https://www.memurai.com/" target="_blank">Memurai</a>（兼容 Redis 7），
       PostgreSQL 用 <a href="https://www.postgresql.org/download/windows/" target="_blank">官方安装包</a>。</p>
  </details>
</section>

<section>
  <h2>ClickHouse <span class="opt">可选</span></h2>
  <p class="hint">不填即关闭。仅影响“请求原文详情”和“Agent/网页对话历史”，核心网关不受影响。</p>
  <div class="row" style="margin-top:0">
    <label style="margin:0"><input id="ch_enabled" type="checkbox" onchange="toggleCh()"> 启用 ClickHouse</label>
  </div>
  <div id="ch_fields" class="grid" style="display:none; margin-top:12px">
    <div><label>地址 addr (host:port)</label><input id="ch_addr" type="text" value="127.0.0.1:8123"></div>
    <div><label>数据库</label><input id="ch_database" type="text" value="ai_lubricant_logs"></div>
    <div><label>用户</label><input id="ch_username" type="text" value="default"></div>
    <div><label>密码</label><input id="ch_password" type="password" value=""></div>
  </div>
  <div class="row" id="ch_test_row" style="display:none">
    <button onclick="testCh()">测试连接</button>
    <span id="ch_status" class="status"></span>
  </div>
</section>

<div class="bar">
  <button class="primary" id="saveBtn" onclick="save()">测试并启动</button>
  <span id="save_status" class="status"></span>
</div>

<script>
function pg(){return{host:val('pg_host'),port:+val('pg_port'),user:val('pg_user'),password:val('pg_password'),database:val('pg_database')};}
function rd(){return{host:val('rd_host'),port:+val('rd_port'),db:+val('rd_db'),prefix_key:val('rd_prefix')};}
function ch(){return{enabled:document.getElementById('ch_enabled').checked,addr:val('ch_addr'),database:val('ch_database'),username:val('ch_username'),password:val('ch_password')};}
function val(id){return document.getElementById(id).value;}
function setStatus(id,r){var e=document.getElementById(id);e.className='status '+(r.ok?'ok':'bad');e.textContent=(r.ok?'✓ ':'✗ ')+r.detail;}
function toggleCh(){var on=document.getElementById('ch_enabled').checked;document.getElementById('ch_fields').style.display=on?'grid':'none';document.getElementById('ch_test_row').style.display=on?'flex':'none';}
async function post(url,body){var r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});return r.json();}
async function testPg(){setStatus('pg_status',{ok:true,detail:'测试中…'});setStatus('pg_status',await post('/api/test-pg',pg()));}
async function testRedis(){setStatus('rd_status',{ok:true,detail:'测试中…'});setStatus('rd_status',await post('/api/test-redis',rd()));}
async function testCh(){setStatus('ch_status',{ok:true,detail:'测试中…'});setStatus('ch_status',await post('/api/test-clickhouse',ch()));}
async function save(){
  var btn=document.getElementById('saveBtn');btn.disabled=true;
  setStatus('save_status',{ok:true,detail:'正在测试连接并保存…'});
  try{
    var r=await fetch('/api/save',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({pg:pg(),redis:rd(),clickhouse:ch()})});
    var j=await r.json();
    if(j.ok){setStatus('save_status',{ok:true,detail:'配置已保存，正在启动服务…'});
      setTimeout(function(){location.href='/__wizard_done';},600);}
    else{setStatus('save_status',{ok:false,detail:'['+j.stage+'] '+j.detail});btn.disabled=false;}
  }catch(e){setStatus('save_status',{ok:false,detail:String(e)});btn.disabled=false;}
}
</script>
"""

_WIZARD_HTML = _WIZARD_HTML.replace("<!--WIZARD_BODY-->", _WIZARD_BODY)
