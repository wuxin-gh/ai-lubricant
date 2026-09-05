import json, threading, time, uuid, sys, hashlib
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Dict, Any, Optional, List


def log(*args, **kwargs):
    print(*args, file=sys.stderr, flush=True, **kwargs)


def _tlog(user_id, *args, **kwargs):
    prefix = f"[user:{user_id}] " if user_id is not None else ""
    print(prefix, *args, file=sys.stderr, flush=True, **kwargs)


@dataclass
class TabLease:
    session_key: str
    holder: str
    acquired_at: float
    last_active: float
    expires_at: float


class TabBusyError(RuntimeError):
    def __init__(self, lease: TabLease):
        self.lease = lease
        super().__init__(f"Tab {lease.session_key} is occupied by another conversation or task")


class TabLeaseManager:
    def __init__(self, default_ttl: float = 300):
        self.default_ttl = max(float(default_ttl), 0.001)
        self._leases: Dict[str, TabLease] = {}
        self._holder_sessions: Dict[str, str] = {}
        self._lock = threading.Lock()

    def try_acquire(self, session_key: str, holder: str, ttl: float | None = None) -> TabLease:
        session_key, holder = str(session_key), str(holder)
        if not session_key or not holder:
            raise ValueError("session_key and holder are required")
        now = time.time()
        lifetime = max(float(ttl if ttl is not None else self.default_ttl), 0.001)
        with self._lock:
            current = self._leases.get(session_key)
            if current is not None and current.expires_at <= now:
                self._leases.pop(session_key, None)
                current = None
            if current is not None and current.holder != holder:
                raise TabBusyError(current)
            previous_key = self._holder_sessions.get(holder)
            if previous_key and previous_key != session_key:
                previous = self._leases.get(previous_key)
                if previous is not None and previous.holder == holder:
                    self._leases.pop(previous_key, None)
            if current is None:
                current = TabLease(session_key, holder, now, now, now + lifetime)
                self._leases[session_key] = current
            else:
                current.last_active = now
                current.expires_at = now + lifetime
            self._holder_sessions[holder] = session_key
            return current

    def release(self, session_key: str, holder: str) -> bool:
        with self._lock:
            current = self._leases.get(str(session_key))
            if current is None or current.holder != str(holder):
                return False
            self._leases.pop(str(session_key), None)
            if self._holder_sessions.get(str(holder)) == str(session_key):
                self._holder_sessions.pop(str(holder), None)
            return True

    def release_by_holder(self, holder: str) -> int:
        holder = str(holder)
        with self._lock:
            keys = [key for key, lease in self._leases.items() if lease.holder == holder]
            for key in keys:
                self._leases.pop(key, None)
            self._holder_sessions.pop(holder, None)
            return len(keys)

    def get(self, session_key: str) -> TabLease | None:
        now = time.time()
        with self._lock:
            current = self._leases.get(str(session_key))
            if current is not None and current.expires_at <= now:
                self._leases.pop(str(session_key), None)
                if self._holder_sessions.get(current.holder) == current.session_key:
                    self._holder_sessions.pop(current.holder, None)
                return None
            return current

    def session_for_holder(self, holder: str) -> str | None:
        holder = str(holder)
        with self._lock:
            session_key = self._holder_sessions.get(holder)
            current = self._leases.get(session_key) if session_key else None
            if current is None or current.expires_at <= time.time():
                if session_key:
                    self._leases.pop(session_key, None)
                self._holder_sessions.pop(holder, None)
                return None
            return session_key

    def cleanup_expired(self) -> int:
        now = time.time()
        with self._lock:
            keys = [key for key, lease in self._leases.items() if lease.expires_at <= now]
            for key in keys:
                lease = self._leases.pop(key)
                if self._holder_sessions.get(lease.holder) == key:
                    self._holder_sessions.pop(lease.holder, None)
            return len(keys)


current_holder: ContextVar[str] = ContextVar("cdp_current_holder", default="")
current_session_id: ContextVar[str] = ContextVar("cdp_current_session_id", default="")


class Session:
    def __init__(self, session_id, info, client=None):
        self.id, self.info = session_id, info
        self.connect_at, self.disconnect_at = time.time(), None
        self.type = info.get("type", "ext_ws")
        self.ws_client = client if self.type == "ext_ws" else None
        self.http_queue = client if self.type == "http" else None

    @property
    def url(self): return self.info.get("url", "")

    def is_active(self):
        if self.type == "http" and self.disconnect_at is None and time.time() - self.connect_at > 60:
            self.disconnect_at = time.time()
        return self.disconnect_at is None

    def reconnect(self, client, info):
        self.info, self.type = info, info.get("type", "ext_ws")
        self.ws_client = client if self.type == "ext_ws" else None
        self.http_queue = client if self.type == "http" else None
        self.connect_at, self.disconnect_at = time.time(), None

    def mark_disconnected(self):
        self.disconnect_at = self.disconnect_at or time.time()


class ClientContext:
    def __init__(self, client_id: str, name: str = ""):
        self.client_id, self.name = client_id, name
        self.pages: Dict[str, Session] = {}
        self.connected_at, self.last_active = time.time(), time.time()
        self.ws_client = None


class UserContext:
    """MCP user state containing authenticated CDP clients and their pages."""
    def __init__(self, user_id):
        self.user_id = user_id
        self.clients: Dict[str, ClientContext] = {}
        self.results: Dict[str, Any] = {}
        self.acks: Dict[str, bool] = {}
        self.default_session_id: Optional[str] = None
        self.latest_session_id: Optional[str] = None
        self.created_at, self.last_active = time.time(), time.time()

    @property
    def sessions(self) -> Dict[str, Session]:
        return {key: page for client in self.clients.values() for key, page in client.pages.items()}

    def clean_sessions(self):
        for client in self.clients.values():
            for key, page in list(client.pages.items()):
                if not page.is_active() and time.time() - page.disconnect_at > 600:
                    del client.pages[key]

    def get_all_active_sessions(self):
        return [{"id": page.id, **page.info} for page in self.sessions.values() if page.is_active()]


class TokenManager:
    """Validates SHA-256 hashes and owns user -> clients runtime contexts."""
    def __init__(self, clients: Optional[List[dict]] = None):
        self.contexts: Dict[Any, UserContext] = {}
        self._lock = threading.Lock()
        self.clients_by_hash: Dict[str, dict] = {}
        self.update_clients(clients or [])

    def update_clients(self, clients: List[dict]):
        self.clients_by_hash = {str(row["token_hash"]): dict(row) for row in clients if row.get("enabled", True) and row.get("token_hash")}

    def authenticate(self, token: str) -> dict | None:
        if not token: return None
        return self.clients_by_hash.get(hashlib.sha256(token.encode("utf-8")).hexdigest())

    def get_context(self, user_id) -> UserContext:
        with self._lock:
            if user_id not in self.contexts: self.contexts[user_id] = UserContext(user_id)
            ctx = self.contexts[user_id]; ctx.last_active = time.time(); return ctx

    def cleanup_expired(self, max_idle=3600):
        with self._lock:
            for uid in [uid for uid, ctx in self.contexts.items() if time.time() - ctx.last_active > max_idle]:
                del self.contexts[uid]


class TMWebDriver:
    def __init__(self, clients=None, external_ws=True):
        if not external_ws:
            raise RuntimeError("standalone CDP bridge listeners are removed; use MCP runtime")
        self.external_ws = True
        self.token_manager = TokenManager(clients or [])
        self.leases = TabLeaseManager()
        self.is_remote = False

    def apply_clients(self, clients: List[dict]):
        # 会话池按 client_id 隔离（插件单点登录=一个客户端一个浏览器）。
        # 失效判定只看 client_id → token_hash 是否仍有效，不再依赖 user_id。
        old_hashes = set(self.token_manager.clients_by_hash)
        self.token_manager.update_clients(clients)
        valid_clients = {}
        for row in self.token_manager.clients_by_hash.values():
            client_id = str(row.get("instance_key") or row.get("id") or "")
            if client_id:
                valid_clients[client_id] = str(row["token_hash"])
        for client_id, ctx in list(self.token_manager.contexts.items()):
            for cid, client in list(ctx.clients.items()):
                token_hash = valid_clients.get(cid)
                active_hash = str(getattr(client.ws_client, "_token", "")) if client.ws_client is not None else ""
                if token_hash is None or (active_hash and active_hash != token_hash):
                    self.disconnect_client(client.ws_client)
                    ctx.clients.pop(cid, None)
        return old_hashes - set(self.token_manager.clients_by_hash)

    def authenticate_client(self, token: str, client_id: str | None = None) -> dict | None:
        row = self.token_manager.authenticate(token)
        if not row: return None
        configured_id = str(row.get("instance_key") or row.get("id"))
        if client_id and client_id != configured_id: return None
        if not configured_id: return None
        return {**row, "client_id": configured_id}

    def bind_client(self, ws_client, config: dict) -> ClientContext:
        # 会话池按 client_id 隔离：context key = client_id，每个 context 只承载
        # 同一个 client（插件单点登录=一个客户端一个浏览器）。user_id 仅作元数据保留。
        client_id = str(config["client_id"])
        user_id = config.get("user_id")
        ctx = self.token_manager.get_context(client_id)
        existing = ctx.clients.get(client_id)
        if existing and existing.ws_client is not None and existing.ws_client is not ws_client:
            raise ValueError("client token already connected")
        client = existing or ClientContext(client_id, str(config.get("name") or ""))
        client.ws_client, client.last_active = ws_client, time.time()
        ctx.clients[client_id] = client
        ws_client._user_id, ws_client._client_id, ws_client._token = user_id, client_id, str(config.get("token_hash") or "")
        return client

    def disconnect_client(self, ws_client):
        if ws_client is None: return
        try: ws_client.close_auth(4403, "authorization revoked")
        except Exception: pass
        self.unregister_client(ws_client)

    def get_context(self, client_id=None):
        # 会话池按 client_id 隔离；参数名沿用旧签名（token=）但语义现在是 client_id。
        if client_id is None: raise ValueError("CDP client identity required")
        return self.token_manager.get_context(client_id)

    def get_all_sessions(self, token=None): return self.get_context(token).get_all_active_sessions()

    def find_session(self, url_pattern, token=None):
        ctx = self.get_context(token)
        pages = ctx.sessions
        if not url_pattern:
            page = pages.get(ctx.latest_session_id)
            return [(page.id, page.info)] if page and page.is_active() else []
        return [(page.id, page.info) for page in pages.values() if page.is_active() and url_pattern in page.url]

    def get_session_dict(self, token=None): return {s["id"]: s.get("url", "") for s in self.get_all_sessions(token)}

    def ingest_message(self, raw, client) -> None:
        data = json.loads(raw)
        if not hasattr(client, "_user_id") or not hasattr(client, "_client_id"):
            raise ValueError("client is not authenticated")
        ctx = self.get_context(client._client_id); cctx = ctx.clients.get(client._client_id)
        if not cctx or cctx.ws_client is not client: raise ValueError("client authorization expired")
        kind = data.get("type")
        if kind in ("ext_ready", "tabs_update"):
            tabs = data.get("tabs", [])
            if not isinstance(tabs, list):
                raise ValueError("tabs must be a list")
            current = {f"{client._client_id}:{tab['id']}" for tab in tabs}
            for key, page in list(cctx.pages.items()):
                if key not in current: page.mark_disconnected()
            for tab in tabs:
                raw_id = str(tab["id"]); key = f"{client._client_id}:{raw_id}"
                info = {"url": tab.get("url"), "title": tab.get("title", ""), "connected_at": time.time(), "type": "ext_ws", "client_id": client._client_id, "page_id": raw_id}
                page = cctx.pages.get(key)
                if page: page.reconnect(client, info)
                else: cctx.pages[key] = Session(key, info, client)
                ctx.latest_session_id = key
                if ctx.default_session_id is None: ctx.default_session_id = key
        elif kind in ("ack", "result", "error"):
            target = ctx.acks if kind == "ack" else ctx.results
            target[data.get("id", "")] = True if kind == "ack" else {"success": kind == "result", "data": data.get("result") if kind == "result" else data.get("error"), "newTabs": data.get("newTabs", [])}

    def unregister_client(self, client):
        cid = getattr(client, "_client_id", None)
        if cid is None: return
        cctx = self.token_manager.get_context(cid).clients.get(cid)
        if cctx and cctx.ws_client is client:
            cctx.ws_client = None
            for page in cctx.pages.values(): page.mark_disconnected()

    def snapshot_contexts(self, token_resolver=None):
        # 会话池按 client_id 隔离：每个 context 即一个 CDP 客户端。保留
        # {contexts:[{clients:[...]}]} 外层形状以兼容前端扁平化；context_id
        # 就是 client_id（不再有跨客户端的 user 归属）。
        result = []
        for client_key, ctx in self.token_manager.contexts.items():
            clients = []
            for cid, client in ctx.clients.items():
                pages = [{"id": p.id, "session_key": p.id, "page_id": p.info.get("page_id"), "url": p.url, "title": p.info.get("title", ""), "active": p.is_active(), "status": "connected" if p.is_active() else "disconnected", "connected_at": p.connect_at, "connect_at": p.connect_at, "disconnect_at": p.disconnect_at} for p in client.pages.values()]
                clients.append({"client_id": cid, "name": client.name, "connected": client.ws_client is not None, "pages": pages, "active_count": sum(p["active"] for p in pages)})
            result.append({"context_id": str(client_key), "default_session_id": ctx.default_session_id, "latest_session_id": ctx.latest_session_id, "clients": clients, "active_count": sum(c["active_count"] for c in clients)})
        return result

    def resolve_session(self, session_id=None, token=None, *, require_active=True) -> Session:
        """Resolve a public composite key or an unambiguous legacy raw tab ID."""
        ctx = self.get_context(token)
        requested = str(session_id) if session_id not in (None, "") else ctx.default_session_id
        page = ctx.sessions.get(requested) if requested else None
        if page is None and requested and ":" not in requested:
            matches = [
                candidate for candidate in ctx.sessions.values()
                if str(candidate.info.get("page_id")) == requested
                and (candidate.is_active() or not require_active)
            ]
            if len(matches) > 1:
                raise ValueError(f"Legacy tab ID {requested} is ambiguous; use client_id:page_id")
            if matches:
                page = matches[0]
        if page is None or (require_active and not page.is_active()):
            raise ValueError(f"会话ID {requested} 未连接")
        return page

    def raw_tab_id(self, session_id=None, token=None) -> tuple[Session, int | str]:
        page = self.resolve_session(session_id, token)
        raw_id = str(page.info.get("page_id", page.id.split(":", 1)[-1]))
        return page, int(raw_id) if raw_id.isdigit() else raw_id

    def execute_js(self, code, timeout=15, session_id=None, token=None):
        ctx = self.get_context(token)
        holder = current_holder.get("") or f"rpc:{uuid.uuid4().hex}"
        persistent = bool(current_holder.get(""))
        requested_session = session_id or current_session_id.get("")
        if not requested_session and persistent:
            requested_session = self.leases.session_for_holder(holder)
        try:
            page = self.resolve_session(requested_session, token)
        except ValueError:
            if session_id not in (None, ""):
                raise
            active = [candidate for candidate in ctx.sessions.values() if candidate.is_active()]
            if not active: raise ValueError(f"会话ID {session_id or ctx.default_session_id} 未连接")
            page = active[0]
        lease = self.leases.try_acquire(page.id, holder)
        ctx.default_session_id = page.id
        try:
            exec_id = str(uuid.uuid4())
            raw_tab = str(page.info.get("page_id", page.id.split(":", 1)[-1]))
            page.ws_client.send_message(json.dumps({"id": exec_id, "code": code, "tabId": int(raw_tab) if raw_tab.isdigit() else raw_tab}))
            start, acked = time.time(), False
            while exec_id not in ctx.results:
                time.sleep(.2)
                if exec_id in ctx.acks: acked = True
                if time.time() - start > timeout: return {"result": f"No response data in {timeout}s ({'ACK received' if acked else 'no ACK'})"}
            result = ctx.results.pop(exec_id); ctx.acks.pop(exec_id, None)
            if not result["success"]: raise Exception(result["data"])
            return {"data": result["data"], **({"newTabs": result["newTabs"]} if result.get("newTabs") else {})}
        finally:
            if not persistent:
                self.leases.release(page.id, holder)

    def clean_sessions(self, token=None):
        self.leases.cleanup_expired()
        self.get_context(token).clean_sessions()

    def acquire_tab(self, session_id=None, token=None, holder=None):
        page = self.resolve_session(session_id or current_session_id.get(""), token)
        holder = str(holder or current_holder.get("") or f"rpc:{uuid.uuid4().hex}")
        return page, self.leases.try_acquire(page.id, holder)

    def release_tab(self, session_id, holder=None, token=None):
        holder = str(holder or current_holder.get(""))
        if holder:
            return self.leases.release(str(session_id), holder)
        return False

    def release_holder(self, holder):
        return self.leases.release_by_holder(holder)

    def tab_lease(self, session_id):
        lease = self.leases.get(str(session_id))
        if lease is None:
            return None
        return {
            "holder": lease.holder,
            "acquired_at": lease.acquired_at,
            "last_active": lease.last_active,
            "expires_at": lease.expires_at,
        }
    def set_session(self, url_pattern, token=None):
        matched = self.find_session(url_pattern, token)
        if not matched: return False
        ctx = self.get_context(token); ctx.default_session_id = matched[0][0]; return ctx.default_session_id
    def jump(self, url, timeout=10, token=None): return self.execute_js(f"window.location.href={url!r}", timeout, token=token)
