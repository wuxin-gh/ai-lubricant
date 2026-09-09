"""Remote-only node control client (data service side).

The data service never hosts the node control plane. Every call here dials the
separate control process over the existing Connect unary-over-HTTP contract
(``POST {base}/agentcompose.v2.NodeService/{Method}`` with camelCase JSON, proto
enum names, ``Connect-Protocol-Version: 1``, and a Bearer token). Control-plane
address / token / timeout come from ``agent_compose_base_url`` /
``node_control_token`` / ``agent_compose_timeout``.

Method names / args / return shapes are byte-compatible with the retired
in-process ``LocalNodeClient`` so every data-side caller works unchanged. When
the base url / token are unset, calls raise :class:`NodeServerUnavailable`
rather than degrading silently — the data process must never host the control
plane itself.
"""
from __future__ import annotations

import json
from typing import Any, Optional

from .errors import NodeServerUnavailable, RPCError
from .normalization import (
    NODE_ROLE_TO_PROTO,
    NODE_STARTUP_TO_PROTO,
    NODE_STATUS_TO_PROTO,
    normalize_node_info,
)

# Binary upload limits, kept in lockstep with the node (host_file_upload.go) and
# the control service (node_server/service.py). 1 MiB per chunk keeps each
# Connect frame small; 10 MiB total matches the browser-side cap.
_MAX_UPLOAD_CHUNK_BYTES = 1024 * 1024
_MAX_UPLOAD_TOTAL_BYTES = 10 * 1024 * 1024
# Control-plane long operations wait for a node ack after a download/extract or
# npm install. Keep this aligned with node_server.service.EDITOR_ACK_TIMEOUT.
NODE_LONG_RPC_TIMEOUT = 620.0


class NodeClient:
    """Async facade that dials the control process over Connect unary HTTP."""

    @property
    def _base_url(self) -> str:
        from ..config import settings

        return (settings.agent_compose_base_url or "").rstrip("/")

    @property
    def _token(self) -> str:
        from ..config import settings

        return settings.node_control_token or ""

    @property
    def _timeout(self) -> int:
        from ..config import settings

        return max(1, settings.agent_compose_timeout)

    @property
    def enabled(self) -> bool:
        """True when a control base url + token are configured.

        Actual reachability of the control process is only checked on a call.
        """
        return bool(self._base_url and self._token)

    def _require_remote(self) -> None:
        if not self._base_url or not self._token:
            raise NodeServerUnavailable(
                "未配置控制面 agent_compose_base_url/token，无法连接节点控制服务"
            )

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Connect-Protocol-Version": "1",
            "Authorization": f"Bearer {self._token}",
        }

    async def _rpc(
        self, method: str, msg: dict | None = None, *, timeout: float | None = None
    ) -> dict[str, Any]:
        """Call the control process using the Connect unary contract.

        Transport failures become ``NodeServerUnavailable`` so existing route
        guards map them to a bounded 503 and node listings degrade safely.
        Connect error envelopes preserve their domain code as ``RPCError``.

        ``timeout`` overrides the global ``agent_compose_timeout`` for calls
        that legitimately run long (node-side installs wait for an ack that
        only arrives after the node finishes downloading/extracting).
        """
        self._require_remote()
        import aiohttp

        url = f"{self._base_url}/agentcompose.v2.NodeService/{method}"
        timeout = aiohttp.ClientTimeout(total=timeout or self._timeout)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=msg or {}, headers=self._headers()) as resp:
                    try:
                        body = await resp.json(content_type=None)
                    except Exception as exc:  # noqa: BLE001
                        raise NodeServerUnavailable(
                            f"控制面 {method} 返回了无效响应: {exc}"
                        ) from exc
                    if resp.status >= 400:
                        if isinstance(body, dict) and body.get("code"):
                            raise RPCError(
                                str(body.get("code")),
                                str(body.get("message") or f"control RPC {method} failed"),
                            )
                        raise NodeServerUnavailable(
                            f"控制面 {method} HTTP {resp.status}: {body}"
                        )
                    return body if isinstance(body, dict) else {}
        except (NodeServerUnavailable, RPCError):
            raise
        except Exception as exc:  # noqa: BLE001
            raise NodeServerUnavailable(f"控制面 {method} 不可用: {exc}") from exc

    # ── management plane ────────────────────────────────────────────────────
    async def list_nodes(self, status: str | None = None) -> list[dict[str, Any]]:
        msg: dict[str, Any] = {}
        proto_status = NODE_STATUS_TO_PROTO.get((status or "").strip().lower()) if status else None
        if proto_status:
            msg["status"] = proto_status
        resp = await self._rpc("ListNodes", msg)
        wire_nodes = resp.get("nodes") or []
        rows = [normalize_node_info(n) for n in wire_nodes]
        for row, wire in zip(rows, wire_nodes):
            row["active_sessions"] = len(row.get("active_session_ids") or [])
            row["capacity"] = dict(wire.get("capacity") or {})
        return rows

    async def get_public_ip_lookup_config(self) -> dict[str, Any]:
        return await self._rpc("GetPublicIPLookupConfig")

    async def update_public_ip_lookup_config(
        self, ipv4_urls: list[str], ipv6_urls: list[str]
    ) -> dict[str, Any]:
        return await self._rpc(
            "UpdatePublicIPLookupConfig",
            {"ipv4Urls": list(ipv4_urls or []), "ipv6Urls": list(ipv6_urls or [])},
        )

    async def get_node_proxy_config(self, node_id: str) -> dict[str, Any]:
        return await self._rpc("GetNodeProxyConfig", {"nodeId": node_id})

    async def update_node_proxy_config(
        self, node_id: str, proxy_config_id: str = ""
    ) -> dict[str, Any]:
        return await self._rpc(
            "UpdateNodeProxyConfig",
            {"nodeId": node_id, "proxyConfigId": (proxy_config_id or "").strip()},
        )

    async def set_node_last_proxy(self, node_id: str, proxy_config_id: str = "") -> dict[str, Any]:
        """记录某节点上次成功升级所用的代理，供升级弹窗预选。"""
        return await self._rpc(
            "SetNodeLastProxy",
            {"nodeId": node_id, "proxyConfigId": (proxy_config_id or "").strip()},
        )

    async def set_node_capacity(self, node_id: str, capacity: dict | None) -> dict[str, Any]:
        return await self._rpc("SetNodeCapacity", {"nodeId": node_id, "capacity": capacity or {}})

    async def approve_node(self, node_id: str) -> dict[str, Any]:
        resp = await self._rpc("ApproveNode", {"nodeId": node_id})
        return normalize_node_info(resp.get("node") or {}) if resp else {}

    async def revoke_node(self, node_id: str) -> dict[str, Any]:
        resp = await self._rpc("RevokeNode", {"nodeId": node_id})
        return normalize_node_info(resp.get("node") or {}) if resp else {}

    async def delete_node(self, node_id: str, *, force: bool = True) -> dict[str, Any]:
        resp = await self._rpc("DeleteNode", {"nodeId": node_id, "force": force})
        return {"deleted": bool(resp.get("deleted"))}

    async def onboard_node(
        self,
        role: str = "execution",
        startup_method: str = "standalone",
        node_name: str | None = None,
        manager_node_id: str | None = None,
        labels: dict[str, str] | None = None,
        proxy_config_id: str = "",
    ) -> dict[str, Any]:
        msg: dict[str, Any] = {
            "role": NODE_ROLE_TO_PROTO.get(role, "NODE_ROLE_EXECUTION"),
            "startupMethod": NODE_STARTUP_TO_PROTO.get(
                startup_method, "NODE_STARTUP_METHOD_STANDALONE"
            ),
        }
        if node_name:
            msg["nodeName"] = node_name
        if manager_node_id:
            msg["managerNodeId"] = manager_node_id
        if labels:
            msg["labels"] = {str(k): str(v) for k, v in labels.items()}
        msg["proxyConfigId"] = (proxy_config_id or "").strip()
        resp = await self._rpc("OnboardNode", msg)
        node = normalize_node_info(resp.get("node") or {}) if resp else {}
        return {
            "node_id": resp.get("nodeId") or node.get("node_id") or "",
            "secret": resp.get("secret") or "",
            "otpauth_uri": resp.get("otpauthUri") or "",
            "install_command": resp.get("installCommand") or "",
            "script_url": resp.get("scriptUrl") or "",
            "launched": bool(resp.get("launched")),
            "node": node,
        }

    async def revoke_onboard_node(self, node_id: str) -> dict[str, Any]:
        resp = await self._rpc("RevokeOnboardNode", {"nodeId": node_id})
        return {
            "deleted": bool(resp.get("deleted")),
            "revoked": bool(resp.get("revoked")),
            "node": normalize_node_info(resp.get("node") or {}) if resp.get("node") else {},
        }

    async def move_node(self, node_id: str, manager_node_id: str) -> dict[str, Any]:
        resp = await self._rpc("MoveNode", {"nodeId": node_id, "managerNodeId": manager_node_id})
        return normalize_node_info(resp.get("node") or {})

    async def manage_editor(self, node_id: str, editor: str, action: str) -> dict[str, Any]:
        # 节点跑官方安装/升级命令（npm i -g …）可能数分钟；控制面 ack 窗口 600s，
        # 数据面调用必须覆盖这个窗口，否则 30s 全局超时会先掐断 → 503「不可用」假象。
        return await self._rpc(
            "ManageEditor",
            {"nodeId": node_id, "editor": editor, "action": action},
            timeout=NODE_LONG_RPC_TIMEOUT,
        )

    async def install_host_tool(
        self, node_id: str, tool: str, *, target: dict | None = None
    ) -> dict[str, Any]:
        """InstallHostTool：让在线节点安装宿主级运行依赖（当前仅 nodejs）。

        下载 URL/sha/代理三字段与 runtime 升级的 ``target`` 同构，由数据侧
        （node_upgrade_targets.resolve_proxy + 版本/镜像配置）解析好再下发。
        节点下载 ~30MB 归档 + 解压 + 探测可能数分钟，控制面 ack 窗口 600s，
        这里用同口径长超时，避免全局 30s 先掐断报假 503。
        """
        return await self._rpc(
            "InstallHostTool",
            {"nodeId": node_id, "tool": tool, "target": target},
            timeout=NODE_LONG_RPC_TIMEOUT,
        )

    async def list_node_environments(self, node_id: str) -> list[dict[str, Any]]:
        """List the named shared environments owned by a node (server ledger)."""
        resp = await self._rpc("ListNodeEnvironments", {"nodeId": node_id})
        envs = resp.get("environments") or []
        return [e for e in envs if isinstance(e, dict)]

    async def manage_environment(self, node_id: str, env_id: str, action: str) -> dict[str, Any]:
        """Create ("create") or remove ("remove") a shared-environment directory."""
        return await self._rpc(
            "ManageNodeEnvironment", {"nodeId": node_id, "envId": env_id, "action": action}
        )

    async def sync_environment(
        self, node_id: str, env_id: str, skills: list[dict], plugins: list[dict]
    ) -> dict[str, Any]:
        """Install the exact desired skill/plugin set into an environment HOME."""
        return await self._rpc(
            "SyncNodeEnvironment",
            {"nodeId": node_id, "envId": env_id, "skills": skills, "plugins": plugins},
        )

    async def inspect_environment(self, node_id: str, env_id: str) -> dict[str, Any]:
        """Report what is physically installed in an environment HOME."""
        return await self._rpc(
            "InspectNodeEnvironment", {"nodeId": node_id, "envId": env_id}
        )

    async def inspect_system_env(self, node_id: str, provider: str = "") -> dict[str, Any]:
        """Report what the providers would discover in the operator's real HOME."""
        return await self._rpc(
            "InspectNodeSystemEnv", {"nodeId": node_id, "provider": provider}
        )

    async def sync_system_env(
        self,
        node_id: str,
        skills: list[dict],
        plugins: list[dict],
        overwrite: bool = False,
        remove: list[str] | None = None,
    ) -> dict[str, Any]:
        """Install platform resources into the operator's HOME (incremental)."""
        return await self._rpc(
            "SyncNodeSystemEnv",
            {
                "nodeId": node_id,
                "skills": skills,
                "plugins": plugins,
                "overwrite": bool(overwrite),
                "remove": list(remove or []),
            },
        )

    async def archive_system_env_resource(
        self, node_id: str, kind: str, name: str, upload_url: str, upload_token: str
    ) -> dict[str, Any]:
        """Tar one resource out of the operator's HOME and POST it to upload_url."""
        return await self._rpc(
            "ArchiveNodeSystemEnvResource",
            {
                "nodeId": node_id,
                "kind": kind,
                "name": name,
                "uploadUrl": upload_url,
                "uploadToken": upload_token,
            },
        )

    async def ios_discover(self, node_id: str) -> dict[str, Any]:
        """Request an ios_host to enumerate its attached iOS devices."""
        return await self._rpc("IosDiscover", {"nodeId": node_id})

    async def ios_claim_device(
        self, node_id: str, udid: str, device_label: str, pairing_code: str
    ) -> dict[str, Any]:
        """Claim an iOS device using a one-time pairing code."""
        return await self._rpc(
            "IosClaimDevice",
            {
                "nodeId": node_id,
                "udid": udid,
                "deviceLabel": device_label,
                "pairingCode": pairing_code,
            },
        )

    async def ios_release_device(
        self, node_id: str, device_id: str, udid: str, *, delete_credential: bool = False
    ) -> dict[str, Any]:
        """Release a claimed iOS device and optionally delete its credential."""
        return await self._rpc(
            "IosReleaseDevice",
            {
                "nodeId": node_id,
                "deviceId": device_id,
                "udid": udid,
                "deleteCredential": delete_credential,
            },
        )

    async def ios_configure_device(
        self,
        node_id: str,
        device_id: str,
        udid: str,
        config_revision: int,
        *,
        transport: str = "",
        wda_bundle_id: str = "",
        xctest_config_name: str = "",
        auto_prepare: bool = False,
        renew_before_days: int = 14,
    ) -> dict[str, Any]:
        """Push WDA/transport configuration to a claimed iOS device.

        auto_prepare/renew_before_days 透传到节点：节点据此在周期 Rescan 里
        把 wdaState 从 READY 流转到 RENEWAL_DUE/EXPIRED（自动续签徽章来源）。
        节点不自主派发 renew job——派发仍在服务端扫描器（ios_auto_renew）。
        """
        return await self._rpc(
            "IosConfigureDevice",
            {
                "nodeId": node_id,
                "deviceId": device_id,
                "udid": udid,
                "configRevision": config_revision,
                "transport": transport,
                "wdaBundleId": wda_bundle_id,
                "xctestConfigName": xctest_config_name,
                "autoPrepare": bool(auto_prepare),
                "renewBeforeDays": int(renew_before_days or 0) or 14,
            },
        )

    async def get_ios_devices(self, node_id: str) -> dict[str, Any]:
        """Retrieve the cached device inventory for one ios_host node."""
        return await self._rpc("GetIosDevices", {"nodeId": node_id})

    async def ios_start_wda_job(
        self,
        node_id: str,
        job_id: str,
        udid: str,
        device_id: str,
        action: str,
        *,
        artifact: dict[str, Any] | None = None,
        signing_profile: dict[str, Any] | None = None,
        wda_bundle_id: str = "",
        xctest_config_name: str = "",
    ) -> dict[str, Any]:
        """Dispatch a WDA job (prepare/renew/reinstall) to an ios_host node."""
        return await self._rpc(
            "IosStartWdaJob",
            {
                "nodeId": node_id,
                "jobId": job_id,
                "udid": udid,
                "deviceId": device_id,
                "action": action,
                "artifact": artifact,
                "signingProfile": signing_profile,
                "wdaBundleId": wda_bundle_id,
                "xctestConfigName": xctest_config_name,
            },
        )

    async def ios_cancel_wda_job(self, node_id: str, job_id: str) -> dict[str, Any]:
        """Cancel a running WDA job."""
        return await self._rpc("IosCancelWdaJob", {"nodeId": node_id, "jobId": job_id})

    async def get_ios_wda_job_status(self, node_id: str, job_id: str) -> dict[str, Any]:
        """Query WDA job status from registry snapshot."""
        return await self._rpc("GetIosWdaJobStatus", {"nodeId": node_id, "jobId": job_id})

    # ── generic node builds (project-page「构建」tab) ─────────────────────────

    async def start_node_build(
        self,
        node_id: str,
        build_id: str,
        *,
        recipe_kind: str,
        source_url: str,
        source_ref: str = "",
        steps: list[str] | None = None,
        artifact_glob: str = "",
        upload_url: str = "",
        upload_token: str = "",
        timeout_seconds: int = 0,
        artifact_name: str = "",
        artifact_version: str = "",
    ) -> dict[str, Any]:
        """Dispatch a build job to a node that mounts the build runner.

        Returns after the node acks receipt (not completion); progress streams
        back as NodeBuildEvent/result frames the data side polls via
        get_node_build_status.
        """
        return await self._rpc(
            "StartNodeBuild",
            {
                "nodeId": node_id,
                "buildId": build_id,
                "recipeKind": recipe_kind,
                "sourceUrl": source_url,
                "sourceRef": source_ref,
                "steps": list(steps or []),
                "artifactGlob": artifact_glob,
                "uploadUrl": upload_url,
                "uploadToken": upload_token,
                "timeoutSeconds": int(timeout_seconds or 0),
                "artifactName": artifact_name,
                "artifactVersion": artifact_version,
            },
        )

    async def get_node_build_status(self, node_id: str, build_id: str) -> dict[str, Any]:
        """Query build status from registry snapshot."""
        return await self._rpc("GetNodeBuildStatus", {"nodeId": node_id, "buildId": build_id})

    async def cancel_node_build(self, node_id: str, build_id: str) -> dict[str, Any]:
        """Cancel a running build."""
        return await self._rpc("CancelNodeBuild", {"nodeId": node_id, "buildId": build_id})

    async def self_upgrade_node(self, node_id: str, *, target: dict | None = None) -> dict[str, Any]:
        return await self._rpc("SelfUpgradeNode", {"nodeId": node_id, "target": target})

    async def runtime_upgrade_node(self, node_id: str, *, target: dict | None = None) -> dict[str, Any]:
        return await self._rpc("RuntimeUpgradeNode", {"nodeId": node_id, "target": target})

    async def upgrade_node(
        self,
        node_id: str,
        *,
        runtime_target: dict | None = None,
        node_target: dict | None = None,
    ) -> dict[str, Any]:
        return await self._rpc(
            "UpgradeNode",
            {
                "nodeId": node_id,
                "runtimeTarget": runtime_target,
                "nodeTarget": node_target,
            },
        )

    # ── dispatch plane ──────────────────────────────────────────────────────
    # Protobuf JSON requires real arrays for repeated fields. Normalize at the
    # sending boundary as well as the control-server parser so mixed-version
    # deployments never emit `skills: null` or JSON-encoded strings like
    # `skills: "[]"` (JSONB/legacy rows can surface in that form).
    _SESSION_REPEATED_FIELDS = ("mcps", "skills", "plugins", "env", "volumes")

    @staticmethod
    def _normalize_session_repeated(value: Any) -> list:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError:
                return []
            return decoded if isinstance(decoded, list) else []
        return []

    async def dispatch_session(self, node_id: str | None, session: dict) -> dict[str, Any]:
        normalized = dict(session or {})
        for field in self._SESSION_REPEATED_FIELDS:
            if field in normalized:
                normalized[field] = self._normalize_session_repeated(normalized[field])
        msg: dict[str, Any] = {"session": normalized}
        if node_id:
            msg["nodeId"] = node_id
        return await self._rpc("DispatchSession", msg)

    async def start_node_session_runtime(self, session_id: str) -> dict[str, Any]:
        return await self._rpc("StartNodeSessionRuntime", {"sessionId": session_id})

    async def apply_node_session_mcps(self, session_id: str, mcps: list[dict]) -> dict[str, Any]:
        return await self._rpc("ApplyNodeSessionMCPs", {"sessionId": session_id, "mcps": mcps or []})

    async def apply_node_session_skills(self, session_id: str, skills: list[dict]) -> dict[str, Any]:
        return await self._rpc("ApplyNodeSessionSkills", {"sessionId": session_id, "skills": skills or []})

    async def apply_node_session_plugins(self, session_id: str, plugins: list[dict]) -> dict[str, Any]:
        return await self._rpc("ApplyNodeSessionPlugins", {"sessionId": session_id, "plugins": plugins or []})

    async def restart_node_session_runtime(self, session_id: str, *, fresh: bool = False) -> dict[str, Any]:
        return await self._rpc("RestartNodeSessionRuntime", {"sessionId": session_id, "fresh": fresh})

    async def configure_node_session_llm(self, session_id: str, llm: dict) -> dict[str, Any]:
        return await self._rpc("ConfigureNodeSessionLLM", {"sessionId": session_id, "llm": llm or {}})

    async def configure_node_session_mode(self, session_id: str, mode: str) -> dict[str, Any]:
        return await self._rpc("ConfigureNodeSessionMode", {"sessionId": session_id, "mode": mode or ""})

    async def delete_node_session(self, session_id: str) -> dict[str, Any]:
        return await self._rpc("DeleteNodeSession", {"sessionId": session_id})

    async def send_session_input(
        self,
        session_id: str,
        kind: str,
        text: str = "",
        *,
        model: str = "",
        mode: str = "",
        llm: dict | None = None,
        client_message_id: str = "",
        delivery_attempt: int = 1,
    ) -> dict[str, Any]:
        """Send one turn (or eof/cancel) into an interactive session's stdin.

        model/mode/llm carry the session's current config snapshot so the node
        re-prepares the provider for THIS turn. Left empty, the node fills them
        from its own session mirror — so existing callers that pass only text
        keep working and still pick up the latest config.

        ``client_message_id`` + ``delivery_attempt`` form the end-to-end
        idempotency key: the node/runtime ACK receipt with it (input_status)
        and drop same-key replays. Retries of a failed turn bump the attempt;
        transport replays reuse it. Older control planes ignore the extra
        fields.
        """
        msg: dict[str, Any] = {"sessionId": session_id, "kind": kind, "text": text}
        if model:
            msg["model"] = model
        if mode:
            msg["mode"] = mode
        if llm is not None:
            msg["llm"] = llm
        if client_message_id:
            msg["clientMessageId"] = client_message_id
            msg["deliveryAttempt"] = max(int(delivery_attempt or 1), 1)
        return await self._rpc("SendSessionInput", msg)

    async def follow_session_events(self, session_id: str):
        """Stream a session's live events as decoded JSON dicts.

        Consumes the control plane's ``FollowNodeSessionJSON`` NDJSON stream
        (one JSON object per line) and yields one dict per event so callers
        (the browser SSE bridge) never see protobuf. Each dict has a ``kind``
        of ``output`` / ``structured`` / ``result``. The generator ends after
        a ``result`` event or when the upstream stream closes.
        """
        self._require_remote()
        import aiohttp

        url = f"{self._base_url}/agentcompose.v2.NodeService/FollowNodeSessionJSON"
        headers = {
            "Content-Type": "application/json",
            "Connect-Protocol-Version": "1",
            "Authorization": f"Bearer {self._token}",
        }
        timeout = aiohttp.ClientTimeout(total=None)
        # Wrap transport failures (node-server down / refused / dropped mid-stream)
        # as NodeServerUnavailable, the same way ``_rpc`` does, so callers can
        # catch one type instead of also handling raw aiohttp exceptions. Without
        # this a connection-refused surfaced as ClientConnectorError through the
        # SSE bridge and the task page showed a bare 500 with no reason.
        try:
            async with aiohttp.ClientSession(timeout=timeout) as client:
                async with client.post(url, json={"sessionId": session_id}, headers=headers) as response:
                    if response.status >= 400:
                        detail = await response.text()
                        raise NodeServerUnavailable(
                            f"follow session {session_id} HTTP {response.status}: {detail}"
                        )
                    async for raw_line in response.content:
                        line = raw_line.strip()
                        if not line:
                            continue
                        try:
                            decoded = json.loads(line)
                        except (ValueError, TypeError):  # noqa: BLE001 - skip a torn line
                            continue
                        if not isinstance(decoded, dict) or "kind" not in decoded:
                            # An error trailer (or anything unexpected) ends the stream.
                            return
                        yield decoded
                        if decoded.get("kind") == "result":
                            return
        except NodeServerUnavailable:
            raise
        except Exception as exc:  # noqa: BLE001 - transport failure → NodeServerUnavailable
            raise NodeServerUnavailable(
                f"follow session {session_id} 不可用: {exc}"
            ) from exc

    async def host_exec(
        self,
        node_id: str,
        command: str,
        *,
        cwd: str = "",
        timeout_ms: int = 0,
        max_output_bytes: int = 0,
    ) -> dict[str, Any]:
        return await self._rpc("HostExec", {
            "nodeId": node_id, "command": command, "cwd": cwd,
            "timeoutMs": max(int(timeout_ms or 0), 0),
            "maxOutputBytes": max(int(max_output_bytes or 0), 0),
        })

    async def start_tool_run(
        self,
        node_id: str,
        run_id: str,
        binary_path: str,
        args: list[str],
        *,
        env: dict[str, str] | None = None,
        cwd: str = "",
        revision: int = 0,
    ) -> dict[str, Any]:
        """Start a long-running external CLI (tunnel manager) on a node.

        Returns once the start frame is dispatched; the node streams output +
        exit as NodeToolRunEvent frames. (Event-stream consumption / cloudflared
        stdout capture is a follow-up; frpc is deterministic and needs none.)
        """
        return await self._rpc("StartToolRun", {
            "nodeId": node_id, "runId": run_id, "binaryPath": binary_path,
            "args": list(args or []), "env": dict(env or {}), "cwd": cwd or "",
            "revision": max(int(revision or 0), 0),
        })

    async def list_active_tool_runs(self, node_id: str) -> list[dict[str, Any]]:
        data = await self._rpc("ListActiveToolRuns", {"nodeId": node_id})
        return list(data.get("toolRuns") or [])

    async def stop_tool_run(self, node_id: str, run_id: str, *, grace_ms: int = 0) -> dict[str, Any]:
        return await self._rpc("StopToolRun", {
            "nodeId": node_id, "runId": run_id,
            "graceMs": max(int(grace_ms or 0), 0),
        })

    async def follow_tool_run_events(self, node_id: str, run_id: str):
        """Stream a tool run's live events as decoded dicts.

        Consumes the control plane's ``FollowToolRunJSON`` NDJSON stream (one
        JSON object per line) and yields one dict per event:
        ``{kind: "stdout"|"stderr"|"exited", data: bytes, exit_code: int,
        error: str, revision: int, pid: int}``. ``data`` arrives base64-encoded
        (JSON cannot carry raw bytes) and is decoded back here so consumers see
        the same bytes the node produced. The generator ends after an
        ``exited`` event or when the upstream stream closes. Used by the tunnel
        manager to capture cloudflared's trycloudflare domain from the
        client's stdout.
        """
        self._require_remote()
        import base64

        import aiohttp

        url = f"{self._base_url}/agentcompose.v2.NodeService/FollowToolRunJSON"
        headers = {
            "Content-Type": "application/json",
            "Connect-Protocol-Version": "1",
            "Authorization": f"Bearer {self._token}",
        }
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(timeout=timeout) as client:
            async with client.post(url, json={"nodeId": node_id, "runId": run_id}, headers=headers) as response:
                if response.status >= 400:
                    detail = await response.text()
                    raise NodeServerUnavailable(
                        f"follow tool run {run_id} HTTP {response.status}: {detail}"
                    )
                async for raw_line in response.content:
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        decoded = json.loads(line)
                    except (ValueError, TypeError):  # noqa: BLE001 - skip a torn line
                        continue
                    if not isinstance(decoded, dict) or "kind" not in decoded:
                        # An error trailer (or anything unexpected) ends the stream.
                        return
                    decoded = dict(decoded)
                    decoded["data"] = base64.b64decode(decoded.get("data") or "")
                    yield decoded
                    if decoded.get("kind") == "exited":
                        return

    async def upload_file(
        self,
        node_id: str,
        path: str,
        data: bytes,
        *,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        """Upload a whole file to a node host by streaming ≤1 MiB chunks.

        Each chunk is a ``HostFileUpload`` call; the node appends to a temp file
        and, on the ``final`` chunk, verifies size + sha256 and atomically
        renames into place. We drive chunk ordering (offset advances by the
        acked ``bytesWritten``) and stop on the first non-ok chunk so a failure
        surfaces immediately with the node's error text.
        """
        import base64
        import hashlib
        import uuid

        total = len(data)
        if total > _MAX_UPLOAD_TOTAL_BYTES:
            raise RPCError(
                "invalid_argument",
                f"文件超过 {_MAX_UPLOAD_TOTAL_BYTES} 字节上限",
            )
        upload_id = uuid.uuid4().hex
        sha256 = hashlib.sha256(data).hexdigest()

        offset = 0
        last: dict[str, Any] = {}
        # An empty file still needs one final chunk to create + rename it.
        while True:
            chunk = data[offset : offset + _MAX_UPLOAD_CHUNK_BYTES]
            is_final = offset + len(chunk) >= total
            last = await self._rpc("HostFileUpload", {
                "nodeId": node_id,
                "uploadId": upload_id,
                "path": path,
                "offset": offset,
                "data": base64.b64encode(chunk).decode("ascii"),
                "totalSize": total,
                "sha256": sha256 if is_final else "",
                "overwrite": bool(overwrite),
                "final": is_final,
            })
            if not last.get("ok"):
                # The node aborts and drops the temp file on any failure; stop
                # rather than sending further chunks against a closed session.
                break
            offset += len(chunk)
            if is_final:
                break
        return {
            "ok": bool(last.get("ok")),
            "bytes_written": int(last.get("bytesWritten") or 0),
            "path": last.get("path") or path,
            "error": last.get("error") or "",
        }


_node_client = NodeClient()


def get_local_node_client() -> NodeClient:
    """Return the shared remote node client (name kept for caller compatibility)."""
    return _node_client
