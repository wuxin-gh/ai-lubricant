"""跨实例同步 handler 注册：把 pubsub 事件落到本实例内存。

设计（见方案 Part E/F）：
- 每类事件的 handler 从 DB/Redis 重读**单条**，更新本实例内存（消息不带全量数据）。
- 60s 对账循环强制刷渠道配置内存快照，兜底 pubsub 丢消息。
- 由 main.py lifespan 在"先订阅、再全量加载"之前调用 register_all()。
"""

from __future__ import annotations

from loguru import logger

import runtime_sync


async def _on_channel_event(name: str | None, payload: dict) -> None:
    """渠道基础配置变更：重读该渠道行 → 刷新内存快照 + 更新 pool.channel。"""
    if not name:
        return
    try:
        import config
        from rate_limiter import ModelClientPool

        try:
            cfg = await config.CONFIG_STORE.read_provider_async(name)  # 刷新 _providers_cache[name]
        except FileNotFoundError:
            # 渠道被删：read_provider_async 对不存在的渠道抛 FileNotFoundError，借此清内存快照 + 移除运行池
            store = config.CONFIG_STORE
            if getattr(store, "_providers_cache", None) is not None:
                store._providers_cache.pop(name, None)
            ModelClientPool._provider_pools.pop(name, None)
            return
        pool = ModelClientPool.get_provider_pool(name)
        if pool is not None and pool.channel is not None:
            provider_extra = {k: v for k, v in cfg.items() if k not in ("accounts", "rate_limit")}
            provider_extra.update({"provider_name": name, "rate_limit": cfg.get("rate_limit", {})})
            pool.channel.apply(provider_extra)
    except Exception as exc:
        logger.warning(f"[runtime_handlers] channel 事件 {name} 处理失败: {exc}")


async def _on_account_event(name: str | None, payload: dict) -> None:
    """单账号变更：重读该渠道配置 → 热加载/移除该账号入池。

    name 形如 "provider:username"；extra 也带 provider/username。
    """
    extra = payload.get("extra") or {}
    provider = extra.get("provider")
    username = extra.get("username")
    if not provider and name and ":" in str(name):
        provider, username = str(name).split(":", 1)
    if not provider or not username:
        return
    try:
        import config
        import admin
        from rate_limiter import ModelClientPool

        # 账号的 proxy_id 要靠 main 配置里的代理池才能解析成 proxy_config_id（出站路由
        # 真相字段）。read_provider_async 只刷渠道快照，不碰 _main_cache——本机若还没
        # 收到 EVENT_PROXY，新建的代理条目查不到就会把账号误解析成直连。故先刷 main。
        try:
            await config.CONFIG_STORE.read_main_async()
        except Exception as exc:  # noqa: BLE001
            # 尽力而为：读失败仍用现有 main 缓存继续。阻断整条账号更新（含凭据变更）
            # 比短暂的代理解析漂移代价更大，且 EVENT_PROXY / 60s 对账会纠正。
            logger.warning(f"[runtime_handlers] account 事件 {name} 刷新 main 配置失败: {exc}")

        try:
            cfg = await config.CONFIG_STORE.read_provider_async(provider)
        except FileNotFoundError:
            return
        account = next(
            (a for a in cfg.get("accounts", []) if a.get("username") == username), None
        )
        if account is None:
            # 该账号已被删：从运行池移除。
            admin._remove_account_from_pool(provider, username)
            return
        if ModelClientPool.get_provider_pool(provider):
            admin._reload_account_into_pool(
                provider, account, admin._provider_rpm(cfg), admin._provider_extra(provider, cfg)
            )
    except Exception as exc:
        logger.warning(f"[runtime_handlers] account 事件 {name} 处理失败: {exc}")


async def _on_model_event(name: str | None, payload: dict) -> None:
    """渠道模型行变更：重灌该渠道 Channel.models + 置脏 _models。"""
    if not name:
        return
    try:
        from db import PostgresClient
        from rate_limiter import ModelClientPool

        rows = await PostgresClient.list_provider_models(name)
        ModelClientPool.reload_channel_models(name, rows)
    except Exception as exc:
        logger.warning(f"[runtime_handlers] model 事件 {name} 处理失败: {exc}")


async def _reload_catalog_and_mark_models_dirty(reason: str) -> bool:
    """从 DB 重载目录；仅快照变化时置脏 /v1/models。"""
    import model_catalog
    from rate_limiter import ModelClientPool

    changed = await model_catalog.reload_from_db(reason=reason)
    if changed:
        ModelClientPool._mark_models_dirty()
    return changed


async def _on_metadata_event(name: str | None, payload: dict) -> None:
    """模型元数据变更：重载本实例目录，变化时置脏 /v1/models。"""
    try:
        await _reload_catalog_and_mark_models_dirty(f"sync:metadata:{name or '__all__'}")
    except Exception as exc:
        logger.warning(f"[runtime_handlers] metadata 事件 {name} 处理失败: {exc}")


async def _on_group_event(name: str | None, payload: dict) -> None:
    """模型组变更：重载本实例目录，变化时置脏 /v1/models。"""
    try:
        await _reload_catalog_and_mark_models_dirty(f"sync:group:{name or '__all__'}")
    except Exception as exc:
        logger.warning(f"[runtime_handlers] group 事件 {name} 处理失败: {exc}")


async def _on_apikey_event(name: str | None, payload: dict) -> None:
    """API Key 事件：两类语义，按 extra 区分。

    - ``extra.snapshot``：Key 增删改。鉴权走内存快照，本实例必须重读 DB 才能看到
      其它进程的写入，否则新建的 Key 在这里一直 401、被删的 Key 一直可用。
      重灌时不再广播，避免与发起方互相激发。
    - ``extra.blocked_field``：限流 block 状态变更。广播只作为触发信号；
      blocked_until 的权威值在 Redis 计数窗口的 TTL 里。不直接采用
      payload.extra.until，避免广播体失真时内存与权威存储脱离。
    """
    subject = name
    extra = payload.get("extra") or {}
    if extra.get("snapshot"):
        try:
            import config

            await config.Config.refresh_api_keys_cache(broadcast=False)
        except Exception as exc:
            logger.warning(f"[runtime_handlers] apikey 快照事件处理失败: {exc}")
        return
    field = extra.get("blocked_field")
    if not subject or not field:
        return
    try:
        from limits.backend import RedisLimitBackend
        from rate_limiter import RateLimiter

        until = await RedisLimitBackend.get_api_key_block_until(subject, field)
        if until is None:
            # Redis 里已无该 block（过期或被清）：与权威对齐，清掉本实例内存 block。
            RateLimiter.clear_block_event(subject, field)
            return
        RateLimiter.apply_block_event(subject, field, until)
    except Exception as exc:
        logger.warning(f"[runtime_handlers] apikey 事件 {subject} 处理失败: {exc}")


async def _on_cooldown_event(name: str | None, payload: dict) -> None:
    """账号/模型级冻结解冻事件：按 name 回查 Redis 权威冷却，落到本实例内存镜像。

    name 形如 ``provider:username``（账号级）或 ``model-tpm:model_id``（模型 TPM）。
    广播只作为触发信号；冷却到期时间权威在 Redis（TTL），不采用 payload.extra.until。
    """
    extra = payload.get("extra") or {}
    if extra.get("scope") == "model_tpm":
        model_id = str(extra.get("model_id") or "")
        if not model_id:
            return
        try:
            from limits.backend import RedisLimitBackend
            from rate_limiter import ModelClientPool

            until = await RedisLimitBackend.get_model_tpm_cooldown_until(model_id)
            if until is None:
                ModelClientPool.clear_model_tpm_cooldown_event(model_id)
                return
            ModelClientPool.apply_model_tpm_cooldown_event(model_id, until)
        except Exception as exc:
            logger.warning(f"[runtime_handlers] model_tpm cooldown 事件处理失败: {exc}")
        return

    provider = extra.get("provider")
    username = extra.get("username")
    if not provider or not username:
        if name and ":" in str(name):
            provider, username = str(name).split(":", 1)
    if not provider or not username:
        return
    model_id = extra.get("model_id")
    cleared = bool(extra.get("cleared"))
    try:
        from limits.backend import RedisLimitBackend
        from rate_limiter import ModelClientPool

        if model_id:
            # 模型级：显式解冻事件（cleared）才清镜像；否则以 Redis 为权威——读到就设镜像，
            # 读不到（SET 尚未落地/竞态、Redis 抖动）不清，避免误清导致月底冻结被选号重新选中。
            if cleared:
                ModelClientPool.clear_cooldown_event(provider, username, model_id)
                return
            info = await RedisLimitBackend.get_account_model_cooldown_info(provider, username, model_id)
            if info is None:
                return
            # permanent → until=None；临时 → until 为到期点。
            until = None if info.get("permanent") else float(info.get("until") or 0)
            ModelClientPool.apply_cooldown_event(
                provider, username, model_id, until, str(info.get("reason") or "")
            )
            return
        info = await RedisLimitBackend.get_account_cooldown_info(provider, username)
        if info is None:
            ModelClientPool.clear_cooldown_event(provider, username, None)
            return
        until = None if info.get("permanent") else float(info.get("until") or 0)
        ModelClientPool.apply_cooldown_event(
            provider, username, None, until, str(info.get("reason") or "")
        )
    except Exception as exc:
        logger.warning(f"[runtime_handlers] cooldown 事件 {name} 处理失败: {exc}")


async def _refresh_proxy_runtime() -> None:
    """重读主配置代理列表 → 原地刷新运行中账号代理（不重建账号池）。

    代理池存于 main 配置；接收方必须先刷新本实例 _main_cache，否则
    _read_proxies 读到的是旧缓存。事件/对账均走此路径，幂等无害。
    """
    try:
        import config
        import admin
        await config.CONFIG_STORE.read_main_async()
        admin._hot_update_proxy_pool()
        # 同步共享 ProxyManager：provider 出站按 proxy_config_id 路由，其配置源就是
        # 代理池。update_proxy_config 内容变自动递增 version、触发旧实例重建，故改
        # 代理 url/密码/模式在这里即时生效（事件 + 60s 对账都过此路径，幂等）。
        # 已删除的代理条目也会在此被剪掉并关闭残留实例。
        try:
            from proxy_utils import sync_proxy_manager_configs
            main_cfg = config.Config._load()
            await sync_proxy_manager_configs(main_cfg.get("proxies", []) or [])
        except Exception as exc:
            logger.warning(f"[runtime_handlers] ProxyManager 代理配置同步失败: {exc}")
    except Exception as exc:
        logger.warning(f"[runtime_handlers] 代理池运行态刷新失败: {exc}")


async def _on_proxy_event(name: str | None, payload: dict) -> None:
    """代理池变更：重读主配置 + 原地刷新运行中账号代理。"""
    await _refresh_proxy_runtime()


async def _on_main_config_event(name: str | None, payload: dict) -> None:
    """主配置变更：从权威存储重读并替换本实例内存缓存。

    output_interception_rules 等请求热路径配置均由 Config._load() 读取 _main_cache；
    编译后的拦截正则使用规则内容作为缓存键，因此主配置刷新后下一请求会自然重编译。
    """
    try:
        import config

        await config.CONFIG_STORE.read_main_async()
    except Exception as exc:
        logger.warning(f"[runtime_handlers] 主配置事件处理失败: {exc}")


async def _on_header_templates_event(name: str | None, payload: dict) -> None:
    """全局 Header 模板变更：从 DB 重读列表并替换本机同步缓存。"""
    try:
        from db import PostgresClient
        from rate_limiter import ModelClientPool

        templates = await PostgresClient.get_header_templates()
        ModelClientPool.set_header_template_cache(templates)
    except Exception as exc:
        logger.warning(f"[runtime_handlers] Header 模板事件处理失败: {exc}")


async def _on_model_rule_templates_event(name: str | None, payload: dict) -> None:
    """全局模型规则模版变更：从 DB 重读列表并替换本机模版缓存。

    模版引用是活引用——改模版不改任何渠道配置，靠这里回灌让下一轮模型同步生效。
    """
    try:
        import channel
        from db import PostgresClient

        templates = await PostgresClient.get_model_rule_templates()
        channel.set_model_rule_template_cache(templates)
    except Exception as exc:
        logger.warning(f"[runtime_handlers] 模型规则模版事件处理失败: {exc}")


async def _reconcile_catalog() -> None:
    """60s 对账：重载模型组/元数据目录，兜底 pubsub 丢消息。"""
    try:
        await _reload_catalog_and_mark_models_dirty("reconcile:catalog")
    except Exception as exc:
        logger.warning(f"[runtime_handlers] 模型目录对账失败: {exc}")


async def _reconcile_providers() -> None:
    """60s 对账：强制从 DB 重灌渠道配置内存快照 + 账号运行态，兜底 pubsub 丢消息。

    reload_from_db 只刷 _providers_cache（快照层），运行池里的 AccountClient 对象
    （concurrent/balance/disabled/is_frozen/credentials 等）不会被它纠正。跨实例丢
    EVENT_ACCOUNT 时，本机快照已含新值但运行对象仍持旧值——选路读的是运行对象。
    所以刷完快照再遍历渠道账号调 _reload_account_into_pool，就地对齐运行对象字段
    （保留 _in_flight/冷却/统计/auth 缓存）。就地更新已确保每 60s 跑一次无副作用。

    遍历 DB 账号只能修正「存在但字段漂移」，修不了「DB 已删、运行池还留着」——丢一条
    EVENT_ACCOUNT 删除事件的实例会无限期拿已删账号继续发请求。故本函数还要反向对账：
    DB 账号名集合之外的运行对象一律移出池。
    """
    try:
        import config
        await config.Config.reload_from_db()
    except Exception as exc:
        logger.warning(f"[runtime_handlers] 渠道配置对账失败: {exc}")
    # 账号运行态对账：兜底 EVENT_ACCOUNT pubsub 丢消息导致的运行对象漂移。
    try:
        import admin
        from rate_limiter import ModelClientPool
        providers = await config.Config.get_providers()
        # 先刷新 main 配置并只规范化一次代理池，再复用于全部账号；否则每个账号各自
        # _read_proxies 会产生 O(账号数 × 代理数) 的重复解析，且可能读到陈旧 main 缓存。
        try:
            await config.CONFIG_STORE.read_main_async()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[runtime_handlers] 账号对账刷新 main 配置失败: {exc}")
        proxies = admin._read_proxies()
        for provider_name, cfg in (providers or {}).items():
            if not isinstance(cfg, dict):
                continue
            pool = ModelClientPool.get_provider_pool(provider_name)
            if pool is None:
                continue  # 渠道未入池（禁用/未初始化），跳过；入池由 channel 事件负责
            rpm = admin._provider_rpm(cfg)
            extra = admin._provider_extra(provider_name, cfg)
            accounts = cfg.get("accounts", [])
            for account in accounts:
                try:
                    admin._reload_account_into_pool(
                        provider_name, account, rpm, extra, proxies=proxies
                    )
                except Exception as exc:
                    logger.warning(f"[runtime_handlers] 账号对账 provider={provider_name} account={account.get('username')} 失败: {exc}")
            # 反向对账：清理 DB 已不存在的运行对象（丢删除事件的兜底）。
            # 空账号列表也照常执行——渠道账号被清空同样要清池；但只在快照确实读到该渠道
            # 配置时才走（外层已按 providers 遍历，cfg 来自 DB 快照，不会是缺失态）。
            db_usernames = {a.get("username") for a in accounts if isinstance(a, dict)}
            stale = [c.username for c in pool.clients if c.username not in db_usernames]
            if stale:
                # 移除前用一次直连 PG 读复核。上面的快照取自本轮开头的 reload_from_db，
                # 而 add_account 是「先写 DB 再入池」：若某账号的写入刚好在快照之后、入池
                # 又在这里之前，它就会被误判成已删而遭误删（虽然下一轮对账会补回，但中间
                # 60s 该账号不可用）。复核只在确实要删时发生，无 stale 的常态零额外开销。
                # 不走 CONFIG_STORE.read_provider_async——那层有 Redis 缓存，复核要读权威源。
                try:
                    from db import PostgresClient
                    fresh = await PostgresClient.get_provider_config(provider_name)
                except Exception as exc:  # noqa: BLE001
                    fresh, exc_hint = None, exc
                    logger.warning(f"[runtime_handlers] 账号对账复核 provider={provider_name} 读 DB 失败，跳过本轮移除: {exc_hint}")
                    stale = []
                else:
                    if fresh is None:
                        # 渠道行已不存在：整渠道退场由 channel 事件负责，这里不越权清池。
                        stale = []
                    else:
                        confirmed = {
                            a.get("username")
                            for a in (fresh.get("accounts") or [])
                            if isinstance(a, dict)
                        }
                        stale = [u for u in stale if u not in confirmed]
            for username in stale:
                try:
                    admin._remove_account_from_pool(provider_name, username)
                    logger.info(f"[runtime_handlers] 账号对账移除已删账号 provider={provider_name} account={username}")
                except Exception as exc:
                    logger.warning(f"[runtime_handlers] 账号对账移除 provider={provider_name} account={username} 失败: {exc}")
    except Exception as exc:
        logger.warning(f"[runtime_handlers] 账号运行态对账失败: {exc}")
    # 代理池对账：兜底代理池 pubsub 丢消息/Redis 不可用/订阅重连期间的遗漏。
    await _refresh_proxy_runtime()


async def _reconcile_main_config() -> None:
    """60s 对账：强制重读主配置，兜底 PubSub 丢消息或 Redis 不可用。"""
    await _on_main_config_event("__default__", {})


async def _reconcile_header_templates() -> None:
    """60s 对账：重读 Header 模板列表，兜底 PubSub 丢消息。"""
    await _on_header_templates_event("__default__", {})


async def _reconcile_model_rule_templates() -> None:
    """60s 对账：重读模型规则模版列表，兜底 PubSub 丢消息。"""
    await _on_model_rule_templates_event("__default__", {})


async def _reconcile_api_keys() -> None:
    """60s 对账：重灌 API Key 内存快照，兜底 pubsub 丢消息。

    鉴权直接读这份快照，漏一条事件的后果是「新建 Key 一直 401 / 已删 Key 一直可用」，
    必须有对账兜底。另外快照事件全部共用同一个 name 哨兵，_dispatch 的按 (type,name)
    版本去重在多实例时钟偏斜下会丢掉本该处理的一条——这里同样是那种情况的兜底。
    """
    try:
        import config

        await config.Config.refresh_api_keys_cache(broadcast=False)
    except Exception as exc:
        logger.warning(f"[runtime_handlers] API Key 快照对账失败: {exc}")


async def _on_channel_catalog_event(_name: str | None, _payload: dict) -> None:
    from user_platform.marketplace.channel_catalog import reload_from_db
    await reload_from_db()


async def _reconcile_channel_catalog() -> None:
    await _on_channel_catalog_event("__all__", {})


async def _on_node_release_event(_name: str | None, _payload: dict) -> None:
    """节点发行版本广播：从 PG 重载快照，让其他实例无需等 sync_loop 即时生效。"""
    try:
        import node_release_catalog
        await node_release_catalog.reload_from_db()
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[runtime_handlers] 节点发行快照重载失败: {exc}")


async def _reconcile_node_release() -> None:
    await _on_node_release_event("__all__", {})


async def _on_mobile_release_event(_name: str | None, _payload: dict) -> None:
    """移动端发行版本广播：从 PG 重载快照（与节点同构）。"""
    try:
        import mobile_release_catalog
        await mobile_release_catalog.reload_from_db()
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[runtime_handlers] 移动端发行快照重载失败: {exc}")


async def _reconcile_mobile_release() -> None:
    await _on_mobile_release_event("__all__", {})


async def _on_device_control_release_event(_name: str | None, _payload: dict) -> None:
    """设备控制 App 发行版本广播：从 PG 重载快照（与节点/移动端同构）。"""
    try:
        import device_control_release_catalog
        await device_control_release_catalog.reload_from_db()
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[runtime_handlers] 设备控制 App 发行快照重载失败: {exc}")


async def _reconcile_device_control_release() -> None:
    await _on_device_control_release_event("__all__", {})


async def _on_tokenizer_vocab_event(repo: str | None, _payload: dict) -> None:
    """词表更新广播：失效本实例缓存并从 PG 重载该 repo，保证估算口径一致。"""
    if not repo:
        return
    try:
        import usage_utils
        usage_utils.invalidate_hf_tokenizer_cache(repo)
        await usage_utils.load_hf_tokenizer_from_pg(repo)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[runtime_handlers] 词表重载失败 repo={repo}: {exc}")


async def _reconcile_tokenizer_vocabs() -> None:
    """60s 对账：PG 词表更新晚于本实例内存 seen 时，重载该 repo。

    兜底 pubsub 丢消息（Redis 不可用 / 订阅重连空窗）。词表更新很罕见，
    每轮只比对 updated_at 版本号，不读 content，开销可忽略。
    """
    try:
        import usage_utils
        from db import PostgresClient
        if not PostgresClient.pool:
            return
        versions = await PostgresClient.get_tokenizer_vocab_versions()
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"[runtime_handlers] 词表对账读 PG 失败: {exc}")
        return
    for repo, updated_at in versions:
        seen = (usage_utils._HF_VOCAB_META.get(repo) or {}).get("updated_at") or 0
        if updated_at and updated_at > seen:
            try:
                usage_utils.invalidate_hf_tokenizer_cache(repo)
                await usage_utils.load_hf_tokenizer_from_pg(repo)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"[runtime_handlers] 词表对账重载失败 repo={repo}: {exc}")


def register_all() -> None:
    """注册全部事件 handler + 对账回调。lifespan 在订阅前调用。"""
    registrations = (
        (runtime_sync.EVENT_CHANNEL, _on_channel_event),
        (runtime_sync.EVENT_ACCOUNT, _on_account_event),
        (runtime_sync.EVENT_MODEL, _on_model_event),
        (runtime_sync.EVENT_METADATA, _on_metadata_event),
        (runtime_sync.EVENT_GROUP, _on_group_event),
        (runtime_sync.EVENT_APIKEY, _on_apikey_event),
        (runtime_sync.EVENT_COOLDOWN, _on_cooldown_event),
        (runtime_sync.EVENT_PROXY, _on_proxy_event),
        (runtime_sync.EVENT_MAIN_CONFIG, _on_main_config_event),
        (runtime_sync.EVENT_HEADER_TEMPLATES, _on_header_templates_event),
        (runtime_sync.EVENT_MODEL_RULE_TEMPLATES, _on_model_rule_templates_event),
        (runtime_sync.EVENT_CHANNEL_CATALOG, _on_channel_catalog_event),
        (runtime_sync.EVENT_NODE_RELEASE, _on_node_release_event),
        (runtime_sync.EVENT_MOBILE_RELEASE, _on_mobile_release_event),
        (runtime_sync.EVENT_DEVICE_CONTROL_RELEASE, _on_device_control_release_event),
        (runtime_sync.EVENT_TOKENIZER_VOCAB, _on_tokenizer_vocab_event),
    )
    for event_type, handler in registrations:
        handlers = runtime_sync._handlers.get(event_type, [])
        if handler not in handlers:
            runtime_sync.register_handler(event_type, handler)
    for reconciler in (_reconcile_catalog, _reconcile_providers, _reconcile_api_keys, _reconcile_main_config, _reconcile_header_templates, _reconcile_model_rule_templates, _reconcile_channel_catalog, _reconcile_node_release, _reconcile_mobile_release, _reconcile_device_control_release, _reconcile_tokenizer_vocabs):
        if reconciler not in runtime_sync._reconcilers:
            runtime_sync.register_reconciler(reconciler)
    logger.info("[runtime_handlers] 已注册全部同步 handler 与对账回调")
