# 局域网发现（LAN Discovery）

手机 App 登录页「扫描」按钮的底层机制：手机向局域网发一个 UDP 广播质询，同网段
的 Ai Lubricant 服务器应答自己的名字 / 版本 / HTTP 端口，手机把「应答数据报的
源 IP + 通告端口」拼成登录地址自动填入表单。

服务端实现：`lan_discovery.py`（main.py lifespan 挂载，三种部署形态统一生效）。
手机端实现：`mobile/plugins/withLanDiscovery.js`（config plugin 注入 Kotlin /
Swift 原生模块）+ `mobile/src/native/lanDiscover.ts`（TS 包装）+ 登录页扫描入口。

## 协议

```
手机  --UDP-->  255.255.255.255:58160   payload: "AILUBRICANT_DISCOVER_V1"
服务器 --UDP--> 手机源地址:源端口        payload: {"app":"ai-lubricant","name":"Ai Lubricant","version":"<APP_VERSION>","http_port":8001}
```

- 端口默认 **58160**（`LAN_DISCOVERY_PORT` 改服务端；手机端写死同一默认值，改动需与移动端同步重新打包）。
- 手机 Android 端同时向本网 **directed broadcast**（如 `192.168.1.255`）多发一份——部分路由器对 limited broadcast 处理不一致，双保险提升命中率。
- 响应按源 IP 限速（500ms/次），放大系数仅 ~6x，无敏感信息。
- 服务器**无需自知本机 IP**：手机取应答包的源地址。docker 端口映射下响应源地址也会被 NAT 改写为宿主 IP，因此 compose 里要通告**宿主映射端口**（已自动配置）。

## 配置（.env）

| 变量 | 默认 | 说明 |
|---|---|---|
| `LAN_DISCOVERY_ENABLED` | `true` | 总开关；`false` 完全关闭 |
| `LAN_DISCOVERY_PORT` | `58160` | UDP 监听端口 |
| `LAN_DISCOVERY_HTTP_PORT` | 自动 | 响应里通告的 HTTP 端口；优先级：显式值 → `DESKTOP_MAIN_PORT` → 8001。docker compose 已自动设为 `AI_LUBRICANT_HOST_PORT` |
| `LAN_DISCOVERY_SERVER_NAME` | `Ai Lubricant` | 展示名，多台部署可区分机器 |
| `LAN_DISCOVERY_ALLOW_LOOPBACK` | `false` | HTTP 仅监听 loopback 时发现被抑制；`true` 强制应答（仅本机联调） |

## 部署形态与可靠性

| 形态 | 可用性 | 说明 |
|---|---|---|
| 裸跑 `python main.py` | ✅ 可靠 | 默认绑 `0.0.0.0:8001`（dual-stack） |
| desktop exe + `DESKTOP_MAIN_HOST=0.0.0.0` | ✅ 可靠 | 默认 `127.0.0.1` 时**自动抑制响应**并打启动 warning |
| docker（Linux 宿主） | ⚠️ 不稳定 | published UDP 端口下广播穿透依内核/网络栈而异（SSDP/mDNS-in-docker 同类问题）；要可靠请 `network_mode: host` |
| docker（Windows Docker Desktop） | ❌ 不可用 | 容器在 WSL2/Hyper-V VM 内，广播不可能穿透 VM 边界 |

## 已知限制

- **Windows 防火墙首启弹窗**：服务端第一次绑 UDP 58160 会触发 Defender 提示，点「取消」则入向广播被丢弃、发现失效（服务本身不受影响）。
- **路由器 AP 隔离 / 客户端隔离**：会丢弃无线客户端的广播转发，手机与有线服务器同网也发现不到——需在路由器关闭隔离。
- **iOS 本地网络权限**（iOS 14+）：首次扫描触发「查找并连接本地网络上的设备」弹窗。权限被拒时 `sendto` 仍返回成功但包被静默丢弃，**App 无法可靠区分「被拒」与「无服务器」**，只能给引导文案（登录页已实现：零结果时 iOS 自动补扫一轮，仍为零则提示去 设置 > 隐私与安全 > 本地网络 开启）。iOS 模拟器不触发该权限，真机才能验证。
- **手机多网络**：若手机同时连 Wi-Fi + VPN/蜂窝且默认路由不走局域网，广播发不进目标网段。Android 原生模块已按接口枚举 directed broadcast 缓解。
- 发现 ≠ 登录可用：响应不含任何用户/凭据信息；`AI_LUBRICANT_COMPAT_ENABLED=false` 时手机虽能发现服务器，但登录路由不存在（`/api/v1/users/password-login` 挂在 user_platform 下）。

## 手机端改动清单

- `mobile/plugins/withLanDiscovery.js`：config plugin。Android 写 `LanDiscoverModule.kt`（DatagramSocket 广播 + 轮询收包）并 patch `MainApplication.kt`；iOS 写 `LanDiscover.swift`（BSD socket `SO_BROADCAST`）+ `LanDiscoverBridge.m` 注入 Xcode 工程；`NSLocalNetworkUsageDescription` 加进 Info.plist。
- **不新增 Android 权限**：只发送广播 + 接收单播回复，`INTERNET` 已够（`CHANGE_WIFI_MULTICAST_STATE` 只影响接收组播）。
- `mobile/src/native/lanDiscover.ts`：原生只透传 `{source_ip, raw_payload}`，TS 层统一 JSON 解析、校验 `payload.app === 'ai-lubricant'`（防同端口其他流量）、按 IP 去重、拼登录 URL。
- 原生目录（`android/` `ios/`）是 prebuild 产物且被 .gitignore 忽略：`expo prebuild --clean` 会重建，**一切原生改动必须经 plugin**（幂等 `writeFileIfChanged`）。

## 验证

```bash
# 服务端单测（零依赖，无需 PG）
pytest tests/test_lan_discovery.py

# 手动广播模拟手机（在同网段另一台机器）
python -c "
import socket, json
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.settimeout(2); s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
s.sendto(b'AILUBRICANT_DISCOVER_V1', ('255.255.255.255', 58160))
print(s.recvfrom(2048))
"
# 预期：({"app":"ai-lubricant",...,"http_port":8001}, ('<服务器LAN IP>', 58160))
```
