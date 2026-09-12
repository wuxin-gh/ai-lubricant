#!/usr/bin/env bash
# 探测本机出网到 Apple GSA 登录端点是否被拒（503）。
#
# 背景：gsa.apple.com/grandslam 是 Apple 设备专用认证通道，对「非 Apple 认可
# 的网络 IP」（多数数据中心/机房段）直接回 HTTP 503（不是友好错误码，是 Apple
# 边缘节点的 IP 层拒收）。住宅宽带/移动网络 IP 通常放行。
#
# Apple ID 登录（签名配置 / 未来的 Xcode 自动下载）走这个端点，所以想让某台
# 机器当「登录出口」（直连，或作为节点隧道的出口节点），先在那台机器上跑本
# 脚本，确认它的公网 IP 不被 Apple 拒。
#
# 用法（在目标机器上，比如 Mac 执行节点的宿主机）：
#     bash probe_apple_gsa.sh
#
# 判读：
#   * 200 / 400 / 401 / 未 prolog 的 plist  → 出网 IP 被 Apple 放行（可用作登录出口）
#   * 503 (body 里 Server: Apple)          → 该 IP 被 Apple 拒收，换出口（家宽/移动网络）
#   * 连接超时 / TLS 失败                    → 网络不通或证书问题（另一类问题，非 IP 拒收）
#
# 只读探测：一次 POST 到 GsService2（不带真实凭据，Apple 只会回「需要认证」或
# 直接 503），不发送任何 Apple ID / 密码。
set -euo pipefail

GSA_URL="https://gsa.apple.com/grandslam/GsService2"
UA="akd/1.0 CFNetwork/978.0.7 Darwin/18.7.0"

echo "== Apple GSA 出网探测 =="
echo

# 1. 本机公网 IP（判读 503 时对照——是不是机房段）。
echo "-- 本机公网出口 IP --"
PUBIP="$(curl -fsS --max-time 10 https://api.ipify.org 2>/dev/null || true)"
[ -z "$PUBIP" ] && PUBIP="$(curl -fsS --max-time 10 https://ifconfig.me 2>/dev/null || true)"
[ -z "$PUBIP" ] && PUBIP="(取不到)"
echo "   $PUBIP"
echo

# 2. DNS 解析（gsa.apple.com 走 Akamai，解析结果因地区而异）。
echo "-- gsa.apple.com 解析 --"
if command -v nslookup >/dev/null 2>&1; then
  nslookup gsa.apple.com 2>/dev/null | awk '/Address/ && !/#/ {print "   " $0}' | head -4
fi
echo

# 3. 核心：POST 到 GSA init。用最小 plist body，只看 HTTP 状态码。
echo "-- POST $GSA_URL --"
BODY='<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Header</key><dict><key>Version</key><string>1.0.1</string></dict>
<key>Request</key><dict><key>o</key><string>init</string><key>u</key><string>probe@example.com</string></dict>
</dict></plist>'

HTTP_CODE="$(curl -s -o /tmp/gsa_probe_body.txt -w '%{http_code}' \
  --max-time 25 \
  -X POST "$GSA_URL" \
  -H "Content-Type: text/x-xml-plist" \
  -H "Accept: */*" \
  -H "User-Agent: $UA" \
  --data "$BODY" 2>/dev/null || true)"
[ -z "$HTTP_CODE" ] && HTTP_CODE="000"

echo "   HTTP 状态码: $HTTP_CODE"
echo "   响应前 200 字节:"
head -c 200 /tmp/gsa_probe_body.txt 2>/dev/null | sed 's/^/     /'
echo
echo

# 4. 判读
echo "== 结论 =="
case "$HTTP_CODE" in
  503)
    echo "   ✗ 出网 IP（$PUBIP）被 Apple GSA 拒收（503）。"
    echo "     这台机器不能作为 Apple ID 登录出口。换一个住宅宽带/移动网络出口"
    echo "     （家用路由器、手机热点，或一个出口在这类网络的 network 代理）。"
    ;;
  200|400|401|472)
    echo "   ✓ 出网 IP（$PUBIP）被 Apple 放行（HTTP $HTTP_CODE = 到达了认证逻辑）。"
    echo "     这台机器可作为 Apple ID 登录出口（直连，或作为节点隧道的出口节点）。"
    ;;
  000)
    echo "   ! 连不上 gsa.apple.com（超时/TLS 失败）。先解决网络连通，再谈 IP 是否被拒。"
    ;;
  *)
    echo "   ? 非预期状态码 $HTTP_CODE。把上面「响应前 200 字节」贴回去分析。"
    ;;
esac
rm -f /tmp/gsa_probe_body.txt 2>/dev/null || true
