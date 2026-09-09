# 引擎包标记。Apple ID 免费签名引擎见各模块 docstring；统一约定：
#  1. 纯函数 + 显式参数——所有可变状态（session token / cert / profile 缓存）
#     由调用方持有（routes_ios 落 PG secret_data），引擎自身只缓存机器级
#     anisette 身份（anisette.bin，重置会触发 Apple 反滥用）。
#  2. 同步 requests 网络——路由层必须 asyncio.to_thread 包装。
#  3. Apple ID 密码绝不落库/落日志：只进函数参数，用完即弃。
