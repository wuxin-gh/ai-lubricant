"""device-control built-in: drive a paired Android device over protocol v0.

The device app (a separate MIT-licensed project) dials in over WebSocket and
executes commands — read screen, tap, type, swipe, screenshot. This package is
the server half: protocol constants, the in-process device registry, and the
pairing store.

No code from the device-side project is vendored here; only the wire protocol
is reimplemented, so this package carries no third-party license obligation.
"""
