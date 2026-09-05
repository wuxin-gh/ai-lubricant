"""Data-side error types mirroring Connect error codes."""
from __future__ import annotations


class Code:
    CANCELED = "canceled"
    UNKNOWN = "unknown"
    INVALID_ARGUMENT = "invalid_argument"
    DEADLINE_EXCEEDED = "deadline_exceeded"
    NOT_FOUND = "not_found"
    PERMISSION_DENIED = "permission_denied"
    FAILED_PRECONDITION = "failed_precondition"
    UNAVAILABLE = "unavailable"
    INTERNAL = "internal"


class RPCError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class AgentComposeError(RuntimeError):
    """A Connect unary call to the control process returned an error envelope."""


class NodeServerUnavailable(RuntimeError):
    """The data service cannot reach the node control service."""
