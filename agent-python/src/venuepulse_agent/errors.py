"""Typed error hierarchy. Each error maps to an SSE `error` event code and a
`retryable` flag (see docs/architecture.md section 8.2)."""


class AgentError(Exception):
    code = "agent_error"
    retryable = False

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class GatewayError(AgentError):
    """The Go gateway's internal API failed or was unreachable."""

    code = "gateway_error"
    retryable = True


class ToolInputError(AgentError):
    """The model produced tool input that failed validation."""

    code = "tool_input_error"


class ModelError(AgentError):
    """The Anthropic API returned an error."""

    code = "model_error"

    def __init__(self, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.retryable = retryable


class BudgetExceeded(AgentError):
    """The tool loop hit its iteration or wall-clock budget."""

    code = "budget_exceeded"
