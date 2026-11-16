"""Exception types shared by every member's code."""


class ETLError(Exception):
    """Base class for all expected, user-facing errors."""


class ConfigError(ETLError):
    """config.yaml is missing, malformed, or points at something that does not exist."""


class PipelineError(ETLError):
    """A pipeline failed. Pipelines raise this instead of returning partial rows."""


class ContractError(ETLError):
    """A pipeline's return value violates interfaces.md."""
