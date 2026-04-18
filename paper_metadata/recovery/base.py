from __future__ import annotations

from typing import NamedTuple, Protocol, runtime_checkable


class RecoveryResult(NamedTuple):
    abstract: str
    source: str


@runtime_checkable
class AbstractRecoveryProvider(Protocol):
    name: str

    def recover(self, paper: dict) -> RecoveryResult | None:
        ...