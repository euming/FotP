from __future__ import annotations

from abc import ABC, abstractmethod


class AgentSelector(ABC):
    @abstractmethod
    def select(self, free_agents: list[str], task_context: dict) -> str | None:
        ...


class RoundRobinSelector(AgentSelector):
    def __init__(self):
        self._idx = -1

    def select(self, free_agents: list[str], task_context: dict) -> str | None:
        if not free_agents:
            return None
        self._idx = (self._idx + 1) % len(free_agents)
        return free_agents[self._idx]
