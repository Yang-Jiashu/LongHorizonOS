"""Budget manager (spec section 17).

Usage is tracked in memory per run and mirrored into BUDGET_UPDATED events;
on exhaustion a single BUDGET_EXHAUSTED event is appended and the scheduler
stops starting new nodes (invariant 10).
"""

from __future__ import annotations

from lhos.domain.budgets import BudgetLimits, BudgetState, budget_exhausted
from lhos.domain.events import ActorType, EventType, RuntimeEvent


class BudgetManager:
    def __init__(self, event_store, limits: BudgetLimits):  # noqa: ANN001
        self._events = event_store
        self._limits = limits
        self._states: dict[str, BudgetState] = {}
        self._exhausted_signalled: set[str] = set()

    @property
    def limits(self) -> BudgetLimits:
        return self._limits

    def get_state(self, run_id: str) -> BudgetState:
        return self._states.setdefault(run_id, BudgetState())

    def can_continue(self, run_id: str) -> bool:
        return not budget_exhausted(self._limits, self.get_state(run_id))

    def _update(self, run_id: str) -> BudgetState:
        state = self.get_state(run_id)
        self._events.append(
            RuntimeEvent(
                run_id=run_id,
                event_type=EventType.BUDGET_UPDATED,
                actor_type=ActorType.SYSTEM,
                payload={"budget": state.model_dump()},
            )
        )
        if budget_exhausted(self._limits, state) and run_id not in self._exhausted_signalled:
            self._exhausted_signalled.add(run_id)
            self._events.append(
                RuntimeEvent(
                    run_id=run_id,
                    event_type=EventType.BUDGET_EXHAUSTED,
                    actor_type=ActorType.SYSTEM,
                    payload={"budget": state.model_dump()},
                )
            )
        return state

    def record_model_usage(
        self,
        run_id: str,
        input_tokens: int,
        output_tokens: int,
        cost_usd: float = 0.0,
    ) -> BudgetState:
        state = self.get_state(run_id)
        state.input_tokens += input_tokens
        state.output_tokens += output_tokens
        state.model_calls += 1
        state.cost_usd += cost_usd
        return self._update(run_id)

    def record_tool_call(self, run_id: str) -> BudgetState:
        state = self.get_state(run_id)
        state.tool_calls += 1
        return self._update(run_id)

    def record_elapsed(self, run_id: str, seconds: float) -> BudgetState:
        state = self.get_state(run_id)
        state.elapsed_seconds += seconds
        return self._update(run_id)
