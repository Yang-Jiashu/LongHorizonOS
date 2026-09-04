"""Shrinking a Context manifest from observation, safely.

The dangerous direction here is over-shrinking: dropping a ref that a future path
would have needed silently degrades an agent's answer, and unlike a page fault
there is no trap-and-fill to recover. So the safety rules are what these tests
pin down, not the size of the saving.
"""

from __future__ import annotations

from types import SimpleNamespace

from lhos.sdk.context_budget import recommend_context_budget


def _ref(ref_id: str, *, required: bool = False) -> SimpleNamespace:
    return SimpleNamespace(ref_id=ref_id, required=required)


def _manifest(*refs: SimpleNamespace, token_budget: int = 1000) -> SimpleNamespace:
    return SimpleNamespace(manifest_id="m1", refs=refs, token_budget=token_budget)


def test_never_read_optional_ref_is_a_drop_candidate() -> None:
    manifest = _manifest(_ref("spec"), _ref("changelog"))

    rec = recommend_context_budget(manifest, [{"spec"}, {"spec"}])

    assert rec.drop_ref_ids == ("changelog",)
    assert rec.keep_ref_ids == ("spec",)
    assert rec.changed is True


def test_a_ref_read_by_even_one_attempt_is_kept() -> None:
    """One quiet attempt is not evidence the ref is dead."""

    manifest = _manifest(_ref("spec"), _ref("rare"))

    rec = recommend_context_budget(manifest, [{"spec"}, {"spec"}, {"spec", "rare"}])

    assert rec.drop_ref_ids == ()
    assert "rare" in rec.keep_ref_ids


def test_required_ref_is_never_dropped_even_when_unread() -> None:
    manifest = _manifest(_ref("spec"), _ref("licence", required=True))

    rec = recommend_context_budget(manifest, [{"spec"}, {"spec"}])

    assert rec.drop_ref_ids == ()
    assert rec.required_ref_ids == ("licence",)
    assert any("required" in reason for reason in rec.reasons)


def test_zero_observations_recommend_nothing_and_say_so() -> None:
    manifest = _manifest(_ref("spec"), _ref("changelog"))

    rec = recommend_context_budget(manifest, [])

    assert rec.observed_attempts == 0
    assert rec.drop_ref_ids == ()
    assert rec.keep_ref_ids == ("changelog", "spec")
    assert rec.recommended_token_budget is None
    assert rec.changed is False
    assert any(item.name == "read_ref_ids_per_attempt" for item in rec.unavailable)


def test_budget_is_lowered_to_the_observed_peak() -> None:
    manifest = _manifest(_ref("spec"), token_budget=1000)

    rec = recommend_context_budget(
        manifest, [{"spec"}, {"spec"}], observed_peak_tokens=[120, 340, 210]
    )

    assert rec.recommended_token_budget == 340
    assert rec.declared_token_budget == 1000


def test_budget_is_never_raised_above_the_declared_one() -> None:
    manifest = _manifest(_ref("spec"), token_budget=200)

    rec = recommend_context_budget(manifest, [{"spec"}], observed_peak_tokens=[900])

    assert rec.recommended_token_budget == 200


def test_missing_token_totals_leave_the_budget_unjustified() -> None:
    manifest = _manifest(_ref("spec"), _ref("changelog"))

    rec = recommend_context_budget(manifest, [{"spec"}])

    assert rec.recommended_token_budget is None
    assert any(item.name == "observed_peak_tokens" for item in rec.unavailable)
    # Ref-level advice is still available even when the budget is not.
    assert rec.drop_ref_ids == ("changelog",)


def test_every_ref_gets_a_stated_reason() -> None:
    manifest = _manifest(_ref("spec"), _ref("changelog"), _ref("licence", required=True))

    rec = recommend_context_budget(manifest, [{"spec"}])

    for ref_id in ("spec", "changelog", "licence"):
        assert any(reason.startswith(f"{ref_id}:") for reason in rec.reasons)
