"""Materialized-versus-read context accounting.

The load-bearing test here is the circularity guard.  The Context VM records the
pages it materialized into the provenance read-set itself, so comparing pages
against an unfiltered read-set makes "everything was used" true by construction.
A measure that cannot fail is not a measure.
"""

from __future__ import annotations

from types import SimpleNamespace

from lhos.sdk.context_utilization import (
    CONTEXT_VM_SOURCE,
    executor_read_keys,
    measure_context_utilization,
)


def _page(page_id: str, uri: str, artifact_id: str, size: int) -> SimpleNamespace:
    return SimpleNamespace(
        page_id=page_id,
        canonical_uri=uri,
        artifact_id=artifact_id,
        byte_start=0,
        byte_end=size,
    )


def _binding(uri: str, artifact_id: str, source: str) -> SimpleNamespace:
    return SimpleNamespace(resource_uri=uri, artifact_id=artifact_id, source=source)


_PAGES = (
    _page("p1", "vpg://spec", "spec", 400),
    _page("p2", "vpg://api", "api", 600),
)


def test_context_vm_self_records_are_excluded() -> None:
    """Otherwise every page counts as read and the metric is vacuous."""

    read_set = (
        _binding("vpg://spec", "spec", CONTEXT_VM_SOURCE),
        _binding("vpg://api", "api", CONTEXT_VM_SOURCE),
    )

    assert executor_read_keys(read_set) == frozenset()


def test_unread_page_is_counted_as_wasted_context() -> None:
    reads = executor_read_keys((_binding("vpg://spec", "spec", "runtime"),))

    result = measure_context_utilization(_PAGES, reads)

    assert result.observable is True
    assert result.materialized_pages == 2
    assert result.read_pages == 1
    assert result.unread_pages == 1
    assert result.unread_page_ids == ("p2",)
    assert result.materialized_bytes == 1_000
    assert result.unread_bytes == 600
    assert result.wasted_fraction_basis_points == 6_000


def test_fully_used_context_reports_measured_zero_waste() -> None:
    reads = executor_read_keys(
        (
            _binding("vpg://spec", "spec", "runtime"),
            _binding("vpg://api", "api", "runtime"),
        )
    )

    result = measure_context_utilization(_PAGES, reads)

    assert result.unread_pages == 0
    assert result.unread_bytes == 0
    assert result.wasted_fraction_basis_points == 0


def test_absent_observation_is_unavailable_not_zero_waste() -> None:
    result = measure_context_utilization(_PAGES, None)

    assert result.observable is False
    assert result.unread_pages is None
    assert result.unread_bytes is None
    assert result.wasted_fraction_basis_points is None
    assert any(item.name == "executor_reads" for item in result.unavailable)
    # The materialized side is still known: we know what we paid for.
    assert result.materialized_pages == 2
    assert result.materialized_bytes == 1_000


def test_a_page_matches_under_either_identity_form() -> None:
    """Declarations use bare ids; provenance keeps raw uris."""

    by_artifact = measure_context_utilization(
        _PAGES, executor_read_keys((_binding("", "api", "runtime"),))
    )
    by_uri = measure_context_utilization(
        _PAGES, executor_read_keys((_binding("vpg://api", "", "runtime"),))
    )

    assert by_artifact.unread_page_ids == ("p1",)
    assert by_uri.unread_page_ids == ("p1",)


def test_omitted_refs_are_reported_alongside_waste() -> None:
    """Budget pressure cut these; they are the other side of the same decision."""

    result = measure_context_utilization(
        _PAGES,
        executor_read_keys((_binding("vpg://spec", "spec", "runtime"),)),
        omitted_ref_ids=("tests", "changelog", "tests"),
    )

    assert result.omitted_ref_ids == ("changelog", "tests")
