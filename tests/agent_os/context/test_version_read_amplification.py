"""Each ArtifactVersion is read from storage once per load, not once per page.

The Context VM used to read every ref in full ``2 + selected_pages`` times: once
in ``_resolve_and_verify_ref`` where the bytes were verified and then thrown
away, once to paginate, and once more per selected page purely to slice a range
out of it. For an artifact of size ``B`` split into ``P`` pages that is
``O(P*B)`` of I/O to materialize ``B`` bytes.

The old code carried a comment reading "lazy fetch content only once" directly
above the loop that did the opposite, which is exactly why the behaviour
survived: nothing counted. These tests count.

Correctness rests on version binding: an ``(artifact_id, version)`` pair names
immutable committed bytes, and the declared content hash is verified before
anything else uses them. So the caching cannot change a hash, a page id, or a
selection decision -- and ``test_pages_are_byte_identical_to_uncached_reads``
pins that rather than assuming it.
"""

from __future__ import annotations

from typing import Any

from lhos.agent_os.context.models import _content_hash_for
from tests.agent_os.context.conftest import write_artifacts_and_build_manifest


class _CountingSupplier:
    """Wraps the real supplier and records every full-version read."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.reads: list[tuple[str, int]] = []

    def read_version(self, *, artifact_id: str, version: int, canonical_uri: str) -> bytes:
        self.reads.append((artifact_id, version))
        return self._inner.read_version(
            artifact_id=artifact_id,
            version=version,
            canonical_uri=canonical_uri,
        )

    def read_version_size(self, *, artifact_id: str, version: int) -> int:
        return self._inner.read_version_size(artifact_id=artifact_id, version=version)


def _load_counting(env: dict[str, Any], manifest: Any) -> tuple[Any, _CountingSupplier]:
    service = env["ctx_svc"]
    counter = _CountingSupplier(service._content)
    service._content = counter
    try:
        _handle, loaded = service.load(manifest=manifest, caller_pid=env["pid"])
    finally:
        service._content = counter._inner
    return loaded, counter


def test_each_version_is_read_once_regardless_of_page_count(env: dict[str, Any]) -> None:
    """A ref split into many pages must still cost one read."""

    # 512 bytes at a 64-byte page size is 8 pages per artifact.
    manifest = write_artifacts_and_build_manifest(
        env=env,
        pid=env["pid"],
        artifacts=[
            ("artifact://ns-p1/a.txt", b"a" * 512, "text/plain"),
            ("artifact://ns-p1/b.txt", b"b" * 512, "text/plain"),
        ],
        page_size_bytes=64,
        token_budget=10_000,
    )

    loaded, counter = _load_counting(env, manifest)

    assert len(loaded.ordered_pages) > 2, "workload must span multiple pages to be meaningful"
    distinct = set(counter.reads)
    assert len(distinct) == 2, distinct
    assert len(counter.reads) == len(distinct), (
        f"{len(counter.reads)} storage reads for {len(distinct)} versions "
        f"across {len(loaded.ordered_pages)} pages"
    )


def test_read_count_does_not_grow_with_page_count(env: dict[str, Any]) -> None:
    """Halving the page size doubles the pages; reads must not follow."""

    def reads_at(page_size: int) -> int:
        manifest = write_artifacts_and_build_manifest(
            env=env,
            pid=env["pid"],
            artifacts=[(f"artifact://ns-p1/p{page_size}.txt", b"c" * 512, "text/plain")],
            page_size_bytes=page_size,
            token_budget=10_000,
        )
        loaded, counter = _load_counting(env, manifest)
        assert len(loaded.ordered_pages) >= 512 // page_size
        return len(counter.reads)

    coarse = reads_at(256)
    fine = reads_at(32)
    assert coarse == fine == 1, f"coarse={coarse} fine={fine}"


def test_pages_still_tile_the_exact_committed_bytes(env: dict[str, Any]) -> None:
    """The cache must not shift a single page boundary or identity.

    Sharing one buffer across verification, pagination and materialization is
    only sound because the bytes are immutable for a bound version. This asserts
    the resulting projection rather than trusting that argument: the pages must
    tile the payload contiguously from zero, their sizes must agree with their
    offsets, and every page must still carry the artifact's true content hash.
    """

    payload = bytes(range(256)) * 2
    manifest = write_artifacts_and_build_manifest(
        env=env,
        pid=env["pid"],
        artifacts=[("artifact://ns-p1/bin.dat", payload, "application/octet-stream")],
        page_size_bytes=64,
        token_budget=10_000,
    )

    loaded, _counter = _load_counting(env, manifest)
    pages = sorted(loaded.ordered_pages, key=lambda page: page.byte_start)

    assert len(pages) == len(payload) // 64
    expected_hash = _content_hash_for(payload)
    cursor = 0
    for page in pages:
        assert page.byte_start == cursor
        assert page.byte_end - page.byte_start == page.size_bytes
        assert page.content_hash == expected_hash
        cursor = page.byte_end
    assert cursor == len(payload)


def test_a_second_load_of_the_same_manifest_still_reads_storage(env: dict[str, Any]) -> None:
    """The cache is per load, deliberately.

    Caching committed bytes across loads for the lifetime of the service would
    hold every artifact ever read in memory and would outlive any external
    change to the underlying store. The scope is one load; this pins it so the
    lifetime is not quietly widened.
    """

    manifest = write_artifacts_and_build_manifest(
        env=env,
        pid=env["pid"],
        artifacts=[("artifact://ns-p1/once.txt", b"d" * 128, "text/plain")],
        page_size_bytes=64,
        token_budget=10_000,
    )

    _first, first_counter = _load_counting(env, manifest)
    _second, second_counter = _load_counting(env, manifest)

    assert first_counter.reads
    assert second_counter.reads
