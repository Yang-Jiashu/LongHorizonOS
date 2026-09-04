import pytest

from pricing_service.audit import build_audit_event


def test_build_audit_event_is_stable() -> None:
    assert build_audit_event("order-7", "12.5") == {
        "event": "order_priced",
        "order_id": "order-7",
        "total": "12.50",
    }


def test_build_audit_event_rejects_blank_order_id() -> None:
    with pytest.raises(ValueError):
        build_audit_event(" ", "1.00")
