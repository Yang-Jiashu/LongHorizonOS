from pricing_service.service import process_order


def test_process_order_joins_pricing_and_audit_v1() -> None:
    assert process_order(
        "order-9",
        {"subtotal": "100", "discount_percent": "15"},
    ) == {
        "quote": {"total": "85.00"},
        "audit": {
            "event": "order_priced",
            "order_id": "order-9",
            "total": "85.00",
        },
    }
