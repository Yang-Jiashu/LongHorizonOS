from pricing_service.service import process_order


def test_process_order_joins_pricing_and_audit_v2() -> None:
    assert process_order(
        "order-9",
        {
            "subtotal": "100",
            "discount_percent": "15",
            "tax_rate": "10",
            "currency": "gbp",
        },
    ) == {
        "quote": {
            "subtotal": "100.00",
            "discount": "15.00",
            "tax": "8.50",
            "total": "93.50",
            "currency": "GBP",
        },
        "audit": {
            "event": "order_priced",
            "order_id": "order-9",
            "total": "93.50",
        },
    }
