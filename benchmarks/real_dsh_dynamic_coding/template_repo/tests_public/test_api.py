from pricing_service.api import quote


def test_quote_returns_v1_wire_shape() -> None:
    assert quote({"subtotal": "25.00", "discount_percent": "20"}) == {
        "total": "20.00"
    }
