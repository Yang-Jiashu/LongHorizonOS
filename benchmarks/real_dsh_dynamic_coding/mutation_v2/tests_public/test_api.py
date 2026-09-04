from pricing_service.api import quote


def test_quote_returns_v2_wire_shape() -> None:
    assert quote(
        {
            "subtotal": "50",
            "discount_percent": "20",
            "tax_rate": "5",
            "currency": "eur",
        }
    ) == {
        "subtotal": "50.00",
        "discount": "10.00",
        "tax": "2.00",
        "total": "42.00",
        "currency": "EUR",
    }
