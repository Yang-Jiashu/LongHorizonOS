from decimal import Decimal

import pytest
from pricing_service.api import quote
from pricing_service.audit import build_audit_event
from pricing_service.core import PriceBreakdown, calculate_total
from pricing_service.service import process_order


def test_v2_rounding_is_applied_per_component() -> None:
    result = calculate_total(
        "19.995",
        discount_percent="12.5",
        tax_rate="7.75",
        currency="jpy",
    )
    assert isinstance(result, PriceBreakdown)
    assert result.discount == Decimal("2.50")
    assert result.tax == Decimal("1.36")
    assert result.total == Decimal("18.86")
    assert result.currency == "JPY"


def test_v2_quote_defaults_discount_but_requires_new_fields() -> None:
    assert quote({"subtotal": 10, "tax_rate": 10, "currency": "usd"}) == {
        "subtotal": "10.00",
        "discount": "0.00",
        "tax": "1.00",
        "total": "11.00",
        "currency": "USD",
    }
    with pytest.raises(KeyError):
        quote({"subtotal": 10, "currency": "USD"})


def test_independent_audit_contract_remains_unchanged() -> None:
    assert build_audit_event("stable-1", Decimal("3")) == {
        "event": "order_priced",
        "order_id": "stable-1",
        "total": "3.00",
    }


def test_final_service_uses_v2_total_for_audit() -> None:
    result = process_order(
        "hidden-5",
        {
            "subtotal": "80",
            "discount_percent": "25",
            "tax_rate": "20",
            "currency": "cad",
        },
    )
    assert result["quote"]["total"] == "72.00"
    assert result["quote"]["currency"] == "CAD"
    assert result["audit"]["total"] == "72.00"
