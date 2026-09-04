from decimal import Decimal

import pytest

from pricing_service.core import PriceBreakdown, calculate_total


def test_calculate_total_returns_v2_breakdown() -> None:
    result = calculate_total(
        "100.00",
        discount_percent="10",
        tax_rate="8.25",
        currency="usd",
    )
    assert result == PriceBreakdown(
        subtotal=Decimal("100.00"),
        discount=Decimal("10.00"),
        tax=Decimal("7.43"),
        total=Decimal("97.43"),
        currency="USD",
    )


def test_calculate_total_v2_validates_policy_inputs() -> None:
    with pytest.raises(ValueError):
        calculate_total("-1", tax_rate="1", currency="USD")
    with pytest.raises(ValueError):
        calculate_total("10", discount_percent="101", tax_rate="1", currency="USD")
    with pytest.raises(ValueError):
        calculate_total("10", tax_rate="-1", currency="USD")
    with pytest.raises(ValueError):
        calculate_total("10", tax_rate="1", currency="US")
