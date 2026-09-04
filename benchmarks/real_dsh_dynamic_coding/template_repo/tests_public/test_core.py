from decimal import Decimal

import pytest

from pricing_service.core import calculate_total


def test_calculate_total_applies_discount_and_rounds_half_up() -> None:
    assert calculate_total("19.995", "10") == Decimal("18.00")


def test_calculate_total_validates_inputs() -> None:
    with pytest.raises(ValueError):
        calculate_total("-1")
    with pytest.raises(ValueError):
        calculate_total("10", "101")
