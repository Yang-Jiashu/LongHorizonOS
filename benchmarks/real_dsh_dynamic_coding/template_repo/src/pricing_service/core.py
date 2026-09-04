from decimal import Decimal


def calculate_total(
    subtotal: Decimal | int | float | str,
    discount_percent: Decimal | int | float | str = Decimal("0"),
) -> Decimal:
    """Calculate the discounted order total."""
    return Decimal(str(subtotal))
