from decimal import Decimal
from typing import Any

from .core import calculate_total


def quote(payload: dict[str, Any]) -> dict[str, str]:
    total = calculate_total(Decimal(str(payload["subtotal"])))
    return {"total": str(total)}
