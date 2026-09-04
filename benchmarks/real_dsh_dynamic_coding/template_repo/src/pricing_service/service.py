from typing import Any

from .api import quote


def process_order(order_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"quote": quote(payload)}
