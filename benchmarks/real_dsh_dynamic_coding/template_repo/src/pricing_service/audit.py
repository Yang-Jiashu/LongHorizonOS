from decimal import Decimal


def build_audit_event(order_id: str, total: Decimal | int | float | str) -> dict[str, str]:
    return {"order_id": str(order_id)}
