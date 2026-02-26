from __future__ import annotations

from decimal import Decimal


def money(value: Decimal | float | int) -> str:
    return f"{Decimal(value):.2f}€".replace(".", ",")
