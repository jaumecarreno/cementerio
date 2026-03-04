from __future__ import annotations

import os


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")
    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DATABASE_URL",
        "sqlite:///cemetery.db",
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    APP_ENV = os.getenv("APP_ENV", "")
    # Fecha de corte para bloquear escrituras de facturacion legacy (/tasas/*).
    # Formato ISO YYYY-MM-DD. Vacio = sin corte activo.
    BILLING_V2_CUTOVER_DATE = os.getenv("BILLING_V2_CUTOVER_DATE", "")
