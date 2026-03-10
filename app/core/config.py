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
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
    INHUMATION_AI_MODEL = os.getenv("INHUMATION_AI_MODEL", "gpt-4.1-mini")
    INHUMATION_AI_MAX_UPLOAD_MB = os.getenv("INHUMATION_AI_MAX_UPLOAD_MB", "15")
