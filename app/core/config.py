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
    INHUMATION_AI_MIN_CONFIDENCE = os.getenv("INHUMATION_AI_MIN_CONFIDENCE", "0.80")
    INHUMATION_AI_BLANK_TEMPLATE_PATH = os.getenv(
        "INHUMATION_AI_BLANK_TEMPLATE_PATH", ""
    )
    INHUMATION_AI_OPENAI_TIMEOUT_SEC = os.getenv("INHUMATION_AI_OPENAI_TIMEOUT_SEC", "45")
    INHUMATION_AI_OPENAI_MAX_OUTPUT_TOKENS = os.getenv(
        "INHUMATION_AI_OPENAI_MAX_OUTPUT_TOKENS", "2500"
    )
