from flask import Blueprint

cemetery_bp = Blueprint("cemetery", __name__, url_prefix="/cementerio")

from app.cemetery import routes  # noqa: E402,F401
