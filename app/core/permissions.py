from __future__ import annotations

from functools import wraps

from flask import abort, g
from flask_login import current_user


def require_membership(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated:
            abort(401)
        if getattr(g, "org", None) is None:
            abort(403)
        return fn(*args, **kwargs)

    return wrapper
