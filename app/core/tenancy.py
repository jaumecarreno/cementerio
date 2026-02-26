from __future__ import annotations

from flask import abort, g
from flask_login import current_user

from app.core.models import Membership


def load_tenant_context() -> None:
    g.org = None
    g.membership = None
    if not current_user.is_authenticated:
        return
    membership = (
        Membership.query.filter_by(user_id=current_user.id)
        .order_by(Membership.id.asc())
        .first()
    )
    if membership is None:
        abort(403)
    g.org = membership.organization
    g.membership = membership
