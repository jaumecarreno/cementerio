from __future__ import annotations

import click
from flask import Flask, redirect, url_for
from flask_login import current_user

from app.cemetery import cemetery_bp
from app.core.auth import auth_bp
from app.core.config import Config
from app.core.extensions import db, login_manager, migrate
from app.core.i18n import get_locale, translate
from app.core.models import Organization, User, seed_demo_data
from app.core.tenancy import load_tenant_context


def create_app(config_object: type[Config] | None = None) -> Flask:
    app = Flask(__name__)
    app.config.from_object(config_object or Config)

    db.init_app(app)
    migrate.init_app(app, db)
    login_manager.init_app(app)

    app.before_request(load_tenant_context)
    app.context_processor(_template_context)

    app.register_blueprint(auth_bp)
    app.register_blueprint(cemetery_bp)

    register_cli(app)
    register_routes(app)
    return app


def register_routes(app: Flask) -> None:
    @app.get("/")
    def home():
        if current_user.is_authenticated:
            return redirect(url_for("cemetery.panel"))
        return redirect(url_for("auth.login"))


def register_cli(app: Flask) -> None:
    @app.cli.command("seed-demo")
    @click.option("--reset", is_flag=True, help="Delete existing data before seed.")
    def seed_demo(reset: bool) -> None:
        """Seed demo data for MVP."""
        if reset:
            db.drop_all()
            db.create_all()
        if not Organization.query.first():
            seed_demo_data(db.session)
            click.echo("Demo data seeded.")
        else:
            click.echo("Seed skipped: existing organizations found.")


def _template_context() -> dict[str, object]:
    return {
        "t": translate,
        "current_lang": get_locale(),
    }


@login_manager.user_loader
def load_user(user_id: str) -> User | None:
    return User.query.get(int(user_id))
