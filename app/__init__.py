from __future__ import annotations

import click
import os

from flask import Flask, flash, g, redirect, render_template, url_for
from flask_login import current_user, login_required

from app.cemetery import cemetery_bp
from app.core.auth import auth_bp
from app.core.config import Config
from app.core.extensions import db, login_manager, migrate
from app.core.i18n import get_locale, translate
from app.core.models import Organization, User, seed_demo_data
from app.core.permissions import require_membership, require_role
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
        return redirect(url_for("dashboard_page"))

    @app.get("/dashboard")
    @login_required
    @require_membership
    def dashboard_page():
        from app.cemetery.services import panel_data

        data = panel_data()
        return render_template("dashboard.html", data=data)

    @app.get("/config")
    @login_required
    @require_membership
    def config_page():
        return render_template("config.html", org=g.org)

    @app.get("/modulo/<slug>")
    @login_required
    @require_membership
    def module_pending(slug: str):
        mapping: dict[str, tuple[str, str]] = {
            "servicios-funerarios": ("menu.funeral_services", "funeral_services"),
            "crematorio": ("menu.crematorium", "crematorium"),
            "facturacion": ("menu.billing", "billing"),
            "inventario": ("menu.inventory", "inventory"),
            "reporting-global": ("menu.reports", "reports"),
            "ampliacion-derecho": ("module.right_extension", "cemetery"),
            "prorroga-derecho": ("module.right_renewal", "cemetery"),
        }
        mapped = mapping.get(slug)
        return render_template(
            "module_pending.html",
            title_key=mapped[0] if mapped else "",
            title=slug.replace("-", " ").title(),
            active_global=mapped[1] if mapped else "cemetery",
            tracking_code=f"PEND-{slug.upper()}",
        )

    @app.get("/demo")
    @login_required
    @require_membership
    def demo_page():
        return render_template("demo.html")

    @app.post("/demo/reset")
    @login_required
    @require_membership
    @require_role("admin")
    def demo_reset():
        from app.cemetery.services import reset_demo_org_data_to_zero

        if not _demo_actions_enabled(app):
            flash("Acciones DEMO bloqueadas fuera de entorno DEV/TEST", "error")
            return redirect(url_for("demo_page")), 403
        summary = reset_demo_org_data_to_zero()
        flash(
            f"Reset a cero completado para org {g.org.code}: personas={summary['persons']} sepulturas={summary['sepulturas']} contratos={summary['contracts']} expedientes={summary['expedientes']} casos={summary['casos']}",
            "success",
        )
        return redirect(url_for("demo_page"))

    @app.post("/demo/load-initial")
    @login_required
    @require_membership
    @require_role("admin")
    def demo_load_initial():
        from app.cemetery.services import load_demo_org_initial_dataset

        if not _demo_actions_enabled(app):
            flash("Acciones DEMO bloqueadas fuera de entorno DEV/TEST", "error")
            return redirect(url_for("demo_page")), 403
        summary = load_demo_org_initial_dataset(current_user.id)
        flash(
            f"DEMO inicial cargada para org {g.org.code}: sepulturas={summary['sepulturas']} titulares_activos={summary['titulares_activos']} personas={summary['persons']} contratos={summary['contracts']} expedientes={summary['expedientes']} casos={summary['casos']}",
            "success",
        )
        return redirect(url_for("demo_page"))

    @app.errorhandler(403)
    def forbidden(_error):
        return render_template("errors/403.html"), 403

    @app.errorhandler(404)
    def not_found(_error):
        return render_template("errors/404.html"), 404


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

    @app.cli.command("tickets-generate-year")
    @click.option("--year", type=int, required=True, help="Fiscal year to generate maintenance tickets for.")
    @click.option("--org-code", type=str, default=None, help="Optional organization code.")
    def tickets_generate_year(year: int, org_code: str | None) -> None:
        """Generate yearly maintenance tickets for concession contracts."""
        from app.cemetery.services import generate_maintenance_tickets_for_year

        query = Organization.query
        if org_code:
            query = query.filter_by(code=org_code)
        organizations = query.order_by(Organization.id.asc()).all()
        if not organizations:
            click.echo("No organizations found for ticket generation.")
            return

        for organization in organizations:
            result = generate_maintenance_tickets_for_year(year, organization)
            click.echo(
                f"[{organization.code}] year={year} created={result.created} existing={result.existing}"
            )


def _template_context() -> dict[str, object]:
    return {
        "t": translate,
        "current_lang": get_locale(),
    }


def _demo_actions_enabled(app: Flask) -> bool:
    if app.config.get("TESTING"):
        return True
    if app.debug:
        return True
    flask_env = (os.getenv("FLASK_ENV") or "").strip().lower()
    app_env = (app.config.get("APP_ENV") or os.getenv("APP_ENV") or "").strip().lower()
    return flask_env == "development" or app_env in {"dev", "development"}


@login_manager.user_loader
def load_user(user_id: str) -> User | None:
    return User.query.get(int(user_id))
