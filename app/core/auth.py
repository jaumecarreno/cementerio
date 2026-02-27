from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, session, url_for
from flask_login import login_required, login_user, logout_user
from werkzeug.security import check_password_hash

from app.core.models import User

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")


@auth_bp.get("/login")
def login():
    return render_template("auth/login.html")


@auth_bp.post("/login")
def login_post():
    email = request.form.get("email", "").strip().lower()
    password = request.form.get("password", "")
    user = User.query.filter_by(email=email).first()
    if not user or not check_password_hash(user.password_hash, password):
        flash("Credenciales invÃ¡lidas", "error")
        return redirect(url_for("auth.login"))
    login_user(user)
    return redirect(url_for("dashboard_page"))


@auth_bp.post("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth.login"))


@auth_bp.post("/lang")
def set_lang():
    lang = request.form.get("lang", "es")
    if lang not in {"es", "ca"}:
        lang = "es"
    session["lang"] = lang
    next_url = request.form.get("next") or request.referrer or url_for("dashboard_page")
    return redirect(next_url)

