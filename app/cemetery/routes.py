from __future__ import annotations

from datetime import date

from flask import abort, flash, redirect, render_template, request, url_for
from flask_login import login_required

from app.cemetery import cemetery_bp
from app.cemetery.services import (
    change_sepultura_state,
    collect_tickets,
    create_mass_sepulturas,
    generate_invoice_for_tickets,
    panel_data,
    preview_mass_create,
    search_sepulturas,
    sepultura_by_id,
    sepultura_tabs_data,
    sepultura_tickets_and_invoices,
)
from app.core.models import SepulturaEstado
from app.core.permissions import require_membership
from app.core.utils import money


def _is_htmx() -> bool:
    return request.headers.get("HX-Request", "").lower() == "true"


@cemetery_bp.get("/panel")
@login_required
@require_membership
def panel():
    # Spec 9.0 + mockups_v2/page-2 - Panel de trabajo Cementerio
    data = panel_data()
    return render_template("cemetery/panel.html", data=data)


@cemetery_bp.route("/sepulturas/buscar", methods=["GET", "POST"])
@login_required
@require_membership
def search_graves():
    # Spec 5.3.4 + 9.4.x - Buscar sepultura por ubicación/titular/difunto
    filters = {
        "bloque": request.values.get("bloque", "").strip(),
        "fila": request.values.get("fila", "").strip(),
        "columna": request.values.get("columna", "").strip(),
        "numero": request.values.get("numero", "").strip(),
        "titular": request.values.get("titular", "").strip(),
        "difunto": request.values.get("difunto", "").strip(),
    }
    rows = search_sepulturas(filters) if any(filters.values()) else []
    if _is_htmx():
        return render_template("cemetery/_search_results.html", rows=rows, money=money)
    return render_template("cemetery/search.html", filters=filters, rows=rows, money=money)


@cemetery_bp.get("/sepulturas/<int:sepultura_id>")
@login_required
@require_membership
def grave_detail(sepultura_id: int):
    # Spec 9.4.3 / 9.4.4 / 9.4.5 - Ficha de sepultura con tabs
    tab = request.args.get("tab", "movimientos")
    mov_filters = {
        "tipo": request.args.get("tipo", "").strip(),
        "desde": request.args.get("desde", "").strip(),
        "hasta": request.args.get("hasta", "").strip(),
    }
    try:
        data = sepultura_tabs_data(sepultura_id, tab, mov_filters)
    except ValueError:
        abort(404)
    return render_template("cemetery/detail.html", data=data, SepulturaEstado=SepulturaEstado, money=money)


@cemetery_bp.post("/sepulturas/<int:sepultura_id>/estado")
@login_required
@require_membership
def change_state(sepultura_id: int):
    # Spec 9.4.2 - Canvi d'estat de les sepultures
    try:
        sep = sepultura_by_id(sepultura_id)
    except ValueError:
        abort(404)
    requested = request.form.get("estado", "").strip().upper()
    try:
        new_state = SepulturaEstado[requested]
        change_sepultura_state(sep, new_state)
        flash("Estado actualizado", "success")
    except KeyError:
        flash("Estado inválido", "error")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.grave_detail", sepultura_id=sepultura_id, tab="resumen"))


@cemetery_bp.get("/tasas/cobro")
@login_required
@require_membership
def fee_collection():
    # Spec 9.1.3 + 5.3.4 - Cobrament de taxes
    sepultura_id = request.args.get("sepultura_id", type=int)
    if not sepultura_id:
        flash("Selecciona una sepultura para cobrar tasas", "error")
        return redirect(url_for("cemetery.search_graves"))
    try:
        data = sepultura_tickets_and_invoices(sepultura_id)
    except ValueError:
        abort(404)
    return render_template("cemetery/fees.html", data=data, money=money, today=date.today())


def _selected_ticket_ids() -> list[int]:
    values = request.form.getlist("ticket_ids")
    return [int(v) for v in values if v.isdigit()]


@cemetery_bp.post("/tasas/cobro/facturar")
@login_required
@require_membership
def fee_generate_invoice():
    # Spec 9.1.3 - Generar factura para tiquets pendientes
    sepultura_id = request.form.get("sepultura_id", type=int)
    selected_ids = _selected_ticket_ids()
    try:
        invoice = generate_invoice_for_tickets(sepultura_id, selected_ids)
        flash(f"Factura generada: {invoice.numero}", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.fee_collection", sepultura_id=sepultura_id))


@cemetery_bp.post("/tasas/cobro/cobrar")
@login_required
@require_membership
def fee_collect_and_receipt():
    # Spec 9.1.3 - Cobrar y emitir recibo en mostrador
    sepultura_id = request.form.get("sepultura_id", type=int)
    selected_ids = _selected_ticket_ids()
    payment_method = request.form.get("payment_method", "EFECTIVO")
    try:
        invoice, payment = collect_tickets(sepultura_id, selected_ids, payment_method)
        flash(f"Cobro registrado. Factura {invoice.numero} / Recibo {payment.receipt_number}", "success")
        return redirect(url_for("cemetery.receipt", payment_id=payment.id))
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("cemetery.fee_collection", sepultura_id=sepultura_id))


@cemetery_bp.get("/tasas/recibo/<int:payment_id>")
@login_required
@require_membership
def receipt(payment_id: int):
    # Spec 9.1.3 - Justificante de cobro
    from app.core.models import Payment

    payment = Payment.query.filter_by(id=payment_id).first_or_404()
    return render_template("cemetery/receipt.html", payment=payment, money=money)


@cemetery_bp.route("/sepulturas/alta-masiva", methods=["GET", "POST"])
@login_required
@require_membership
def mass_create():
    # Spec 9.4.1 - Alta masiva de sepulturas por bloque
    defaults = {
        "bloque": request.form.get("bloque", "B-16"),
        "via": request.form.get("via", "V-3"),
        "tipo_bloque": request.form.get("tipo_bloque", "Nínxols"),
        "modalidad": request.form.get("modalidad", "Nínxol nou"),
        "tipo_lapida": request.form.get("tipo_lapida", "Resina fenòlica"),
        "orientacion": request.form.get("orientacion", "Nord"),
        "filas": request.form.get("filas", "1-12"),
        "columnas": request.form.get("columnas", "1-24"),
    }
    preview = None
    if request.method == "POST":
        action = request.form.get("action", "preview")
        try:
            preview = preview_mass_create(defaults)
            if action == "create":
                created = create_mass_sepulturas(defaults)
                flash(f"Sepulturas creadas en estado Lliure: {created}", "success")
                return redirect(url_for("cemetery.mass_create"))
        except ValueError as exc:
            flash(str(exc), "error")

    if _is_htmx():
        return render_template("cemetery/_mass_preview.html", preview=preview)
    return render_template("cemetery/mass_create.html", data=defaults, preview=preview)
