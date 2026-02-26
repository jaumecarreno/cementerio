from __future__ import annotations

from datetime import date

from flask import abort, flash, make_response, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from app.cemetery import cemetery_bp
from app.cemetery.services import (
    add_case_party,
    add_case_publication,
    approve_ownership_case,
    change_ownership_case_status,
    change_sepultura_state,
    collect_tickets,
    close_ownership_case,
    create_ownership_case,
    create_funeral_right_contract,
    create_mass_sepulturas,
    funeral_right_title_pdf,
    generate_maintenance_tickets_for_year,
    list_ownership_cases,
    nominate_contract_beneficiary,
    ownership_case_detail,
    ownership_case_resolution_pdf,
    org_record,
    panel_data,
    preview_mass_create,
    reject_ownership_case,
    search_sepulturas,
    sepultura_by_id,
    sepultura_tabs_data,
    sepultura_tickets_and_invoices,
    upload_case_document,
    verify_case_document,
)
from app.core.models import OwnershipTransferStatus, OwnershipTransferType, SepulturaEstado
from app.core.permissions import require_membership, require_role
from app.core.utils import money


def _is_htmx() -> bool:
    return request.headers.get("HX-Request", "").lower() == "true"


@cemetery_bp.get("/panel")
@login_required
@require_membership
def panel():
    # Spec 9.0 + mockups_v2/page-2 - Panel de trabajo Cementerio
    data = panel_data()
    return render_template("cemetery/panel.html", data=data, current_year=date.today().year)


@cemetery_bp.post("/admin/tickets/generar")
@login_required
@require_membership
@require_role("admin")
def admin_generate_tickets():
    # Spec 5.2.5.2.2 / 5.3.4 - Generar tiquets anuales de mantenimiento
    year = request.form.get("year", type=int)
    if not year:
        flash("Indica un ano valido", "error")
        return redirect(url_for("cemetery.panel"))
    result = generate_maintenance_tickets_for_year(year, org_record())
    flash(
        f"Tiquets generados {year}: creados={result.created}, existentes={result.existing}",
        "success",
    )
    return redirect(url_for("cemetery.panel"))


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
    # Spec 9.4.3 / 9.4.4 / 9.4.5 / 9.1.7 - Ficha de sepultura con tabs
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


@cemetery_bp.route("/titularidad/casos", methods=["GET", "POST"])
@login_required
@require_membership
def ownership_cases():
    if request.method == "POST":
        payload = {k: v for k, v in request.form.items()}
        try:
            created = create_ownership_case(payload, current_user.id)
            flash(f"Caso {created.case_number} creado", "success")
            return redirect(url_for("cemetery.ownership_case_detail_page", case_id=created.id))
        except ValueError as exc:
            flash(str(exc), "error")
    filters = {
        "type": request.args.get("type", "").strip(),
        "status": request.args.get("status", "").strip(),
        "opened_from": request.args.get("opened_from", "").strip(),
        "opened_to": request.args.get("opened_to", "").strip(),
        "contract_id": request.args.get("contract_id", "").strip(),
        "sepultura_id": request.args.get("sepultura_id", "").strip(),
        "party_name": request.args.get("party_name", "").strip(),
    }
    rows = list_ownership_cases(filters)
    if _is_htmx():
        return render_template("cemetery/_ownership_cases_table.html", rows=rows)
    return render_template(
        "cemetery/ownership_cases.html",
        rows=rows,
        filters=filters,
        OwnershipTransferType=OwnershipTransferType,
        OwnershipTransferStatus=OwnershipTransferStatus,
    )


@cemetery_bp.get("/titularidad/casos/<int:case_id>")
@login_required
@require_membership
def ownership_case_detail_page(case_id: int):
    try:
        data = ownership_case_detail(case_id)
    except ValueError:
        abort(404)
    return render_template(
        "cemetery/ownership_case_detail.html",
        data=data,
        OwnershipTransferType=OwnershipTransferType,
        OwnershipTransferStatus=OwnershipTransferStatus,
    )


@cemetery_bp.post("/titularidad/casos/<int:case_id>/status")
@login_required
@require_membership
def ownership_case_change_status(case_id: int):
    status = request.form.get("status", "")
    try:
        change_ownership_case_status(case_id, status, current_user.id)
        flash("Estado actualizado", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ownership_case_detail_page", case_id=case_id))


@cemetery_bp.post("/titularidad/casos/<int:case_id>/approve")
@login_required
@require_membership
def ownership_case_approve(case_id: int):
    try:
        approve_ownership_case(case_id, current_user.id)
        flash("Caso aprobado", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ownership_case_detail_page", case_id=case_id))


@cemetery_bp.post("/titularidad/casos/<int:case_id>/reject")
@login_required
@require_membership
def ownership_case_reject(case_id: int):
    reason = request.form.get("reason", "")
    try:
        reject_ownership_case(case_id, reason, current_user.id)
        flash("Caso rechazado", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ownership_case_detail_page", case_id=case_id))


@cemetery_bp.post("/titularidad/casos/<int:case_id>/close")
@login_required
@require_membership
def ownership_case_close(case_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        close_ownership_case(case_id, payload, current_user.id)
        flash("Caso cerrado y titularidad aplicada", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ownership_case_detail_page", case_id=case_id))


@cemetery_bp.post("/titularidad/casos/<int:case_id>/parties")
@login_required
@require_membership
def ownership_case_add_party(case_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        add_case_party(case_id, payload)
        flash("Parte guardada", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ownership_case_detail_page", case_id=case_id))


@cemetery_bp.post("/titularidad/casos/<int:case_id>/publications")
@login_required
@require_membership
def ownership_case_add_publication(case_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        add_case_publication(case_id, payload)
        flash("Publicacion guardada", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ownership_case_detail_page", case_id=case_id))


@cemetery_bp.post("/titularidad/casos/<int:case_id>/documents/<int:doc_id>/upload")
@login_required
@require_membership
def ownership_case_upload_document(case_id: int, doc_id: int):
    file_obj = request.files.get("file")
    try:
        upload_case_document(case_id, doc_id, file_obj, current_user.id)
        flash("Documento subido", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ownership_case_detail_page", case_id=case_id))


@cemetery_bp.post("/titularidad/casos/<int:case_id>/documents/<int:doc_id>/verify")
@login_required
@require_membership
def ownership_case_verify_document(case_id: int, doc_id: int):
    action = request.form.get("action", "verify")
    notes = request.form.get("notes", "")
    try:
        verify_case_document(case_id, doc_id, action, notes, current_user.id)
        flash("Documento actualizado", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ownership_case_detail_page", case_id=case_id))


@cemetery_bp.get("/titularidad/casos/<int:case_id>/resolucion.pdf")
@login_required
@require_membership
def ownership_case_resolution_pdf_route(case_id: int):
    try:
        content, filename = ownership_case_resolution_pdf(case_id)
    except ValueError:
        abort(404)
    response = make_response(content)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f'inline; filename="{filename}"'
    return response


@cemetery_bp.post("/sepulturas/<int:sepultura_id>/derecho/contratar")
@login_required
@require_membership
def contract_create(sepultura_id: int):
    # Spec Cementiri 9.1.7 - Contratacion del derecho funerario
    payload = {k: v for k, v in request.form.items()}
    try:
        create_funeral_right_contract(sepultura_id, payload)
        flash("Contrato creado correctamente", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.grave_detail", sepultura_id=sepultura_id, tab="derecho"))


@cemetery_bp.get("/contratos/<int:contract_id>/titulo.pdf")
@login_required
@require_membership
def contract_title_pdf(contract_id: int):
    # Spec Cementiri 9.1.4 - Generacion del titulo del derecho funerario
    try:
        pdf = funeral_right_title_pdf(contract_id)
    except ValueError:
        abort(404)
    response = make_response(pdf)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f'inline; filename="titulo-contrato-{contract_id}.pdf"'
    return response


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


def _selected_discount_ticket_ids() -> set[int]:
    values = request.form.getlist("discount_ticket_ids")
    return {int(v) for v in values if v.isdigit()}


@cemetery_bp.post("/tasas/cobro/facturar")
@login_required
@require_membership
def fee_generate_invoice():
    # Spec 9.1.3 - Criterio de caja: facturar en el momento del cobro
    sepultura_id = request.form.get("sepultura_id", type=int)
    flash("No disponible. Con criterio de caja se factura al cobrar.", "error")
    return redirect(url_for("cemetery.fee_collection", sepultura_id=sepultura_id))


@cemetery_bp.post("/tasas/cobro/cobrar")
@login_required
@require_membership
def fee_collect_and_receipt():
    # Spec 9.1.3 - Cobrar y emitir recibo en mostrador
    sepultura_id = request.form.get("sepultura_id", type=int)
    selected_ids = _selected_ticket_ids()
    discount_ticket_ids = _selected_discount_ticket_ids()
    payment_method = request.form.get("payment_method", "EFECTIVO")
    try:
        invoice, payment = collect_tickets(
            sepultura_id=sepultura_id,
            selected_ids=selected_ids,
            method=payment_method,
            user_id=current_user.id,
            discount_ticket_ids=discount_ticket_ids,
        )
        flash(f"Cobro registrado. Factura {invoice.numero} / Recibo {payment.receipt_number}", "success")
        return redirect(url_for("cemetery.receipt", payment_id=payment.id))
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("cemetery.fee_collection", sepultura_id=sepultura_id))


@cemetery_bp.post("/contratos/<int:contract_id>/beneficiario/nombrar")
@login_required
@require_membership
def nominate_beneficiary(contract_id: int):
    # Spec Cementiri 9.1.6 - Nomenament de beneficiari desde cobro de tasas
    payload = {k: v for k, v in request.form.items()}
    sepultura_id = request.form.get("sepultura_id", type=int)
    try:
        nominate_contract_beneficiary(contract_id, payload)
        flash("Beneficiario guardado", "success")
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
