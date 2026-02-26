from __future__ import annotations

from datetime import date

from flask import abort, flash, g, make_response, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from app.cemetery import cemetery_bp
from app.cemetery.services import (
    add_case_party,
    add_case_publication,
    approve_ownership_case,
    change_ownership_case_status,
    change_sepultura_state,
    complete_expediente_ot,
    collect_tickets,
    close_ownership_case,
    contract_by_id,
    create_expediente,
    create_expediente_ot,
    create_inscripcion_lateral,
    create_ownership_case,
    create_funeral_right_contract,
    create_mass_sepulturas,
    expediente_by_id,
    expediente_ot_pdf,
    funeral_right_title_pdf,
    generate_maintenance_tickets_for_year,
    lapida_stock_entry,
    lapida_stock_exit,
    list_expediente_ots,
    list_expedientes,
    list_inscripciones,
    list_lapida_stock,
    list_lapida_stock_movements,
    list_ownership_cases,
    nominate_contract_beneficiary,
    ownership_case_document_download,
    ownership_case_detail,
    ownership_case_resolution_pdf,
    org_record,
    paginate_rows,
    panel_data,
    preview_mass_create,
    remove_contract_beneficiary,
    reject_ownership_case,
    reporting_csv_bytes,
    reporting_rows,
    search_sepulturas,
    set_contract_holder_pensioner,
    sepultura_by_id,
    sepultura_tabs_data,
    sepultura_tickets_and_invoices,
    transition_expediente_state,
    transition_inscripcion_estado,
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


@cemetery_bp.route("/expedientes", methods=["GET", "POST"])
@login_required
@require_membership
def expedientes():
    if request.method == "POST":
        payload = {k: v for k, v in request.form.items()}
        try:
            expediente = create_expediente(payload, current_user.id)
            flash(f"Expediente {expediente.numero} creado", "success")
            return redirect(url_for("cemetery.expediente_detail", expediente_id=expediente.id))
        except ValueError as exc:
            flash(str(exc), "error")

    filters = {
        "tipo": request.args.get("tipo", "").strip(),
        "estado": request.args.get("estado", "").strip(),
        "created_from": request.args.get("created_from", "").strip(),
        "created_to": request.args.get("created_to", "").strip(),
        "sepultura_id": request.args.get("sepultura_id", "").strip(),
    }
    rows = list_expedientes(filters)
    return render_template(
        "cemetery/expedientes.html",
        rows=rows,
        filters=filters,
        states=["ABIERTO", "EN_TRAMITE", "FINALIZADO", "CANCELADO"],
    )


@cemetery_bp.get("/expedientes/<int:expediente_id>")
@login_required
@require_membership
def expediente_detail(expediente_id: int):
    try:
        expediente = expediente_by_id(expediente_id)
    except ValueError:
        abort(404)
    ots = list_expediente_ots(expediente.id)
    return render_template(
        "cemetery/expediente_detail.html",
        expediente=expediente,
        ots=ots,
        states=["ABIERTO", "EN_TRAMITE", "FINALIZADO", "CANCELADO"],
    )


@cemetery_bp.post("/expedientes/<int:expediente_id>/estado")
@login_required
@require_membership
def expediente_change_state(expediente_id: int):
    new_state = request.form.get("estado", "")
    try:
        transition_expediente_state(expediente_id, new_state, current_user.id)
        flash("Estado de expediente actualizado", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.expediente_detail", expediente_id=expediente_id))


@cemetery_bp.post("/expedientes/<int:expediente_id>/ot")
@login_required
@require_membership
def expediente_create_ot(expediente_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        ot = create_expediente_ot(expediente_id, payload, current_user.id)
        flash(f"OT #{ot.id} creada", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.expediente_detail", expediente_id=expediente_id))


@cemetery_bp.post("/expedientes/<int:expediente_id>/ot/<int:ot_id>/completar")
@login_required
@require_membership
def expediente_complete_ot(expediente_id: int, ot_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        complete_expediente_ot(expediente_id, ot_id, payload, current_user.id)
        flash(f"OT #{ot_id} completada", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.expediente_detail", expediente_id=expediente_id))


@cemetery_bp.get("/expedientes/<int:expediente_id>/ot/<int:ot_id>/orden.pdf")
@login_required
@require_membership
def expediente_ot_order_pdf(expediente_id: int, ot_id: int):
    try:
        content = expediente_ot_pdf(expediente_id, ot_id)
    except ValueError:
        abort(404)
    response = make_response(content)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f'inline; filename="orden-trabajo-{ot_id}.pdf"'
    return response


@cemetery_bp.get("/lapidas")
@login_required
@require_membership
def lapidas():
    filters = {
        "estado": request.args.get("estado", "").strip(),
        "sepultura_id": request.args.get("sepultura_id", "").strip(),
        "texto": request.args.get("texto", "").strip(),
    }
    return render_template(
        "cemetery/lapidas.html",
        stock_rows=list_lapida_stock(),
        stock_movements=list_lapida_stock_movements(),
        inscripciones=list_inscripciones(filters),
        filters=filters,
        states=["PENDIENTE_GRABAR", "PENDIENTE_COLOCAR", "PENDIENTE_NOTIFICAR", "NOTIFICADA"],
    )


@cemetery_bp.post("/lapidas/stock/entrada")
@login_required
@require_membership
def lapidas_stock_entrada():
    payload = {k: v for k, v in request.form.items()}
    try:
        stock = lapida_stock_entry(payload, current_user.id)
        flash(f"Entrada de stock aplicada ({stock.codigo})", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.lapidas"))


@cemetery_bp.post("/lapidas/stock/salida")
@login_required
@require_membership
def lapidas_stock_salida():
    payload = {k: v for k, v in request.form.items()}
    try:
        stock = lapida_stock_exit(payload, current_user.id)
        flash(f"Salida de stock aplicada ({stock.codigo})", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.lapidas"))


@cemetery_bp.post("/lapidas/inscripciones")
@login_required
@require_membership
def lapidas_create_inscripcion():
    payload = {k: v for k, v in request.form.items()}
    try:
        item = create_inscripcion_lateral(payload, current_user.id)
        flash(f"Inscripcion #{item.id} creada", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.lapidas"))


@cemetery_bp.post("/lapidas/inscripciones/<int:inscripcion_id>/estado")
@login_required
@require_membership
def lapidas_change_inscripcion_state(inscripcion_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        item = transition_inscripcion_estado(inscripcion_id, payload, current_user.id)
        flash(f"Inscripcion #{item.id} en estado {item.estado}", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.lapidas"))


def _report_filters() -> dict[str, str]:
    return {
        "estado": request.args.get("estado", "").strip(),
        "modalidad": request.args.get("modalidad", "").strip(),
        "bloque": request.args.get("bloque", "").strip(),
        "tipo": request.args.get("tipo", "").strip(),
        "vigencia": request.args.get("vigencia", "").strip(),
        "titular": request.args.get("titular", "").strip(),
        "contrato_id": request.args.get("contrato_id", "").strip(),
    }


@cemetery_bp.get("/reporting")
@login_required
@require_membership
def reporting():
    report_key = request.args.get("report", "sepulturas").strip().lower() or "sepulturas"
    filters = _report_filters()
    try:
        rows = reporting_rows(report_key, filters)
    except ValueError as exc:
        flash(str(exc), "error")
        report_key = "sepulturas"
        rows = reporting_rows(report_key, filters)
    page = request.args.get("page", type=int, default=1) or 1
    page_size = request.args.get("page_size", type=int, default=25) or 25
    paged = paginate_rows(rows, page=page, page_size=page_size)
    return render_template(
        "cemetery/reporting.html",
        report_key=report_key,
        filters=filters,
        paged=paged,
        money=money,
    )


@cemetery_bp.get("/reporting/export.csv")
@login_required
@require_membership
def reporting_export_csv():
    report_key = request.args.get("report", "sepulturas").strip().lower() or "sepulturas"
    filters = _report_filters()
    try:
        content = reporting_csv_bytes(report_key, filters, export_limit=1000)
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("cemetery.reporting"))
    response = make_response(content)
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = f'attachment; filename="{report_key}.csv"'
    return response


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


@cemetery_bp.get("/titularidad")
@login_required
@require_membership
def ownership_cases_alias():
    return redirect(url_for("cemetery.ownership_cases"))


@cemetery_bp.route("/titularidad/casos", methods=["GET", "POST"])
@login_required
@require_membership
def ownership_cases():
    if request.method == "POST":
        if not g.membership or (g.membership.role or "").lower() != "admin":
            abort(403)
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
@require_role("admin")
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
@require_role("admin")
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
@require_role("admin")
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
@require_role("admin")
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
@require_role("admin")
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
@require_role("admin")
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
@require_role("admin")
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
@require_role("admin")
def ownership_case_verify_document(case_id: int, doc_id: int):
    action = request.form.get("action", "verify")
    notes = request.form.get("notes", "")
    try:
        verify_case_document(case_id, doc_id, action, notes, current_user.id)
        flash("Documento actualizado", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ownership_case_detail_page", case_id=case_id))


@cemetery_bp.get("/titularidad/casos/<int:case_id>/documents/<int:doc_id>/download")
@login_required
@require_membership
def ownership_case_download_document(case_id: int, doc_id: int):
    try:
        content, filename = ownership_case_document_download(case_id, doc_id)
    except ValueError:
        abort(404)
    response = make_response(content)
    response.headers["Content-Type"] = "application/octet-stream"
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


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
@require_role("admin")
def nominate_beneficiary(contract_id: int):
    # Spec Cementiri 9.1.6 - Nomenament de beneficiari desde cobro de tasas
    payload = {k: v for k, v in request.form.items()}
    sepultura_id = request.form.get("sepultura_id", type=int)
    try:
        nominate_contract_beneficiary(contract_id, payload, current_user.id)
        flash("Beneficiario guardado", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.fee_collection", sepultura_id=sepultura_id))


@cemetery_bp.post("/contratos/<int:contract_id>/titular/pensionista")
@login_required
@require_membership
@require_role("admin")
def mark_holder_pensioner(contract_id: int):
    payload = {k: v for k, v in request.form.items()}
    sepultura_id = request.form.get("sepultura_id", type=int)
    try:
        set_contract_holder_pensioner(contract_id, payload, current_user.id)
        flash("Titular marcado como pensionista", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    target_sepultura = sepultura_id
    if not target_sepultura:
        try:
            contrato = contract_by_id(contract_id)
            target_sepultura = contrato.sepultura_id
        except ValueError:
            target_sepultura = None
    if target_sepultura:
        return redirect(url_for("cemetery.grave_detail", sepultura_id=target_sepultura, tab="titulares"))
    return redirect(url_for("cemetery.search_graves"))


@cemetery_bp.post("/contratos/<int:contract_id>/beneficiario/eliminar")
@login_required
@require_membership
@require_role("admin")
def remove_beneficiary(contract_id: int):
    payload = {k: v for k, v in request.form.items()}
    sepultura_id = request.form.get("sepultura_id", type=int)
    try:
        remove_contract_beneficiary(contract_id, payload, current_user.id)
        flash("Beneficiario eliminado", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    target_sepultura = sepultura_id
    if not target_sepultura:
        try:
            contrato = contract_by_id(contract_id)
            target_sepultura = contrato.sepultura_id
        except ValueError:
            target_sepultura = None
    if target_sepultura:
        return redirect(url_for("cemetery.grave_detail", sepultura_id=target_sepultura, tab="beneficiarios"))
    return redirect(url_for("cemetery.search_graves"))


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
