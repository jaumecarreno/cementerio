from __future__ import annotations

from datetime import date

from flask import (
    abort,
    current_app,
    flash,
    g,
    make_response,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_login import current_user, login_required
from sqlalchemy import or_

from app.cemetery import cemetery_bp
from app.cemetery.work_order_service import (
    OT_EVENT_TYPES,
    add_work_order_checklist_item,
    add_work_order_dependency,
    add_work_order_evidence,
    assign_work_order,
    create_work_order,
    create_work_order_event_rule,
    create_work_order_template,
    create_work_order_type,
    detail_payload as work_order_detail_payload,
    kanban_work_orders,
    list_active_types,
    list_event_rules,
    list_templates,
    list_users_for_assignment,
    list_work_orders as list_native_work_orders,
    transition_work_order,
    update_work_order_checklist_item,
    work_order_pdf_bytes,
)
from app.cemetery.operation_service import (
    change_operation_status,
    close_operation_case,
    create_operation_case,
    create_operation_work_order,
    list_operation_cases,
    operation_acta_pdf,
    operation_case_by_id,
    upload_operation_document,
    verify_operation_document,
    verify_operation_permit,
)
from app.cemetery.billing_v2_service import (
    create_credit_note,
    create_invoice_draft,
    issue_invoice,
    payment_receipt_by_id,
    register_payment,
    retry_fiscal_submission,
    workspace_data as billing_workspace_data,
)
from app.cemetery.services import (
    REPORTING_ALL_KEYS,
    REPORTING_SCREEN_KEYS,
    active_contract_for_sepultura,
    add_case_party,
    add_case_publication,
    add_deceased_to_sepultura,
    approve_ownership_case,
    change_ownership_case_status,
    change_sepultura_state,
    complete_expediente_ot,
    close_ownership_case,
    contract_by_id,
    create_expediente,
    create_expediente_ot,
    create_inscripcion_lateral,
    create_person,
    create_ownership_case,
    create_funeral_right_contract,
    create_mass_sepulturas,
    expediente_by_id,
    expediente_ot_pdf,
    funeral_right_title_pdf,
    lapida_stock_entry,
    lapida_stock_exit,
    list_expediente_ots,
    list_work_orders,
    list_expedientes,
    list_inscripciones,
    list_lapida_stock,
    list_lapida_stock_movements,
    list_ownership_cases,
    list_people,
    list_people_paged,
    list_sepultura_blocks,
    list_sepultura_modalidades,
    nominate_contract_beneficiary,
    ownership_case_document_download,
    ownership_case_detail,
    ownership_case_resolution_pdf,
    org_record,
    person_by_id,
    paginate_rows,
    panel_data,
    preview_mass_create,
    remove_contract_beneficiary,
    remove_deceased_from_sepultura,
    reject_ownership_case,
    reporting_csv_bytes,
    reporting_filter_blocks,
    reporting_filter_type_codes,
    reporting_filter_users,
    reporting_pdf_bytes,
    reporting_rows,
    reporting_schedule_schema_ready,
    run_reporting_schedule,
    search_sepulturas_paged,
    sepultura_location_options_by_block,
    set_contract_holder_pensioner,
    sepultura_by_id,
    sepultura_tabs_data,
    transition_expediente_state,
    transition_inscripcion_estado,
    update_sepultura_notes,
    update_person,
    upload_case_document,
    create_reporting_schedule,
    list_reporting_schedules,
    toggle_reporting_schedule,
    verify_case_document,
)
from app.core.models import (
    BillingDocumentStatus,
    DerechoFunerarioContrato,
    OperationStatus,
    OperationType,
    PaymentMethod,
    Person,
    ReportDeliveryLog,
    Sepultura,
    WorkOrderAreaType,
    WorkOrderCategory,
    WorkOrderPriority,
    WorkOrderStatus,
    OwnershipTransferStatus,
    OwnershipTransferType,
    SepulturaEstado,
)
from app.core.permissions import require_membership, require_role
from app.core.utils import money


def _is_htmx() -> bool:
    return request.headers.get("HX-Request", "").lower() == "true"


def _actor_role() -> str:
    membership = getattr(g, "membership", None)
    return (membership.role or "").lower() if membership else ""


def _normalize_operation_payload(payload: dict[str, str]) -> dict[str, str]:
    normalized = dict(payload)
    if "type" not in normalized and normalized.get("tipo"):
        normalized["type"] = normalized["tipo"]
    if "source_sepultura_id" not in normalized and normalized.get("sepultura_id"):
        normalized["source_sepultura_id"] = normalized["sepultura_id"]
    if "deceased_person_id" not in normalized and normalized.get("difunto_id"):
        normalized["deceased_person_id"] = normalized["difunto_id"]
    if "declarant_person_id" not in normalized and normalized.get("declarante_id"):
        normalized["declarant_person_id"] = normalized["declarante_id"]
    if "scheduled_at" not in normalized and normalized.get("fecha_prevista"):
        raw_date = (normalized.get("fecha_prevista") or "").strip()
        if raw_date:
            normalized["scheduled_at"] = f"{raw_date}T00:00"
    if "notes" not in normalized and normalized.get("notas"):
        normalized["notes"] = normalized["notas"]
    return normalized


def _operation_case_filters() -> dict[str, str]:
    return {
        "code": request.args.get("code", "").strip(),
        "type": request.args.get("type", "").strip(),
        "status": request.args.get("status", "").strip(),
        "source_sepultura_id": request.args.get("source_sepultura_id", "").strip(),
        "deceased_person_id": request.args.get("deceased_person_id", "").strip(),
        "created_from": request.args.get("created_from", "").strip(),
        "created_to": request.args.get("created_to", "").strip(),
    }


def _render_operation_cases_page():
    if request.method == "POST":
        payload = _normalize_operation_payload({k: v for k, v in request.form.items()})
        try:
            created = create_operation_case(payload, current_user.id)
            flash(f"Expediente {created.code} creado", "success")
            return redirect(url_for("cemetery.expediente_detail", expediente_id=created.id))
        except ValueError as exc:
            flash(str(exc), "error")

    filters = _operation_case_filters()
    rows = list_operation_cases(filters)
    create_type = (request.args.get("create_type") or "").strip().upper()
    if create_type not in {item.value for item in OperationType}:
        create_type = "INHUMACION"
    return render_template(
        "cemetery/expedientes.html",
        rows=rows,
        filters=filters,
        OperationType=OperationType,
        OperationStatus=OperationStatus,
        create_type=create_type,
        prefill_source_sepultura_id=request.args.get("prefill_source_sepultura_id", "").strip(),
        prefill_target_sepultura_id=request.args.get("prefill_target_sepultura_id", "").strip(),
        prefill_deceased_person_id=request.args.get("prefill_deceased_person_id", "").strip(),
    )


@cemetery_bp.get("/panel")
@login_required
@require_membership
def panel():
    # Spec 9.0 + mockups_v2/page-2 - Panel de trabajo Cementerio
    data = panel_data()
    return render_template(
        "cemetery/panel.html", data=data, current_year=date.today().year
    )


@cemetery_bp.get("/inhumaciones/asistente")
@login_required
@require_membership
def inhumation_assistant():
    return render_template("cemetery/inhumation_assistant.html")


@cemetery_bp.get("/personas")
@login_required
@require_membership
def people_list():
    # Spec Cementiri: ver cementerio_extract.md (9.4.3 / 9.4.4)
    filters = {"q": request.args.get("q", "").strip()}
    page = request.args.get("page", type=int, default=1) or 1
    page_size = request.args.get("page_size", type=int, default=25) or 25
    paged = list_people_paged(filters["q"], page=page, page_size=page_size)
    return render_template("cemetery/personas.html", paged=paged, filters=filters)


@cemetery_bp.route("/personas/nueva", methods=["GET", "POST"])
@login_required
@require_membership
def person_new():
    # Spec Cementiri: ver cementerio_extract.md (9.4.3 / 9.4.4 / 9.1.6)
    form_data = dict(request.form.items()) if request.method == "POST" else {}
    if request.method == "POST":
        try:
            person = create_person(form_data, user_id=current_user.id)
            flash(f"Persona {person.full_name} creada", "success")
            return redirect(url_for("cemetery.person_edit", person_id=person.id))
        except ValueError as exc:
            flash(str(exc), "error")
    return render_template(
        "cemetery/person_form.html",
        person=None,
        form_data=form_data,
        form_action=url_for("cemetery.person_new"),
        title="Nueva persona",
    )


@cemetery_bp.route("/personas/<int:person_id>/editar", methods=["GET", "POST"])
@login_required
@require_membership
def person_edit(person_id: int):
    # Spec Cementiri: ver cementerio_extract.md (9.4.3 / 9.4.4)
    try:
        person = person_by_id(person_id)
    except ValueError:
        abort(404)
    form_data = dict(request.form.items()) if request.method == "POST" else {}
    if request.method == "POST":
        try:
            person = update_person(person_id, form_data, user_id=current_user.id)
            flash("Persona actualizada", "success")
            return redirect(url_for("cemetery.person_edit", person_id=person.id))
        except ValueError as exc:
            flash(str(exc), "error")
    return render_template(
        "cemetery/person_form.html",
        person=person,
        form_data=form_data,
        form_action=url_for("cemetery.person_edit", person_id=person.id),
        title=f"Editar persona #{person.id}",
    )


@cemetery_bp.get("/personas/picker/search")
@login_required
@require_membership
def person_picker_search():
    # Spec Cementiri: ver cementerio_extract.md (9.1.6 / 9.1.7)
    picker_id = (request.args.get("picker_id") or "").strip()
    field_name = (request.args.get("field_name") or "person_id").strip() or "person_id"
    query_text = request.args.get("q", "").strip()
    rows = list_people(query_text, limit=25) if query_text else []
    return render_template(
        "components/person_picker_results.html",
        rows=rows,
        picker_id=picker_id,
        field_name=field_name,
        query_text=query_text,
    )


@cemetery_bp.post("/personas/picker/create")
@login_required
@require_membership
def person_picker_create():
    # Spec Cementiri: ver cementerio_extract.md (9.1.6 / 9.1.7)
    picker_id = (request.form.get("picker_id") or "").strip()
    field_name = (request.form.get("field_name") or "person_id").strip() or "person_id"
    payload = {k: v for k, v in request.form.items()}
    try:
        person = create_person(payload, user_id=current_user.id)
        return render_template(
            "components/person_picker_create_result.html",
            picker_id=picker_id,
            field_name=field_name,
            person=person,
            error="",
        )
    except ValueError as exc:
        return render_template(
            "components/person_picker_create_result.html",
            picker_id=picker_id,
            field_name=field_name,
            person=None,
            error=str(exc),
        )


@cemetery_bp.route("/operaciones", methods=["GET", "POST"])
@login_required
@require_membership
def operations():
    return _render_operation_cases_page()


@cemetery_bp.get("/operaciones/<int:case_id>")
@login_required
@require_membership
def operation_detail(case_id: int):
    try:
        case = operation_case_by_id(case_id)
    except ValueError:
        abort(404)
    return render_template(
        "cemetery/expediente_detail.html",
        case=case,
        OperationType=OperationType,
        OperationStatus=OperationStatus,
        WorkOrderStatus=WorkOrderStatus,
    )


@cemetery_bp.post("/operaciones/<int:case_id>/estado")
@login_required
@require_membership
def operation_change_state(case_id: int):
    try:
        change_operation_status(
            case_id=case_id,
            new_status_raw=request.form.get("status", ""),
            reason=request.form.get("reason", ""),
            user_id=current_user.id,
        )
        flash("Estado de expediente actualizado", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.expediente_detail", expediente_id=case_id))


@cemetery_bp.post("/operaciones/<int:case_id>/permisos/<int:permit_id>/verify")
@login_required
@require_membership
def operation_permit_verify(case_id: int, permit_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        verify_operation_permit(case_id, permit_id, payload, current_user.id)
        flash("Permiso actualizado", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.expediente_detail", expediente_id=case_id))


@cemetery_bp.post("/operaciones/<int:case_id>/documentos/upload")
@login_required
@require_membership
def operation_document_upload(case_id: int):
    payload = {k: v for k, v in request.form.items()}
    file_obj = request.files.get("file")
    try:
        upload_operation_document(case_id, payload, file_obj, current_user.id)
        flash("Documento subido", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.expediente_detail", expediente_id=case_id))


@cemetery_bp.post("/operaciones/<int:case_id>/documentos/<int:doc_id>/verify")
@login_required
@require_membership
def operation_document_verify(case_id: int, doc_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        verify_operation_document(case_id, doc_id, payload, current_user.id)
        flash("Documento actualizado", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.expediente_detail", expediente_id=case_id))


@cemetery_bp.post("/operaciones/<int:case_id>/ot")
@login_required
@require_membership
def operation_create_ot(case_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        row = create_operation_work_order(case_id, payload, current_user.id)
        flash(f"OT {row.code} creada para el expediente", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.expediente_detail", expediente_id=case_id))


@cemetery_bp.post("/operaciones/<int:case_id>/cerrar")
@login_required
@require_membership
def operation_close(case_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        close_operation_case(case_id, payload, current_user.id)
        flash("Expediente cerrado", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.expediente_detail", expediente_id=case_id))


@cemetery_bp.get("/operaciones/<int:case_id>/acta.pdf")
@login_required
@require_membership
def operation_acta(case_id: int):
    try:
        content, filename = operation_acta_pdf(case_id, current_user.id)
    except ValueError:
        abort(404)
    response = make_response(content)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f'inline; filename="{filename}"'
    return response


@cemetery_bp.route("/expedientes", methods=["GET", "POST"])
@login_required
@require_membership
def expedientes():
    return _render_operation_cases_page()


@cemetery_bp.get("/expedientes/<int:expediente_id>")
@login_required
@require_membership
def expediente_detail(expediente_id: int):
    return operation_detail(expediente_id)


@cemetery_bp.post("/expedientes/<int:expediente_id>/estado")
@login_required
@require_membership
def expediente_change_state(expediente_id: int):
    return operation_change_state(expediente_id)


@cemetery_bp.post("/expedientes/<int:expediente_id>/ot")
@login_required
@require_membership
def expediente_create_ot(expediente_id: int):
    return operation_create_ot(expediente_id)


@cemetery_bp.post("/expedientes/<int:expediente_id>/ot/<int:ot_id>/completar")
@login_required
@require_membership
def expediente_complete_ot(expediente_id: int, ot_id: int):
    abort(404)


@cemetery_bp.get("/expedientes/<int:expediente_id>/ot/<int:ot_id>/orden.pdf")
@login_required
@require_membership
def expediente_ot_order_pdf(expediente_id: int, ot_id: int):
    abort(404)


@cemetery_bp.post("/expedientes/<int:expediente_id>/permisos/<int:permit_id>/verify")
@login_required
@require_membership
def expediente_permit_verify(expediente_id: int, permit_id: int):
    return operation_permit_verify(expediente_id, permit_id)


@cemetery_bp.post("/expedientes/<int:expediente_id>/documentos/upload")
@login_required
@require_membership
def expediente_document_upload(expediente_id: int):
    return operation_document_upload(expediente_id)


@cemetery_bp.post("/expedientes/<int:expediente_id>/documentos/<int:doc_id>/verify")
@login_required
@require_membership
def expediente_document_verify(expediente_id: int, doc_id: int):
    return operation_document_verify(expediente_id, doc_id)


@cemetery_bp.post("/expedientes/<int:expediente_id>/cerrar")
@login_required
@require_membership
def expediente_close(expediente_id: int):
    return operation_close(expediente_id)


@cemetery_bp.get("/expedientes/<int:expediente_id>/acta.pdf")
@login_required
@require_membership
def expediente_acta(expediente_id: int):
    return operation_acta(expediente_id)


@cemetery_bp.get("/expedientes/picker/sepulturas")
@login_required
@require_membership
def expediente_sepultura_picker():
    target_field = (request.args.get("target_field") or "source_sepultura_id").strip()
    label_field = (request.args.get("label_field") or "").strip()
    value_mode = (request.args.get("value_mode") or "sepultura_id").strip().lower()
    if value_mode not in {"sepultura_id", "contract_id"}:
        value_mode = "sepultura_id"
    filters = {
        "q": request.args.get("q", "").strip(),
        "sepultura_id": request.args.get("sepultura_id", "").strip(),
        "bloque": request.args.get("bloque", "").strip(),
        "fila": request.args.get("fila", "").strip(),
        "columna": request.args.get("columna", "").strip(),
        "numero": request.args.get("numero", "").strip(),
        "via": request.args.get("via", "").strip(),
        "modalidad": request.args.get("modalidad", "").strip(),
        "estado": request.args.get("estado", "").strip(),
    }

    query = Sepultura.query.filter_by(org_id=org_record().id)
    has_invalid_numeric = False

    if filters["sepultura_id"]:
        if filters["sepultura_id"].isdigit():
            query = query.filter(Sepultura.id == int(filters["sepultura_id"]))
        else:
            has_invalid_numeric = True
    if filters["bloque"]:
        query = query.filter(Sepultura.bloque.ilike(f"%{filters['bloque']}%"))
    if filters["fila"]:
        if filters["fila"].isdigit():
            query = query.filter(Sepultura.fila == int(filters["fila"]))
        else:
            has_invalid_numeric = True
    if filters["columna"]:
        if filters["columna"].isdigit():
            query = query.filter(Sepultura.columna == int(filters["columna"]))
        else:
            has_invalid_numeric = True
    if filters["numero"]:
        if filters["numero"].isdigit():
            query = query.filter(Sepultura.numero == int(filters["numero"]))
        else:
            has_invalid_numeric = True
    if filters["via"]:
        query = query.filter(Sepultura.via.ilike(f"%{filters['via']}%"))
    if filters["modalidad"]:
        query = query.filter(Sepultura.modalidad.ilike(f"%{filters['modalidad']}%"))
    if filters["estado"]:
        try:
            query = query.filter(Sepultura.estado == SepulturaEstado[filters["estado"].upper()])
        except KeyError:
            query = query.filter(Sepultura.id == -1)

    if filters["q"]:
        pattern = f"%{filters['q']}%"
        conditions = [
            Sepultura.bloque.ilike(pattern),
            Sepultura.via.ilike(pattern),
            Sepultura.modalidad.ilike(pattern),
        ]
        if filters["q"].isdigit():
            value = int(filters["q"])
            conditions.extend(
                [
                    Sepultura.id == value,
                    Sepultura.numero == value,
                    Sepultura.fila == value,
                    Sepultura.columna == value,
                ]
            )
        query = query.filter(or_(*conditions))

    if has_invalid_numeric:
        rows = []
    else:
        rows = (
            query.order_by(
                Sepultura.bloque.asc(),
                Sepultura.fila.asc(),
                Sepultura.columna.asc(),
                Sepultura.numero.asc(),
            )
            .limit(100)
            .all()
        )

    active_contract_id_by_sepultura: dict[int, int] = {}
    if rows and value_mode == "contract_id":
        today = date.today()
        sep_ids = [row.id for row in rows]
        contracts = (
            DerechoFunerarioContrato.query.filter_by(org_id=org_record().id, estado="ACTIVO")
            .filter(DerechoFunerarioContrato.sepultura_id.in_(sep_ids))
            .filter(DerechoFunerarioContrato.fecha_inicio <= today)
            .filter(DerechoFunerarioContrato.fecha_fin >= today)
            .order_by(DerechoFunerarioContrato.sepultura_id.asc(), DerechoFunerarioContrato.id.asc())
            .all()
        )
        for contract in contracts:
            active_contract_id_by_sepultura.setdefault(contract.sepultura_id, contract.id)

    return render_template(
        "cemetery/expediente_picker_sepulturas.html",
        rows=rows,
        filters=filters,
        target_field=target_field,
        label_field=label_field,
        value_mode=value_mode,
        active_contract_id_by_sepultura=active_contract_id_by_sepultura,
        sepultura_blocks=list_sepultura_blocks(),
        sepultura_states=[state.value for state in SepulturaEstado],
    )


@cemetery_bp.get("/expedientes/picker/personas")
@login_required
@require_membership
def expediente_person_picker():
    target_field = (request.args.get("target_field") or "deceased_person_id").strip()
    label_field = (request.args.get("label_field") or "").strip()
    filters = {
        "q": request.args.get("q", "").strip(),
        "person_id": request.args.get("person_id", "").strip(),
        "first_name": request.args.get("first_name", "").strip(),
        "last_name": request.args.get("last_name", "").strip(),
        "dni_nif": request.args.get("dni_nif", "").strip(),
    }

    query = Person.query.filter_by(org_id=org_record().id)
    if filters["person_id"]:
        if filters["person_id"].isdigit():
            query = query.filter(Person.id == int(filters["person_id"]))
        else:
            query = query.filter(Person.id == -1)
    if filters["first_name"]:
        query = query.filter(Person.first_name.ilike(f"%{filters['first_name']}%"))
    if filters["last_name"]:
        query = query.filter(Person.last_name.ilike(f"%{filters['last_name']}%"))
    if filters["dni_nif"]:
        query = query.filter(Person.dni_nif.ilike(f"%{filters['dni_nif']}%"))
    if filters["q"]:
        pattern = f"%{filters['q']}%"
        conditions = [
            Person.first_name.ilike(pattern),
            Person.last_name.ilike(pattern),
            Person.dni_nif.ilike(pattern),
            Person.telefono.ilike(pattern),
            Person.telefono2.ilike(pattern),
            Person.email.ilike(pattern),
            Person.email2.ilike(pattern),
            Person.direccion_linea.ilike(pattern),
            Person.poblacion.ilike(pattern),
        ]
        if filters["q"].isdigit():
            conditions.append(Person.id == int(filters["q"]))
        query = query.filter(or_(*conditions))

    rows = (
        query.order_by(Person.last_name.asc(), Person.first_name.asc(), Person.id.asc())
        .limit(100)
        .all()
    )
    return render_template(
        "cemetery/expediente_picker_personas.html",
        rows=rows,
        filters=filters,
        target_field=target_field,
        label_field=label_field,
    )




@cemetery_bp.route("/ordenes-trabajo", methods=["GET", "POST"])
@login_required
@require_membership
def work_orders():
    if request.method == "POST":
        return redirect(url_for("cemetery.ot_new"))
    return ot_list()


@cemetery_bp.get("/ot")
@login_required
@require_membership
def ot_list():
    filters = {
        "status": request.args.get("status", "").strip(),
        "priority": request.args.get("priority", "").strip(),
        "category": request.args.get("category", "").strip(),
        "type_code": request.args.get("type_code", "").strip(),
        "sepultura_id": request.args.get("sepultura_id", "").strip(),
        "area_type": request.args.get("area_type", "").strip(),
        "assigned_user_id": request.args.get("assigned_user_id", "").strip(),
        "sla_overdue": request.args.get("sla_overdue", "").strip(),
        "date_from": request.args.get("date_from", "").strip(),
        "date_to": request.args.get("date_to", "").strip(),
        "q": request.args.get("q", "").strip(),
    }
    view_mode = (request.args.get("view") or "table").strip().lower() or "table"
    rows = list_native_work_orders(filters)
    kanban = kanban_work_orders(filters) if view_mode == "kanban" else {}
    return render_template(
        "cemetery/ot_list.html",
        rows=rows,
        kanban=kanban,
        filters=filters,
        view_mode=view_mode,
        users=list_users_for_assignment(),
        types=list_active_types(),
        statuses=[status.value for status in WorkOrderStatus],
        priorities=[priority.value for priority in WorkOrderPriority],
        categories=[category.value for category in WorkOrderCategory],
        area_types=[area.value for area in WorkOrderAreaType],
    )


@cemetery_bp.route("/ot/nueva", methods=["GET", "POST"])
@login_required
@require_membership
def ot_new():
    role = _actor_role()
    if role != "admin":
        abort(403)

    form_data = dict(request.form.items()) if request.method == "POST" else {
        "sepultura_id": request.args.get("sepultura_id", "").strip(),
        "category": request.args.get("category", "").strip(),
    }
    if request.method == "POST":
        try:
            row = create_work_order(form_data, current_user.id)
            flash(f"OT {row.code} creada", "success")
            return redirect(url_for("cemetery.ot_detail", ot_id=row.id))
        except ValueError as exc:
            flash(str(exc), "error")
    return render_template(
        "cemetery/ot_form.html",
        form_data=form_data,
        users=list_users_for_assignment(),
        templates=list_templates(active_only=True),
        types=list_active_types(),
        statuses=[status.value for status in WorkOrderStatus],
        priorities=[priority.value for priority in WorkOrderPriority],
        categories=[category.value for category in WorkOrderCategory],
        area_types=[area.value for area in WorkOrderAreaType],
    )


@cemetery_bp.get("/ot/<int:ot_id>")
@login_required
@require_membership
def ot_detail(ot_id: int):
    try:
        data = work_order_detail_payload(ot_id)
    except ValueError:
        abort(404)
    return render_template(
        "cemetery/ot_detail.html",
        data=data,
        users=list_users_for_assignment(),
    )


@cemetery_bp.get("/ot/<int:ot_id>/orden.pdf")
@login_required
@require_membership
def ot_pdf(ot_id: int):
    try:
        content = work_order_pdf_bytes(ot_id)
    except ValueError:
        abort(404)
    response = make_response(content)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f'inline; filename="orden-trabajo-{ot_id}.pdf"'
    return response


@cemetery_bp.post("/ot/<int:ot_id>/estado")
@login_required
@require_membership
def ot_change_state(ot_id: int):
    try:
        transition_work_order(
            ot_id,
            request.form.get("status", ""),
            request.form.get("reason", ""),
            current_user.id,
            _actor_role(),
        )
        flash("Estado OT actualizado", "success")
    except (ValueError, PermissionError) as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ot_detail", ot_id=ot_id))


@cemetery_bp.post("/ot/<int:ot_id>/asignar")
@login_required
@require_membership
def ot_assign(ot_id: int):
    assigned_user_id = request.form.get("assigned_user_id", type=int)
    try:
        assign_work_order(ot_id, assigned_user_id, current_user.id, _actor_role())
        flash("Asignacion OT actualizada", "success")
    except (ValueError, PermissionError) as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ot_detail", ot_id=ot_id))


@cemetery_bp.post("/ot/<int:ot_id>/checklist")
@login_required
@require_membership
def ot_checklist(ot_id: int):
    item_id = request.form.get("item_id", type=int)
    if not item_id:
        flash("Item checklist invalido", "error")
        return redirect(url_for("cemetery.ot_detail", ot_id=ot_id))
    try:
        update_work_order_checklist_item(
            ot_id,
            item_id,
            request.form.get("done") == "1",
            request.form.get("notes", ""),
            current_user.id,
        )
        flash("Checklist OT actualizada", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ot_detail", ot_id=ot_id))


@cemetery_bp.post("/ot/<int:ot_id>/checklist/add")
@login_required
@require_membership
def ot_checklist_add(ot_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        add_work_order_checklist_item(ot_id, payload, _actor_role())
        flash("Item checklist agregado", "success")
    except (ValueError, PermissionError) as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ot_detail", ot_id=ot_id))


@cemetery_bp.post("/ot/<int:ot_id>/evidencias")
@login_required
@require_membership
def ot_evidences(ot_id: int):
    file_obj = request.files.get("evidence")
    try:
        add_work_order_evidence(
            ot_id,
            file_obj,
            request.form.get("notes", ""),
            current_user.id,
            current_app.instance_path,
        )
        flash("Evidencia OT subida", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ot_detail", ot_id=ot_id))


@cemetery_bp.post("/ot/<int:ot_id>/dependencias")
@login_required
@require_membership
def ot_add_dependency(ot_id: int):
    depends_on_id = request.form.get("depends_on_work_order_id", type=int)
    if not depends_on_id:
        flash("Dependencia invalida", "error")
        return redirect(url_for("cemetery.ot_detail", ot_id=ot_id))
    try:
        add_work_order_dependency(ot_id, depends_on_id, current_user.id, _actor_role())
        flash("Dependencia agregada", "success")
    except (ValueError, PermissionError) as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.ot_detail", ot_id=ot_id))


@cemetery_bp.route("/ot/config/tipos", methods=["GET", "POST"])
@login_required
@require_membership
@require_role("admin")
def ot_config_types():
    if request.method == "POST":
        payload = {k: v for k, v in request.form.items()}
        try:
            create_work_order_type(payload, _actor_role())
            flash("Tipo OT guardado", "success")
        except (ValueError, PermissionError) as exc:
            flash(str(exc), "error")
    return render_template(
        "cemetery/ot_config_types.html",
        rows=list_active_types(),
        categories=[category.value for category in WorkOrderCategory],
    )


@cemetery_bp.route("/ot/config/plantillas", methods=["GET", "POST"])
@login_required
@require_membership
@require_role("admin")
def ot_config_templates():
    if request.method == "POST":
        payload = {k: v for k, v in request.form.items()}
        try:
            create_work_order_template(payload, _actor_role())
            flash("Plantilla OT guardada", "success")
        except (ValueError, PermissionError) as exc:
            flash(str(exc), "error")
    return render_template(
        "cemetery/ot_config_templates.html",
        rows=list_templates(),
        types=list_active_types(),
        priorities=[priority.value for priority in WorkOrderPriority],
    )


@cemetery_bp.route("/ot/config/reglas", methods=["GET", "POST"])
@login_required
@require_membership
@require_role("admin")
def ot_config_rules():
    if request.method == "POST":
        payload = {k: v for k, v in request.form.items()}
        try:
            create_work_order_event_rule(payload, _actor_role())
            flash("Regla OT guardada", "success")
        except (ValueError, PermissionError) as exc:
            flash(str(exc), "error")
    return render_template(
        "cemetery/ot_config_rules.html",
        rows=list_event_rules(),
        templates=list_templates(active_only=True),
        event_types=OT_EVENT_TYPES,
    )

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
        states=[
            "PENDIENTE_GRABAR",
            "PENDIENTE_COLOCAR",
            "PENDIENTE_NOTIFICAR",
            "NOTIFICADA",
        ],
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
        "profile": request.args.get("profile", "").strip(),
        "date_from": request.args.get("date_from", "").strip(),
        "date_to": request.args.get("date_to", "").strip(),
        "cadence_preset": request.args.get("cadence_preset", "").strip(),
        "assigned_user_id": request.args.get("assigned_user_id", "").strip(),
        "type_code": request.args.get("type_code", "").strip(),
        "category": request.args.get("category", "").strip(),
        "status": request.args.get("status", "").strip(),
        "sepultura_id": request.args.get("sepultura_id", "").strip(),
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
    profile = (request.args.get("profile", "").strip().lower() or "mandos")
    default_report_by_profile = {
        "mandos": "ot_carga_equipos",
        "direccion": "deuda_recaudacion",
        "equipos": "ot_calendario_faenas",
    }
    report_key = (
        request.args.get("report", "").strip().lower()
        or default_report_by_profile.get(profile, "ot_carga_equipos")
    )
    if report_key not in REPORTING_SCREEN_KEYS:
        report_key = "ot_carga_equipos"
    filters = _report_filters()
    filters["profile"] = profile
    try:
        rows = reporting_rows(report_key, filters)
    except ValueError as exc:
        flash(str(exc), "error")
        report_key = "ot_carga_equipos"
        rows = reporting_rows(report_key, filters)
    page = request.args.get("page", type=int, default=1) or 1
    page_size = request.args.get("page_size", type=int, default=25) or 25
    paged = paginate_rows(rows, page=page, page_size=page_size)
    return render_template(
        "cemetery/reporting.html",
        report_key=report_key,
        filters=filters,
        profiles=[
            ("mandos", "Mandos"),
            ("direccion", "Direccion"),
            ("equipos", "Equipos"),
        ],
        report_users=reporting_filter_users(),
        report_type_codes=reporting_filter_type_codes(),
        report_blocks=reporting_filter_blocks(),
        report_categories=[item.value for item in WorkOrderCategory],
        report_statuses=[item.value for item in WorkOrderStatus],
        report_options=sorted(REPORTING_SCREEN_KEYS),
        schedule_schema_ready=reporting_schedule_schema_ready(),
        paged=paged,
        money=money,
    )


@cemetery_bp.get("/reporting/export.csv")
@login_required
@require_membership
def reporting_export_csv():
    report_key = (
        request.args.get("report", "ot_carga_equipos").strip().lower()
        or "ot_carga_equipos"
    )
    if report_key not in REPORTING_ALL_KEYS:
        report_key = "ot_carga_equipos"
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


@cemetery_bp.get("/reporting/export.pdf")
@login_required
@require_membership
def reporting_export_pdf():
    report_key = (
        request.args.get("report", "ot_carga_equipos").strip().lower()
        or "ot_carga_equipos"
    )
    if report_key not in REPORTING_ALL_KEYS:
        report_key = "ot_carga_equipos"
    filters = _report_filters()
    try:
        content = reporting_pdf_bytes(report_key, filters, export_limit=400)
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("cemetery.reporting"))
    response = make_response(content)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f'attachment; filename="{report_key}.pdf"'
    return response


@cemetery_bp.route("/reporting/schedules", methods=["GET", "POST"])
@login_required
@require_membership
@require_role("admin")
def reporting_schedules():
    if not reporting_schedule_schema_ready():
        flash("Falta migracion de informes. Ejecuta: flask db upgrade", "error")
        return redirect(url_for("cemetery.reporting"))

    if request.method == "POST":
        payload = {k: v for k, v in request.form.items()}
        formats = request.form.getlist("formats")
        if formats:
            payload["formats"] = ",".join(formats)
        try:
            created = create_reporting_schedule(payload, current_user.id)
            flash(f"Programacion '{created.name}' creada", "success")
        except ValueError as exc:
            flash(str(exc), "error")
        return redirect(url_for("cemetery.reporting_schedules"))

    schedules = list_reporting_schedules()
    schedule_ids = [item.id for item in schedules]
    delivery_logs = []
    if schedule_ids:
        delivery_logs = (
            ReportDeliveryLog.query.filter_by(org_id=g.org.id)
            .filter(ReportDeliveryLog.schedule_id.in_(schedule_ids))
            .order_by(ReportDeliveryLog.run_at.desc(), ReportDeliveryLog.id.desc())
            .limit(100)
            .all()
        )
    return render_template(
        "cemetery/reporting_schedules.html",
        schedules=schedules,
        delivery_logs=delivery_logs,
        report_options=sorted(REPORTING_ALL_KEYS),
    )


@cemetery_bp.post("/reporting/schedules/<int:schedule_id>/toggle")
@login_required
@require_membership
@require_role("admin")
def reporting_schedule_toggle(schedule_id: int):
    if not reporting_schedule_schema_ready():
        flash("Falta migracion de informes. Ejecuta: flask db upgrade", "error")
        return redirect(url_for("cemetery.reporting"))
    try:
        schedule = toggle_reporting_schedule(schedule_id)
        state = "activa" if schedule.active else "pausada"
        flash(f"Programacion {schedule.name}: {state}", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.reporting_schedules"))


@cemetery_bp.post("/reporting/schedules/<int:schedule_id>/run-now")
@login_required
@require_membership
@require_role("admin")
def reporting_schedule_run_now(schedule_id: int):
    if not reporting_schedule_schema_ready():
        flash("Falta migracion de informes. Ejecuta: flask db upgrade", "error")
        return redirect(url_for("cemetery.reporting"))
    try:
        log = run_reporting_schedule(schedule_id, current_user.id)
        if log.status == "ERROR":
            flash(f"Ejecucion con error: {log.error}", "error")
        else:
            flash(
                f"Ejecucion completada. Filas={log.rows_count} estado={log.status}",
                "success",
            )
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.reporting_schedules"))


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
        "modalidad": request.values.get("modalidad", "").strip(),
        "estado": request.values.get("estado", "").strip(),
        "con_deuda": request.values.get("con_deuda", "").strip(),
        "titular": request.values.get("titular", "").strip(),
        "difunto": request.values.get("difunto", "").strip(),
    }
    has_search_filters = any(
        filters.get(key)
        for key in [
            "bloque",
            "fila",
            "columna",
            "numero",
            "modalidad",
            "estado",
            "con_deuda",
            "titular",
            "difunto",
        ]
    )
    page = request.values.get("page", type=int, default=1) or 1
    page_size = request.values.get("page_size", type=int, default=25) or 25
    sort_by = request.values.get("sort_by", "ubicacion").strip() or "ubicacion"
    sort_dir = request.values.get("sort_dir", "asc").strip() or "asc"
    paged = (
        search_sepulturas_paged(
            filters, page=page, page_size=page_size, sort_by=sort_by, sort_dir=sort_dir
        )
        if has_search_filters
        else {
            "rows": [],
            "total": 0,
            "shown": 0,
            "page": 1,
            "page_size": page_size,
            "total_pages": 1,
            "has_prev": False,
            "has_next": False,
        }
    )
    if _is_htmx():
        return render_template(
            "cemetery/_search_results.html",
            paged=paged,
            filters=filters,
            sort_by=sort_by,
            sort_dir=sort_dir,
            money=money,
        )
    return render_template(
        "cemetery/search.html",
        filters=filters,
        paged=paged,
        sort_by=sort_by,
        sort_dir=sort_dir,
        money=money,
        blocks=list_sepultura_blocks(),
        block_location_options=sepultura_location_options_by_block(),
        modalidades=list_sepultura_modalidades(),
        sepultura_states=[state.value for state in SepulturaEstado],
    )


@cemetery_bp.get("/sepulturas/<int:sepultura_id>")
@login_required
@require_membership
def grave_detail(sepultura_id: int):
    # Spec 9.4.3 / 9.4.4 / 9.4.5 / 9.1.7 - Ficha de sepultura con tabs
    tab = request.args.get("tab", "principal")
    valid_tabs = {
        "principal",
        "resumen",
        "titularidad",
        "titulares",
        "beneficiarios",
        "difuntos",
        "movimientos",
        "facturacion",
        "derecho",
        "notas",
        "lapidas",
    }
    if tab not in valid_tabs:
        tab = "principal"
    mov_filters = {
        "tipo": request.args.get("tipo", "").strip(),
        "desde": request.args.get("desde", "").strip(),
        "hasta": request.args.get("hasta", "").strip(),
    }
    try:
        data = sepultura_tabs_data(sepultura_id, tab, mov_filters)
    except ValueError:
        abort(404)
    return render_template(
        "cemetery/detail.html", data=data, SepulturaEstado=SepulturaEstado, money=money
    )


@cemetery_bp.post("/sepulturas/<int:sepultura_id>/ot")
@login_required
@require_membership
def grave_create_ot(sepultura_id: int):
    payload = {k: v for k, v in request.form.items()}
    redirect_url = (
        url_for("cemetery.grave_detail", sepultura_id=sepultura_id, tab="principal")
        + "#ordenes-trabajo"
    )
    try:
        sep = sepultura_by_id(sepultura_id)
        payload["sepultura_id"] = str(sep.id)
        if not (payload.get("title") or "").strip():
            payload["title"] = (payload.get("titulo") or "").strip() or f"OT sepultura {sep.location_label}"
        if "notes" in payload and "description" not in payload:
            payload["description"] = payload.get("notes", "")
        if not (payload.get("category") or "").strip():
            payload["category"] = WorkOrderCategory.FUNERARIA.value
        if not (payload.get("priority") or "").strip():
            payload["priority"] = WorkOrderPriority.MEDIA.value
        if not (payload.get("status") or "").strip():
            payload["status"] = WorkOrderStatus.PENDIENTE_PLANIFICACION.value
        row = create_work_order(payload, current_user.id)
        flash(f"OT {row.code} creada", "success")
    except (ValueError, PermissionError) as exc:
        flash(str(exc), "error")
    return redirect(redirect_url)


@cemetery_bp.post("/sepulturas/<int:sepultura_id>/notas")
@login_required
@require_membership
def grave_notes_update(sepultura_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        update_sepultura_notes(sepultura_id, payload)
        flash("Notas actualizadas", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.grave_detail", sepultura_id=sepultura_id, tab="notas"))


@cemetery_bp.post("/sepulturas/<int:sepultura_id>/cambiar-titular")
@login_required
@require_membership
@require_role("admin")
def change_holder_direct(sepultura_id: int):
    try:
        sep = sepultura_by_id(sepultura_id)
        contract = active_contract_for_sepultura(sep.id)
        if not contract:
            raise ValueError("No hay contrato activo asociado a esta sepultura")
        return redirect(
            url_for("cemetery.ownership_cases", prefill_contract_id=contract.id)
        )
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(url_for("cemetery.grave_detail", sepultura_id=sepultura_id))


@cemetery_bp.get("/titularidad")
@login_required
@require_membership
def ownership_cases_alias():
    return redirect(url_for("cemetery.ownership_cases"))


@cemetery_bp.route("/titularidad/casos", methods=["GET", "POST"])
@login_required
@require_membership
def ownership_cases():
    prefill_contract_id = ""
    if request.method == "GET":
        raw_prefill = request.args.get("prefill_contract_id", "").strip()
        if raw_prefill.isdigit():
            try:
                contract = contract_by_id(int(raw_prefill))
                prefill_contract_id = str(contract.id)
            except ValueError:
                prefill_contract_id = ""

    if request.method == "POST":
        if not g.membership or (g.membership.role or "").lower() != "admin":
            abort(403)
        payload = {k: v for k, v in request.form.items()}
        try:
            created = create_ownership_case(payload, current_user.id)
            flash(f"Caso {created.case_number} creado", "success")
            return redirect(
                url_for("cemetery.ownership_case_detail_page", case_id=created.id)
            )
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
        prefill_contract_id=prefill_contract_id,
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




@cemetery_bp.post("/sepulturas/<int:sepultura_id>/difuntos")
@login_required
@require_membership
def add_deceased(sepultura_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        add_deceased_to_sepultura(sepultura_id, payload, current_user.id)
        flash("Difunto registrado en la sepultura", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(
        url_for("cemetery.grave_detail", sepultura_id=sepultura_id, tab="difuntos")
    )


@cemetery_bp.post("/sepulturas/<int:sepultura_id>/difuntos/<int:sepultura_difunto_id>/eliminar")
@login_required
@require_membership
@require_role("admin")
def remove_deceased(sepultura_id: int, sepultura_difunto_id: int):
    try:
        remove_deceased_from_sepultura(sepultura_id, sepultura_difunto_id, current_user.id)
        flash("Difunto eliminado de la sepultura", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return redirect(
        url_for("cemetery.grave_detail", sepultura_id=sepultura_id, tab="difuntos")
    )

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
    return redirect(
        url_for("cemetery.grave_detail", sepultura_id=sepultura_id, tab="derecho")
    )


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
    response.headers["Content-Disposition"] = (
        f'inline; filename="titulo-contrato-{contract_id}.pdf"'
    )
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
    return redirect(
        url_for("cemetery.grave_detail", sepultura_id=sepultura_id, tab="resumen")
    )


@cemetery_bp.post("/contratos/<int:contract_id>/beneficiario/nombrar")
@login_required
@require_membership
@require_role("admin")
def nominate_beneficiary(contract_id: int):
    # Spec Cementiri 9.1.6 - Nomenament de beneficiari
    payload = {k: v for k, v in request.form.items()}
    sepultura_id = request.form.get("sepultura_id", type=int)
    try:
        nominate_contract_beneficiary(contract_id, payload, current_user.id)
        flash("Beneficiario guardado", "success")
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
        return redirect(
            url_for(
                "cemetery.grave_detail",
                sepultura_id=target_sepultura,
                tab="beneficiarios",
            )
        )
    return redirect(url_for("cemetery.search_graves"))


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
        return redirect(
            url_for(
                "cemetery.grave_detail", sepultura_id=target_sepultura, tab="titulares"
            )
        )
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
        return redirect(
            url_for(
                "cemetery.grave_detail",
                sepultura_id=target_sepultura,
                tab="beneficiarios",
            )
        )
    return redirect(url_for("cemetery.search_graves"))

def _billing_workspace_filters() -> dict[str, str]:
    view = (request.args.get("view") or "pendientes").strip().lower()
    if view not in {"pendientes", "cobros", "rectificativas", "fiscal"}:
        view = "pendientes"
    return {
        "status": request.args.get("status", "").strip(),
        "contract_id": request.args.get("contract_id", "").strip(),
        "sepultura_id": request.args.get("sepultura_id", "").strip(),
        "view": view,
    }


@cemetery_bp.get("/facturacion")
@login_required
@require_membership
def billing_workspace():
    filters = _billing_workspace_filters()
    data = billing_workspace_data(filters)
    return render_template(
        "cemetery/billing_workspace.html",
        filters=filters,
        data=data,
        money=money,
        BillingDocumentStatus=BillingDocumentStatus,
        PaymentMethod=PaymentMethod,
    )


def _billing_redirect_with_view(default_view: str = "pendientes"):
    view = (request.form.get("_view") or request.args.get("view") or default_view).strip().lower()
    if view not in {"pendientes", "cobros", "rectificativas", "fiscal"}:
        view = default_view
    return redirect(url_for("cemetery.billing_workspace", view=view))


@cemetery_bp.post("/facturacion/invoices")
@login_required
@require_membership
def billing_create_invoice():
    payload = {k: v for k, v in request.form.items()}
    try:
        document = create_invoice_draft(payload, current_user.id)
        flash(f"Factura borrador creada ({document.id})", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return _billing_redirect_with_view(default_view="pendientes")


@cemetery_bp.post("/facturacion/invoices/<int:document_id>/issue")
@login_required
@require_membership
def billing_issue_invoice(document_id: int):
    try:
        document = issue_invoice(document_id, current_user.id)
        flash(f"Factura emitida: {document.number}", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return _billing_redirect_with_view(default_view="pendientes")


@cemetery_bp.post("/facturacion/invoices/<int:document_id>/payments")
@login_required
@require_membership
def billing_register_payment(document_id: int):
    payload = {k: v for k, v in request.form.items()}
    idempotency_key = (request.headers.get("Idempotency-Key", "") or "").strip()
    if not idempotency_key:
        # Fallback for HTML forms: deterministic key to prevent immediate double submit.
        idempotency_key = (
            request.form.get("idempotency_key", "").strip()
            or f"form-{current_user.id}-{document_id}-{request.form.get('amount', '').strip()}-{request.form.get('method', '').strip()}"
        )
    try:
        payment, reused = register_payment(
            document_id=document_id,
            payload=payload,
            user_id=current_user.id,
            idempotency_key=idempotency_key,
            endpoint="POST:/cementerio/facturacion/invoices/<id>/payments",
        )
        if reused:
            flash(f"Cobro ya registrado previamente. Recibo {payment.receipt_number}", "success")
        else:
            flash(f"Cobro registrado. Recibo {payment.receipt_number}", "success")
        return redirect(url_for("cemetery.billing_receipt", payment_id=payment.id))
    except ValueError as exc:
        flash(str(exc), "error")
        return _billing_redirect_with_view(default_view="cobros")


@cemetery_bp.post("/facturacion/invoices/<int:document_id>/credit-note")
@login_required
@require_membership
def billing_create_credit_note(document_id: int):
    payload = {k: v for k, v in request.form.items()}
    try:
        note = create_credit_note(document_id, payload, current_user.id)
        flash(f"Rectificativa emitida: {note.number}", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return _billing_redirect_with_view(default_view="rectificativas")


@cemetery_bp.get("/facturacion/receipts/<int:payment_id>")
@login_required
@require_membership
def billing_receipt(payment_id: int):
    try:
        payment = payment_receipt_by_id(payment_id)
    except ValueError:
        abort(404)
    return render_template("cemetery/billing_receipt.html", payment=payment, money=money)


@cemetery_bp.post("/facturacion/fiscal/submissions/<int:submission_id>/retry")
@login_required
@require_membership
@require_role("admin")
def billing_retry_submission(submission_id: int):
    try:
        submission = retry_fiscal_submission(submission_id, current_user.id)
        flash(f"Reintento fiscal registrado en estado {submission.status.value}", "success")
    except ValueError as exc:
        flash(str(exc), "error")
    return _billing_redirect_with_view(default_view="fiscal")


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
