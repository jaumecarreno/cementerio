from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

from flask import g
from sqlalchemy import func
from sqlalchemy.orm import joinedload
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from app.core.extensions import db
from app.core.models import (
    ActivityLog,
    Membership,
    OperationCase,
    Sepultura,
    User,
    WorkOrder,
    WorkOrderAreaType,
    WorkOrderCategory,
    WorkOrderChecklistItem,
    WorkOrderDependency,
    WorkOrderDependencyType,
    WorkOrderEvidence,
    WorkOrderEventLog,
    WorkOrderEventRule,
    WorkOrderPriority,
    WorkOrderStatus,
    WorkOrderStatusLog,
    WorkOrderTemplate,
    WorkOrderTemplateChecklistItem,
    WorkOrderType,
)


WORK_ORDER_STATUS_TRANSITIONS: dict[WorkOrderStatus, set[WorkOrderStatus]] = {
    WorkOrderStatus.BORRADOR: {WorkOrderStatus.PENDIENTE_PLANIFICACION, WorkOrderStatus.CANCELADA},
    WorkOrderStatus.PENDIENTE_PLANIFICACION: {
        WorkOrderStatus.PLANIFICADA,
        WorkOrderStatus.BLOQUEADA,
        WorkOrderStatus.CANCELADA,
    },
    WorkOrderStatus.PLANIFICADA: {WorkOrderStatus.ASIGNADA, WorkOrderStatus.BLOQUEADA, WorkOrderStatus.CANCELADA},
    WorkOrderStatus.ASIGNADA: {WorkOrderStatus.EN_CURSO, WorkOrderStatus.BLOQUEADA, WorkOrderStatus.CANCELADA},
    WorkOrderStatus.EN_CURSO: {WorkOrderStatus.EN_VALIDACION, WorkOrderStatus.BLOQUEADA, WorkOrderStatus.CANCELADA},
    WorkOrderStatus.BLOQUEADA: {
        WorkOrderStatus.PLANIFICADA,
        WorkOrderStatus.ASIGNADA,
        WorkOrderStatus.EN_CURSO,
        WorkOrderStatus.CANCELADA,
    },
    WorkOrderStatus.EN_VALIDACION: {
        WorkOrderStatus.COMPLETADA,
        WorkOrderStatus.EN_CURSO,
        WorkOrderStatus.BLOQUEADA,
    },
    WorkOrderStatus.COMPLETADA: set(),
    WorkOrderStatus.CANCELADA: set(),
}

WORK_ORDER_PENDING_STATUSES = {
    WorkOrderStatus.BORRADOR,
    WorkOrderStatus.PENDIENTE_PLANIFICACION,
    WorkOrderStatus.PLANIFICADA,
    WorkOrderStatus.ASIGNADA,
}
WORK_ORDER_OPEN_STATUSES = {
    WorkOrderStatus.BORRADOR,
    WorkOrderStatus.PENDIENTE_PLANIFICACION,
    WorkOrderStatus.PLANIFICADA,
    WorkOrderStatus.ASIGNADA,
    WorkOrderStatus.EN_CURSO,
    WorkOrderStatus.BLOQUEADA,
    WorkOrderStatus.EN_VALIDACION,
}
WORK_ORDER_TERMINAL_STATUSES = {WorkOrderStatus.COMPLETADA, WorkOrderStatus.CANCELADA}

OT_EVENT_TYPES = (
    "DECEASED_ADDED_TO_SEPULTURA",
    "DECEASED_REMOVED_FROM_SEPULTURA",
    "OWNERSHIP_CASE_APPROVED",
    "LAPIDA_ORDER_CREATED",
    "LOW_STOCK_DETECTED",
    "OPERATION_CASE_CREATED_INHUMACION",
    "OPERATION_CASE_CREATED_EXHUMACION",
    "OPERATION_CASE_CREATED_TRASLADO_CORTO",
    "OPERATION_CASE_CREATED_TRASLADO_LARGO",
    "OPERATION_CASE_CREATED_RESCATE",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _org_id() -> int:
    return g.org.id


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _parse_bool(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "on", "yes", "si"}


def _parse_optional_int(value: str | None) -> int | None:
    raw = (value or "").strip()
    if not raw:
        return None
    return int(raw) if raw.isdigit() else None


def _parse_optional_datetime(value: str | None) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("Formato de fecha/hora invalido") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _parse_category(value: str | None) -> WorkOrderCategory | None:
    raw = (value or "").strip().upper()
    if not raw:
        return None
    try:
        return WorkOrderCategory[raw]
    except KeyError as exc:
        raise ValueError("Categoria OT invalida") from exc


def _parse_priority(value: str | None) -> WorkOrderPriority | None:
    raw = (value or "").strip().upper()
    if not raw:
        return None
    try:
        return WorkOrderPriority[raw]
    except KeyError as exc:
        raise ValueError("Prioridad OT invalida") from exc


def _parse_status(value: str | None) -> WorkOrderStatus | None:
    raw = (value or "").strip().upper()
    if not raw:
        return None
    try:
        return WorkOrderStatus[raw]
    except KeyError as exc:
        raise ValueError("Estado OT invalido") from exc


def _parse_area_type(value: str | None) -> WorkOrderAreaType | None:
    raw = (value or "").strip().upper()
    if not raw:
        return None
    try:
        return WorkOrderAreaType[raw]
    except KeyError as exc:
        raise ValueError("Tipo de area invalido") from exc


def _load_work_order_type(type_code: str | None) -> WorkOrderType | None:
    code = (type_code or "").strip().upper()
    if not code:
        return None
    return WorkOrderType.query.filter_by(org_id=_org_id(), code=code).first()


def _next_work_order_code() -> str:
    year = date.today().year
    prefix = f"OT-{year}-"
    count = (
        db.session.query(func.count(WorkOrder.id))
        .filter(WorkOrder.org_id == _org_id())
        .filter(WorkOrder.code.like(f"{prefix}%"))
        .scalar()
        or 0
    )
    return f"{prefix}{count + 1:06d}"


def _log_activity(action_type: str, details: str, user_id: int | None, sepultura_id: int | None = None) -> None:
    db.session.add(
        ActivityLog(
            org_id=_org_id(),
            sepultura_id=sepultura_id,
            action_type=action_type,
            details=(details or "").strip(),
            user_id=user_id,
        )
    )


def _location_label(work_order: WorkOrder) -> str:
    if work_order.sepultura:
        return work_order.sepultura.location_label
    area = work_order.area_type.value if work_order.area_type else "AREA"
    code = (work_order.area_code or "").strip()
    text = (work_order.location_text or "").strip()
    if code and text:
        return f"{area} {code} - {text}"
    if code:
        return f"{area} {code}"
    if text:
        return f"{area} - {text}"
    return "-"


def work_order_sla_overdue(work_order: WorkOrder) -> bool:
    due_at = _as_utc(work_order.due_at)
    return bool(due_at and work_order.status not in WORK_ORDER_TERMINAL_STATUSES and due_at < _now())


def work_order_telemetry(work_order: WorkOrder) -> dict[str, object]:
    lead_time_hours = None
    created_at = _as_utc(work_order.created_at) or _now()
    completed_at = _as_utc(work_order.completed_at)
    if completed_at:
        lead_time_hours = round((completed_at - created_at).total_seconds() / 3600, 2)
    elapsed_hours = round((_now() - created_at).total_seconds() / 3600, 2)
    return {"sla_overdue": work_order_sla_overdue(work_order), "lead_time_hours": lead_time_hours, "elapsed_hours": elapsed_hours}


def get_work_order(work_order_id: int) -> WorkOrder:
    row = (
        WorkOrder.query.options(
            joinedload(WorkOrder.sepultura),
            joinedload(WorkOrder.assigned_user),
            joinedload(WorkOrder.created_by),
            joinedload(WorkOrder.updated_by),
        )
        .filter_by(org_id=_org_id(), id=work_order_id)
        .first()
    )
    if not row:
        raise ValueError("Orden de trabajo no encontrada")
    return row


def list_work_orders(filters: dict[str, str] | None = None) -> list[WorkOrder]:
    filters = filters or {}
    query = (
        WorkOrder.query.options(joinedload(WorkOrder.sepultura), joinedload(WorkOrder.assigned_user))
        .filter(WorkOrder.org_id == _org_id())
        .order_by(WorkOrder.created_at.desc(), WorkOrder.id.desc())
    )
    status_raw = (filters.get("status") or "").strip().upper()
    if status_raw:
        try:
            query = query.filter(WorkOrder.status == WorkOrderStatus[status_raw])
        except KeyError:
            return []
    priority_raw = (filters.get("priority") or "").strip().upper()
    if priority_raw:
        try:
            query = query.filter(WorkOrder.priority == WorkOrderPriority[priority_raw])
        except KeyError:
            return []
    category_raw = (filters.get("category") or "").strip().upper()
    if category_raw:
        try:
            query = query.filter(WorkOrder.category == WorkOrderCategory[category_raw])
        except KeyError:
            return []
    type_code = (filters.get("type_code") or "").strip().upper()
    if type_code:
        query = query.filter(WorkOrder.type_code == type_code)
    area_type = (filters.get("area_type") or "").strip().upper()
    if area_type:
        try:
            query = query.filter(WorkOrder.area_type == WorkOrderAreaType[area_type])
        except KeyError:
            return []
    assigned_user_id = (filters.get("assigned_user_id") or "").strip()
    if assigned_user_id:
        if not assigned_user_id.isdigit():
            return []
        query = query.filter(WorkOrder.assigned_user_id == int(assigned_user_id))
    sepultura_id = (filters.get("sepultura_id") or "").strip()
    if sepultura_id:
        if not sepultura_id.isdigit():
            return []
        query = query.filter(WorkOrder.sepultura_id == int(sepultura_id))
    date_from = _parse_optional_datetime(filters.get("date_from"))
    if date_from:
        query = query.filter(WorkOrder.created_at >= date_from)
    date_to = _parse_optional_datetime(filters.get("date_to"))
    if date_to:
        query = query.filter(WorkOrder.created_at <= date_to)
    search = (filters.get("q") or "").strip()
    if search:
        query = query.filter(
            db.or_(
                WorkOrder.code.ilike(f"%{search}%"),
                WorkOrder.title.ilike(f"%{search}%"),
                WorkOrder.description.ilike(f"%{search}%"),
                WorkOrder.location_text.ilike(f"%{search}%"),
            )
        )
    rows = query.all()
    if _parse_bool(filters.get("sla_overdue")):
        rows = [row for row in rows if work_order_sla_overdue(row)]
    return rows


def kanban_work_orders(filters: dict[str, str] | None = None) -> dict[WorkOrderStatus, list[WorkOrder]]:
    rows = list_work_orders(filters)
    grouped: dict[WorkOrderStatus, list[WorkOrder]] = {status: [] for status in WorkOrderStatus}
    for row in rows:
        grouped[row.status].append(row)
    return grouped


def list_active_types() -> list[WorkOrderType]:
    return (
        WorkOrderType.query.filter_by(org_id=_org_id(), active=True)
        .order_by(WorkOrderType.category.asc(), WorkOrderType.name.asc())
        .all()
    )


def list_users_for_assignment() -> list[User]:
    return (
        User.query.join(Membership, Membership.user_id == User.id)
        .filter(Membership.org_id == _org_id())
        .order_by(User.full_name.asc(), User.id.asc())
        .all()
    )


def list_templates(active_only: bool = False) -> list[WorkOrderTemplate]:
    query = (
        WorkOrderTemplate.query.options(joinedload(WorkOrderTemplate.type))
        .filter(WorkOrderTemplate.org_id == _org_id())
        .order_by(WorkOrderTemplate.name.asc(), WorkOrderTemplate.id.asc())
    )
    if active_only:
        query = query.filter(WorkOrderTemplate.active.is_(True))
    return query.all()


def list_event_rules() -> list[WorkOrderEventRule]:
    return (
        WorkOrderEventRule.query.options(joinedload(WorkOrderEventRule.template).joinedload(WorkOrderTemplate.type))
        .filter(WorkOrderEventRule.org_id == _org_id())
        .order_by(WorkOrderEventRule.priority.asc(), WorkOrderEventRule.id.asc())
        .all()
    )


def _validate_location(
    category: WorkOrderCategory,
    sepultura_id: int | None,
    area_type: WorkOrderAreaType | None,
    area_code: str,
    location_text: str,
) -> Sepultura | None:
    sepultura = None
    if sepultura_id:
        sepultura = Sepultura.query.filter_by(org_id=_org_id(), id=sepultura_id).first()
        if not sepultura:
            raise ValueError("Sepultura no encontrada")
    if category == WorkOrderCategory.FUNERARIA and not sepultura_id:
        raise ValueError("Las OT funerarias requieren sepultura")
    if not sepultura_id:
        if not area_type:
            raise ValueError("Debes indicar el tipo de area cuando no hay sepultura")
        if not area_code and not location_text:
            raise ValueError("Debes indicar codigo o texto de ubicacion de area")
    return sepultura


def _seed_checklist_from_template(work_order_id: int, template_id: int) -> None:
    items = (
        WorkOrderTemplateChecklistItem.query.filter_by(template_id=template_id)
        .order_by(WorkOrderTemplateChecklistItem.sort_order.asc(), WorkOrderTemplateChecklistItem.id.asc())
        .all()
    )
    for item in items:
        db.session.add(
            WorkOrderChecklistItem(
                work_order_id=work_order_id,
                label=item.label,
                required=item.required,
                sort_order=item.sort_order,
            )
        )


def create_work_order(payload: dict[str, str], user_id: int | None, auto_commit: bool = True, source_event: str | None = None) -> WorkOrder:
    template_id = _parse_optional_int(payload.get("template_id"))
    template = WorkOrderTemplate.query.filter_by(org_id=_org_id(), id=template_id).first() if template_id else None
    if template_id and not template:
        raise ValueError("Plantilla OT no encontrada")
    type_code_raw = (payload.get("type_code") or "").strip().upper() or (template.type.code if template and template.type else "")
    type_row = _load_work_order_type(type_code_raw)
    category = _parse_category(payload.get("category")) or (type_row.category if type_row else WorkOrderCategory.MANTENIMIENTO)
    priority = _parse_priority(payload.get("priority")) or (template.default_priority if template else WorkOrderPriority.MEDIA)
    status = _parse_status(payload.get("status")) or WorkOrderStatus.BORRADOR
    operation_case_id = _parse_optional_int(payload.get("operation_case_id"))
    operation_case = None
    if operation_case_id is not None:
        operation_case = OperationCase.query.filter_by(org_id=_org_id(), id=operation_case_id).first()
        if not operation_case:
            raise ValueError("Expediente asociado no encontrado")
    sepultura_id = _parse_optional_int(payload.get("sepultura_id"))
    area_type = _parse_area_type(payload.get("area_type"))
    area_code = (payload.get("area_code") or "").strip()
    location_text = (payload.get("location_text") or "").strip()
    if template:
        if template.requires_sepultura and not sepultura_id:
            raise ValueError("La plantilla requiere sepultura")
        if not template.allows_area and not sepultura_id:
            raise ValueError("La plantilla no permite OT por zona/area")
    sepultura = _validate_location(category, sepultura_id, area_type, area_code, location_text)
    title = (payload.get("title") or "").strip() or (template.name if template else "")
    if not title:
        raise ValueError("Titulo OT obligatorio")
    row = WorkOrder(
        org_id=_org_id(),
        code=_next_work_order_code(),
        title=title,
        description=(payload.get("description") or "").strip(),
        category=category,
        type_code=type_row.code if type_row else (type_code_raw or None),
        priority=priority,
        status=status,
        operation_case_id=operation_case.id if operation_case else None,
        sepultura_id=sepultura_id,
        area_type=area_type,
        area_code=area_code or None,
        location_text=location_text or None,
        assigned_user_id=_parse_optional_int(payload.get("assigned_user_id")),
        planned_start_at=_parse_optional_datetime(payload.get("planned_start_at")),
        planned_end_at=_parse_optional_datetime(payload.get("planned_end_at")),
        due_at=_parse_optional_datetime(payload.get("due_at")) or (_now() + timedelta(hours=int(template.sla_hours)) if template and template.sla_hours else None),
        created_by_user_id=user_id,
        updated_by_user_id=user_id,
    )
    db.session.add(row)
    db.session.flush()
    if template:
        _seed_checklist_from_template(row.id, template.id)
    db.session.add(
        WorkOrderStatusLog(
            work_order_id=row.id,
            from_status="",
            to_status=row.status.value,
            changed_by_user_id=user_id,
            reason=(source_event or "Alta OT"),
        )
    )
    _log_activity("OT_CREADA", f"{row.code} - {row.title}", user_id, sepultura.id if sepultura else sepultura_id)
    if auto_commit:
        db.session.commit()
    return row


def create_work_order_from_template(
    template: WorkOrderTemplate,
    payload: dict[str, object] | None,
    user_id: int | None,
    auto_commit: bool = True,
    source_event: str | None = None,
) -> WorkOrder:
    payload = payload or {}
    data: dict[str, str] = {
        "template_id": str(template.id),
        "status": WorkOrderStatus.PENDIENTE_PLANIFICACION.value,
        "title": str(payload.get("title") or template.name),
        "description": str(payload.get("description") or f"OT automatica por evento {source_event or ''}".strip()),
    }
    for key in ("sepultura_id", "area_type", "area_code", "location_text", "assigned_user_id", "due_at"):
        if payload.get(key) is not None:
            data[key] = str(payload.get(key))
    return create_work_order(data, user_id=user_id, auto_commit=auto_commit, source_event=source_event)


def allowed_transitions(status: WorkOrderStatus) -> list[WorkOrderStatus]:
    return sorted(WORK_ORDER_STATUS_TRANSITIONS.get(status, set()), key=lambda item: item.value)


def _operator_transition_allowed(current: WorkOrderStatus, target: WorkOrderStatus) -> bool:
    return (current, target) in {
        (WorkOrderStatus.ASIGNADA, WorkOrderStatus.EN_CURSO),
        (WorkOrderStatus.ASIGNADA, WorkOrderStatus.BLOQUEADA),
        (WorkOrderStatus.EN_CURSO, WorkOrderStatus.EN_VALIDACION),
        (WorkOrderStatus.EN_CURSO, WorkOrderStatus.BLOQUEADA),
        (WorkOrderStatus.BLOQUEADA, WorkOrderStatus.EN_CURSO),
        (WorkOrderStatus.EN_VALIDACION, WorkOrderStatus.EN_CURSO),
        (WorkOrderStatus.EN_VALIDACION, WorkOrderStatus.BLOQUEADA),
        (WorkOrderStatus.EN_VALIDACION, WorkOrderStatus.COMPLETADA),
    }


def _assert_transition_permission(actor_role: str, current: WorkOrderStatus, target: WorkOrderStatus) -> None:
    role = (actor_role or "").strip().lower()
    if role == "admin":
        return
    if role == "operador" and _operator_transition_allowed(current, target):
        return
    raise PermissionError("No tienes permisos para esta transicion")


def _assert_ready_for_completion(work_order: WorkOrder) -> None:
    pending_required = (
        WorkOrderChecklistItem.query.filter_by(work_order_id=work_order.id, required=True)
        .filter(WorkOrderChecklistItem.done.is_(False))
        .count()
    )
    if pending_required:
        raise ValueError("No se puede completar: checklist obligatoria incompleta")
    dependencies = (
        WorkOrderDependency.query.options(joinedload(WorkOrderDependency.depends_on))
        .filter_by(work_order_id=work_order.id)
        .all()
    )
    if any(dep.depends_on and dep.depends_on.status != WorkOrderStatus.COMPLETADA for dep in dependencies):
        raise ValueError("No se puede completar: dependencias pendientes")
    type_row = _load_work_order_type(work_order.type_code)
    if type_row and type_row.is_critical:
        evidence_count = WorkOrderEvidence.query.filter_by(work_order_id=work_order.id).count()
        if evidence_count <= 0:
            raise ValueError("No se puede completar: faltan evidencias para OT critica")


def transition_work_order(work_order_id: int, new_status_raw: str, reason: str, actor_user_id: int | None, actor_role: str) -> WorkOrder:
    row = get_work_order(work_order_id)
    target = _parse_status(new_status_raw)
    if not target:
        raise ValueError("Estado OT obligatorio")
    current = row.status
    if target == current:
        return row
    if target not in WORK_ORDER_STATUS_TRANSITIONS.get(current, set()):
        raise ValueError(f"Transicion invalida: {current.value} -> {target.value}")
    _assert_transition_permission(actor_role, current, target)
    note = (reason or "").strip()
    if target == WorkOrderStatus.COMPLETADA:
        _assert_ready_for_completion(row)
        row.completed_at = _now()
        if note:
            row.close_notes = note
    if target == WorkOrderStatus.CANCELADA:
        row.cancelled_at = _now()
        if note:
            row.cancel_reason = note
    if target == WorkOrderStatus.BLOQUEADA and note:
        row.block_reason = note
    if target == WorkOrderStatus.EN_CURSO and not row.started_at:
        row.started_at = _now()
    row.status = target
    row.updated_by_user_id = actor_user_id
    db.session.add(row)
    db.session.add(
        WorkOrderStatusLog(
            work_order_id=row.id,
            from_status=current.value,
            to_status=target.value,
            changed_by_user_id=actor_user_id,
            reason=note,
        )
    )
    _log_activity("OT_ESTADO", f"{row.code}: {current.value} -> {target.value}", actor_user_id, row.sepultura_id)
    db.session.commit()
    return row


def assign_work_order(work_order_id: int, assigned_user_id: int | None, actor_user_id: int | None, actor_role: str) -> WorkOrder:
    if (actor_role or "").strip().lower() != "admin":
        raise PermissionError("Solo admin puede asignar OT")
    row = get_work_order(work_order_id)
    if assigned_user_id is not None:
        membership = Membership.query.filter_by(org_id=_org_id(), user_id=assigned_user_id).first()
        if not membership:
            raise ValueError("Usuario de asignacion no valido")
    row.assigned_user_id = assigned_user_id
    row.updated_by_user_id = actor_user_id
    db.session.add(row)
    _log_activity("OT_ASIGNACION", f"{row.code} asignada a usuario #{assigned_user_id or 0}", actor_user_id, row.sepultura_id)
    db.session.commit()
    return row


def list_work_order_checklist(work_order_id: int) -> list[WorkOrderChecklistItem]:
    get_work_order(work_order_id)
    return (
        WorkOrderChecklistItem.query.filter_by(work_order_id=work_order_id)
        .order_by(WorkOrderChecklistItem.sort_order.asc(), WorkOrderChecklistItem.id.asc())
        .all()
    )


def update_work_order_checklist_item(work_order_id: int, checklist_item_id: int, done: bool, notes: str, actor_user_id: int | None) -> WorkOrderChecklistItem:
    get_work_order(work_order_id)
    item = WorkOrderChecklistItem.query.filter_by(work_order_id=work_order_id, id=checklist_item_id).first()
    if not item:
        raise ValueError("Item de checklist no encontrado")
    item.done = bool(done)
    item.done_at = _now() if item.done else None
    item.done_by_user_id = actor_user_id if item.done else None
    item.notes = (notes or "").strip()
    db.session.add(item)
    db.session.commit()
    return item


def add_work_order_checklist_item(work_order_id: int, payload: dict[str, str], actor_role: str) -> WorkOrderChecklistItem:
    if (actor_role or "").strip().lower() != "admin":
        raise PermissionError("Solo admin puede modificar checklist")
    get_work_order(work_order_id)
    label = (payload.get("label") or "").strip()
    if not label:
        raise ValueError("Etiqueta de checklist obligatoria")
    max_sort = (
        db.session.query(func.max(WorkOrderChecklistItem.sort_order))
        .filter(WorkOrderChecklistItem.work_order_id == work_order_id)
        .scalar()
        or 0
    )
    item = WorkOrderChecklistItem(
        work_order_id=work_order_id,
        label=label,
        required=_parse_bool(payload.get("required")),
        sort_order=max_sort + 1,
    )
    db.session.add(item)
    db.session.commit()
    return item


def list_work_order_evidences(work_order_id: int) -> list[WorkOrderEvidence]:
    get_work_order(work_order_id)
    return (
        WorkOrderEvidence.query.options(joinedload(WorkOrderEvidence.uploaded_by))
        .filter_by(work_order_id=work_order_id)
        .order_by(WorkOrderEvidence.uploaded_at.desc(), WorkOrderEvidence.id.desc())
        .all()
    )


def add_work_order_evidence(work_order_id: int, file_obj: FileStorage, notes: str, actor_user_id: int | None, instance_path: str) -> WorkOrderEvidence:
    row = get_work_order(work_order_id)
    if not file_obj or not file_obj.filename:
        raise ValueError("Debes seleccionar un fichero")
    filename = secure_filename(file_obj.filename) or f"evidence-{row.id}.bin"
    storage_dir = Path(instance_path) / "storage" / "cemetery" / "work_orders" / str(_org_id()) / str(row.id) / "evidence"
    storage_dir.mkdir(parents=True, exist_ok=True)
    absolute = storage_dir / filename
    file_obj.save(absolute)
    evidence = WorkOrderEvidence(
        work_order_id=row.id,
        file_path=absolute.relative_to(Path(instance_path)).as_posix(),
        file_name=filename,
        mime_type=(file_obj.mimetype or "application/octet-stream"),
        uploaded_by_user_id=actor_user_id,
        notes=(notes or "").strip(),
    )
    db.session.add(evidence)
    _log_activity("OT_EVIDENCIA", f"{row.code}: evidencia {filename}", actor_user_id, row.sepultura_id)
    db.session.commit()
    return evidence


def list_work_order_dependencies(work_order_id: int) -> list[WorkOrderDependency]:
    get_work_order(work_order_id)
    return (
        WorkOrderDependency.query.options(joinedload(WorkOrderDependency.depends_on))
        .filter_by(work_order_id=work_order_id)
        .order_by(WorkOrderDependency.id.asc())
        .all()
    )


def add_work_order_dependency(work_order_id: int, depends_on_work_order_id: int, actor_user_id: int | None, actor_role: str) -> WorkOrderDependency:
    if (actor_role or "").strip().lower() != "admin":
        raise PermissionError("Solo admin puede crear dependencias")
    row = get_work_order(work_order_id)
    depends_on = get_work_order(depends_on_work_order_id)
    if row.id == depends_on.id:
        raise ValueError("Una OT no puede depender de si misma")
    exists = WorkOrderDependency.query.filter_by(work_order_id=row.id, depends_on_work_order_id=depends_on.id).first()
    if exists:
        return exists
    dependency = WorkOrderDependency(
        work_order_id=row.id,
        depends_on_work_order_id=depends_on.id,
        dependency_type=WorkOrderDependencyType.FINISH_TO_START,
    )
    db.session.add(dependency)
    _log_activity("OT_DEPENDENCIA", f"{row.code} depende de {depends_on.code}", actor_user_id, row.sepultura_id)
    db.session.commit()
    return dependency


def create_work_order_type(payload: dict[str, str], actor_role: str) -> WorkOrderType:
    if (actor_role or "").strip().lower() != "admin":
        raise PermissionError("Solo admin puede gestionar tipos OT")
    code = (payload.get("code") or "").strip().upper()
    name = (payload.get("name") or "").strip()
    if not code or not name:
        raise ValueError("Codigo y nombre de tipo son obligatorios")
    category = _parse_category(payload.get("category"))
    if not category:
        raise ValueError("Categoria de tipo obligatoria")
    row = WorkOrderType.query.filter_by(org_id=_org_id(), code=code).first()
    if not row:
        row = WorkOrderType(org_id=_org_id(), code=code, name=name, category=category)
    row.name = name
    row.category = category
    row.is_critical = _parse_bool(payload.get("is_critical"))
    row.active = _parse_bool(payload.get("active")) if payload.get("active") is not None else True
    db.session.add(row)
    db.session.commit()
    return row


def create_work_order_template(payload: dict[str, str], actor_role: str) -> WorkOrderTemplate:
    if (actor_role or "").strip().lower() != "admin":
        raise PermissionError("Solo admin puede gestionar plantillas OT")
    code = (payload.get("code") or "").strip().upper()
    name = (payload.get("name") or "").strip()
    if not code or not name:
        raise ValueError("Codigo y nombre de plantilla son obligatorios")
    type_id = _parse_optional_int(payload.get("type_id"))
    type_row = WorkOrderType.query.filter_by(org_id=_org_id(), id=type_id).first() if type_id else None
    if type_id and not type_row:
        raise ValueError("Tipo OT no encontrado")
    row = WorkOrderTemplate.query.filter_by(org_id=_org_id(), code=code).first()
    if not row:
        row = WorkOrderTemplate(org_id=_org_id(), code=code, name=name)
    row.name = name
    row.type_id = type_row.id if type_row else None
    row.default_priority = _parse_priority(payload.get("default_priority")) or WorkOrderPriority.MEDIA
    row.sla_hours = _parse_optional_int(payload.get("sla_hours"))
    row.auto_create = _parse_bool(payload.get("auto_create"))
    row.requires_sepultura = _parse_bool(payload.get("requires_sepultura"))
    row.allows_area = _parse_bool(payload.get("allows_area")) or not row.requires_sepultura
    row.active = _parse_bool(payload.get("active")) if payload.get("active") is not None else True
    db.session.add(row)
    db.session.flush()
    checklist_lines = (payload.get("checklist") or "").splitlines()
    if checklist_lines:
        db.session.query(WorkOrderTemplateChecklistItem).filter_by(template_id=row.id).delete()
        order = 1
        for line in checklist_lines:
            text = line.strip()
            if not text:
                continue
            required = text.startswith("*")
            label = text[1:].strip() if required else text
            if not label:
                continue
            db.session.add(
                WorkOrderTemplateChecklistItem(
                    template_id=row.id,
                    label=label,
                    required=required,
                    sort_order=order,
                )
            )
            order += 1
    db.session.commit()
    return row


def create_work_order_event_rule(payload: dict[str, str], actor_role: str) -> WorkOrderEventRule:
    if (actor_role or "").strip().lower() != "admin":
        raise PermissionError("Solo admin puede gestionar reglas OT")
    event_type = (payload.get("event_type") or "").strip().upper()
    if event_type not in OT_EVENT_TYPES:
        raise ValueError("Evento OT invalido")
    template_id = _parse_optional_int(payload.get("template_id"))
    if not template_id:
        raise ValueError("Plantilla de regla obligatoria")
    template = WorkOrderTemplate.query.filter_by(org_id=_org_id(), id=template_id).first()
    if not template:
        raise ValueError("Plantilla no encontrada")
    conditions_raw = (payload.get("conditions_json") or "{}").strip() or "{}"
    try:
        parsed = json.loads(conditions_raw)
    except json.JSONDecodeError as exc:
        raise ValueError("conditions_json invalido") from exc
    if not isinstance(parsed, dict):
        raise ValueError("conditions_json debe ser un objeto JSON")
    row = WorkOrderEventRule(
        org_id=_org_id(),
        event_type=event_type,
        template_id=template.id,
        conditions_json=json.dumps(parsed, ensure_ascii=True),
        active=_parse_bool(payload.get("active")) if payload.get("active") is not None else True,
        priority=_parse_optional_int(payload.get("priority")) or 100,
    )
    db.session.add(row)
    db.session.commit()
    return row


def _conditions_match(conditions: dict[str, object], payload: dict[str, object]) -> bool:
    for key, expected in conditions.items():
        if key not in payload:
            return False
        value = payload.get(key)
        if isinstance(expected, list):
            if value not in expected:
                return False
            continue
        if isinstance(expected, dict):
            if not isinstance(value, dict):
                return False
            for sub_key, sub_value in expected.items():
                if value.get(sub_key) != sub_value:
                    return False
            continue
        if value != expected:
            return False
    return True


def emit_work_order_event(event_type: str, payload: dict[str, object] | None, user_id: int | None = None) -> list[WorkOrder]:
    normalized_event = (event_type or "").strip().upper()
    if not normalized_event:
        return []
    data = payload or {}
    rules = (
        WorkOrderEventRule.query.options(joinedload(WorkOrderEventRule.template).joinedload(WorkOrderTemplate.type))
        .filter_by(org_id=_org_id(), event_type=normalized_event, active=True)
        .order_by(WorkOrderEventRule.priority.asc(), WorkOrderEventRule.id.asc())
        .all()
    )
    created: list[WorkOrder] = []
    log_items: list[dict[str, object]] = []
    for rule in rules:
        template = rule.template
        if not template or not template.active:
            log_items.append({"rule_id": rule.id, "status": "skipped", "reason": "template_inactive"})
            continue
        try:
            conditions = json.loads(rule.conditions_json or "{}")
        except Exception:
            log_items.append({"rule_id": rule.id, "status": "error", "reason": "invalid_conditions_json"})
            continue
        if isinstance(conditions, dict) and conditions and not _conditions_match(conditions, data):
            log_items.append({"rule_id": rule.id, "status": "skipped", "reason": "conditions_not_met"})
            continue
        try:
            row = create_work_order_from_template(template, data, user_id, auto_commit=False, source_event=normalized_event)
            created.append(row)
            log_items.append({"rule_id": rule.id, "status": "created", "work_order_id": row.id, "work_order_code": row.code})
        except Exception as exc:
            log_items.append({"rule_id": rule.id, "status": "error", "reason": str(exc)})
    db.session.add(
        WorkOrderEventLog(
            org_id=_org_id(),
            event_type=normalized_event,
            payload_json=json.dumps(data, ensure_ascii=True),
            result=json.dumps({"created": len(created), "items": log_items}, ensure_ascii=True),
        )
    )
    db.session.commit()
    return created


def _pdf_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _simple_pdf(lines: list[str]) -> bytes:
    text_ops = ["BT", "/F1 12 Tf", "50 800 Td"]
    for line in lines:
        text_ops.append(f"({_pdf_escape(line)}) Tj")
        text_ops.append("0 -16 Td")
    text_ops.append("ET")
    stream = "\n".join(text_ops).encode("latin-1", errors="ignore")
    objects: list[bytes] = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Contents 5 0 R /Resources << /Font << /F1 4 0 R >> >> >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        f"<< /Length {len(stream)} >>\nstream\n".encode("ascii") + stream + b"\nendstream",
    ]
    pdf = BytesIO()
    pdf.write(b"%PDF-1.4\n")
    offsets = [0]
    for idx, obj in enumerate(objects, start=1):
        offsets.append(pdf.tell())
        pdf.write(f"{idx} 0 obj\n".encode("ascii"))
        pdf.write(obj)
        pdf.write(b"\nendobj\n")
    xref_start = pdf.tell()
    pdf.write(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.write(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        pdf.write(f"{off:010d} 00000 n \n".encode("ascii"))
    pdf.write(f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n".encode("ascii"))
    pdf.write(f"startxref\n{xref_start}\n%%EOF".encode("ascii"))
    return pdf.getvalue()


def work_order_pdf_bytes(work_order_id: int) -> bytes:
    row = get_work_order(work_order_id)
    lines = [
        "GSF - Orden de trabajo",
        f"OT: {row.code}",
        f"Titulo: {row.title}",
        f"Categoria: {row.category.value}",
        f"Tipo: {row.type_code or '-'}",
        f"Estado: {row.status.value}",
        f"Prioridad: {row.priority.value}",
        f"Ubicacion: {_location_label(row)}",
        f"Asignado: {row.assigned_user.full_name if row.assigned_user else '-'}",
        f"Creada: {row.created_at.strftime('%Y-%m-%d %H:%M')}",
        f"Vencimiento SLA: {row.due_at.strftime('%Y-%m-%d %H:%M') if row.due_at else '-'}",
        f"Notas cierre: {row.close_notes or '-'}",
    ]
    return _simple_pdf(lines)


def detail_payload(work_order_id: int) -> dict[str, object]:
    row = get_work_order(work_order_id)
    status_log = (
        WorkOrderStatusLog.query.options(joinedload(WorkOrderStatusLog.changed_by))
        .filter_by(work_order_id=row.id)
        .order_by(WorkOrderStatusLog.changed_at.desc(), WorkOrderStatusLog.id.desc())
        .all()
    )
    available_dependencies = (
        WorkOrder.query.filter_by(org_id=_org_id())
        .filter(WorkOrder.id != row.id)
        .order_by(WorkOrder.created_at.desc())
        .limit(100)
        .all()
    )
    return {
        "work_order": row,
        "checklist": list_work_order_checklist(row.id),
        "evidences": list_work_order_evidences(row.id),
        "dependencies": list_work_order_dependencies(row.id),
        "status_log": status_log,
        "allowed_transitions": allowed_transitions(row.status),
        "telemetry": work_order_telemetry(row),
        "location_label": _location_label(row),
        "available_dependencies": available_dependencies,
    }


def counts_for_sepultura(sepultura_id: int) -> dict[str, int]:
    rows = WorkOrder.query.filter_by(org_id=_org_id(), sepultura_id=sepultura_id).all()
    pendientes = sum(1 for row in rows if row.status in WORK_ORDER_PENDING_STATUSES)
    abiertas = sum(1 for row in rows if row.status in WORK_ORDER_OPEN_STATUSES)
    historicas = sum(1 for row in rows if row.status in WORK_ORDER_TERMINAL_STATUSES)
    return {"pendientes": pendientes, "abiertas": abiertas, "historicas": historicas, "todas": len(rows)}
