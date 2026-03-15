from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from io import BytesIO
from pathlib import Path

from flask import current_app, g
from sqlalchemy import func
from sqlalchemy.orm import joinedload
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from app.cemetery.work_order_service import create_work_order, emit_work_order_event
from app.core.extensions import db
from app.core.models import (
    ActivityLog,
    Beneficiario,
    Cemetery,
    DerechoFunerarioContrato,
    DerechoTipo,
    MovimientoSepultura,
    MovimientoTipo,
    OperationCase,
    OperationDocument,
    OperationPermit,
    OperationPermitStatus,
    OperationStatus,
    OperationStatusLog,
    OperationType,
    OwnershipRecord,
    Sepultura,
    SepulturaDifunto,
    WorkOrder,
    WorkOrderCategory,
    WorkOrderPriority,
    WorkOrderStatus,
)

OPERATION_STATUS_TRANSITIONS: dict[OperationStatus, set[OperationStatus]] = {
    OperationStatus.BORRADOR: {
        OperationStatus.DOCS_PENDIENTES,
        OperationStatus.CANCELADA,
    },
    OperationStatus.DOCS_PENDIENTES: {
        OperationStatus.PROGRAMADA,
        OperationStatus.CANCELADA,
    },
    OperationStatus.PROGRAMADA: {
        OperationStatus.EN_EJECUCION,
        OperationStatus.CANCELADA,
    },
    OperationStatus.EN_EJECUCION: {
        OperationStatus.EN_VALIDACION,
        OperationStatus.CANCELADA,
    },
    OperationStatus.EN_VALIDACION: {
        OperationStatus.CERRADA,
        OperationStatus.CANCELADA,
    },
    OperationStatus.CERRADA: set(),
    OperationStatus.CANCELADA: set(),
}

PermitRequirement = tuple[str, bool]

INHUMACION_DOCUMENTATION_ORDER: tuple[str, ...] = (
    "DNI_TITULAR",
    "DNI_BENEFICIARIO",
    "DNI_DIFUNTO",
    "LICENCIA_ENTERRAMIENTO",
    "CERTIFICADO_DEFUNCION",
    "CERTIFICADO_MEDICO_DEFUNCION",
)

PERMIT_LABELS: dict[str, str] = {
    "DNI_TITULAR": "DNI titular",
    "DNI_BENEFICIARIO": "DNI beneficiario",
    "DNI_DIFUNTO": "DNI difunto",
    "LICENCIA_ENTERRAMIENTO": "Licencia enterramiento",
    "CERTIFICADO_DEFUNCION": "Certificado defuncion",
    "CERTIFICADO_MEDICO_DEFUNCION": "Certificado medico defuncion",
    "AUTORIZACION_EXHUMACION": "Autorizacion exhumacion",
    "AUTORIZACION_TRASLADO": "Autorizacion traslado",
    "AUTORIZACION_RETIRO_RESTOS": "Autorizacion retiro restos",
    "AUTORIZACION_FRONTERIZA": "Autorizacion fronteriza",
    "PERMISO_SANITARIO": "Permiso sanitario",
}

OPERATION_PERMIT_REQUIREMENTS: dict[OperationType, tuple[PermitRequirement, ...]] = {
    OperationType.INHUMACION: (
        ("DNI_TITULAR", True),
        ("DNI_BENEFICIARIO", False),
        ("DNI_DIFUNTO", False),
        ("LICENCIA_ENTERRAMIENTO", True),
        ("CERTIFICADO_DEFUNCION", False),
        ("CERTIFICADO_MEDICO_DEFUNCION", False),
    ),
    OperationType.EXHUMACION: (
        ("AUTORIZACION_EXHUMACION", True),
        ("PERMISO_SANITARIO", True),
    ),
    OperationType.TRASLADO_CORTO: (
        ("AUTORIZACION_TRASLADO", True),
        ("PERMISO_SANITARIO", True),
    ),
    OperationType.TRASLADO_LARGO: (
        ("AUTORIZACION_TRASLADO", True),
        ("PERMISO_SANITARIO", True),
    ),
    OperationType.RESCATE: (
        ("AUTORIZACION_RETIRO_RESTOS", True),
        ("PERMISO_SANITARIO", True),
    ),
}

ACTA_DOC_TYPE = "ACTA_OPERACION"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _today() -> date:
    return date.today()


def _org_id() -> int:
    return g.org.id


def _parse_bool(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "on", "yes", "si"}


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


def _parse_iso_date(value: str | None, field_name: str) -> date:
    raw = (value or "").strip()
    if not raw:
        raise ValueError(f"Debes informar {field_name}")
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"Formato invalido para {field_name}") from exc


def _add_years(base: date, years: int) -> date:
    try:
        return base.replace(year=base.year + years)
    except ValueError:
        # Leap day edge case.
        return base.replace(month=2, day=28, year=base.year + years)


def _parse_operation_type(raw: str) -> OperationType:
    value = (raw or "").strip().upper()
    try:
        return OperationType[value]
    except KeyError as exc:
        raise ValueError("Tipo de expediente invalido") from exc


def _parse_operation_status(raw: str) -> OperationStatus:
    value = (raw or "").strip().upper()
    try:
        return OperationStatus[value]
    except KeyError as exc:
        raise ValueError("Estado de expediente invalido") from exc


def _sepultura_by_id(sepultura_id: int) -> Sepultura:
    row = Sepultura.query.filter_by(org_id=_org_id(), id=sepultura_id).first()
    if not row:
        raise ValueError("Sepultura no encontrada")
    return row


def _cemetery_by_id(cemetery_id: int) -> Cemetery:
    row = Cemetery.query.filter_by(org_id=_org_id(), id=cemetery_id).first()
    if not row:
        raise ValueError("Cementerio de destino no encontrado")
    return row


def _person_exists(person_id: int, label: str) -> None:
    from app.core.models import Person

    person = Person.query.filter_by(org_id=_org_id(), id=person_id).first()
    if not person:
        raise ValueError(f"{label} no encontrado")


def _next_operation_code() -> str:
    year = datetime.now(timezone.utc).year
    prefix = f"OP-{year}-"
    count = (
        db.session.query(func.count(OperationCase.id))
        .filter(OperationCase.org_id == _org_id())
        .filter(OperationCase.code.like(f"{prefix}%"))
        .scalar()
        or 0
    )
    return f"{prefix}{count + 1:04d}"


def _active_contract_for_sepultura(
    sepultura_id: int, on_date: date | None = None
) -> DerechoFunerarioContrato | None:
    ref_date = on_date or _today()
    return (
        DerechoFunerarioContrato.query.filter_by(
            org_id=_org_id(),
            sepultura_id=sepultura_id,
            estado="ACTIVO",
        )
        .filter(DerechoFunerarioContrato.fecha_inicio <= ref_date)
        .filter(DerechoFunerarioContrato.fecha_fin >= ref_date)
        .order_by(DerechoFunerarioContrato.id.desc())
        .first()
    )


def _default_concession_dates(case: OperationCase) -> tuple[date, date]:
    start = case.concession_start_date or _today()
    end = case.concession_end_date or _add_years(start, 25)
    return start, end


def _apply_concession_dates(case: OperationCase, start_date: date, end_date: date) -> None:
    if end_date < start_date:
        raise ValueError("La fecha fin no puede ser anterior a la fecha inicio")
    case.concession_start_date = start_date
    case.concession_end_date = end_date


def _sync_case_concession_from_contract(
    case: OperationCase, contract: DerechoFunerarioContrato
) -> None:
    case.contract_id = contract.id
    case.concession_start_date = contract.fecha_inicio
    case.concession_end_date = contract.fecha_fin


def _required_permits(
    operation_type: OperationType, cross_border: bool
) -> list[PermitRequirement]:
    permits = list(OPERATION_PERMIT_REQUIREMENTS.get(operation_type, ()))
    if operation_type in {OperationType.TRASLADO_CORTO, OperationType.TRASLADO_LARGO} and cross_border:
        permits.append(("AUTORIZACION_FRONTERIZA", True))
    return permits


def _classify_transfer(
    source_sepultura: Sepultura,
    target_sepultura_id: int | None,
    destination_cemetery_id: int | None,
    destination_municipality: str,
    cross_border: bool,
) -> OperationType:
    if cross_border:
        return OperationType.TRASLADO_LARGO

    source_cemetery = Cemetery.query.filter_by(org_id=_org_id(), id=source_sepultura.cemetery_id).first()
    source_municipality = (source_cemetery.municipality or "").strip().lower() if source_cemetery else ""
    target_municipality = (destination_municipality or "").strip().lower()

    if target_sepultura_id:
        target_sep = _sepultura_by_id(target_sepultura_id)
        target_cemetery = Cemetery.query.filter_by(org_id=_org_id(), id=target_sep.cemetery_id).first()
        target_municipality = (target_cemetery.municipality or "").strip().lower() if target_cemetery else target_municipality
    elif destination_cemetery_id:
        target_cemetery = _cemetery_by_id(destination_cemetery_id)
        target_municipality = (target_cemetery.municipality or "").strip().lower()

    if source_municipality and target_municipality and source_municipality == target_municipality:
        return OperationType.TRASLADO_CORTO
    return OperationType.TRASLADO_LARGO


def _validate_transfer_type(case: OperationCase) -> None:
    if case.type not in {OperationType.TRASLADO_CORTO, OperationType.TRASLADO_LARGO}:
        return
    expected = _classify_transfer(
        source_sepultura=_sepultura_by_id(case.source_sepultura_id),
        target_sepultura_id=case.target_sepultura_id,
        destination_cemetery_id=case.destination_cemetery_id,
        destination_municipality=case.destination_municipality,
        cross_border=case.cross_border,
    )
    if expected != case.type:
        raise ValueError(
            f"Tipo de traslado invalido para destino indicado (esperado: {expected.value})"
        )


def _seed_initial_checklists(case: OperationCase) -> None:
    for permit_type, required in _required_permits(case.type, case.cross_border):
        db.session.add(
            OperationPermit(
                operation_case_id=case.id,
                permit_type=permit_type,
                required=required,
                status=OperationPermitStatus.MISSING,
            )
        )
    db.session.add(
        OperationDocument(
            operation_case_id=case.id,
            doc_type=ACTA_DOC_TYPE,
            required=True,
            status=OperationPermitStatus.MISSING,
        )
    )


def _log_status_change(
    case: OperationCase,
    from_status: OperationStatus | None,
    to_status: OperationStatus,
    user_id: int | None,
    reason: str,
) -> None:
    db.session.add(
        OperationStatusLog(
            operation_case_id=case.id,
            from_status=from_status.value if from_status else "",
            to_status=to_status.value,
            changed_by_user_id=user_id,
            reason=(reason or "").strip(),
        )
    )


def _log_activity(case: OperationCase, action_type: str, details: str, user_id: int | None) -> None:
    db.session.add(
        ActivityLog(
            org_id=_org_id(),
            sepultura_id=case.source_sepultura_id,
            action_type=action_type,
            details=details,
            user_id=user_id,
        )
    )


def _auto_create_work_order(case: OperationCase, user_id: int | None) -> WorkOrder:
    existing = (
        WorkOrder.query.filter_by(org_id=_org_id(), operation_case_id=case.id)
        .order_by(WorkOrder.id.desc())
        .first()
    )
    if existing:
        return existing
    payload = {
        "title": f"{case.type.value} {case.code}",
        "description": f"OT operativa asociada a {case.code}",
        "category": WorkOrderCategory.FUNERARIA.value,
        "priority": WorkOrderPriority.ALTA.value,
        "status": WorkOrderStatus.PENDIENTE_PLANIFICACION.value,
        "sepultura_id": str(case.source_sepultura_id),
        "operation_case_id": str(case.id),
        "type_code": case.type.value,
    }
    return create_work_order(payload, user_id=user_id)


def permit_label(permit_type: str) -> str:
    key = (permit_type or "").strip().upper()
    if not key:
        return "-"
    if key in PERMIT_LABELS:
        return PERMIT_LABELS[key]
    return key.replace("_", " ").title()


def documentation_rows_for_case(case: OperationCase) -> list[OperationPermit]:
    if case.type != OperationType.INHUMACION:
        return sorted(case.permits, key=lambda item: (item.permit_type, item.id))

    inhumation_rank = {code: idx for idx, code in enumerate(INHUMACION_DOCUMENTATION_ORDER)}
    inhumation_rows = [
        item for item in case.permits if item.permit_type in inhumation_rank
    ]
    inhumation_rows.sort(
        key=lambda item: (
            inhumation_rank.get(item.permit_type, 999),
            item.id,
        )
    )
    return inhumation_rows


def list_operation_cases(filters: dict[str, str]) -> list[OperationCase]:
    query = (
        OperationCase.query.options(
            joinedload(OperationCase.source_sepultura),
            joinedload(OperationCase.target_sepultura),
            joinedload(OperationCase.deceased_person),
            joinedload(OperationCase.declarant_person),
            joinedload(OperationCase.holder_person),
            joinedload(OperationCase.beneficiary_person),
        )
        .filter(OperationCase.org_id == _org_id())
        .order_by(OperationCase.created_at.desc(), OperationCase.id.desc())
    )
    code = (filters.get("code") or "").strip()
    if code:
        query = query.filter(OperationCase.code.ilike(f"%{code}%"))
    type_raw = (filters.get("type") or "").strip().upper()
    if type_raw:
        try:
            query = query.filter(OperationCase.type == OperationType[type_raw])
        except KeyError:
            return []
    status_raw = (filters.get("status") or "").strip().upper()
    if status_raw:
        try:
            query = query.filter(OperationCase.status == OperationStatus[status_raw])
        except KeyError:
            return []
    source_sepultura_id = (filters.get("source_sepultura_id") or "").strip()
    if source_sepultura_id:
        if not source_sepultura_id.isdigit():
            return []
        query = query.filter(OperationCase.source_sepultura_id == int(source_sepultura_id))
    deceased_person_id = (filters.get("deceased_person_id") or "").strip()
    if deceased_person_id:
        if not deceased_person_id.isdigit():
            return []
        query = query.filter(OperationCase.deceased_person_id == int(deceased_person_id))
    created_from = _parse_optional_datetime(filters.get("created_from"))
    if created_from:
        query = query.filter(OperationCase.created_at >= created_from)
    created_to = _parse_optional_datetime(filters.get("created_to"))
    if created_to:
        query = query.filter(OperationCase.created_at <= created_to)
    return query.all()


def operation_case_by_id(case_id: int) -> OperationCase:
    row = (
        OperationCase.query.options(
            joinedload(OperationCase.source_sepultura),
            joinedload(OperationCase.target_sepultura),
            joinedload(OperationCase.destination_cemetery),
            joinedload(OperationCase.deceased_person),
            joinedload(OperationCase.declarant_person),
            joinedload(OperationCase.holder_person),
            joinedload(OperationCase.beneficiary_person),
            joinedload(OperationCase.contract),
            joinedload(OperationCase.permits),
            joinedload(OperationCase.documents),
            joinedload(OperationCase.status_logs).joinedload(OperationStatusLog.changed_by),
            joinedload(OperationCase.work_orders).joinedload(WorkOrder.assigned_user),
        )
        .filter_by(org_id=_org_id(), id=case_id)
        .first()
    )
    if not row:
        raise ValueError("Expediente no encontrado")
    return row


def create_operation_case(payload: dict[str, str], user_id: int | None) -> OperationCase:
    operation_type = _parse_operation_type(payload.get("type", ""))
    source_raw = (payload.get("source_sepultura_id") or "").strip()
    if not source_raw.isdigit():
        raise ValueError("Sepultura origen obligatoria")
    source_sepultura = _sepultura_by_id(int(source_raw))

    target_sepultura_id = None
    target_raw = (payload.get("target_sepultura_id") or "").strip()
    if target_raw:
        if not target_raw.isdigit():
            raise ValueError("Sepultura destino invalida")
        target_sepultura_id = int(target_raw)
        _sepultura_by_id(target_sepultura_id)

    destination_cemetery_id = None
    destination_cemetery_raw = (payload.get("destination_cemetery_id") or "").strip()
    if destination_cemetery_raw:
        if not destination_cemetery_raw.isdigit():
            raise ValueError("Cementerio destino invalido")
        destination_cemetery_id = int(destination_cemetery_raw)
        _cemetery_by_id(destination_cemetery_id)

    deceased_person_id = None
    deceased_raw = (payload.get("deceased_person_id") or "").strip()
    if deceased_raw:
        if not deceased_raw.isdigit():
            raise ValueError("Difunto invalido")
        deceased_person_id = int(deceased_raw)
        _person_exists(deceased_person_id, "Difunto")

    declarant_person_id = None
    declarant_raw = (payload.get("declarant_person_id") or "").strip()
    if declarant_raw:
        if not declarant_raw.isdigit():
            raise ValueError("Declarante invalido")
        declarant_person_id = int(declarant_raw)
        _person_exists(declarant_person_id, "Declarante")

    holder_person_id = None
    holder_raw = (payload.get("holder_person_id") or "").strip()
    if holder_raw:
        if not holder_raw.isdigit():
            raise ValueError("Titular invalido")
        holder_person_id = int(holder_raw)
        _person_exists(holder_person_id, "Titular")

    beneficiary_person_id = None
    beneficiary_raw = (payload.get("beneficiary_person_id") or "").strip()
    if beneficiary_raw:
        if not beneficiary_raw.isdigit():
            raise ValueError("Beneficiario invalido")
        beneficiary_person_id = int(beneficiary_raw)
        _person_exists(beneficiary_person_id, "Beneficiario")

    if holder_person_id is None and declarant_person_id is not None:
        holder_person_id = declarant_person_id

    cross_border = _parse_bool(payload.get("cross_border"))
    destination_municipality = (payload.get("destination_municipality") or "").strip()

    if operation_type in {OperationType.TRASLADO_CORTO, OperationType.TRASLADO_LARGO}:
        if not target_sepultura_id and not destination_cemetery_id and not destination_municipality:
            raise ValueError("Debes indicar destino de traslado")

    concession_start_date = None
    concession_end_date = None
    if operation_type == OperationType.INHUMACION:
        concession_start_date = _today()
        concession_end_date = _add_years(concession_start_date, 25)

    case = OperationCase(
        org_id=_org_id(),
        code=_next_operation_code(),
        type=operation_type,
        status=OperationStatus.BORRADOR,
        source_sepultura_id=source_sepultura.id,
        target_sepultura_id=target_sepultura_id,
        deceased_person_id=deceased_person_id,
        declarant_person_id=declarant_person_id,
        holder_person_id=holder_person_id,
        beneficiary_person_id=beneficiary_person_id,
        concession_start_date=concession_start_date,
        concession_end_date=concession_end_date,
        scheduled_at=_parse_optional_datetime(payload.get("scheduled_at")),
        destination_cemetery_id=destination_cemetery_id,
        destination_name=(payload.get("destination_name") or "").strip(),
        destination_municipality=destination_municipality,
        destination_region=(payload.get("destination_region") or "").strip(),
        destination_country=(payload.get("destination_country") or "").strip(),
        cross_border=cross_border,
        notes=(payload.get("notes") or "").strip(),
        created_by_user_id=user_id,
        managed_by_user_id=user_id,
    )
    _validate_transfer_type(case)
    db.session.add(case)
    db.session.flush()

    _seed_initial_checklists(case)
    _log_status_change(
        case=case,
        from_status=None,
        to_status=OperationStatus.BORRADOR,
        user_id=user_id,
        reason="Alta expediente",
    )
    _log_activity(
        case,
        "OPERATION_CASE_CREATED",
        f"Expediente {case.code} ({case.type.value}) creado",
        user_id,
    )
    db.session.commit()
    emit_work_order_event(
        f"OPERATION_CASE_CREATED_{case.type.value}",
        {
            "operation_case_id": case.id,
            "sepultura_id": case.source_sepultura_id,
            "category": WorkOrderCategory.FUNERARIA.value,
            "title": f"{case.type.value} {case.code}",
            "description": case.notes or f"Expediente {case.code}",
        },
        user_id=user_id,
    )
    return case


def update_operation_summary(
    case_id: int, payload: dict[str, str], user_id: int | None
) -> OperationCase:
    case = operation_case_by_id(case_id)
    if case.type != OperationType.INHUMACION:
        raise ValueError("Solo INHUMACION permite editar este resumen")

    source_raw = (payload.get("source_sepultura_id") or "").strip()
    if not source_raw.isdigit() or int(source_raw) <= 0:
        raise ValueError("Debes seleccionar una sepultura valida")
    source_sepultura_id = int(source_raw)
    _sepultura_by_id(source_sepultura_id)

    holder_raw = (payload.get("holder_person_id") or "").strip()
    if not holder_raw.isdigit() or int(holder_raw) <= 0:
        raise ValueError("Debes seleccionar un titular")
    holder_person_id = int(holder_raw)
    _person_exists(holder_person_id, "Titular")

    deceased_person_id = None
    deceased_raw = (payload.get("deceased_person_id") or "").strip()
    if deceased_raw:
        if not deceased_raw.isdigit() or int(deceased_raw) <= 0:
            raise ValueError("Difunto invalido")
        deceased_person_id = int(deceased_raw)
        _person_exists(deceased_person_id, "Difunto")

    beneficiary_person_id = None
    beneficiary_raw = (payload.get("beneficiary_person_id") or "").strip()
    if beneficiary_raw:
        if not beneficiary_raw.isdigit() or int(beneficiary_raw) <= 0:
            raise ValueError("Beneficiario invalido")
        beneficiary_person_id = int(beneficiary_raw)
        _person_exists(beneficiary_person_id, "Beneficiario")

    case.source_sepultura_id = source_sepultura_id
    case.deceased_person_id = deceased_person_id
    case.holder_person_id = holder_person_id
    case.beneficiary_person_id = beneficiary_person_id
    case.declarant_person_id = holder_person_id
    db.session.add(case)

    _log_activity(
        case,
        "OPERATION_CASE_SUMMARY",
        f"{case.code}: resumen de inhumacion actualizado",
        user_id,
    )
    db.session.commit()
    return case


def operation_concession_context(case: OperationCase) -> dict[str, object] | None:
    if case.type != OperationType.INHUMACION:
        return None

    active_contract = _active_contract_for_sepultura(case.source_sepultura_id)
    if active_contract:
        start_date = active_contract.fecha_inicio
        end_date = active_contract.fecha_fin
    else:
        start_date, end_date = _default_concession_dates(case)

    return {
        "readonly": active_contract is not None,
        "active_contract": active_contract,
        "start_date": start_date,
        "end_date": end_date,
        "duration_years": end_date.year - start_date.year,
    }


def update_operation_concession(
    case_id: int, payload: dict[str, str], user_id: int | None
) -> OperationCase:
    case = operation_case_by_id(case_id)
    if case.type != OperationType.INHUMACION:
        raise ValueError("Solo INHUMACION permite editar la concesion")

    active_contract = _active_contract_for_sepultura(case.source_sepultura_id)
    if active_contract:
        raise ValueError(
            f"La concesion se reutiliza desde el contrato activo C{active_contract.id} y no puede editarse"
        )

    start_date = _parse_iso_date(payload.get("concession_start_date"), "fecha inicio")
    end_date = _parse_iso_date(payload.get("concession_end_date"), "fecha fin")
    _apply_concession_dates(case, start_date, end_date)
    db.session.add(case)
    _log_activity(
        case,
        "OPERATION_CASE_CONCESSION",
        f"{case.code}: concesion actualizada ({start_date} - {end_date})",
        user_id,
    )
    db.session.commit()
    return case


def change_operation_status(case_id: int, new_status_raw: str, reason: str, user_id: int | None) -> OperationCase:
    case = operation_case_by_id(case_id)
    target = _parse_operation_status(new_status_raw)
    current = case.status
    if target == current:
        return case
    if target not in OPERATION_STATUS_TRANSITIONS.get(current, set()):
        raise ValueError(f"Transicion invalida: {current.value} -> {target.value}")
    note = (reason or "").strip()
    if target == OperationStatus.CANCELADA and not note:
        raise ValueError("Motivo obligatorio para cancelar")
    if target == OperationStatus.PROGRAMADA:
        missing = [p for p in case.permits if p.required and p.status != OperationPermitStatus.VERIFIED]
        if missing:
            raise ValueError("No se puede programar: faltan permisos verificados")
        _validate_transfer_type(case)
        _auto_create_work_order(case, user_id)
    if target == OperationStatus.EN_EJECUCION and not case.executed_at:
        case.executed_at = _now()
    case.status = target
    db.session.add(case)
    _log_status_change(case, current, target, user_id, note)
    _log_activity(
        case,
        "OPERATION_CASE_STATUS",
        f"{case.code}: {current.value} -> {target.value}",
        user_id,
    )
    db.session.commit()
    return case


def verify_operation_permit(case_id: int, permit_id: int, payload: dict[str, str], user_id: int | None) -> OperationPermit:
    case = operation_case_by_id(case_id)
    permit = OperationPermit.query.filter_by(operation_case_id=case.id, id=permit_id).first()
    if not permit:
        raise ValueError("Permiso no encontrado")
    action = (payload.get("action") or "verify").strip().lower()
    if action not in {"verify", "reject", "provide"}:
        raise ValueError("Accion de permiso invalida")
    if action == "verify":
        permit.status = OperationPermitStatus.VERIFIED
        permit.verified_at = _now()
        permit.verified_by_user_id = user_id
        permit.issued_at = _parse_optional_datetime(payload.get("issued_at")) or permit.issued_at
    elif action == "reject":
        permit.status = OperationPermitStatus.REJECTED
        permit.verified_at = None
        permit.verified_by_user_id = None
    else:
        permit.status = OperationPermitStatus.PROVIDED
    permit.reference_number = (payload.get("reference_number") or permit.reference_number or "").strip()
    if payload.get("notes"):
        permit.notes = (payload.get("notes") or "").strip()
    db.session.add(permit)
    _log_activity(
        case,
        "OPERATION_PERMIT",
        f"{case.code}: permiso {permit.permit_type} -> {permit.status.value}",
        user_id,
    )
    db.session.commit()
    return permit


def upload_operation_document(
    case_id: int,
    payload: dict[str, str],
    file_obj: FileStorage,
    user_id: int | None,
) -> OperationDocument:
    case = operation_case_by_id(case_id)
    if not file_obj or not file_obj.filename:
        raise ValueError("Debes seleccionar un fichero")
    doc_type = (payload.get("doc_type") or "OTROS").strip().upper()
    if not doc_type:
        raise ValueError("Tipo de documento obligatorio")
    required = _parse_bool(payload.get("required"))
    filename = secure_filename(file_obj.filename) or f"document-{case.id}.bin"
    document = (
        OperationDocument.query.filter_by(operation_case_id=case.id, doc_type=doc_type)
        .order_by(OperationDocument.id.asc())
        .first()
    )
    if not document:
        document = OperationDocument(
            operation_case_id=case.id,
            doc_type=doc_type,
            required=required,
            status=OperationPermitStatus.MISSING,
        )
        db.session.add(document)
        db.session.flush()

    root = (
        Path(current_app.instance_path)
        / "storage"
        / "cemetery"
        / "expedientes"
        / str(case.org_id)
        / str(case.id)
        / "documents"
        / str(document.id)
    )
    root.mkdir(parents=True, exist_ok=True)
    absolute = root / filename
    file_obj.save(absolute)

    document.file_path = absolute.relative_to(Path(current_app.instance_path)).as_posix()
    document.status = OperationPermitStatus.PROVIDED
    document.uploaded_at = _now()
    document.required = document.required or required
    if payload.get("notes"):
        document.notes = (payload.get("notes") or "").strip()
    db.session.add(document)
    _log_activity(case, "OPERATION_DOCUMENT", f"{case.code}: documento {doc_type} subido", user_id)
    db.session.commit()
    return document


def verify_operation_document(case_id: int, doc_id: int, payload: dict[str, str], user_id: int | None) -> OperationDocument:
    case = operation_case_by_id(case_id)
    document = OperationDocument.query.filter_by(operation_case_id=case.id, id=doc_id).first()
    if not document:
        raise ValueError("Documento no encontrado")
    action = (payload.get("action") or "verify").strip().lower()
    if action not in {"verify", "reject", "provide"}:
        raise ValueError("Accion de documento invalida")
    if action == "verify":
        document.status = OperationPermitStatus.VERIFIED
        document.verified_at = _now()
        document.verified_by_user_id = user_id
    elif action == "reject":
        document.status = OperationPermitStatus.REJECTED
        document.verified_at = None
        document.verified_by_user_id = None
    else:
        document.status = OperationPermitStatus.PROVIDED
    if payload.get("notes"):
        document.notes = (payload.get("notes") or "").strip()
    db.session.add(document)
    _log_activity(
        case,
        "OPERATION_DOCUMENT_VERIFY",
        f"{case.code}: documento {document.doc_type} -> {document.status.value}",
        user_id,
    )
    db.session.commit()
    return document


def create_operation_work_order(case_id: int, payload: dict[str, str], user_id: int | None) -> WorkOrder:
    case = operation_case_by_id(case_id)
    title = (payload.get("title") or "").strip() or f"{case.type.value} {case.code}"
    row = create_work_order(
        {
            "title": title,
            "description": (payload.get("description") or "").strip() or f"OT vinculada a {case.code}",
            "category": (payload.get("category") or WorkOrderCategory.FUNERARIA.value).strip().upper(),
            "priority": (payload.get("priority") or WorkOrderPriority.MEDIA.value).strip().upper(),
            "status": (payload.get("status") or WorkOrderStatus.PENDIENTE_PLANIFICACION.value).strip().upper(),
            "sepultura_id": str(case.source_sepultura_id),
            "operation_case_id": str(case.id),
            "type_code": case.type.value,
        },
        user_id=user_id,
    )
    _log_activity(case, "OPERATION_OT", f"{case.code}: OT {row.code} creada", user_id)
    db.session.commit()
    return row


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


def _ensure_acta_document(case: OperationCase, user_id: int | None) -> tuple[OperationDocument, bytes, str]:
    lines = [
        "GSF - Acta de expediente",
        f"Expediente: {case.code}",
        f"Tipo: {case.type.value}",
        f"Estado: {case.status.value}",
        f"Sepultura origen: {case.source_sepultura.location_label if case.source_sepultura else '-'}",
        f"Sepultura destino: {case.target_sepultura.location_label if case.target_sepultura else '-'}",
        f"Difunto ID: {case.deceased_person_id or '-'}",
        f"Programada: {case.scheduled_at.strftime('%Y-%m-%d %H:%M') if case.scheduled_at else '-'}",
        f"Ejecutada: {case.executed_at.strftime('%Y-%m-%d %H:%M') if case.executed_at else '-'}",
        f"Cierre: {_now().strftime('%Y-%m-%d %H:%M')}",
    ]
    pdf = _simple_pdf(lines)
    filename = f"acta-{case.code}.pdf"
    root = (
        Path(current_app.instance_path)
        / "storage"
        / "cemetery"
        / "expedientes"
        / str(case.org_id)
        / str(case.id)
        / "acta"
    )
    root.mkdir(parents=True, exist_ok=True)
    absolute = root / filename
    absolute.write_bytes(pdf)

    document = (
        OperationDocument.query.filter_by(operation_case_id=case.id, doc_type=ACTA_DOC_TYPE)
        .order_by(OperationDocument.id.asc())
        .first()
    )
    if not document:
        document = OperationDocument(
            operation_case_id=case.id,
            doc_type=ACTA_DOC_TYPE,
            required=True,
            status=OperationPermitStatus.MISSING,
        )
        db.session.add(document)
        db.session.flush()
    document.file_path = absolute.relative_to(Path(current_app.instance_path)).as_posix()
    document.status = OperationPermitStatus.VERIFIED
    document.uploaded_at = _now()
    document.verified_at = _now()
    document.verified_by_user_id = user_id
    db.session.add(document)
    return document, pdf, filename


def _completed_work_order_count(case_id: int) -> int:
    return (
        WorkOrder.query.filter_by(org_id=_org_id(), operation_case_id=case_id)
        .filter(WorkOrder.status == WorkOrderStatus.COMPLETADA)
        .count()
    )


def _validate_closure(case: OperationCase) -> None:
    if case.status != OperationStatus.EN_VALIDACION:
        raise ValueError("Solo se puede cerrar en estado EN_VALIDACION")
    missing_permits = [p for p in case.permits if p.required and p.status != OperationPermitStatus.VERIFIED]
    if missing_permits:
        raise ValueError("No se puede cerrar: faltan permisos verificados")
    if _completed_work_order_count(case.id) <= 0:
        raise ValueError("No se puede cerrar: no hay OT completada")
    if case.type in {OperationType.EXHUMACION, OperationType.RESCATE}:
        remains = SepulturaDifunto.query.filter_by(org_id=_org_id(), sepultura_id=case.source_sepultura_id).count()
        if remains <= 0:
            raise ValueError("No se puede cerrar: no hay restos previos en la sepultura origen")
    _validate_transfer_type(case)


def _move_remains(case: OperationCase) -> None:
    if case.type == OperationType.INHUMACION:
        if not case.deceased_person_id:
            raise ValueError("INHUMACION requiere difunto asociado")
        exists = SepulturaDifunto.query.filter_by(
            org_id=_org_id(),
            sepultura_id=case.source_sepultura_id,
            person_id=case.deceased_person_id,
        ).first()
        if not exists:
            db.session.add(
                SepulturaDifunto(
                    org_id=_org_id(),
                    sepultura_id=case.source_sepultura_id,
                    person_id=case.deceased_person_id,
                    notes=f"Inhumacion {case.code}",
                )
            )
        return

    if case.type in {OperationType.EXHUMACION, OperationType.RESCATE, OperationType.TRASLADO_CORTO, OperationType.TRASLADO_LARGO}:
        query = SepulturaDifunto.query.filter_by(org_id=_org_id(), sepultura_id=case.source_sepultura_id)
        if case.deceased_person_id:
            query = query.filter_by(person_id=case.deceased_person_id)
        remain = query.order_by(SepulturaDifunto.id.asc()).first()
        if not remain:
            raise ValueError("No hay restos/difunto para operar en sepultura origen")
        person_id = remain.person_id
        db.session.delete(remain)

        if case.type in {OperationType.TRASLADO_CORTO, OperationType.TRASLADO_LARGO} and case.target_sepultura_id:
            db.session.add(
                SepulturaDifunto(
                    org_id=_org_id(),
                    sepultura_id=case.target_sepultura_id,
                    person_id=person_id,
                    notes=f"Traslado desde expediente {case.code}",
                )
            )


def _ensure_inhumacion_contract(case: OperationCase, user_id: int | None) -> DerechoFunerarioContrato | None:
    if case.type != OperationType.INHUMACION:
        return None

    today = _today()
    active_contract = _active_contract_for_sepultura(case.source_sepultura_id, today)
    if active_contract:
        _sync_case_concession_from_contract(case, active_contract)
        db.session.add(case)
        return active_contract

    if not case.holder_person_id:
        raise ValueError("No se puede cerrar: INHUMACION requiere titular para crear la concesion")

    start_date, end_date = _default_concession_dates(case)
    _apply_concession_dates(case, start_date, end_date)

    contract = DerechoFunerarioContrato(
        org_id=_org_id(),
        sepultura_id=case.source_sepultura_id,
        tipo=DerechoTipo.CONCESION,
        fecha_inicio=start_date,
        fecha_fin=end_date,
        legacy_99_years=False,
        annual_fee_amount=Decimal("0.00"),
        estado="ACTIVO",
    )
    db.session.add(contract)
    db.session.flush()

    db.session.add(
        OwnershipRecord(
            org_id=_org_id(),
            contract_id=contract.id,
            person_id=case.holder_person_id,
            start_date=start_date,
        )
    )
    if case.beneficiary_person_id:
        db.session.add(
            Beneficiario(
                org_id=_org_id(),
                contrato_id=contract.id,
                person_id=case.beneficiary_person_id,
                activo_desde=start_date,
            )
        )

    _sync_case_concession_from_contract(case, contract)
    db.session.add(case)
    _log_activity(
        case,
        "OPERATION_CASE_CONCESSION_LINKED",
        f"{case.code}: concesion C{contract.id} creada al cerrar inhumacion",
        user_id,
    )
    return contract


def close_operation_case(case_id: int, payload: dict[str, str], user_id: int | None) -> OperationCase:
    case = operation_case_by_id(case_id)
    _validate_closure(case)
    _move_remains(case)
    _ensure_inhumacion_contract(case, user_id)
    _ensure_acta_document(case, user_id)

    previous = case.status
    case.status = OperationStatus.CERRADA
    case.closed_at = _now()
    db.session.add(case)
    _log_status_change(
        case=case,
        from_status=previous,
        to_status=OperationStatus.CERRADA,
        user_id=user_id,
        reason=(payload.get("reason") or "Cierre expediente").strip(),
    )

    movement_map = {
        OperationType.INHUMACION: MovimientoTipo.INHUMACION,
        OperationType.EXHUMACION: MovimientoTipo.EXHUMACION,
        OperationType.TRASLADO_CORTO: MovimientoTipo.TRASLADO_CORTO,
        OperationType.TRASLADO_LARGO: MovimientoTipo.TRASLADO_LARGO,
        OperationType.RESCATE: MovimientoTipo.RESCATE,
    }
    movement_type = movement_map[case.type]
    db.session.add(
        MovimientoSepultura(
            org_id=_org_id(),
            sepultura_id=case.source_sepultura_id,
            tipo=movement_type,
            detalle=f"Expediente {case.code} cerrado",
            user_id=user_id,
        )
    )
    if case.target_sepultura_id and case.type in {OperationType.TRASLADO_CORTO, OperationType.TRASLADO_LARGO}:
        db.session.add(
            MovimientoSepultura(
                org_id=_org_id(),
                sepultura_id=case.target_sepultura_id,
                tipo=movement_type,
                detalle=f"Expediente {case.code} recibido desde sepultura origen",
                user_id=user_id,
            )
        )
    _log_activity(case, "OPERATION_CASE_CLOSED", f"Expediente {case.code} cerrado", user_id)
    db.session.commit()
    return case


def operation_acta_pdf(case_id: int, user_id: int | None = None) -> tuple[bytes, str]:
    case = operation_case_by_id(case_id)
    acta_doc = (
        OperationDocument.query.filter_by(operation_case_id=case.id, doc_type=ACTA_DOC_TYPE)
        .order_by(OperationDocument.id.asc())
        .first()
    )
    if acta_doc and acta_doc.file_path:
        absolute = Path(current_app.instance_path) / acta_doc.file_path
        if absolute.exists():
            return absolute.read_bytes(), absolute.name
    _doc, content, filename = _ensure_acta_document(case, user_id)
    db.session.commit()
    return content, filename
