from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from io import BytesIO, StringIO
from pathlib import Path
import csv
import json
import os
import smtplib
import statistics
import shutil
from email.message import EmailMessage
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from flask import current_app, g
from sqlalchemy import func, inspect, or_, text
from sqlalchemy.orm import joinedload
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from app.core.demo_people import generate_demo_names, is_generic_demo_name
from app.core.extensions import db
from app.core.i18n import translate
from app.core.models import (
    ActivityLog,
    Beneficiario,
    Cemetery,
    DerechoTipo,
    DerechoFunerarioContrato,
    Expediente,
    InscripcionLateral,
    Invoice,
    InvoiceEstado,
    LapidaStock,
    LapidaStockMovimiento,
    OwnershipPartyRole,
    OwnershipRecord,
    OwnershipTransferCase,
    OwnershipTransferParty,
    OwnershipTransferStatus,
    OwnershipTransferType,
    BeneficiaryCloseDecision,
    CaseDocument,
    CaseDocumentStatus,
    ContractEvent,
    Membership,
    MovimientoSepultura,
    MovimientoTipo,
    OrdenTrabajo,
    Organization,
    OWNERSHIP_CASE_CHECKLIST,
    OperationCase,
    OperationDocument,
    OperationPermit,
    OperationPermitStatus,
    OperationStatus,
    OperationStatusLog,
    OperationType,
    Payment,
    Person,
    Publication,
    Sepultura,
    SepulturaDifunto,
    SepulturaEstado,
    SepulturaUbicacion,
    TasaMantenimientoTicket,
    TicketDescuentoTipo,
    TicketEstado,
    ReportDeliveryLog,
    ReportSchedule,
    User,
    WorkOrder,
    WorkOrderChecklistItem,
    WorkOrderDependency,
    WorkOrderEvidence,
    WorkOrderEventLog,
    WorkOrderEventRule,
    WorkOrderAreaType,
    WorkOrderCategory,
    WorkOrderPriority,
    WorkOrderStatus,
    WorkOrderStatusLog,
    WorkOrderTemplate,
    WorkOrderTemplateChecklistItem,
    WorkOrderType,
)
from app.cemetery.work_order_service import emit_work_order_event


@dataclass
class MassCreatePreview:
    total: int
    rows: list[dict[str, int | str]]


@dataclass
class TicketGenerationResult:
    created: int = 0
    existing: int = 0
    skipped_non_concession: int = 0


CASE_STATUS_TRANSITIONS: dict[OwnershipTransferStatus, set[OwnershipTransferStatus]] = {
    OwnershipTransferStatus.DRAFT: {OwnershipTransferStatus.DOCS_PENDING},
    OwnershipTransferStatus.DOCS_PENDING: {
        OwnershipTransferStatus.UNDER_REVIEW,
        OwnershipTransferStatus.REJECTED,
    },
    OwnershipTransferStatus.UNDER_REVIEW: {
        OwnershipTransferStatus.DOCS_PENDING,
        OwnershipTransferStatus.APPROVED,
        OwnershipTransferStatus.REJECTED,
    },
    OwnershipTransferStatus.REJECTED: {OwnershipTransferStatus.DOCS_PENDING},
    OwnershipTransferStatus.APPROVED: {OwnershipTransferStatus.CLOSED},
    OwnershipTransferStatus.CLOSED: set(),
}


CASE_CHECKLIST: dict[OwnershipTransferType, list[tuple[str, bool]]] = (
    OWNERSHIP_CASE_CHECKLIST
)
BENEFICIARY_REPLACE_REQUIRED_DOC_TYPES = (
    "SOLICITUD_BENEFICIARIO",
    "DNI_NUEVO_BENEFICIARIO",
)

EXPEDIENTE_STATES = ("ABIERTO", "EN_TRAMITE", "FINALIZADO", "CANCELADO")
EXPEDIENTE_TRANSITIONS: dict[str, set[str]] = {
    "ABIERTO": {"EN_TRAMITE", "CANCELADO"},
    "EN_TRAMITE": {"FINALIZADO", "CANCELADO"},
    "FINALIZADO": set(),
    "CANCELADO": set(),
}

INSCRIPCION_STATES = (
    "PENDIENTE_GRABAR",
    "PENDIENTE_COLOCAR",
    "PENDIENTE_NOTIFICAR",
    "NOTIFICADA",
)
INSCRIPCION_TRANSITIONS: dict[str, str] = {
    "PENDIENTE_GRABAR": "PENDIENTE_COLOCAR",
    "PENDIENTE_COLOCAR": "PENDIENTE_NOTIFICAR",
    "PENDIENTE_NOTIFICAR": "NOTIFICADA",
}
MAX_ACTIVITY_ITEMS_PER_HOLDER = 5
MAX_ACTIVITY_ITEMS_PER_GRAVE = 8


def org_id() -> int:
    return g.org.id


def org_record() -> Organization:
    return Organization.query.filter_by(id=org_id()).first()


def org_cemetery() -> Cemetery:
    cemetery = (
        Cemetery.query.filter_by(org_id=org_id()).order_by(Cemetery.id.asc()).first()
    )
    if not cemetery:
        raise ValueError("No hay cementerio configurado para esta organización")
    return cemetery


def _parse_iso_date(value: str, field_name: str) -> date:
    raw = (value or "").strip()
    if not raw:
        raise ValueError(f"Falta {field_name}")
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"Formato de fecha invalido para {field_name}") from exc


def _parse_decimal(value: str, field_name: str) -> Decimal:
    raw = (value or "").strip().replace(",", ".")
    if not raw:
        raise ValueError(f"Falta {field_name}")
    try:
        return Decimal(raw).quantize(Decimal("0.01"))
    except Exception as exc:  # pragma: no cover - DecimalException umbrella
        raise ValueError(f"Importe invalido en {field_name}") from exc


def _parse_optional_iso_date(value: str | None) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    return date.fromisoformat(raw)


def _add_years(base: date, years: int) -> date:
    try:
        return base.replace(year=base.year + years)
    except ValueError:
        # Leap day edge-case: move to Feb 28.
        return base.replace(month=2, day=28, year=base.year + years)


def panel_data() -> dict[str, object]:
    oid = org_id()
    ot_abiertas = (
        WorkOrder.query.filter_by(org_id=oid)
        .filter(WorkOrder.status.notin_([WorkOrderStatus.COMPLETADA, WorkOrderStatus.CANCELADA]))
        .count()
    )
    ot_pendientes = (
        WorkOrder.query.filter_by(org_id=oid)
        .filter(
            WorkOrder.status.in_(
                [
                    WorkOrderStatus.BORRADOR,
                    WorkOrderStatus.PENDIENTE_PLANIFICACION,
                    WorkOrderStatus.PLANIFICADA,
                    WorkOrderStatus.ASIGNADA,
                ]
            )
        )
        .count()
    )
    tiquets_impagados = (
        TasaMantenimientoTicket.query.filter_by(org_id=oid)
        .filter(TasaMantenimientoTicket.estado != TicketEstado.COBRADO)
        .count()
    )
    pendientes_notificar = InscripcionLateral.query.filter_by(
        org_id=oid, estado="PENDIENTE_NOTIFICAR"
    ).count()

    recent_work_orders = (
        WorkOrder.query.options(joinedload(WorkOrder.sepultura))
        .filter_by(org_id=oid)
        .order_by(WorkOrder.created_at.desc(), WorkOrder.id.desc())
        .limit(5)
        .all()
    )
    recent_movements = (
        MovimientoSepultura.query.options(
            joinedload(MovimientoSepultura.sepultura),
            joinedload(MovimientoSepultura.user),
        )
        .filter_by(org_id=oid)
        .order_by(MovimientoSepultura.fecha.desc())
        .limit(30)
        .all()
    )
    recent_activity_logs = (
        ActivityLog.query.options(
            joinedload(ActivityLog.sepultura),
            joinedload(ActivityLog.user),
        )
        .filter_by(org_id=oid)
        .order_by(ActivityLog.created_at.desc(), ActivityLog.id.desc())
        .limit(60)
        .all()
    )
    recent_activity = _recent_activity_from_logs(recent_activity_logs)
    if not recent_activity:
        recent_activity = _recent_activity_companywide(recent_movements)
    recent_activity_by_sepultura = _recent_activity_by_sepultura(recent_activity)

    lliures = Sepultura.query.filter_by(
        org_id=oid, estado=SepulturaEstado.LLIURE
    ).count()
    alerts: list[str] = []
    pending_not_invoiced = TasaMantenimientoTicket.query.filter_by(
        org_id=oid, estado=TicketEstado.PENDIENTE
    ).count()
    if pending_not_invoiced > 0:
        alerts.append(
            translate("dashboard.alert.pending_tickets").format(
                count=pending_not_invoiced
            )
        )
    pending_lateral = InscripcionLateral.query.filter_by(
        org_id=oid, estado="PENDIENTE_COLOCAR"
    ).count()
    if pending_lateral > 0:
        alerts.append(
            translate("dashboard.alert.pending_lateral").format(count=pending_lateral)
        )
    if lliures > 0:
        alerts.append(translate("dashboard.alert.lliures").format(count=lliures))
    if not alerts:
        alerts.append(translate("dashboard.alert.none"))

    return {
        "kpis": {
            "expedientes_abiertos": 0,
            "ot_abiertas": ot_abiertas,
            "ot_pendientes": ot_pendientes,
            "tiquets_impagados": tiquets_impagados,
            "pendientes_notificar": pendientes_notificar,
        },
        "recent_expedientes": [],
        "recent_work_orders": recent_work_orders,
        "recent_activity_by_sepultura": recent_activity_by_sepultura,
        "recent_activity_by_titular": _recent_activity_by_titular(oid, recent_movements),
        "recent_activity": recent_activity,
        "alerts": alerts,
    }


def _recent_activity_from_logs(logs: list[ActivityLog]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in logs:
        sepultura_label = item.sepultura.location_label if item.sepultura else "-"
        open_path = _activity_open_path(item.action_type, item.sepultura_id)
        rows.append(
            {
                "fecha": item.created_at,
                "usuario": item.user.full_name if item.user else "Sistema",
                "tipo": item.action_type,
                "sepultura_id": item.sepultura_id,
                "sepultura": sepultura_label,
                "detalle": item.details,
                "open_path": open_path,
            }
        )
    return rows


def _recent_activity_companywide(
    movements: list[MovimientoSepultura],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for movement in movements:
        sepultura_label = (
            movement.sepultura.location_label
            if movement.sepultura
            else f"#{movement.sepultura_id}"
        )
        movement_type = (
            movement.tipo.value
            if hasattr(movement.tipo, "value")
            else str(movement.tipo)
        )
        rows.append(
            {
                "fecha": movement.fecha,
                "usuario": movement.user.full_name if movement.user else "Sistema",
                "tipo": movement_type,
                "sepultura_id": movement.sepultura_id,
                "sepultura": sepultura_label,
                "detalle": movement.detalle,
                "open_path": _activity_open_path(movement_type, movement.sepultura_id),
            }
        )
    return rows


def _recent_activity_by_sepultura(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    if not rows:
        return []

    grouped: dict[tuple[str, int], dict[str, object]] = {}
    order: list[tuple[str, int]] = []

    for item in rows:
        sepultura_id = item.get("sepultura_id")
        if isinstance(sepultura_id, int) and sepultura_id > 0:
            key = ("sepultura", sepultura_id)
            label = (item.get("sepultura") or "").strip() or translate(
                "dashboard.grave_ref"
            ).format(id=sepultura_id)
            group_open_path = f"/cementerio/sepulturas/{sepultura_id}"
        else:
            key = ("sin_sepultura", 0)
            label = translate("dashboard.no_grave")
            group_open_path = None

        if key not in grouped:
            grouped[key] = {
                "sepultura": label,
                "sepultura_id": sepultura_id if isinstance(sepultura_id, int) else None,
                "open_path": group_open_path,
                "items": [],
            }
            order.append(key)

        items = grouped[key]["items"]
        if len(items) >= MAX_ACTIVITY_ITEMS_PER_GRAVE:
            continue
        items.append(item)

    return [grouped[key] for key in order]


def _activity_open_path(action_type: str | None, sepultura_id: int | None) -> str | None:
    if sepultura_id:
        return f"/cementerio/sepulturas/{sepultura_id}"

    action = (action_type or "").strip().upper()
    if not action:
        return None
    if action.startswith("OT_"):
        return "/cementerio/ot"
    if action.startswith("PERSONA_"):
        return "/cementerio/personas"
    if "TITULAR" in action or "TRANSMISION" in action or "BENEFICIARIO" in action:
        return "/cementerio/titularidad/casos"
    if "TASA" in action or "COBRO" in action:
        return "/cementerio/tasas"
    if "LAPIDA" in action or "INSCRIPCION" in action:
        return "/cementerio/lapidas"
    return None


def _recent_activity_by_titular(
    oid: int, movements: list[MovimientoSepultura]
) -> list[dict[str, object]]:
    if not movements:
        return []

    sepultura_ids = sorted({movement.sepultura_id for movement in movements})
    contracts = (
        DerechoFunerarioContrato.query.filter_by(org_id=oid, estado="ACTIVO")
        .filter(DerechoFunerarioContrato.sepultura_id.in_(sepultura_ids))
        .order_by(DerechoFunerarioContrato.id.desc())
        .all()
    )
    contract_by_sepultura: dict[int, DerechoFunerarioContrato] = {}
    for contract in contracts:
        contract_by_sepultura.setdefault(contract.sepultura_id, contract)

    owner_by_contract: dict[int, OwnershipRecord] = {}
    contract_ids = [contract.id for contract in contract_by_sepultura.values()]
    if contract_ids:
        owners = (
            OwnershipRecord.query.options(joinedload(OwnershipRecord.person))
            .filter_by(org_id=oid)
            .filter(OwnershipRecord.contract_id.in_(contract_ids))
            .filter(
                or_(
                    OwnershipRecord.end_date.is_(None),
                    OwnershipRecord.end_date >= date.today(),
                )
            )
            .order_by(
                OwnershipRecord.contract_id.asc(), OwnershipRecord.start_date.desc()
            )
            .all()
        )
        for owner in owners:
            owner_by_contract.setdefault(owner.contract_id, owner)

    grouped: dict[tuple[str, int], dict[str, object]] = {}
    group_order: list[tuple[str, int]] = []

    for movement in movements:
        contract = contract_by_sepultura.get(movement.sepultura_id)
        owner = owner_by_contract.get(contract.id) if contract else None
        if owner and owner.person:
            key = ("person", owner.person_id)
            titular_label = owner.person.full_name
        else:
            key = ("unassigned", 0)
            titular_label = translate("dashboard.no_holder")

        if key not in grouped:
            grouped[key] = {"titular": titular_label, "movements": []}
            group_order.append(key)

        movements_rows = grouped[key]["movements"]
        if len(movements_rows) >= MAX_ACTIVITY_ITEMS_PER_HOLDER:
            continue

        sepultura_label = (
            movement.sepultura.location_label
            if movement.sepultura
            else translate("dashboard.grave_ref").format(id=movement.sepultura_id)
        )
        movement_type = (
            movement.tipo.value
            if hasattr(movement.tipo, "value")
            else str(movement.tipo)
        )
        movements_rows.append(
            {
                "fecha": movement.fecha,
                "tipo": movement_type,
                "detalle": movement.detalle,
                "sepultura": sepultura_label,
            }
        )

    return [grouped[key] for key in group_order]


def active_contract_for_sepultura(sepultura_id: int) -> DerechoFunerarioContrato | None:
    return (
        DerechoFunerarioContrato.query.filter_by(
            org_id=org_id(),
            sepultura_id=sepultura_id,
            estado="ACTIVO",
        )
        .order_by(DerechoFunerarioContrato.id.desc())
        .first()
    )


def active_titular_for_contract(contract_id: int) -> OwnershipRecord | None:
    today = date.today()
    return (
        OwnershipRecord.query.filter_by(org_id=org_id(), contract_id=contract_id)
        .filter(
            or_(OwnershipRecord.end_date.is_(None), OwnershipRecord.end_date >= today)
        )
        .order_by(OwnershipRecord.start_date.desc())
        .first()
    )


def active_beneficiario_for_contract(contract_id: int) -> Beneficiario | None:
    today = date.today()
    return (
        Beneficiario.query.filter_by(org_id=org_id(), contrato_id=contract_id)
        .filter(
            or_(Beneficiario.activo_hasta.is_(None), Beneficiario.activo_hasta > today)
        )
        .order_by(Beneficiario.activo_desde.desc(), Beneficiario.id.desc())
        .first()
    )


def _clean_dni_nif(value: str | None) -> str | None:
    raw = (value or "").strip().upper()
    return raw or None


def _person_by_org(person_id: int, role_label: str = "persona") -> Person:
    person = Person.query.filter_by(org_id=org_id(), id=person_id).first()
    if not person:
        raise ValueError(f"{role_label.capitalize()} no encontrada")
    return person


def _create_or_reuse_person(
    first_name: str, last_name: str, dni_nif: str | None
) -> Person:
    # Spec Cementiri: ver cementerio_extract.md (9.1.5 / 9.1.6)
    first_name = (first_name or "").strip()
    last_name = (last_name or "").strip()
    dni_nif = _clean_dni_nif(dni_nif)
    if not first_name:
        raise ValueError("El nombre de la persona es obligatorio")
    if dni_nif:
        existing = Person.query.filter_by(org_id=org_id(), dni_nif=dni_nif).first()
        if existing:
            return existing
    person = Person(
        org_id=org_id(),
        first_name=first_name,
        last_name=last_name,
        dni_nif=dni_nif,
    )
    db.session.add(person)
    db.session.flush()
    return person


def list_people(search_text: str = "", limit: int = 200) -> list[Person]:
    # Spec Cementiri: ver cementerio_extract.md (9.4.3 / 9.4.4)
    query = Person.query.filter_by(org_id=org_id())
    term = (search_text or "").strip()
    if term:
        pattern = f"%{term}%"
        query = query.filter(
            or_(
                Person.first_name.ilike(pattern),
                Person.last_name.ilike(pattern),
                Person.dni_nif.ilike(pattern),
                Person.telefono.ilike(pattern),
                Person.telefono2.ilike(pattern),
                Person.email.ilike(pattern),
                Person.email2.ilike(pattern),
                Person.direccion_linea.ilike(pattern),
                Person.codigo_postal.ilike(pattern),
                Person.poblacion.ilike(pattern),
                Person.provincia.ilike(pattern),
                Person.pais.ilike(pattern),
            )
        )
    return (
        query.order_by(Person.last_name.asc(), Person.first_name.asc(), Person.id.asc())
        .limit(max(1, min(limit, 500)))
        .all()
    )


def list_people_paged(
    search_text: str = "",
    page: int = 1,
    page_size: int = 25,
) -> dict[str, object]:
    query = Person.query.filter_by(org_id=org_id())
    term = (search_text or "").strip()
    if term:
        pattern = f"%{term}%"
        query = query.filter(
            or_(
                Person.first_name.ilike(pattern),
                Person.last_name.ilike(pattern),
                Person.dni_nif.ilike(pattern),
                Person.telefono.ilike(pattern),
                Person.telefono2.ilike(pattern),
                Person.email.ilike(pattern),
                Person.email2.ilike(pattern),
                Person.direccion_linea.ilike(pattern),
                Person.codigo_postal.ilike(pattern),
                Person.poblacion.ilike(pattern),
                Person.provincia.ilike(pattern),
                Person.pais.ilike(pattern),
            )
        )

    total = query.count()
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)

    rows = (
        query.order_by(Person.last_name.asc(), Person.first_name.asc(), Person.id.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return {
        "rows": rows,
        "total": total,
        "shown": len(rows),
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }


def person_by_id(person_id: int) -> Person:
    # Spec Cementiri: ver cementerio_extract.md (9.4.3 / 9.4.4)
    person = Person.query.filter_by(org_id=org_id(), id=person_id).first()
    if not person:
        raise ValueError("Persona no encontrada")
    return person


def _validate_email(value: str) -> str:
    email = (value or "").strip()
    if not email:
        return ""
    if "@" not in email or email.startswith("@") or email.endswith("@"):
        raise ValueError("Email invalido")
    return email


def _compose_person_address(
    direccion_linea: str,
    codigo_postal: str,
    poblacion: str,
    provincia: str,
    pais: str,
) -> str:
    parts: list[str] = []
    street = (direccion_linea or "").strip()
    if street:
        parts.append(street)

    locality = " ".join(
        [part for part in [(codigo_postal or "").strip(), (poblacion or "").strip()] if part]
    ).strip()
    if locality:
        parts.append(locality)

    province = (provincia or "").strip()
    if province:
        parts.append(province)

    country = (pais or "").strip()
    if country:
        parts.append(country)

    return ", ".join(parts)


def _person_payload(payload: dict[str, str]) -> dict[str, str | None]:
    direccion_linea = (
        payload.get("direccion_linea")
        or payload.get("adreca")
        or payload.get("direccion")
        or payload.get("address")
        or ""
    ).strip()
    codigo_postal = (payload.get("codigo_postal") or payload.get("postal_code") or "").strip()
    poblacion = (payload.get("poblacion") or payload.get("city") or "").strip()
    provincia = (payload.get("provincia") or payload.get("province") or "").strip()
    pais = (payload.get("pais") or payload.get("country") or "").strip()
    direccion = _compose_person_address(
        direccion_linea=direccion_linea,
        codigo_postal=codigo_postal,
        poblacion=poblacion,
        provincia=provincia,
        pais=pais,
    )
    return {
        "first_name": (
            payload.get("nombre") or payload.get("first_name") or ""
        ).strip(),
        "last_name": (
            payload.get("apellidos") or payload.get("last_name") or ""
        ).strip(),
        "dni_nif": _clean_dni_nif(payload.get("dni_nif") or payload.get("document_id")),
        "telefono": (payload.get("telefono") or payload.get("phone") or "").strip(),
        "telefono2": (payload.get("telefono2") or payload.get("phone2") or "").strip(),
        "email": _validate_email(payload.get("email") or ""),
        "email2": _validate_email(payload.get("email2") or ""),
        "direccion_linea": direccion_linea,
        "codigo_postal": codigo_postal,
        "poblacion": poblacion,
        "provincia": provincia,
        "pais": pais,
        "direccion": direccion,
        "notas": (payload.get("notas") or payload.get("notes") or "").strip(),
    }


def create_person(payload: dict[str, str], user_id: int | None = None) -> Person:
    # Spec Cementiri: ver cementerio_extract.md (9.4.3 / 9.4.4 / 9.1.6)
    values = _person_payload(payload)
    if not values["first_name"]:
        raise ValueError("El nombre es obligatorio")
    if values["dni_nif"]:
        existing = Person.query.filter_by(
            org_id=org_id(), dni_nif=values["dni_nif"]
        ).first()
        if existing:
            raise ValueError("Ya existe una persona con ese DNI/NIF")
    person = Person(
        org_id=org_id(),
        first_name=str(values["first_name"]),
        last_name=str(values["last_name"]),
        dni_nif=values["dni_nif"],
        telefono=str(values["telefono"]),
        telefono2=str(values["telefono2"]),
        email=str(values["email"]),
        email2=str(values["email2"]),
        direccion=str(values["direccion"]),
        direccion_linea=str(values["direccion_linea"]),
        codigo_postal=str(values["codigo_postal"]),
        poblacion=str(values["poblacion"]),
        provincia=str(values["provincia"]),
        pais=str(values["pais"]),
        notas=str(values["notas"]),
    )
    db.session.add(person)
    _log_activity_event("PERSONA_ALTA", f"Persona creada: {person.full_name}", user_id)
    db.session.commit()
    return person


def update_person(
    person_id: int, payload: dict[str, str], user_id: int | None = None
) -> Person:
    # Spec Cementiri: ver cementerio_extract.md (9.4.3 / 9.4.4)
    person = person_by_id(person_id)
    previous_name = person.full_name
    values = _person_payload(payload)
    if not values["first_name"]:
        raise ValueError("El nombre es obligatorio")
    if values["dni_nif"]:
        existing = (
            Person.query.filter_by(org_id=org_id(), dni_nif=values["dni_nif"])
            .filter(Person.id != person.id)
            .first()
        )
        if existing:
            raise ValueError("Ya existe otra persona con ese DNI/NIF")
    person.first_name = str(values["first_name"])
    person.last_name = str(values["last_name"])
    person.dni_nif = values["dni_nif"]
    person.telefono = str(values["telefono"])
    person.telefono2 = str(values["telefono2"])
    person.email = str(values["email"])
    person.email2 = str(values["email2"])
    person.direccion = str(values["direccion"])
    person.direccion_linea = str(values["direccion_linea"])
    person.codigo_postal = str(values["codigo_postal"])
    person.poblacion = str(values["poblacion"])
    person.provincia = str(values["provincia"])
    person.pais = str(values["pais"])
    person.notas = str(values["notas"])
    db.session.add(person)
    current_name = person.full_name
    if previous_name != current_name:
        _log_activity_event(
            "PERSONA_CAMBIO_NOMBRE",
            f"Cambio de nombre: {previous_name} -> {current_name}",
            user_id,
        )
    else:
        _log_activity_event(
            "PERSONA_ACTUALIZADA",
            f"Persona actualizada: {current_name}",
            user_id,
        )
    db.session.commit()
    return person


def create_funeral_right_contract(
    sepultura_id: int, payload: dict[str, str]
) -> DerechoFunerarioContrato:
    # Spec Cementiri: ver cementerio_extract.md (9.1.7)
    sep = sepultura_by_id(sepultura_id)
    if sep.estado != SepulturaEstado.DISPONIBLE:
        raise ValueError("Solo se puede contratar en sepulturas en estado DISPONIBLE")
    if active_contract_for_sepultura(sep.id):
        raise ValueError("La sepultura ya tiene un contrato activo")

    tipo_raw = (payload.get("tipo") or "").strip().upper()
    try:
        tipo = DerechoTipo[tipo_raw]
    except KeyError as exc:
        raise ValueError("Tipo de contrato invalido") from exc

    fecha_inicio = _parse_iso_date(payload.get("fecha_inicio", ""), "fecha inicio")
    fecha_fin = _parse_iso_date(payload.get("fecha_fin", ""), "fecha fin")
    annual_fee_amount = _parse_decimal(
        payload.get("annual_fee_amount", ""), "importe anual"
    )
    legacy_99_years = (payload.get("legacy_99_years") or "").lower() in {
        "1",
        "on",
        "true",
        "yes",
    }

    titular_person_id = (payload.get("titular_person_id") or "").strip()
    if titular_person_id.isdigit():
        titular = _person_by_org(int(titular_person_id), "titular")
    else:
        titular = _create_or_reuse_person(
            payload.get("titular_first_name", ""),
            payload.get("titular_last_name", ""),
            payload.get("titular_dni_nif") or payload.get("titular_document_id"),
        )

    contrato = DerechoFunerarioContrato(
        org_id=org_id(),
        sepultura_id=sep.id,
        tipo=tipo,
        fecha_inicio=fecha_inicio,
        legacy_99_years=legacy_99_years,
        fecha_fin=fecha_fin,
        annual_fee_amount=annual_fee_amount,
        estado="ACTIVO",
    )
    db.session.add(contrato)
    db.session.flush()

    pensionista = (payload.get("pensionista") or "").lower() in {
        "1",
        "on",
        "true",
        "yes",
    }
    pensionista_desde = payload.get("pensionista_desde", "").strip()
    pensionista_desde_date = None
    if pensionista_desde:
        pensionista_desde_date = _parse_iso_date(pensionista_desde, "fecha pensionista")

    db.session.add(
        OwnershipRecord(
            org_id=org_id(),
            contract_id=contrato.id,
            person_id=titular.id,
            start_date=fecha_inicio,
            is_pensioner=pensionista,
            pensioner_since_date=pensionista_desde_date,
        )
    )

    beneficiario_person_id = (payload.get("beneficiario_person_id") or "").strip()
    if beneficiario_person_id.isdigit():
        beneficiario = _person_by_org(int(beneficiario_person_id), "beneficiario")
        db.session.add(
            Beneficiario(
                org_id=org_id(),
                contrato_id=contrato.id,
                person_id=beneficiario.id,
                activo_desde=fecha_inicio,
            )
        )
    else:
        beneficiario_first_name = (payload.get("beneficiario_first_name") or "").strip()
        if beneficiario_first_name:
            beneficiario = _create_or_reuse_person(
                beneficiario_first_name,
                payload.get("beneficiario_last_name", ""),
                payload.get("beneficiario_dni_nif")
                or payload.get("beneficiario_document_id"),
            )
            db.session.add(
                Beneficiario(
                    org_id=org_id(),
                    contrato_id=contrato.id,
                    person_id=beneficiario.id,
                    activo_desde=fecha_inicio,
                )
            )

    db.session.commit()
    return contrato


def contract_by_id(contract_id: int) -> DerechoFunerarioContrato:
    contrato = DerechoFunerarioContrato.query.filter_by(
        org_id=org_id(), id=contract_id
    ).first()
    if not contrato:
        raise ValueError("Contrato no encontrado")
    return contrato


def nominate_contract_beneficiary(
    contract_id: int,
    payload: dict[str, str],
    user_id: int | None = None,
) -> Beneficiario:
    # Spec Cementiri: ver cementerio_extract.md (9.1.6)
    contrato = contract_by_id(contract_id)
    person_id_raw = (payload.get("person_id") or "").strip()
    if person_id_raw.isdigit():
        person = _person_by_org(int(person_id_raw), "beneficiario")
    else:
        first_name = (payload.get("first_name") or payload.get("nombre") or "").strip()
        if not first_name:
            raise ValueError("Debes seleccionar o crear un beneficiario")
        person = _create_or_reuse_person(
            first_name,
            payload.get("last_name") or payload.get("apellidos") or "",
            payload.get("dni_nif") or payload.get("document_id"),
        )

    active = active_beneficiario_for_contract(contrato.id)
    if active and active.person_id == person.id:
        detail = f"Beneficiario mantenido: {person.full_name}"
        _log_case_movement(contrato, MovimientoTipo.BENEFICIARIO, detail, user_id)
        _log_contract_event(contrato.id, None, "BENEFICIARIO", detail, user_id)
        db.session.commit()
        return active
    if active and active.person_id != person.id:
        active.activo_hasta = date.today()
        db.session.add(active)

    beneficiary = Beneficiario(
        org_id=org_id(),
        contrato_id=contrato.id,
        person_id=person.id,
        activo_desde=date.today(),
    )
    db.session.add(beneficiary)
    detail = f"Beneficiario nombrado: {person.full_name}"
    _log_case_movement(contrato, MovimientoTipo.BENEFICIARIO, detail, user_id)
    _log_contract_event(contrato.id, None, "BENEFICIARIO", detail, user_id)
    db.session.commit()
    return beneficiary


def set_contract_holder_pensioner(
    contract_id: int, payload: dict[str, str], user_id: int | None
) -> OwnershipRecord:
    # Spec Cementiri 9.1.5 - marcar titular activo como pensionista no retroactivo por defecto
    contrato = contract_by_id(contract_id)
    titular = active_titular_for_contract(contrato.id)
    if not titular:
        raise ValueError("No hay titular activo")

    since_date = _parse_optional_iso_date(payload.get("since_date")) or date.today()
    allow_retroactive = (payload.get("allow_retroactive") or "").strip().lower() in {
        "1",
        "on",
        "true",
        "yes",
    }
    if since_date < date.today() and not allow_retroactive:
        raise ValueError(
            "La pensionista se aplica desde hoy o fecha futura (no retroactivo por defecto)"
        )

    titular.is_pensioner = True
    titular.pensioner_since_date = since_date
    db.session.add(titular)

    detail = f"Titular pensionista desde {since_date.isoformat()}: {titular.person.full_name}"
    _log_case_movement(contrato, MovimientoTipo.PENSIONISTA, detail, user_id)
    _log_contract_event(contrato.id, None, "PENSIONISTA", detail, user_id)
    db.session.commit()
    return titular


def remove_contract_beneficiary(
    contract_id: int, payload: dict[str, str], user_id: int | None
) -> Beneficiario:
    # Spec Cementiri 9.1.6 - baja de beneficiario activo
    contrato = contract_by_id(contract_id)
    active = (
        Beneficiario.query.filter_by(org_id=org_id(), contrato_id=contrato.id)
        .filter(Beneficiario.activo_hasta.is_(None))
        .order_by(Beneficiario.activo_desde.desc(), Beneficiario.id.desc())
        .first()
    )
    if not active:
        raise ValueError("No hay beneficiario activo")

    end_date = _parse_optional_iso_date(payload.get("end_date")) or date.today()
    if end_date < active.activo_desde:
        raise ValueError("Fecha de baja invalida para beneficiario")
    active.activo_hasta = end_date
    db.session.add(active)

    detail = (
        f"Beneficiario dado de baja: {active.person.full_name} ({end_date.isoformat()})"
    )
    _log_case_movement(contrato, MovimientoTipo.BENEFICIARIO, detail, user_id)
    _log_contract_event(contrato.id, None, "BENEFICIARIO_BAJA", detail, user_id)
    db.session.commit()
    return active


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
        f"<< /Length {len(stream)} >>\nstream\n".encode("ascii")
        + stream
        + b"\nendstream",
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


def funeral_right_title_pdf(contract_id: int) -> bytes:
    # Spec Cementiri 9.1.4 - generacion de titulo del derecho funerario
    contrato = contract_by_id(contract_id)
    sep = sepultura_by_id(contrato.sepultura_id)
    titular = active_titular_for_contract(contrato.id)
    beneficiario = active_beneficiario_for_contract(contrato.id)

    lines = [
        "GSF - Titulo de Derecho Funerario",
        f"Contrato: {contrato.id}",
        f"Tipo: {'LLOGUER' if contrato.tipo == DerechoTipo.USO_INMEDIATO else contrato.tipo.value}",
        f"Sepultura: {sep.location_label}",
        f"Fecha inicio: {contrato.fecha_inicio.isoformat()}",
        f"Fecha fin: {contrato.fecha_fin.isoformat()}",
        f"Titular: {titular.person.full_name if titular else 'N/A'}",
        f"Beneficiario: {beneficiario.person.full_name if beneficiario else 'N/A'}",
        f"Emitido: {datetime.now(timezone.utc).date().isoformat()}",
    ]

    db.session.add(
        MovimientoSepultura(
            org_id=org_id(),
            sepultura_id=sep.id,
            tipo=MovimientoTipo.CONTRATO,
            detalle="Titulo emitido/duplicado",
            user_id=getattr(getattr(g, "user", None), "id", None),
        )
    )
    db.session.commit()
    return _simple_pdf(lines)


def _active_titular_for_contract_on(
    contract_id: int,
    reference_date: date,
    organization_id: int,
) -> OwnershipRecord | None:
    return (
        OwnershipRecord.query.filter_by(org_id=organization_id, contract_id=contract_id)
        .filter(OwnershipRecord.start_date <= reference_date)
        .filter(
            or_(
                OwnershipRecord.end_date.is_(None),
                OwnershipRecord.end_date >= reference_date,
            )
        )
        .order_by(OwnershipRecord.start_date.desc())
        .first()
    )


def _apply_discount(amount: Decimal, discount_pct: Decimal) -> Decimal:
    factor = (Decimal("100.00") - Decimal(discount_pct)) / Decimal("100.00")
    return (Decimal(amount) * factor).quantize(Decimal("0.01"))


def generate_maintenance_tickets_for_year(
    year: int, organization: Organization
) -> TicketGenerationResult:
    # Spec 5.2.5.2.2 / 5.3.4 - generacion de tiquets el 1 de enero para concesiones
    jan_1 = date(year, 1, 1)
    result = TicketGenerationResult()
    contracts = (
        DerechoFunerarioContrato.query.join(
            Sepultura, Sepultura.id == DerechoFunerarioContrato.sepultura_id
        )
        .filter(DerechoFunerarioContrato.org_id == organization.id)
        .filter(DerechoFunerarioContrato.estado == "ACTIVO")
        .filter(DerechoFunerarioContrato.tipo == DerechoTipo.CONCESION)
        .filter(DerechoFunerarioContrato.fecha_inicio <= jan_1)
        .filter(DerechoFunerarioContrato.fecha_fin >= jan_1)
        .filter(Sepultura.estado != SepulturaEstado.PROPIA)
        .order_by(DerechoFunerarioContrato.id.asc())
        .all()
    )

    for contract in contracts:
        existing = TasaMantenimientoTicket.query.filter_by(
            org_id=organization.id,
            contrato_id=contract.id,
            anio=year,
        ).first()
        if existing:
            result.existing += 1
            continue

        titular = _active_titular_for_contract_on(contract.id, jan_1, organization.id)
        discount_pct = Decimal(organization.pensionista_discount_pct or Decimal("0.00"))
        apply_pensionista = bool(
            titular
            and titular.is_pensioner
            and titular.pensioner_since_date
            and year >= titular.pensioner_since_date.year
        )
        base_amount = Decimal(contract.annual_fee_amount or Decimal("0.00"))
        amount = (
            _apply_discount(base_amount, discount_pct)
            if apply_pensionista
            else base_amount
        )
        discount_tipo = (
            TicketDescuentoTipo.PENSIONISTA
            if apply_pensionista
            else TicketDescuentoTipo.NONE
        )

        db.session.add(
            TasaMantenimientoTicket(
                org_id=organization.id,
                contrato_id=contract.id,
                anio=year,
                importe=amount,
                descuento_tipo=discount_tipo,
                estado=TicketEstado.PENDIENTE,
            )
        )
        result.created += 1

    db.session.commit()
    return result


def search_sepulturas(filters: dict[str, str]) -> list[dict[str, object]]:
    paged = search_sepulturas_paged(filters)
    return list(paged["rows"])


def search_sepulturas_paged(
    filters: dict[str, str],
    page: int = 1,
    page_size: int = 25,
    sort_by: str = "ubicacion",
    sort_dir: str = "asc",
) -> dict[str, object]:
    def _empty_result() -> dict[str, object]:
        return {
            "rows": [],
            "total": 0,
            "shown": 0,
            "page": 1,
            "page_size": page_size,
            "total_pages": 1,
            "has_prev": False,
            "has_next": False,
        }

    oid = org_id()
    query = Sepultura.query.filter_by(org_id=oid)

    if filters.get("bloque"):
        query = query.filter(Sepultura.bloque.ilike(f"%{filters['bloque']}%"))
    if filters.get("fila"):
        try:
            query = query.filter(Sepultura.fila == int(filters["fila"]))
        except ValueError:
            return _empty_result()
    if filters.get("columna"):
        try:
            query = query.filter(Sepultura.columna == int(filters["columna"]))
        except ValueError:
            return _empty_result()
    if filters.get("numero"):
        try:
            query = query.filter(Sepultura.numero == int(filters["numero"]))
        except ValueError:
            return _empty_result()
    if filters.get("modalidad"):
        query = query.filter(Sepultura.modalidad == filters["modalidad"])
    if filters.get("estado"):
        status_raw = (filters.get("estado") or "").strip().upper()
        try:
            query = query.filter(Sepultura.estado == SepulturaEstado[status_raw])
        except KeyError:
            return _empty_result()

    only_with_debt = (filters.get("con_deuda") or "").strip().lower() in {
        "1",
        "on",
        "true",
        "yes",
    }

    sepulturas = query.order_by(
        Sepultura.bloque, Sepultura.fila, Sepultura.columna, Sepultura.numero
    ).all()
    if not sepulturas:
        return _empty_result()

    titular_filter = filters.get("titular", "").strip().lower()
    difunto_filter = filters.get("difunto", "").strip().lower()

    rows: list[dict[str, object]] = []
    for sep in sepulturas:
        contrato = active_contract_for_sepultura(sep.id)
        titular_name = ""
        titular = None
        beneficiario = None
        debt = Decimal("0.00")
        if contrato:
            titular = active_titular_for_contract(contrato.id)
            beneficiario = active_beneficiario_for_contract(contrato.id)
            if titular:
                titular_name = titular.person.full_name
            debt = (
                db.session.query(
                    func.coalesce(func.sum(TasaMantenimientoTicket.importe), 0)
                )
                .filter_by(org_id=oid, contrato_id=contrato.id)
                .filter(TasaMantenimientoTicket.estado != TicketEstado.COBRADO)
                .scalar()
            )

        difuntos = [sd.person.full_name for sd in sep.difuntos]
        if titular_filter and titular_filter not in titular_name.lower():
            continue
        if difunto_filter and not any(difunto_filter in d.lower() for d in difuntos):
            continue
        if only_with_debt and debt <= Decimal("0.00"):
            continue

        rows.append(
            {
                "sepultura": sep,
                "titular_name": titular_name or "—",
                "beneficiario_name": (
                    beneficiario.person.full_name if beneficiario else ""
                ),
                "deuda": debt,
                "difuntos": difuntos,
            }
        )
    reverse = (sort_dir or "asc").lower() == "desc"
    sort_key = (sort_by or "ubicacion").lower()
    if sort_key == "estado":
        rows.sort(key=lambda item: str(item["sepultura"].estado.value), reverse=reverse)
    elif sort_key == "titular":
        rows.sort(key=lambda item: str(item["titular_name"]).lower(), reverse=reverse)
    elif sort_key == "deuda":
        rows.sort(key=lambda item: item["deuda"], reverse=reverse)
    else:
        rows.sort(
            key=lambda item: (
                str(item["sepultura"].bloque),
                item["sepultura"].fila,
                item["sepultura"].columna,
                item["sepultura"].numero,
            ),
            reverse=reverse,
        )

    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    total = len(rows)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)
    start = (page - 1) * page_size
    page_rows = rows[start : start + page_size]
    return {
        "rows": page_rows,
        "total": total,
        "shown": len(page_rows),
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
    }


def list_sepultura_blocks() -> list[str]:
    rows = (
        db.session.query(Sepultura.bloque)
        .filter(Sepultura.org_id == org_id())
        .distinct()
        .order_by(Sepultura.bloque.asc())
        .all()
    )
    return [str(bloque) for (bloque,) in rows if bloque]


def list_sepultura_modalidades() -> list[str]:
    rows = (
        db.session.query(Sepultura.modalidad)
        .filter(Sepultura.org_id == org_id())
        .distinct()
        .order_by(Sepultura.modalidad.asc())
        .all()
    )
    return [str(modalidad) for (modalidad,) in rows if modalidad]


def sepultura_by_id(sepultura_id: int) -> Sepultura:
    sep = Sepultura.query.filter_by(org_id=org_id(), id=sepultura_id).first()
    if not sep:
        raise ValueError("Sepultura no encontrada")
    return sep


def change_sepultura_state(sepultura: Sepultura, new_state: SepulturaEstado) -> None:
    # Spec 9.4.2 - cambio de estado manual no permite asignar OCUPADA
    if new_state == SepulturaEstado.OCUPADA:
        raise ValueError(
            "El estado Ocupada se asigna automáticamente al crear contrato"
        )
    if (
        sepultura.estado == SepulturaEstado.OCUPADA
        and new_state == SepulturaEstado.LLIURE
    ):
        raise ValueError("No se puede pasar de Ocupada a Lliure manualmente")
    if (
        sepultura.estado == SepulturaEstado.PROPIA
        and new_state == SepulturaEstado.OCUPADA
    ):
        raise ValueError("Una sepultura Pròpia no puede contratarse")
    sepultura.estado = new_state
    db.session.add(sepultura)
    db.session.commit()


def update_sepultura_notes(sepultura_id: int, payload: dict[str, str]) -> Sepultura:
    sepultura = sepultura_by_id(sepultura_id)
    postit = (payload.get("postit") or "").strip()
    notas = (payload.get("notas") or payload.get("notes") or "").strip()
    if len(postit) > 255:
        raise ValueError("El Post it no puede superar 255 caracteres")
    sepultura.postit = postit
    sepultura.notas = notas
    db.session.add(sepultura)
    _log_activity_event(
        "SEPULTURA_NOTAS",
        f"Notas actualizadas en sepultura {sepultura.location_label}",
        None,
        sepultura.id,
    )
    db.session.commit()
    return sepultura


def sepultura_tickets_and_invoices(sepultura_id: int) -> dict[str, object]:
    sep = sepultura_by_id(sepultura_id)
    contrato = active_contract_for_sepultura(sep.id)
    if not contrato:
        return {
            "sepultura": sep,
            "contrato": None,
            "titularidad": None,
            "beneficiario": None,
            "pending_tickets": [],
            "unpaid_invoices": [],
            "total_pending": Decimal("0.00"),
        }

    titularidad = active_titular_for_contract(contrato.id)
    beneficiario = active_beneficiario_for_contract(contrato.id)
    pending_tickets = (
        TasaMantenimientoTicket.query.filter_by(
            org_id=org_id(),
            contrato_id=contrato.id,
            estado=TicketEstado.PENDIENTE,
        )
        .order_by(TasaMantenimientoTicket.anio.asc())
        .all()
    )
    unpaid_invoices = (
        Invoice.query.filter_by(
            org_id=org_id(),
            contrato_id=contrato.id,
            estado=InvoiceEstado.IMPAGADA,
        )
        .order_by(Invoice.created_at.asc())
        .all()
    )
    total_pending = sum((ticket.importe for ticket in pending_tickets), Decimal("0.00"))
    return {
        "sepultura": sep,
        "contrato": contrato,
        "titularidad": titularidad,
        "beneficiario": beneficiario,
        "pending_tickets": pending_tickets,
        "unpaid_invoices": unpaid_invoices,
        "total_pending": total_pending,
    }


def validate_oldest_prefix_selection(
    tickets: list[TasaMantenimientoTicket], selected_ids: list[int]
) -> None:
    if not selected_ids:
        raise ValueError("Selecciona al menos un año")
    ordered = sorted(tickets, key=lambda t: t.anio)
    selected_set = set(selected_ids)
    prefix_count = 0
    for ticket in ordered:
        if ticket.id in selected_set:
            prefix_count += 1
        else:
            break
    expected = {ticket.id for ticket in ordered[:prefix_count]}
    if selected_set != expected:
        raise ValueError("Debes cobrar empezando por el año pendiente más antiguo")


def _next_invoice_number() -> str:
    current_year = date.today().year
    prefix = f"F-CEM-{current_year}-"
    count = (
        db.session.query(func.count(Invoice.id))
        .filter(Invoice.org_id == org_id())
        .filter(Invoice.numero.like(f"{prefix}%"))
        .scalar()
    )
    return f"{prefix}{count + 1:04d}"


def _next_receipt_number() -> str:
    current_year = date.today().year
    prefix = f"R-CEM-{current_year}-"
    count = (
        db.session.query(func.count(Payment.id))
        .filter(Payment.org_id == org_id())
        .filter(Payment.receipt_number.like(f"{prefix}%"))
        .scalar()
    )
    return f"{prefix}{count + 1:04d}"


def _selected_pending_tickets(
    contract_id: int, selected_ids: list[int]
) -> list[TasaMantenimientoTicket]:
    return (
        TasaMantenimientoTicket.query.filter_by(
            org_id=org_id(), contrato_id=contract_id, estado=TicketEstado.PENDIENTE
        )
        .filter(TasaMantenimientoTicket.id.in_(selected_ids))
        .order_by(TasaMantenimientoTicket.anio.asc())
        .all()
    )


def generate_invoice_for_tickets(sepultura_id: int, selected_ids: list[int]) -> Invoice:
    # Spec 9.1.3 - criterio de caja: no se factura antes del cobro
    raise ValueError("Operacion no disponible por criterio de caja")


def _ticket_amount_with_discount(
    ticket: TasaMantenimientoTicket,
    contrato: DerechoFunerarioContrato,
    titularidad: OwnershipRecord | None,
    discount_ticket_ids: set[int],
    discount_pct: Decimal,
) -> tuple[Decimal, TicketDescuentoTipo]:
    base_amount = Decimal(contrato.annual_fee_amount or 0).quantize(Decimal("0.01"))
    if base_amount <= 0:
        base_amount = Decimal(ticket.importe).quantize(Decimal("0.01"))
    if (
        not titularidad
        or not titularidad.is_pensioner
        or not titularidad.pensioner_since_date
    ):
        return base_amount, TicketDescuentoTipo.NONE

    since_year = titularidad.pensioner_since_date.year
    should_apply = ticket.anio >= since_year or ticket.id in discount_ticket_ids
    if should_apply:
        return (
            _apply_discount(base_amount, discount_pct),
            TicketDescuentoTipo.PENSIONISTA,
        )
    return base_amount, TicketDescuentoTipo.NONE


def collect_tickets(
    sepultura_id: int,
    selected_ids: list[int],
    method: str = "EFECTIVO",
    user_id: int | None = None,
    discount_ticket_ids: set[int] | None = None,
) -> tuple[Invoice, Payment]:
    data = sepultura_tickets_and_invoices(sepultura_id)
    contrato = data["contrato"]
    if contrato is None:
        raise ValueError("La sepultura no tiene contrato activo")
    if data["sepultura"].estado == SepulturaEstado.PROPIA:
        raise ValueError("Las sepulturas Propia no generan tiquets de contribucion")
    selected = _selected_pending_tickets(contrato.id, selected_ids)
    validate_oldest_prefix_selection(data["pending_tickets"], selected_ids)

    discount_ticket_ids = discount_ticket_ids or set()
    titularidad = data["titularidad"]
    discount_pct = Decimal(org_record().pensionista_discount_pct or Decimal("0.00"))

    total = Decimal("0.00")
    for ticket in selected:
        amount, discount_tipo = _ticket_amount_with_discount(
            ticket=ticket,
            contrato=contrato,
            titularidad=titularidad,
            discount_ticket_ids=discount_ticket_ids,
            discount_pct=discount_pct,
        )
        ticket.importe = amount
        ticket.descuento_tipo = discount_tipo
        total += amount

    invoice = Invoice(
        org_id=org_id(),
        contrato_id=contrato.id,
        sepultura_id=sepultura_id,
        numero=_next_invoice_number(),
        estado=InvoiceEstado.PAGADA,
        total_amount=total,
        issued_at=datetime.now(timezone.utc),
    )
    db.session.add(invoice)
    db.session.flush()

    payment = Payment(
        org_id=org_id(),
        invoice_id=invoice.id,
        user_id=user_id,
        amount=total,
        method=method,
        receipt_number=_next_receipt_number(),
    )
    db.session.add(payment)
    for ticket in selected:
        ticket.estado = TicketEstado.COBRADO
        ticket.invoice_id = invoice.id
        db.session.add(ticket)
    _log_activity_event(
        "TASAS_COBRO",
        f"Cobro de tasas en sepultura #{sepultura_id}: {len(selected)} tiquet(s), total {total}",
        user_id,
        sepultura_id,
    )
    db.session.commit()
    return invoice, payment


def parse_range(value: str) -> tuple[int, int]:
    cleaned = value.replace(" ", "")
    parts = cleaned.split("-")
    if len(parts) != 2:
        raise ValueError("Formato de rango inválido, usa desde-hasta")
    start, end = int(parts[0]), int(parts[1])
    if start <= 0 or end < start:
        raise ValueError("Rango inválido")
    return start, end


def preview_mass_create(payload: dict[str, str]) -> MassCreatePreview:
    f_from, f_to = parse_range(payload["filas"])
    c_from, c_to = parse_range(payload["columnas"])
    rows: list[dict[str, int | str]] = []
    for fila in range(f_from, f_to + 1):
        for col in range(c_from, c_to + 1):
            rows.append(
                {
                    "bloque": payload["bloque"],
                    "fila": fila,
                    "columna": col,
                    "via": payload["via"],
                    "numero": ((fila - f_from) * (c_to - c_from + 1)) + col,
                    "modalidad": payload["modalidad"],
                }
            )
    return MassCreatePreview(total=len(rows), rows=rows[:15])


def create_mass_sepulturas(payload: dict[str, str]) -> int:
    # Spec 9.4.1 - Alta de sepultures (estado inicial LLIURE)
    preview_mass_create(payload)
    cemetery = org_cemetery()
    created = 0
    f_from, f_to = parse_range(payload["filas"])
    c_from, c_to = parse_range(payload["columnas"])
    for fila in range(f_from, f_to + 1):
        for col in range(c_from, c_to + 1):
            numero = ((fila - f_from) * (c_to - c_from + 1)) + col
            exists = Sepultura.query.filter_by(
                org_id=org_id(),
                cemetery_id=cemetery.id,
                bloque=payload["bloque"],
                fila=fila,
                columna=col,
                numero=numero,
            ).first()
            if exists:
                continue
            db.session.add(
                Sepultura(
                    org_id=org_id(),
                    cemetery_id=cemetery.id,
                    bloque=payload["bloque"],
                    fila=fila,
                    columna=col,
                    via=payload["via"],
                    numero=numero,
                    modalidad=payload["modalidad"],
                    estado=SepulturaEstado.LLIURE,
                    tipo_bloque=payload["tipo_bloque"],
                    tipo_lapida=payload["tipo_lapida"],
                    orientacion=payload["orientacion"],
                )
            )
            created += 1
    _log_activity_event(
        "SEPULTURA_ALTA_MASIVA",
        f"Alta masiva de sepulturas en bloque {payload['bloque']}: {created} creada(s)",
        None,
    )
    db.session.commit()
    return created


def sepultura_tabs_data(
    sepultura_id: int, tab: str, mov_filters: dict[str, str]
) -> dict[str, object]:
    sep = sepultura_by_id(sepultura_id)
    contrato = active_contract_for_sepultura(sep.id)
    titulares = []
    beneficiarios = []
    tasas = []
    active_titular = None
    active_beneficiario = None
    representante = None
    if contrato:
        active_titular = active_titular_for_contract(contrato.id)
        active_beneficiario = active_beneficiario_for_contract(contrato.id)
        titulares = (
            OwnershipRecord.query.filter_by(org_id=org_id(), contract_id=contrato.id)
            .order_by(OwnershipRecord.start_date.desc())
            .all()
        )
        beneficiarios = (
            Beneficiario.query.filter_by(org_id=org_id(), contrato_id=contrato.id)
            .order_by(Beneficiario.activo_desde.desc())
            .all()
        )
        tasas = (
            TasaMantenimientoTicket.query.filter_by(
                org_id=org_id(), contrato_id=contrato.id
            )
            .order_by(TasaMantenimientoTicket.anio.desc())
            .all()
        )
        representante_party = (
            OwnershipTransferParty.query.options(joinedload(OwnershipTransferParty.person))
            .join(
                OwnershipTransferCase,
                OwnershipTransferCase.id == OwnershipTransferParty.case_id,
            )
            .filter(OwnershipTransferParty.org_id == org_id())
            .filter(OwnershipTransferCase.org_id == org_id())
            .filter(OwnershipTransferCase.contract_id == contrato.id)
            .filter(OwnershipTransferParty.role == OwnershipPartyRole.REPRESENTANTE)
            .order_by(
                OwnershipTransferCase.opened_at.desc(),
                OwnershipTransferCase.id.desc(),
                OwnershipTransferParty.id.desc(),
            )
            .first()
        )
        representante = representante_party.person if representante_party else None

    difuntos = sorted(sep.difuntos, key=lambda item: item.created_at, reverse=True)
    inscripciones = (
        InscripcionLateral.query.filter_by(org_id=org_id(), sepultura_id=sep.id)
        .order_by(InscripcionLateral.created_at.desc(), InscripcionLateral.id.desc())
        .all()
    )
    ot_rows = (
        WorkOrder.query.options(joinedload(WorkOrder.assigned_user))
        .filter_by(org_id=org_id(), sepultura_id=sep.id)
        .order_by(WorkOrder.created_at.desc(), WorkOrder.id.desc())
        .all()
    )
    pending_count = sum(
        1
        for row in ot_rows
        if row.status
        in {
            WorkOrderStatus.BORRADOR,
            WorkOrderStatus.PENDIENTE_PLANIFICACION,
            WorkOrderStatus.PLANIFICADA,
            WorkOrderStatus.ASIGNADA,
        }
    )
    open_count = sum(
        1
        for row in ot_rows
        if row.status
        in {
            WorkOrderStatus.BORRADOR,
            WorkOrderStatus.PENDIENTE_PLANIFICACION,
            WorkOrderStatus.PLANIFICADA,
            WorkOrderStatus.ASIGNADA,
            WorkOrderStatus.EN_CURSO,
            WorkOrderStatus.BLOQUEADA,
            WorkOrderStatus.EN_VALIDACION,
        }
    )
    historic_count = sum(
        1
        for row in ot_rows
        if row.status in {WorkOrderStatus.COMPLETADA, WorkOrderStatus.CANCELADA}
    )
    all_count = len(ot_rows)

    movements_query = MovimientoSepultura.query.filter_by(
        org_id=org_id(), sepultura_id=sep.id
    )
    if mov_filters.get("tipo"):
        try:
            mtype = MovimientoTipo[mov_filters["tipo"]]
            movements_query = movements_query.filter_by(tipo=mtype)
        except KeyError:
            pass
    if mov_filters.get("desde"):
        movements_query = movements_query.filter(
            MovimientoSepultura.fecha >= mov_filters["desde"]
        )
    if mov_filters.get("hasta"):
        movements_query = movements_query.filter(
            MovimientoSepultura.fecha <= mov_filters["hasta"]
        )
    movimientos = movements_query.order_by(MovimientoSepultura.fecha.desc()).all()

    return {
        "sepultura": sep,
        "contrato": contrato,
        "tab": tab,
        "difuntos": difuntos,
        "difuntos_count": len(difuntos),
        "active_titular": active_titular,
        "active_beneficiario": active_beneficiario,
        "representante": representante,
        "titulares": titulares,
        "beneficiarios": beneficiarios,
        "movimientos": movimientos,
        "tasas": tasas,
        "inscripciones": inscripciones,
        "expedientes": [],
        "ot_rows": ot_rows,
        "ot_counts": {
            "pendientes": pending_count,
            "abiertas": open_count,
            "historicas": historic_count,
            "todas": all_count,
        },
    }


def add_deceased_to_sepultura(
    sepultura_id: int,
    payload: dict[str, str],
    user_id: int | None,
) -> SepulturaDifunto:
    sepultura = sepultura_by_id(sepultura_id)

    person_id_raw = (payload.get("person_id") or "").strip()
    person_id = int(person_id_raw) if person_id_raw.isdigit() else None

    if not person_id:
        person_data = {
            "first_name": payload.get("first_name", ""),
            "last_name": payload.get("last_name", ""),
            "document_id": payload.get("document_id", ""),
            "telefono": payload.get("telefono", ""),
            "email": payload.get("email", ""),
            "direccion": payload.get("direccion", ""),
            "notas": payload.get("notas", ""),
        }
        person = create_person(person_data, user_id=user_id)
    else:
        person = Person.query.filter_by(org_id=org_id(), id=person_id).first()
        if not person:
            raise ValueError("Difunto no encontrado")

    exists = SepulturaDifunto.query.filter_by(
        org_id=org_id(),
        sepultura_id=sepultura.id,
        person_id=person.id,
    ).first()
    if exists:
        raise ValueError("El difunto ya consta en esta sepultura")

    deceased = SepulturaDifunto(
        org_id=org_id(),
        sepultura_id=sepultura.id,
        person_id=person.id,
        notes=(payload.get("notes") or "").strip(),
    )
    db.session.add(deceased)
    sepultura.estado = SepulturaEstado.OCUPADA
    _log_sepultura_movement(
        sepultura.id,
        MovimientoTipo.INHUMACION,
        f"Inhumacion de {person.full_name}",
        user_id,
    )
    db.session.commit()
    emit_work_order_event(
        "DECEASED_ADDED_TO_SEPULTURA",
        {
            "sepultura_id": sepultura.id,
            "deceased_id": person.id,
            "deceased_name": person.full_name,
            "category": WorkOrderCategory.FUNERARIA.value,
        },
        user_id=user_id,
    )
    return deceased


def remove_deceased_from_sepultura(
    sepultura_id: int,
    sepultura_difunto_id: int,
    user_id: int | None,
) -> None:
    sepultura = sepultura_by_id(sepultura_id)
    deceased = SepulturaDifunto.query.filter_by(
        org_id=org_id(),
        id=sepultura_difunto_id,
        sepultura_id=sepultura.id,
    ).first()
    if not deceased:
        raise ValueError("Registro de difunto no encontrado")

    full_name = deceased.person.full_name
    db.session.delete(deceased)
    db.session.flush()

    remaining = SepulturaDifunto.query.filter_by(
        org_id=org_id(),
        sepultura_id=sepultura.id,
    ).count()
    if remaining == 0 and sepultura.estado == SepulturaEstado.OCUPADA:
        sepultura.estado = SepulturaEstado.DISPONIBLE

    _log_sepultura_movement(
        sepultura.id,
        MovimientoTipo.EXHUMACION,
        f"Exhumacion de {full_name}",
        user_id,
    )
    db.session.commit()
    emit_work_order_event(
        "DECEASED_REMOVED_FROM_SEPULTURA",
        {
            "sepultura_id": sepultura.id,
            "deceased_name": full_name,
            "category": WorkOrderCategory.FUNERARIA.value,
        },
        user_id=user_id,
    )


def _log_sepultura_movement(
    sepultura_id: int | None,
    movement_type: MovimientoTipo,
    detail: str,
    user_id: int | None,
) -> None:
    if sepultura_id:
        db.session.add(
            MovimientoSepultura(
                org_id=org_id(),
                sepultura_id=sepultura_id,
                tipo=movement_type,
                detalle=detail,
                user_id=user_id,
            )
        )
    _log_activity_event(movement_type.value, detail, user_id, sepultura_id)


def _next_expediente_number(year: int) -> str:
    prefix = f"C-{year}-"
    count = (
        db.session.query(func.count(Expediente.id))
        .filter(Expediente.org_id == org_id())
        .filter(Expediente.numero.like(f"{prefix}%"))
        .scalar()
    )
    return f"{prefix}{count + 1:04d}"


def list_expedientes(filters: dict[str, str]) -> list[Expediente]:
    query = (
        Expediente.query.options(
            joinedload(Expediente.difunto), joinedload(Expediente.declarante)
        )
        .filter(Expediente.org_id == org_id())
        .order_by(Expediente.created_at.desc(), Expediente.id.desc())
    )
    tipo = (filters.get("tipo") or "").strip().upper()
    if tipo:
        query = query.filter(Expediente.tipo == tipo)
    estado = (filters.get("estado") or "").strip().upper()
    if estado:
        query = query.filter(Expediente.estado == estado)
    sepultura_id = (filters.get("sepultura_id") or "").strip()
    if sepultura_id:
        if not sepultura_id.isdigit():
            return []
        query = query.filter(Expediente.sepultura_id == int(sepultura_id))

    created_from = _parse_optional_iso_date(filters.get("created_from"))
    created_to = _parse_optional_iso_date(filters.get("created_to"))
    if created_from:
        query = query.filter(
            Expediente.created_at >= datetime.combine(created_from, datetime.min.time())
        )
    if created_to:
        query = query.filter(
            Expediente.created_at <= datetime.combine(created_to, datetime.max.time())
        )
    return query.all()


def expediente_by_id(expediente_id: int) -> Expediente:
    expediente = Expediente.query.filter_by(org_id=org_id(), id=expediente_id).first()
    if not expediente:
        raise ValueError("Expediente no encontrado")
    return expediente


def create_expediente(payload: dict[str, str], user_id: int | None) -> Expediente:
    # Spec Cementiri 9.1.1 / 9.1.2 - alta de expediente operativo
    tipo = (payload.get("tipo") or "").strip().upper()
    if not tipo:
        raise ValueError("Tipo de expediente obligatorio")

    sepultura_id_raw = (payload.get("sepultura_id") or "").strip()
    sepultura_id = int(sepultura_id_raw) if sepultura_id_raw.isdigit() else None
    if sepultura_id:
        sepultura_by_id(sepultura_id)
        if tipo in {"EXHUMACION", "RESCATE"}:
            active_contract = active_contract_for_sepultura(sepultura_id)
            active_owner = (
                active_titular_for_contract(active_contract.id)
                if active_contract
                else None
            )
            has_prior_remains = SepulturaDifunto.query.filter_by(
                org_id=org_id(), sepultura_id=sepultura_id
            ).first()
            if active_owner and active_owner.is_provisional and has_prior_remains:
                raise ValueError(
                    translate("validation.expediente.provisional_restriction")
                )

    difunto_id_raw = (payload.get("difunto_id") or "").strip()
    difunto_id = int(difunto_id_raw) if difunto_id_raw.isdigit() else None
    if difunto_id:
        difunto = Person.query.filter_by(org_id=org_id(), id=difunto_id).first()
        if not difunto:
            raise ValueError("Difunto no encontrado")

    declarante_id_raw = (payload.get("declarante_id") or "").strip()
    declarante_id = int(declarante_id_raw) if declarante_id_raw.isdigit() else None
    if declarante_id:
        declarante = Person.query.filter_by(org_id=org_id(), id=declarante_id).first()
        if not declarante:
            raise ValueError("Declarante no encontrado")

    expediente = Expediente(
        org_id=org_id(),
        numero=_next_expediente_number(date.today().year),
        tipo=tipo,
        estado="ABIERTO",
        sepultura_id=sepultura_id,
        difunto_id=difunto_id,
        declarante_id=declarante_id,
        fecha_prevista=_parse_optional_iso_date(payload.get("fecha_prevista")),
        notas=(payload.get("notas") or "").strip(),
    )
    db.session.add(expediente)
    db.session.flush()

    _log_sepultura_movement(
        expediente.sepultura_id,
        MovimientoTipo.ALTA_EXPEDIENTE,
        f"Alta expediente {expediente.numero} ({expediente.tipo})",
        user_id,
    )
    db.session.commit()
    return expediente


def transition_expediente_state(
    expediente_id: int, new_state: str, user_id: int | None
) -> Expediente:
    expediente = expediente_by_id(expediente_id)
    current = (expediente.estado or "").upper()
    target = (new_state or "").strip().upper()
    if target not in EXPEDIENTE_STATES:
        raise ValueError("Estado de expediente invalido")
    if target not in EXPEDIENTE_TRANSITIONS.get(current, set()):
        raise ValueError(f"Transicion invalida: {current} -> {target}")

    expediente.estado = target
    db.session.add(expediente)
    _log_sepultura_movement(
        expediente.sepultura_id,
        MovimientoTipo.CAMBIO_ESTADO_EXPEDIENTE,
        f"Expediente {expediente.numero}: {current} -> {target}",
        user_id,
    )
    db.session.commit()
    return expediente


def list_expediente_ots(expediente_id: int) -> list[OrdenTrabajo]:
    expediente = expediente_by_id(expediente_id)
    return (
        OrdenTrabajo.query.filter_by(org_id=org_id(), expediente_id=expediente.id)
        .order_by(OrdenTrabajo.created_at.desc(), OrdenTrabajo.id.desc())
        .all()
    )


def list_work_orders(
    filters: dict[str, str] | None = None,
) -> list[tuple[OrdenTrabajo, Expediente | None]]:
    filters = filters or {}
    state = (filters.get("estado") or "").strip().upper()
    case_number = (filters.get("expediente") or "").strip()

    query = (
        db.session.query(OrdenTrabajo, Expediente)
        .outerjoin(Expediente, OrdenTrabajo.expediente_id == Expediente.id)
        .filter(OrdenTrabajo.org_id == org_id())
    )
    if state:
        query = query.filter(OrdenTrabajo.estado == state)
    if case_number:
        query = query.filter(Expediente.numero.ilike(f"%{case_number}%"))
    return query.order_by(OrdenTrabajo.created_at.desc(), OrdenTrabajo.id.desc()).all()


def create_expediente_ot(
    expediente_id: int, payload: dict[str, str], user_id: int | None
) -> OrdenTrabajo:
    expediente = expediente_by_id(expediente_id)
    title = (payload.get("titulo") or "").strip()
    if not title:
        title = f"OT {expediente.numero}"
    ot = OrdenTrabajo(
        org_id=org_id(),
        expediente_id=expediente.id,
        titulo=title,
        estado="PENDIENTE",
        notes=(payload.get("notes") or "").strip(),
    )
    db.session.add(ot)
    db.session.flush()
    _log_sepultura_movement(
        expediente.sepultura_id,
        MovimientoTipo.OT_EXPEDIENTE,
        f"OT creada #{ot.id} para expediente {expediente.numero}",
        user_id,
    )
    db.session.commit()
    return ot


def complete_expediente_ot(
    expediente_id: int,
    ot_id: int,
    payload: dict[str, str],
    user_id: int | None,
) -> OrdenTrabajo:
    expediente = expediente_by_id(expediente_id)
    ot = OrdenTrabajo.query.filter_by(
        org_id=org_id(), expediente_id=expediente.id, id=ot_id
    ).first()
    if not ot:
        raise ValueError("Orden de trabajo no encontrada")
    if ot.estado == "COMPLETADA":
        return ot

    ot.estado = "COMPLETADA"
    ot.completed_at = datetime.now(timezone.utc)
    note = (payload.get("notes") or "").strip()
    if note:
        ot.notes = note
    db.session.add(ot)
    _log_sepultura_movement(
        expediente.sepultura_id,
        MovimientoTipo.OT_EXPEDIENTE,
        f"OT completada #{ot.id} para expediente {expediente.numero}",
        user_id,
    )
    db.session.commit()
    return ot


def expediente_ot_pdf(expediente_id: int, ot_id: int) -> bytes:
    expediente = expediente_by_id(expediente_id)
    ot = OrdenTrabajo.query.filter_by(
        org_id=org_id(), expediente_id=expediente.id, id=ot_id
    ).first()
    if not ot:
        raise ValueError("Orden de trabajo no encontrada")
    sep = sepultura_by_id(expediente.sepultura_id) if expediente.sepultura_id else None
    lines = [
        "GSF - Orden de trabajo",
        f"OT: {ot.id}",
        f"Expediente: {expediente.numero}",
        f"Tipo expediente: {expediente.tipo}",
        f"Estado OT: {ot.estado}",
        f"Sepultura: {sep.location_label if sep else '-'}",
        f"Titulo: {ot.titulo}",
        f"Notas: {ot.notes or '-'}",
        f"Emitido: {datetime.now(timezone.utc).date().isoformat()}",
    ]
    return _simple_pdf(lines)


def list_lapida_stock() -> list[LapidaStock]:
    return (
        LapidaStock.query.filter_by(org_id=org_id())
        .order_by(LapidaStock.codigo.asc(), LapidaStock.id.asc())
        .all()
    )


def list_lapida_stock_movements(limit: int = 50) -> list[LapidaStockMovimiento]:
    return (
        LapidaStockMovimiento.query.filter_by(org_id=org_id())
        .order_by(
            LapidaStockMovimiento.created_at.desc(), LapidaStockMovimiento.id.desc()
        )
        .limit(limit)
        .all()
    )


def _find_lapida_stock(stock_id_raw: str | None, codigo_raw: str | None) -> LapidaStock:
    stock = None
    if (stock_id_raw or "").strip().isdigit():
        stock = LapidaStock.query.filter_by(
            org_id=org_id(), id=int(stock_id_raw)
        ).first()
    if not stock and (codigo_raw or "").strip():
        stock = LapidaStock.query.filter_by(
            org_id=org_id(), codigo=(codigo_raw or "").strip()
        ).first()
    if not stock:
        raise ValueError("Stock de lapida no encontrado")
    return stock


def lapida_stock_entry(payload: dict[str, str], user_id: int | None) -> LapidaStock:
    quantity_raw = (payload.get("quantity") or "").strip()
    if not quantity_raw.isdigit() or int(quantity_raw) <= 0:
        raise ValueError("Cantidad de entrada invalida")
    quantity = int(quantity_raw)

    stock = None
    stock_id_raw = (payload.get("stock_id") or "").strip()
    if stock_id_raw.isdigit():
        stock = LapidaStock.query.filter_by(
            org_id=org_id(), id=int(stock_id_raw)
        ).first()

    if not stock:
        code = (payload.get("codigo") or "").strip().upper()
        if not code:
            raise ValueError("Codigo de lapida obligatorio")
        stock = LapidaStock.query.filter_by(org_id=org_id(), codigo=code).first()
        if not stock:
            stock = LapidaStock(
                org_id=org_id(),
                codigo=code,
                descripcion=(payload.get("descripcion") or code).strip(),
                estado="ACTIVO",
                available_qty=0,
            )
            db.session.add(stock)
            db.session.flush()

    stock.available_qty = int(stock.available_qty or 0) + quantity
    db.session.add(stock)
    db.session.add(
        LapidaStockMovimiento(
            org_id=org_id(),
            lapida_stock_id=stock.id,
            movimiento="ENTRADA",
            quantity=quantity,
            notes=(payload.get("notes") or "").strip(),
        )
    )
    db.session.commit()
    return stock


def lapida_stock_exit(payload: dict[str, str], user_id: int | None) -> LapidaStock:
    quantity_raw = (payload.get("quantity") or "").strip()
    if not quantity_raw.isdigit() or int(quantity_raw) <= 0:
        raise ValueError("Cantidad de salida invalida")
    quantity = int(quantity_raw)
    stock = _find_lapida_stock(payload.get("stock_id"), payload.get("codigo"))
    current_qty = int(stock.available_qty or 0)
    if quantity > current_qty:
        raise ValueError("No hay stock suficiente")

    sepultura_id = None
    sep_raw = (payload.get("sepultura_id") or "").strip()
    if sep_raw:
        if not sep_raw.isdigit():
            raise ValueError("Sepultura invalida")
        sepultura_id = int(sep_raw)
        sepultura_by_id(sepultura_id)

    stock.available_qty = current_qty - quantity
    db.session.add(stock)
    db.session.add(
        LapidaStockMovimiento(
            org_id=org_id(),
            lapida_stock_id=stock.id,
            movimiento="SALIDA",
            quantity=quantity,
            sepultura_id=sepultura_id,
            notes=(payload.get("notes") or "").strip(),
        )
    )
    _log_sepultura_movement(
        sepultura_id,
        MovimientoTipo.LAPIDA,
        f"Salida stock lapida {stock.codigo} x{quantity}",
        user_id,
    )
    db.session.commit()
    if int(stock.available_qty or 0) <= 5:
        emit_work_order_event(
            "LOW_STOCK_DETECTED",
            {
                "stock_id": stock.id,
                "stock_code": stock.codigo,
                "available_qty": int(stock.available_qty or 0),
                "area_type": WorkOrderAreaType.GENERAL.value,
                "location_text": f"Almacen lapidas ({stock.codigo})",
                "category": WorkOrderCategory.MANTENIMIENTO.value,
                "title": f"Reposicion stock lapida {stock.codigo}",
            },
            user_id=user_id,
        )
    return stock


def list_inscripciones(filters: dict[str, str]) -> list[InscripcionLateral]:
    query = InscripcionLateral.query.filter_by(org_id=org_id()).order_by(
        InscripcionLateral.created_at.desc(), InscripcionLateral.id.desc()
    )
    estado = (filters.get("estado") or "").strip().upper()
    if estado:
        query = query.filter(InscripcionLateral.estado == estado)
    sepultura_id = (filters.get("sepultura_id") or "").strip()
    if sepultura_id:
        if not sepultura_id.isdigit():
            return []
        query = query.filter(InscripcionLateral.sepultura_id == int(sepultura_id))
    text = (filters.get("texto") or "").strip()
    if text:
        query = query.filter(InscripcionLateral.texto.ilike(f"%{text}%"))
    return query.all()


def create_inscripcion_lateral(
    payload: dict[str, str], user_id: int | None
) -> InscripcionLateral:
    sepultura_id_raw = (payload.get("sepultura_id") or "").strip()
    if not sepultura_id_raw.isdigit():
        raise ValueError("Sepultura obligatoria")
    sepultura = sepultura_by_id(int(sepultura_id_raw))
    text = (payload.get("texto") or "").strip()
    if not text:
        raise ValueError("Texto de inscripcion obligatorio")

    item = InscripcionLateral(
        org_id=org_id(),
        sepultura_id=sepultura.id,
        texto=text,
        estado="PENDIENTE_GRABAR",
    )
    db.session.add(item)
    db.session.flush()
    _log_sepultura_movement(
        sepultura.id,
        MovimientoTipo.INSCRIPCION_LATERAL,
        f"Inscripcion lateral creada #{item.id}",
        user_id,
    )
    db.session.commit()
    emit_work_order_event(
        "LAPIDA_ORDER_CREATED",
        {
            "sepultura_id": sepultura.id,
            "inscripcion_id": item.id,
            "title": f"Coordinar lapida / inscripcion #{item.id}",
            "description": item.texto,
            "category": WorkOrderCategory.FUNERARIA.value,
        },
        user_id=user_id,
    )
    return item


def transition_inscripcion_estado(
    inscripcion_id: int, payload: dict[str, str], user_id: int | None
) -> InscripcionLateral:
    item = InscripcionLateral.query.filter_by(
        org_id=org_id(), id=inscripcion_id
    ).first()
    if not item:
        raise ValueError("Inscripcion no encontrada")

    current = (item.estado or "").upper()
    requested = (payload.get("estado") or "").strip().upper()
    target = requested or INSCRIPCION_TRANSITIONS.get(current, "")
    if not target:
        raise ValueError("Estado de inscripcion invalido")
    if target not in INSCRIPCION_STATES:
        raise ValueError("Estado de inscripcion invalido")
    if INSCRIPCION_TRANSITIONS.get(current) != target:
        raise ValueError(f"Transicion invalida: {current} -> {target}")

    item.estado = target
    db.session.add(item)
    _log_sepultura_movement(
        item.sepultura_id,
        MovimientoTipo.INSCRIPCION_LATERAL,
        f"Inscripcion lateral #{item.id}: {current} -> {target}",
        user_id,
    )
    db.session.commit()
    return item


REPORTING_SCREEN_KEYS = {
    "sepulturas",
    "contratos",
    "deuda",
    "ot_carga_equipos",
    "ot_sla_cumplimiento",
    "ot_calendario_faenas",
    "deuda_aging",
    "deuda_recaudacion",
}
REPORTING_PDF_ONLY_KEYS = {"directivo_operacion_pdf", "directivo_finanzas_pdf"}
REPORTING_ALL_KEYS = REPORTING_SCREEN_KEYS | REPORTING_PDF_ONLY_KEYS
REPORTING_SCHEDULE_CADENCES = {"WEEKLY", "MONTHLY"}
REPORTING_SCHEDULE_FORMATS = {"CSV", "PDF"}
REPORTING_OT_OPEN_STATUSES = {
    WorkOrderStatus.BORRADOR,
    WorkOrderStatus.PENDIENTE_PLANIFICACION,
    WorkOrderStatus.PLANIFICADA,
    WorkOrderStatus.ASIGNADA,
    WorkOrderStatus.EN_CURSO,
    WorkOrderStatus.EN_VALIDACION,
}
REPORTING_OT_TERMINAL_STATUSES = {
    WorkOrderStatus.COMPLETADA,
    WorkOrderStatus.CANCELADA,
}


def reporting_schedule_schema_ready() -> bool:
    bind = db.session.get_bind()
    inspector = inspect(bind)
    return inspector.has_table("report_schedule") and inspector.has_table(
        "report_delivery_log"
    )


def _to_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )


def _parse_filter_date(value: str | None) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _resolve_reporting_range(filters: dict[str, str]) -> tuple[date, date]:
    today = date.today()
    preset = (filters.get("cadence_preset") or "").strip().lower()
    default_from = today - timedelta(days=6)
    default_to = today
    if preset in {"diario", "daily"}:
        default_from = today
        default_to = today
    elif preset in {"semanal", "weekly"}:
        default_from = today - timedelta(days=6)
        default_to = today
    elif preset in {"mensual", "monthly"}:
        default_from = today.replace(day=1)
        default_to = today
    date_from = _parse_filter_date(filters.get("date_from")) or default_from
    date_to = _parse_filter_date(filters.get("date_to")) or default_to
    if date_from > date_to:
        date_from, date_to = date_to, date_from
    return date_from, date_to


def _in_date_range(value: datetime | None, from_day: date, to_day: date) -> bool:
    stamp = _to_utc(value)
    if stamp is None:
        return False
    day = stamp.date()
    return from_day <= day <= to_day


def _safe_decimal(value: object) -> Decimal:
    if value is None:
        return Decimal("0.00")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0.00")


def _reporting_contracts_query(filters: dict[str, str]):
    query = (
        DerechoFunerarioContrato.query.filter_by(org_id=org_id())
        .join(Sepultura, Sepultura.id == DerechoFunerarioContrato.sepultura_id)
    )
    contrato_id = (filters.get("contrato_id") or "").strip()
    if contrato_id:
        if not contrato_id.isdigit():
            return None
        query = query.filter(DerechoFunerarioContrato.id == int(contrato_id))
    sepultura_id = (filters.get("sepultura_id") or "").strip()
    if sepultura_id:
        if not sepultura_id.isdigit():
            return None
        query = query.filter(DerechoFunerarioContrato.sepultura_id == int(sepultura_id))
    bloque = (filters.get("bloque") or "").strip()
    if bloque:
        query = query.filter(Sepultura.bloque.ilike(f"%{bloque}%"))
    return query


def _reporting_work_orders_base_query(filters: dict[str, str]):
    query = (
        WorkOrder.query.options(
            joinedload(WorkOrder.sepultura),
            joinedload(WorkOrder.assigned_user),
        )
        .filter(WorkOrder.org_id == org_id())
        .outerjoin(Sepultura, Sepultura.id == WorkOrder.sepultura_id)
    )

    assigned_user_id = (filters.get("assigned_user_id") or "").strip()
    if assigned_user_id:
        if not assigned_user_id.isdigit():
            return None
        query = query.filter(WorkOrder.assigned_user_id == int(assigned_user_id))
    type_code = (filters.get("type_code") or "").strip().upper()
    if type_code:
        query = query.filter(WorkOrder.type_code == type_code)
    category = (filters.get("category") or "").strip().upper()
    if category:
        try:
            query = query.filter(WorkOrder.category == WorkOrderCategory[category])
        except KeyError:
            return None
    status = (filters.get("status") or "").strip().upper()
    if status:
        try:
            query = query.filter(WorkOrder.status == WorkOrderStatus[status])
        except KeyError:
            return None
    sepultura_id = (filters.get("sepultura_id") or "").strip()
    if sepultura_id:
        if not sepultura_id.isdigit():
            return None
        query = query.filter(WorkOrder.sepultura_id == int(sepultura_id))
    bloque = (filters.get("bloque") or "").strip()
    if bloque:
        query = query.filter(Sepultura.bloque.ilike(f"%{bloque}%"))
    return query


def _work_order_sla_hours_map() -> dict[str, int]:
    rows = WorkOrderType.query.filter_by(org_id=org_id(), active=True).all()
    mapping: dict[str, int] = {}
    for row in rows:
        mapping[row.code.upper()] = 48
    templates = (
        WorkOrderTemplate.query.filter_by(org_id=org_id(), active=True)
        .filter(WorkOrderTemplate.type_id.is_not(None))
        .filter(WorkOrderTemplate.sla_hours.is_not(None))
        .all()
    )
    type_by_id = {row.id: row for row in rows}
    for template in templates:
        type_row = type_by_id.get(template.type_id or -1)
        if not type_row:
            continue
        try:
            mapping[type_row.code.upper()] = max(1, int(template.sla_hours or 48))
        except Exception:
            mapping[type_row.code.upper()] = 48
    return mapping


def _work_order_deadline(row: WorkOrder, sla_hours_by_type: dict[str, int]) -> datetime:
    due = _to_utc(row.due_at)
    if due:
        return due
    created = _to_utc(row.created_at) or datetime.now(timezone.utc)
    code = (row.type_code or "").strip().upper()
    hours = sla_hours_by_type.get(code, 48)
    return created + timedelta(hours=hours)


def _work_order_location(row: WorkOrder) -> str:
    if row.sepultura:
        return row.sepultura.location_label
    area = row.area_type.value if row.area_type else ""
    code = (row.area_code or "").strip()
    text = (row.location_text or "").strip()
    if area and code and text:
        return f"{area} {code} - {text}"
    if area and code:
        return f"{area} {code}"
    if area and text:
        return f"{area} - {text}"
    return code or text or "-"


def reporting_sepulturas_rows(filters: dict[str, str]) -> list[dict[str, object]]:
    query = Sepultura.query.filter_by(org_id=org_id()).order_by(
        Sepultura.bloque.asc(),
        Sepultura.fila.asc(),
        Sepultura.columna.asc(),
        Sepultura.numero.asc(),
    )
    estado = (filters.get("estado") or "").strip().upper()
    if estado:
        try:
            query = query.filter(Sepultura.estado == SepulturaEstado[estado])
        except KeyError:
            return []
    modalidad = (filters.get("modalidad") or "").strip()
    if modalidad:
        query = query.filter(Sepultura.modalidad.ilike(f"%{modalidad}%"))
    bloque = (filters.get("bloque") or "").strip()
    if bloque:
        query = query.filter(Sepultura.bloque.ilike(f"%{bloque}%"))
    rows = []
    for sep in query.all():
        rows.append(
            {
                "id": sep.id,
                "sepultura": sep.location_label,
                "bloque": sep.bloque,
                "modalidad": sep.modalidad,
                "estado": sep.estado.value,
            }
        )
    return rows


def reporting_contratos_rows(filters: dict[str, str]) -> list[dict[str, object]]:
    today = date.today()
    query = _reporting_contracts_query(filters)
    if query is None:
        return []
    query = query.order_by(DerechoFunerarioContrato.id.desc())
    tipo = (filters.get("tipo") or "").strip().upper()
    if tipo:
        try:
            query = query.filter(DerechoFunerarioContrato.tipo == DerechoTipo[tipo])
        except KeyError:
            return []
    vigencia = (filters.get("vigencia") or "").strip().upper()
    if vigencia == "VIGENTE":
        query = query.filter(DerechoFunerarioContrato.fecha_inicio <= today).filter(
            DerechoFunerarioContrato.fecha_fin >= today
        )
    elif vigencia == "VENCIDO":
        query = query.filter(DerechoFunerarioContrato.fecha_fin < today)

    titular_filter = (filters.get("titular") or "").strip().lower()
    rows = []
    for contract in query.all():
        titular = (
            OwnershipRecord.query.filter_by(org_id=org_id(), contract_id=contract.id)
            .filter(
                or_(
                    OwnershipRecord.end_date.is_(None),
                    OwnershipRecord.end_date >= today,
                )
            )
            .order_by(OwnershipRecord.start_date.desc())
            .first()
        )
        titular_name = titular.person.full_name if titular else "-"
        if titular_filter and titular_filter not in titular_name.lower():
            continue
        rows.append(
            {
                "id": contract.id,
                "tipo": contract.tipo.value,
                "vigencia": "VIGENTE" if contract.fecha_fin >= today else "VENCIDO",
                "titular": titular_name,
                "sepultura": (
                    contract.sepultura.location_label if contract.sepultura else "-"
                ),
            }
        )
    return rows


def reporting_deuda_rows(filters: dict[str, str]) -> list[dict[str, object]]:
    # Deuda consolidada por contrato: tiquets pendientes + facturas impagadas.
    query = _reporting_contracts_query(filters)
    if query is None:
        return []
    query = query.order_by(DerechoFunerarioContrato.id.desc())
    rows = []
    for contract in query.all():
        pending_tickets = (
            TasaMantenimientoTicket.query.filter_by(
                org_id=org_id(), contrato_id=contract.id
            )
            .filter(TasaMantenimientoTicket.estado != TicketEstado.COBRADO)
            .all()
        )
        unpaid_invoices = Invoice.query.filter_by(
            org_id=org_id(),
            contrato_id=contract.id,
            estado=InvoiceEstado.IMPAGADA,
        ).all()
        if not pending_tickets and not unpaid_invoices:
            continue
        ticket_amount = sum(
            (_safe_decimal(t.importe) for t in pending_tickets), Decimal("0.00")
        )
        invoice_amount = sum(
            (_safe_decimal(i.total_amount) for i in unpaid_invoices), Decimal("0.00")
        )
        rows.append(
            {
                "contrato_id": contract.id,
                "sepultura": (
                    contract.sepultura.location_label if contract.sepultura else "-"
                ),
                "tickets_pendientes": len(pending_tickets),
                "importe_tickets": ticket_amount,
                "facturas_impagadas": len(unpaid_invoices),
                "importe_facturas": invoice_amount,
                "deuda_total": ticket_amount + invoice_amount,
            }
        )
    return rows


def reporting_ot_carga_equipos_rows(filters: dict[str, str]) -> list[dict[str, object]]:
    query = _reporting_work_orders_base_query(filters)
    if query is None:
        return []
    rows = query.all()
    date_from, date_to = _resolve_reporting_range(filters)
    grouped: dict[tuple[str, str, str], dict[str, object]] = {}
    open_by_type: dict[str, int] = {}
    for row in rows:
        assigned_name = row.assigned_user.full_name if row.assigned_user else "Sin asignar"
        type_code = (row.type_code or "SIN_TIPO").strip().upper()
        category = row.category.value
        key = (assigned_name, type_code, category)
        bucket = grouped.setdefault(
            key,
            {
                "assigned_user": assigned_name,
                "type_code": type_code,
                "category": category,
                "status_scope": (filters.get("status") or "").strip().upper() or "TODOS",
                "ot_abiertas": 0,
                "ot_nuevas": 0,
                "ot_completadas": 0,
                "backlog_neto": 0,
                "pct_carga_tipo": 0.0,
            },
        )
        if row.status in REPORTING_OT_OPEN_STATUSES:
            bucket["ot_abiertas"] = int(bucket["ot_abiertas"]) + 1
            open_by_type[type_code] = open_by_type.get(type_code, 0) + 1
        if _in_date_range(row.created_at, date_from, date_to):
            bucket["ot_nuevas"] = int(bucket["ot_nuevas"]) + 1
        if row.status == WorkOrderStatus.COMPLETADA and _in_date_range(
            row.completed_at, date_from, date_to
        ):
            bucket["ot_completadas"] = int(bucket["ot_completadas"]) + 1
    result: list[dict[str, object]] = []
    for bucket in grouped.values():
        type_total = max(1, open_by_type.get(str(bucket["type_code"]), 0))
        abiertas = int(bucket["ot_abiertas"])
        nuevas = int(bucket["ot_nuevas"])
        completadas = int(bucket["ot_completadas"])
        bucket["backlog_neto"] = abiertas + nuevas - completadas
        bucket["pct_carga_tipo"] = round((abiertas / type_total) * 100.0, 2)
        result.append(bucket)
    result.sort(
        key=lambda item: (
            -int(item["ot_abiertas"]),
            -int(item["ot_nuevas"]),
            str(item["assigned_user"]),
            str(item["type_code"]),
        )
    )
    return result


def reporting_ot_sla_cumplimiento_rows(filters: dict[str, str]) -> list[dict[str, object]]:
    query = _reporting_work_orders_base_query(filters)
    if query is None:
        return []
    rows = query.all()
    date_from, date_to = _resolve_reporting_range(filters)
    sla_hours_by_type = _work_order_sla_hours_map()
    now_utc = datetime.now(timezone.utc)
    grouped: dict[str, dict[str, object]] = {}
    total: dict[str, object] = {
        "type_code": "TOTAL",
        "total_con_sla": 0,
        "cumplidas": 0,
        "vencidas": 0,
        "pct_cumplimiento": 0.0,
        "lead_time_media_h": 0.0,
        "lead_time_mediana_h": 0.0,
        "_lead_times": [],
    }

    def _bucket_for(code: str) -> dict[str, object]:
        key = (code or "SIN_TIPO").upper()
        return grouped.setdefault(
            key,
            {
                "type_code": key,
                "total_con_sla": 0,
                "cumplidas": 0,
                "vencidas": 0,
                "pct_cumplimiento": 0.0,
                "lead_time_media_h": 0.0,
                "lead_time_mediana_h": 0.0,
                "_lead_times": [],
            },
        )

    for row in rows:
        if not (
            _in_date_range(row.created_at, date_from, date_to)
            or _in_date_range(row.completed_at, date_from, date_to)
        ):
            continue
        bucket = _bucket_for(row.type_code or "SIN_TIPO")
        deadline = _work_order_deadline(row, sla_hours_by_type)
        for item in (bucket, total):
            item["total_con_sla"] = int(item["total_con_sla"]) + 1

        completed_at = _to_utc(row.completed_at)
        created_at = _to_utc(row.created_at) or now_utc
        if row.status == WorkOrderStatus.COMPLETADA and completed_at is not None:
            lead_time_hours = round(
                max(0.0, (completed_at - created_at).total_seconds() / 3600.0), 2
            )
            bucket_leads = bucket["_lead_times"]
            total_leads = total["_lead_times"]
            if isinstance(bucket_leads, list):
                bucket_leads.append(lead_time_hours)
            if isinstance(total_leads, list):
                total_leads.append(lead_time_hours)
            if completed_at <= deadline:
                bucket["cumplidas"] = int(bucket["cumplidas"]) + 1
                total["cumplidas"] = int(total["cumplidas"]) + 1
            else:
                bucket["vencidas"] = int(bucket["vencidas"]) + 1
                total["vencidas"] = int(total["vencidas"]) + 1
        elif row.status not in REPORTING_OT_TERMINAL_STATUSES and deadline < now_utc:
            bucket["vencidas"] = int(bucket["vencidas"]) + 1
            total["vencidas"] = int(total["vencidas"]) + 1

    result: list[dict[str, object]] = []
    for data in [total, *grouped.values()]:
        total_con_sla = max(0, int(data["total_con_sla"]))
        cumplidas = max(0, int(data["cumplidas"]))
        lead_times = data.pop("_lead_times", [])
        if total_con_sla > 0:
            data["pct_cumplimiento"] = round((cumplidas / total_con_sla) * 100.0, 2)
        if isinstance(lead_times, list) and lead_times:
            data["lead_time_media_h"] = round(sum(lead_times) / len(lead_times), 2)
            data["lead_time_mediana_h"] = round(float(statistics.median(lead_times)), 2)
        result.append(data)
    return result


def reporting_ot_calendario_faenas_rows(filters: dict[str, str]) -> list[dict[str, object]]:
    query = _reporting_work_orders_base_query(filters)
    if query is None:
        return []
    rows = query.all()
    date_from, date_to = _resolve_reporting_range(filters)
    result: list[dict[str, object]] = []
    for row in rows:
        planned = _to_utc(row.planned_start_at)
        planned_end = _to_utc(row.planned_end_at)
        due = _to_utc(row.due_at)
        anchor = planned or due
        if anchor is None:
            continue
        anchor_day = anchor.date()
        if anchor_day < date_from or anchor_day > date_to:
            continue
        assigned = row.assigned_user.full_name if row.assigned_user else "Sin asignar"
        result.append(
            {
                "fecha": anchor_day.isoformat(),
                "assigned_user": assigned,
                "ot_code": row.code,
                "title": row.title,
                "priority": row.priority.value,
                "status": row.status.value,
                "ubicacion": _work_order_location(row),
                "planned_start_at": planned.isoformat() if planned else "",
                "planned_end_at": planned_end.isoformat() if planned_end else "",
                "due_at": due.isoformat() if due else "",
                "type_code": (row.type_code or "SIN_TIPO").upper(),
                "category": row.category.value,
            }
        )
    result.sort(
        key=lambda item: (
            str(item["fecha"]),
            str(item["assigned_user"]),
            str(item["planned_start_at"]),
            str(item["ot_code"]),
        )
    )
    return result


def reporting_deuda_aging_rows(filters: dict[str, str]) -> list[dict[str, object]]:
    query = _reporting_contracts_query(filters)
    if query is None:
        return []
    contract_ids = [row.id for row in query.with_entities(DerechoFunerarioContrato.id).all()]
    if not contract_ids:
        return []
    _, date_to = _resolve_reporting_range(filters)
    as_of = date_to

    bucket_order = ["0-60", "61-120", "+120"]
    buckets = {
        key: {"bucket": key, "casos": 0, "importe": Decimal("0.00"), "contratos": set()}
        for key in bucket_order
    }

    def _bucket_for_days(days: int) -> str:
        if days <= 60:
            return "0-60"
        if days <= 120:
            return "61-120"
        return "+120"

    invoices = (
        Invoice.query.filter(Invoice.org_id == org_id())
        .filter(Invoice.contrato_id.in_(contract_ids))
        .filter(Invoice.estado == InvoiceEstado.IMPAGADA)
        .all()
    )
    for inv in invoices:
        anchor = (_to_utc(inv.issued_at) or _to_utc(inv.created_at) or datetime.now(timezone.utc)).date()
        days = max(0, (as_of - anchor).days)
        bucket = buckets[_bucket_for_days(days)]
        bucket["casos"] = int(bucket["casos"]) + 1
        bucket["importe"] = _safe_decimal(bucket["importe"]) + _safe_decimal(inv.total_amount)
        casted_contracts = bucket["contratos"]
        if isinstance(casted_contracts, set):
            casted_contracts.add(inv.contrato_id)

    tickets = (
        TasaMantenimientoTicket.query.filter(TasaMantenimientoTicket.org_id == org_id())
        .filter(TasaMantenimientoTicket.contrato_id.in_(contract_ids))
        .filter(TasaMantenimientoTicket.estado != TicketEstado.COBRADO)
        .filter(TasaMantenimientoTicket.invoice_id.is_(None))
        .all()
    )
    for ticket in tickets:
        anchor = date(ticket.anio, 1, 1)
        days = max(0, (as_of - anchor).days)
        bucket = buckets[_bucket_for_days(days)]
        bucket["casos"] = int(bucket["casos"]) + 1
        bucket["importe"] = _safe_decimal(bucket["importe"]) + _safe_decimal(ticket.importe)
        casted_contracts = bucket["contratos"]
        if isinstance(casted_contracts, set):
            casted_contracts.add(ticket.contrato_id)

    total_importe = Decimal("0.00")
    total_contracts: set[int] = set()
    rows: list[dict[str, object]] = []
    for key in bucket_order:
        bucket = buckets[key]
        amount = _safe_decimal(bucket["importe"])
        contracts = bucket["contratos"] if isinstance(bucket["contratos"], set) else set()
        total_importe += amount
        total_contracts |= contracts
        rows.append(
            {
                "bucket": key,
                "casos": int(bucket["casos"]),
                "importe": amount,
                "contratos": len(contracts),
            }
        )
    rows.append(
        {
            "bucket": "TOTAL",
            "casos": sum(int(row["casos"]) for row in rows),
            "importe": total_importe,
            "contratos": len(total_contracts),
        }
    )
    return rows


def _recaudacion_period_metrics(
    contract_ids: list[int],
    date_from: date,
    date_to: date,
) -> dict[str, Decimal]:
    emitted = Decimal("0.00")
    paid = Decimal("0.00")
    pending = Decimal("0.00")

    invoices_emitted = (
        Invoice.query.filter(Invoice.org_id == org_id())
        .filter(Invoice.contrato_id.in_(contract_ids))
        .filter(Invoice.issued_at.is_not(None))
        .all()
    )
    for inv in invoices_emitted:
        issued = _to_utc(inv.issued_at)
        if issued and date_from <= issued.date() <= date_to:
            emitted += _safe_decimal(inv.total_amount)

    payments = (
        Payment.query.join(Invoice, Invoice.id == Payment.invoice_id)
        .filter(Payment.org_id == org_id())
        .filter(Invoice.contrato_id.in_(contract_ids))
        .all()
    )
    for payment in payments:
        paid_at = _to_utc(payment.paid_at)
        if paid_at and date_from <= paid_at.date() <= date_to:
            paid += _safe_decimal(payment.amount)

    close_dt = datetime.combine(date_to, time.max, tzinfo=timezone.utc)
    invoices_to_close = (
        Invoice.query.options(joinedload(Invoice.payments))
        .filter(Invoice.org_id == org_id())
        .filter(Invoice.contrato_id.in_(contract_ids))
        .filter(Invoice.issued_at.is_not(None))
        .all()
    )
    for inv in invoices_to_close:
        issued = _to_utc(inv.issued_at)
        if not issued or issued > close_dt:
            continue
        if inv.estado == InvoiceEstado.PAGADA:
            continue
        paid_until_close = Decimal("0.00")
        for payment in inv.payments:
            paid_at = _to_utc(payment.paid_at)
            if paid_at and paid_at <= close_dt:
                paid_until_close += _safe_decimal(payment.amount)
        remaining = _safe_decimal(inv.total_amount) - paid_until_close
        if remaining > 0:
            pending += remaining

    return {"emitido": emitted, "cobrado": paid, "pendiente": pending}


def _pct_variation(current: Decimal, previous: Decimal) -> float:
    if previous == 0:
        return 100.0 if current > 0 else 0.0
    return float(round(((current - previous) / previous) * 100, 2))


def reporting_deuda_recaudacion_rows(filters: dict[str, str]) -> list[dict[str, object]]:
    query = _reporting_contracts_query(filters)
    if query is None:
        return []
    contract_ids = [row.id for row in query.with_entities(DerechoFunerarioContrato.id).all()]
    if not contract_ids:
        return []

    date_from, date_to = _resolve_reporting_range(filters)
    metrics = _recaudacion_period_metrics(contract_ids, date_from, date_to)
    period_days = max(1, (date_to - date_from).days + 1)
    prev_to = date_from - timedelta(days=1)
    prev_from = prev_to - timedelta(days=period_days - 1)
    prev_metrics = _recaudacion_period_metrics(contract_ids, prev_from, prev_to)

    emitido = _safe_decimal(metrics["emitido"])
    cobrado = _safe_decimal(metrics["cobrado"])
    pendiente = _safe_decimal(metrics["pendiente"])
    tasa_cobro = float(round((cobrado / emitido) * 100, 2)) if emitido > 0 else 0.0
    return [
        {
            "periodo": f"{date_from.isoformat()}..{date_to.isoformat()}",
            "emitido": emitido,
            "cobrado": cobrado,
            "pendiente": pendiente,
            "tasa_cobro_pct": tasa_cobro,
            "variacion_emitido_pct": _pct_variation(emitido, _safe_decimal(prev_metrics["emitido"])),
            "variacion_cobrado_pct": _pct_variation(cobrado, _safe_decimal(prev_metrics["cobrado"])),
            "periodo_anterior": f"{prev_from.isoformat()}..{prev_to.isoformat()}",
        }
    ]


def reporting_directivo_operacion_rows(filters: dict[str, str]) -> list[dict[str, object]]:
    carga = reporting_ot_carga_equipos_rows(filters)
    sla = reporting_ot_sla_cumplimiento_rows(filters)
    total_sla = next((row for row in sla if row.get("type_code") == "TOTAL"), None)
    top_bottlenecks = sorted(carga, key=lambda row: int(row["ot_abiertas"]), reverse=True)[:5]
    rows: list[dict[str, object]] = []
    if total_sla:
        rows.append(
            {
                "kpi": "Cumplimiento SLA global",
                "valor": f"{total_sla.get('pct_cumplimiento', 0)}%",
            }
        )
        rows.append(
            {
                "kpi": "OT vencidas (global)",
                "valor": str(total_sla.get("vencidas", 0)),
            }
        )
    for item in top_bottlenecks:
        rows.append(
            {
                "kpi": f"Cuello: {item['assigned_user']} / {item['type_code']}",
                "valor": f"abiertas={item['ot_abiertas']} backlog={item['backlog_neto']}",
            }
        )
    return rows


def reporting_directivo_finanzas_rows(filters: dict[str, str]) -> list[dict[str, object]]:
    aging = reporting_deuda_aging_rows(filters)
    recaudacion = reporting_deuda_recaudacion_rows(filters)
    deuda = sorted(
        reporting_deuda_rows(filters),
        key=lambda row: _safe_decimal(row["deuda_total"]),
        reverse=True,
    )[:5]
    rows: list[dict[str, object]] = []
    total = next((row for row in aging if row.get("bucket") == "TOTAL"), None)
    if total:
        rows.append(
            {
                "kpi": "Deuda total",
                "valor": str(_safe_decimal(total.get("importe"))),
            }
        )
        rows.append(
            {
                "kpi": "Contratos con deuda",
                "valor": str(total.get("contratos", 0)),
            }
        )
    if recaudacion:
        first = recaudacion[0]
        rows.append(
            {
                "kpi": "Tasa de cobro",
                "valor": f"{first.get('tasa_cobro_pct', 0)}%",
            }
        )
        rows.append(
            {
                "kpi": "Variacion cobrado",
                "valor": f"{first.get('variacion_cobrado_pct', 0)}%",
            }
        )
    for item in deuda:
        rows.append(
            {
                "kpi": f"Contrato #{item['contrato_id']} ({item['sepultura']})",
                "valor": str(_safe_decimal(item["deuda_total"])),
            }
        )
    return rows


def reporting_rows(report_key: str, filters: dict[str, str]) -> list[dict[str, object]]:
    key = (report_key or "").strip().lower()
    if key == "sepulturas":
        return reporting_sepulturas_rows(filters)
    if key == "contratos":
        return reporting_contratos_rows(filters)
    if key == "deuda":
        return reporting_deuda_rows(filters)
    if key == "ot_carga_equipos":
        return reporting_ot_carga_equipos_rows(filters)
    if key == "ot_sla_cumplimiento":
        return reporting_ot_sla_cumplimiento_rows(filters)
    if key == "ot_calendario_faenas":
        return reporting_ot_calendario_faenas_rows(filters)
    if key == "deuda_aging":
        return reporting_deuda_aging_rows(filters)
    if key == "deuda_recaudacion":
        return reporting_deuda_recaudacion_rows(filters)
    if key == "directivo_operacion_pdf":
        return reporting_directivo_operacion_rows(filters)
    if key == "directivo_finanzas_pdf":
        return reporting_directivo_finanzas_rows(filters)
    raise ValueError("Informe invalido")


def paginate_rows(
    rows: list[dict[str, object]], page: int, page_size: int
) -> dict[str, object]:
    safe_page = page if page > 0 else 1
    safe_size = max(1, min(page_size, 100))
    total = len(rows)
    start = (safe_page - 1) * safe_size
    end = start + safe_size
    return {
        "rows": rows[start:end],
        "page": safe_page,
        "page_size": safe_size,
        "total": total,
        "pages": max(1, (total + safe_size - 1) // safe_size),
    }


def reporting_headers(report_key: str) -> list[str]:
    key = (report_key or "").strip().lower()
    if key == "sepulturas":
        return ["id", "sepultura", "bloque", "modalidad", "estado"]
    if key == "contratos":
        return ["id", "tipo", "vigencia", "titular", "sepultura"]
    if key == "deuda":
        return [
            "contrato_id",
            "sepultura",
            "tickets_pendientes",
            "importe_tickets",
            "facturas_impagadas",
            "importe_facturas",
            "deuda_total",
        ]
    if key == "ot_carga_equipos":
        return [
            "assigned_user",
            "type_code",
            "category",
            "status_scope",
            "ot_abiertas",
            "ot_nuevas",
            "ot_completadas",
            "backlog_neto",
            "pct_carga_tipo",
        ]
    if key == "ot_sla_cumplimiento":
        return [
            "type_code",
            "total_con_sla",
            "cumplidas",
            "vencidas",
            "pct_cumplimiento",
            "lead_time_media_h",
            "lead_time_mediana_h",
        ]
    if key == "ot_calendario_faenas":
        return [
            "fecha",
            "assigned_user",
            "ot_code",
            "title",
            "priority",
            "status",
            "ubicacion",
            "planned_start_at",
            "planned_end_at",
            "due_at",
            "type_code",
            "category",
        ]
    if key == "deuda_aging":
        return ["bucket", "casos", "importe", "contratos"]
    if key == "deuda_recaudacion":
        return [
            "periodo",
            "emitido",
            "cobrado",
            "pendiente",
            "tasa_cobro_pct",
            "variacion_emitido_pct",
            "variacion_cobrado_pct",
            "periodo_anterior",
        ]
    if key in REPORTING_PDF_ONLY_KEYS:
        return ["kpi", "valor"]
    raise ValueError("Informe invalido")


def reporting_csv_bytes(
    report_key: str,
    filters: dict[str, str],
    export_limit: int = 1000,
) -> bytes:
    rows = reporting_rows(report_key, filters)
    limited = rows[: max(1, min(export_limit, 5000))]
    headers = reporting_headers(report_key)
    stream = StringIO()
    writer = csv.DictWriter(stream, fieldnames=headers)
    writer.writeheader()
    for row in limited:
        normalized = {k: row.get(k, "") for k in headers}
        writer.writerow(normalized)
    return stream.getvalue().encode("utf-8")


def reporting_pdf_bytes(
    report_key: str,
    filters: dict[str, str],
    export_limit: int = 200,
) -> bytes:
    headers = reporting_headers(report_key)
    rows = reporting_rows(report_key, filters)[: max(1, min(export_limit, 2000))]
    lines = [
        "GSF - Reporting Cementerio",
        f"Informe: {report_key}",
        f"Fecha: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
    ]
    filters_desc = ", ".join(
        f"{k}={v}" for k, v in sorted(filters.items()) if (v or "").strip()
    )
    if filters_desc:
        lines.append(f"Filtros: {filters_desc}")
    if not rows:
        lines.append("Sin resultados")
        return _simple_pdf(lines)
    lines.append("----")
    lines.append(" | ".join(headers))
    lines.append("----")
    for row in rows:
        values = [str(row.get(key, "")) for key in headers]
        lines.append(" | ".join(values))
    return _simple_pdf(lines)


def reporting_filter_users() -> list[dict[str, object]]:
    rows = (
        User.query.join(Membership, Membership.user_id == User.id)
        .filter(Membership.org_id == org_id())
        .order_by(User.full_name.asc(), User.email.asc())
        .all()
    )
    return [{"id": row.id, "name": row.full_name} for row in rows]


def reporting_filter_type_codes() -> list[str]:
    rows = (
        WorkOrder.query.filter(WorkOrder.org_id == org_id())
        .with_entities(WorkOrder.type_code)
        .filter(WorkOrder.type_code.is_not(None))
        .distinct()
        .order_by(WorkOrder.type_code.asc())
        .all()
    )
    return [str(row[0]).upper() for row in rows if row[0]]


def reporting_filter_blocks() -> list[str]:
    rows = (
        Sepultura.query.filter_by(org_id=org_id())
        .with_entities(Sepultura.bloque)
        .distinct()
        .order_by(Sepultura.bloque.asc())
        .all()
    )
    return [str(row[0]) for row in rows if row[0]]


def _normalize_schedule_time(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return "07:00"
    try:
        parsed = time.fromisoformat(value if len(value) > 5 else f"{value}:00")
        return f"{parsed.hour:02d}:{parsed.minute:02d}"
    except ValueError as exc:
        raise ValueError("Hora de ejecucion invalida (usa HH:MM)") from exc


def _normalize_schedule_timezone(raw: str) -> str:
    zone = (raw or "Europe/Madrid").strip() or "Europe/Madrid"
    try:
        ZoneInfo(zone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError("Zona horaria invalida") from exc
    return zone


def _normalize_schedule_formats(raw: str) -> str:
    pieces = [part.strip().upper() for part in (raw or "").replace(";", ",").split(",")]
    values = [part for part in pieces if part in REPORTING_SCHEDULE_FORMATS]
    if not values:
        values = ["CSV"]
    return ",".join(sorted(set(values)))


def _normalize_schedule_filters_json(raw: str) -> str:
    payload = (raw or "").strip()
    if not payload:
        return "{}"
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError("Filtros JSON invalidos") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Filtros JSON invalidos")
    normalized = {
        str(k): str(v) for k, v in parsed.items() if v is not None and str(v).strip()
    }
    return json.dumps(normalized, ensure_ascii=True, sort_keys=True)


def list_reporting_schedules() -> list[ReportSchedule]:
    return (
        ReportSchedule.query.filter_by(org_id=org_id())
        .order_by(ReportSchedule.active.desc(), ReportSchedule.id.desc())
        .all()
    )


def create_reporting_schedule(
    payload: dict[str, str], user_id: int | None
) -> ReportSchedule:
    name = (payload.get("name") or "").strip()
    if not name:
        raise ValueError("Nombre obligatorio")
    report_key = (payload.get("report_key") or "").strip().lower()
    if report_key not in REPORTING_ALL_KEYS:
        raise ValueError("Informe invalido")
    cadence = (payload.get("cadence") or "").strip().upper()
    if cadence not in REPORTING_SCHEDULE_CADENCES:
        raise ValueError("Cadencia invalida")

    day_of_week = None
    day_of_month = None
    if cadence == "WEEKLY":
        raw = (payload.get("day_of_week") or "").strip()
        if not raw.isdigit():
            raise ValueError("Dia semana obligatorio (0-6)")
        day_of_week = int(raw)
        if day_of_week < 0 or day_of_week > 6:
            raise ValueError("Dia semana invalido (0-6)")
    if cadence == "MONTHLY":
        raw = (payload.get("day_of_month") or "").strip()
        if not raw.isdigit():
            raise ValueError("Dia mes obligatorio (1-31)")
        day_of_month = int(raw)
        if day_of_month < 1 or day_of_month > 31:
            raise ValueError("Dia mes invalido (1-31)")

    schedule = ReportSchedule(
        org_id=org_id(),
        name=name,
        report_key=report_key,
        cadence=cadence,
        day_of_week=day_of_week,
        day_of_month=day_of_month,
        run_time=_normalize_schedule_time(payload.get("run_time") or ""),
        timezone=_normalize_schedule_timezone(payload.get("timezone") or ""),
        recipients=(payload.get("recipients") or "").strip(),
        filters_json=_normalize_schedule_filters_json(
            payload.get("filters_json") or ""
        ),
        formats=_normalize_schedule_formats(payload.get("formats") or ""),
        active=(payload.get("active") or "").strip().lower()
        in {"1", "true", "on", "yes", "si"},
        created_by_user_id=user_id,
    )
    db.session.add(schedule)
    db.session.commit()
    return schedule


def toggle_reporting_schedule(schedule_id: int) -> ReportSchedule:
    schedule = ReportSchedule.query.filter_by(org_id=org_id(), id=schedule_id).first()
    if not schedule:
        raise ValueError("Programacion no encontrada")
    schedule.active = not schedule.active
    db.session.add(schedule)
    db.session.commit()
    return schedule


def _schedule_local_now(schedule: ReportSchedule, now_utc: datetime) -> datetime:
    utc_now = _to_utc(now_utc) or datetime.now(timezone.utc)
    try:
        zone = ZoneInfo(schedule.timezone or "Europe/Madrid")
    except ZoneInfoNotFoundError:
        zone = ZoneInfo("Europe/Madrid")
    return utc_now.astimezone(zone)


def _schedule_last_run_local(schedule: ReportSchedule) -> datetime | None:
    last = _to_utc(schedule.last_run_at)
    if last is None:
        return None
    try:
        zone = ZoneInfo(schedule.timezone or "Europe/Madrid")
    except ZoneInfoNotFoundError:
        zone = ZoneInfo("Europe/Madrid")
    return last.astimezone(zone)


def _is_schedule_due(schedule: ReportSchedule, now_utc: datetime) -> bool:
    if not schedule.active:
        return False
    local_now = _schedule_local_now(schedule, now_utc)
    run_parts = (schedule.run_time or "07:00").split(":", maxsplit=1)
    hour = int(run_parts[0]) if run_parts and run_parts[0].isdigit() else 7
    minute = int(run_parts[1]) if len(run_parts) == 2 and run_parts[1].isdigit() else 0
    scheduled_clock = time(hour=hour, minute=minute)
    if local_now.time() < scheduled_clock:
        return False

    cadence = (schedule.cadence or "").upper()
    if cadence == "WEEKLY":
        if schedule.day_of_week is None or local_now.weekday() != schedule.day_of_week:
            return False
    elif cadence == "MONTHLY":
        if schedule.day_of_month is None or local_now.day != schedule.day_of_month:
            return False
    else:
        return False

    last_local = _schedule_last_run_local(schedule)
    if last_local is None:
        return True
    if cadence == "WEEKLY":
        last_iso = last_local.isocalendar()
        now_iso = local_now.isocalendar()
        return (last_iso.year, last_iso.week) != (now_iso.year, now_iso.week)
    return (last_local.year, last_local.month) != (local_now.year, local_now.month)


def _schedule_recipient_list(schedule: ReportSchedule) -> list[str]:
    raw = (schedule.recipients or "").replace(";", ",")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _schedule_formats_list(schedule: ReportSchedule) -> list[str]:
    pieces = [part.strip().upper() for part in (schedule.formats or "").split(",")]
    values = [part for part in pieces if part in REPORTING_SCHEDULE_FORMATS]
    return values or ["CSV"]


def _schedule_filters(schedule: ReportSchedule) -> dict[str, str]:
    raw = (schedule.filters_json or "").strip() or "{}"
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): str(value) for key, value in payload.items()}


def _schedule_storage_root(schedule: ReportSchedule, run_at: datetime) -> Path:
    ts = (_to_utc(run_at) or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return (
        Path(current_app.instance_path)
        / "storage"
        / "cemetery"
        / "reporting"
        / str(schedule.org_id)
        / str(schedule.id)
        / ts
    )


def _send_reporting_email(
    recipients: list[str],
    subject: str,
    body: str,
    attachments: list[Path],
) -> str:
    if not recipients:
        return "Sin destinatarios: entrega solo almacenada"
    host = current_app.config.get("SMTP_HOST") or os.getenv("SMTP_HOST", "").strip()
    port_raw = current_app.config.get("SMTP_PORT") or os.getenv("SMTP_PORT", "25")
    username = current_app.config.get("SMTP_USER") or os.getenv("SMTP_USER", "").strip()
    password = (
        current_app.config.get("SMTP_PASSWORD") or os.getenv("SMTP_PASSWORD", "").strip()
    )
    sender = (
        current_app.config.get("SMTP_FROM")
        or os.getenv("SMTP_FROM", "").strip()
        or "no-reply@localhost"
    )
    if not host:
        return "SMTP no configurado: entrega solo almacenada"
    try:
        port = int(str(port_raw))
    except Exception:
        port = 25
    use_tls = str(
        current_app.config.get("SMTP_USE_TLS") or os.getenv("SMTP_USE_TLS", "1")
    ).strip().lower() in {"1", "true", "yes", "on"}

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content(body)
    for item in attachments:
        content = item.read_bytes()
        subtype = "csv" if item.suffix.lower() == ".csv" else "pdf"
        message.add_attachment(
            content,
            maintype="application",
            subtype=subtype,
            filename=item.name,
        )
    try:
        with smtplib.SMTP(host, port, timeout=20) as smtp:
            if use_tls:
                smtp.starttls()
            if username:
                smtp.login(username, password)
            smtp.send_message(message)
    except Exception as exc:
        return f"Error SMTP: {exc}"
    return ""


def run_reporting_schedule(
    schedule_id: int, user_id: int | None = None
) -> ReportDeliveryLog:
    schedule = ReportSchedule.query.filter_by(org_id=org_id(), id=schedule_id).first()
    if not schedule:
        raise ValueError("Programacion no encontrada")
    run_at = datetime.now(timezone.utc)
    filters = _schedule_filters(schedule)
    headers = reporting_headers(schedule.report_key)
    rows = reporting_rows(schedule.report_key, filters)

    storage = _schedule_storage_root(schedule, run_at)
    storage.mkdir(parents=True, exist_ok=True)
    artifacts: list[str] = []
    attachment_paths: list[Path] = []
    for fmt in _schedule_formats_list(schedule):
        if fmt == "CSV":
            content = reporting_csv_bytes(
                schedule.report_key, filters, export_limit=5000
            )
            filename = f"{schedule.report_key}.csv"
        else:
            content = reporting_pdf_bytes(
                schedule.report_key, filters, export_limit=1500
            )
            filename = f"{schedule.report_key}.pdf"
        absolute = storage / filename
        absolute.write_bytes(content)
        rel = absolute.relative_to(Path(current_app.instance_path)).as_posix()
        artifacts.append(rel)
        attachment_paths.append(absolute)

    email_error = _send_reporting_email(
        recipients=_schedule_recipient_list(schedule),
        subject=f"[GSF] Informe programado: {schedule.name}",
        body=(
            f"Informe: {schedule.report_key}\n"
            f"Filas: {len(rows)}\n"
            f"Cabeceras: {', '.join(headers)}\n"
            f"Generado: {run_at.isoformat()}\n"
        ),
        attachments=attachment_paths,
    )
    status = (
        "SUCCESS"
        if not email_error or "solo almacenada" in email_error.lower()
        else "ERROR"
    )
    log = ReportDeliveryLog(
        org_id=org_id(),
        schedule_id=schedule.id,
        run_at=run_at,
        status=status,
        rows_count=len(rows),
        artifacts_json=json.dumps(artifacts, ensure_ascii=True),
        error=email_error,
    )
    db.session.add(log)
    schedule.last_run_at = run_at
    db.session.add(schedule)
    db.session.commit()
    return log


def run_due_reporting_schedules(now_utc: datetime | None = None) -> dict[str, int]:
    now_value = _to_utc(now_utc) or datetime.now(timezone.utc)
    schedules = (
        ReportSchedule.query.filter_by(org_id=org_id(), active=True)
        .order_by(ReportSchedule.id.asc())
        .all()
    )
    executed = 0
    failed = 0
    for schedule in schedules:
        if not _is_schedule_due(schedule, now_value):
            continue
        try:
            log = run_reporting_schedule(schedule.id)
            if log.status == "ERROR":
                failed += 1
            else:
                executed += 1
        except Exception:
            failed += 1
    return {"executed": executed, "failed": failed}


def reset_demo_org_data(user_id: int | None = None) -> dict[str, int]:
    # Backward compatibility alias for older callers.
    return load_demo_org_initial_dataset(user_id)


def _demo_storage_roots(oid: int) -> list[Path]:
    base = Path(current_app.instance_path) / "storage" / "cemetery"
    return [
        base / "ownership_cases" / str(oid),
        base / "expedientes" / str(oid),
        base / "reporting" / str(oid),
    ]


def _demo_operational_counts(oid: int) -> dict[str, int]:
    inspector = inspect(db.session.get_bind())
    existing_tables = set(inspector.get_table_names())

    def _has(*tables: str) -> bool:
        return all(table in existing_tables for table in tables)

    def _safe_count(fn) -> int:
        try:
            return int(fn())
        except Exception:
            db.session.rollback()
            return 0

    return {
        "persons": _safe_count(lambda: Person.query.filter_by(org_id=oid).count())
        if _has("person")
        else 0,
        "sepulturas": _safe_count(
            lambda: Sepultura.query.filter_by(org_id=oid).count()
        )
        if _has("sepultura")
        else 0,
        "contracts": _safe_count(
            lambda: DerechoFunerarioContrato.query.filter_by(org_id=oid).count()
        )
        if _has("derecho_funerario_contrato")
        else 0,
        "titulares_activos": _safe_count(
            lambda: OwnershipRecord.query.filter_by(org_id=oid)
            .filter(OwnershipRecord.end_date.is_(None))
            .count()
        )
        if _has("ownership_record")
        else 0,
        "beneficiarios_activos": _safe_count(
            lambda: Beneficiario.query.filter_by(org_id=oid)
            .filter(Beneficiario.activo_hasta.is_(None))
            .count()
        )
        if _has("beneficiario")
        else 0,
        "beneficiarios_historicos": _safe_count(
            lambda: Beneficiario.query.filter_by(org_id=oid)
            .filter(Beneficiario.activo_hasta.is_not(None))
            .count()
        )
        if _has("beneficiario")
        else 0,
        "difuntos": _safe_count(lambda: SepulturaDifunto.query.filter_by(org_id=oid).count())
        if _has("sepultura_difunto")
        else 0,
        "expedientes": _safe_count(lambda: OperationCase.query.filter_by(org_id=oid).count())
        if _has("operation_case")
        else 0,
        "ots": _safe_count(lambda: WorkOrder.query.filter_by(org_id=oid).count())
        if _has("work_order")
        else 0,
        "casos": _safe_count(
            lambda: OwnershipTransferCase.query.filter_by(org_id=oid).count()
        )
        if _has("ownership_transfer_case")
        else 0,
        "operation_docs": _safe_count(
            lambda: OperationDocument.query.join(
                OperationCase, OperationCase.id == OperationDocument.operation_case_id
            )
            .filter(OperationCase.org_id == oid)
            .count()
        )
        if _has("operation_document", "operation_case")
        else 0,
        "operation_permits": _safe_count(
            lambda: OperationPermit.query.join(
                OperationCase, OperationCase.id == OperationPermit.operation_case_id
            )
            .filter(OperationCase.org_id == oid)
            .count()
        )
        if _has("operation_permit", "operation_case")
        else 0,
        "documents": _safe_count(lambda: CaseDocument.query.filter_by(org_id=oid).count())
        if _has("case_document")
        else 0,
        "publications": _safe_count(lambda: Publication.query.filter_by(org_id=oid).count())
        if _has("publication")
        else 0,
        "tickets": _safe_count(
            lambda: TasaMantenimientoTicket.query.filter_by(org_id=oid).count()
        )
        if _has("tasa_mantenimiento_ticket")
        else 0,
        "invoices": _safe_count(lambda: Invoice.query.filter_by(org_id=oid).count())
        if _has("invoice")
        else 0,
        "payments": _safe_count(lambda: Payment.query.filter_by(org_id=oid).count())
        if _has("payment")
        else 0,
        "lapida_stock": _safe_count(lambda: LapidaStock.query.filter_by(org_id=oid).count())
        if _has("lapida_stock")
        else 0,
        "lapida_movements": _safe_count(
            lambda: LapidaStockMovimiento.query.filter_by(org_id=oid).count()
        )
        if _has("lapida_stock_movimiento")
        else 0,
        "inscripciones": _safe_count(
            lambda: InscripcionLateral.query.filter_by(org_id=oid).count()
        )
        if _has("inscripcion_lateral")
        else 0,
        "activity_logs": _safe_count(lambda: ActivityLog.query.filter_by(org_id=oid).count())
        if _has("activity_log")
        else 0,
        "report_schedules": _safe_count(
            lambda: ReportSchedule.query.filter_by(org_id=oid).count()
        )
        if _has("report_schedule")
        else 0,
        "report_deliveries": _safe_count(
            lambda: ReportDeliveryLog.query.filter_by(org_id=oid).count()
        )
        if _has("report_delivery_log")
        else 0,
    }


def _purge_org_operational_data() -> None:
    oid = org_id()
    inspector = inspect(db.session.get_bind())
    existing_tables = set(inspector.get_table_names())

    def _has(*tables: str) -> bool:
        return all(table in existing_tables for table in tables)

    for storage_root in _demo_storage_roots(oid):
        if storage_root.exists():
            shutil.rmtree(storage_root, ignore_errors=True)

    if _has("report_delivery_log"):
        db.session.query(ReportDeliveryLog).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("report_schedule"):
        db.session.query(ReportSchedule).filter_by(org_id=oid).delete(
            synchronize_session=False
        )

    operation_ids: list[int] = []
    if _has("operation_case"):
        operation_ids = [
            row[0]
            for row in db.session.query(OperationCase.id)
            .filter_by(org_id=oid)
            .all()
        ]
    if operation_ids:
        if _has("operation_status_log"):
            db.session.query(OperationStatusLog).filter(
                OperationStatusLog.operation_case_id.in_(operation_ids)
            ).delete(synchronize_session=False)
        if _has("operation_document"):
            db.session.query(OperationDocument).filter(
                OperationDocument.operation_case_id.in_(operation_ids)
            ).delete(synchronize_session=False)
        if _has("operation_permit"):
            db.session.query(OperationPermit).filter(
                OperationPermit.operation_case_id.in_(operation_ids)
            ).delete(synchronize_session=False)
        db.session.query(OperationCase).filter(
            OperationCase.id.in_(operation_ids)
        ).delete(synchronize_session=False)

    if _has("orden_trabajo"):
        db.session.query(OrdenTrabajo).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("expediente"):
        db.session.query(Expediente).filter_by(org_id=oid).delete(
            synchronize_session=False
        )

    if _has("contract_event"):
        db.session.query(ContractEvent).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("activity_log"):
        db.session.query(ActivityLog).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("publication"):
        db.session.query(Publication).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("case_document"):
        db.session.query(CaseDocument).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("ownership_transfer_party"):
        db.session.query(OwnershipTransferParty).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("ownership_transfer_case"):
        db.session.query(OwnershipTransferCase).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("lapida_stock_movimiento"):
        db.session.query(LapidaStockMovimiento).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("inscripcion_lateral"):
        db.session.query(InscripcionLateral).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    work_order_ids = [
        row[0]
        for row in db.session.query(WorkOrder.id).filter_by(org_id=oid).all()
    ] if _has("work_order") else []
    if work_order_ids:
        if _has("work_order_status_log"):
            db.session.query(WorkOrderStatusLog).filter(
                WorkOrderStatusLog.work_order_id.in_(work_order_ids)
            ).delete(synchronize_session=False)
        if _has("work_order_checklist_item"):
            db.session.query(WorkOrderChecklistItem).filter(
                WorkOrderChecklistItem.work_order_id.in_(work_order_ids)
            ).delete(synchronize_session=False)
        if _has("work_order_evidence"):
            db.session.query(WorkOrderEvidence).filter(
                WorkOrderEvidence.work_order_id.in_(work_order_ids)
            ).delete(synchronize_session=False)
        if _has("work_order_dependency"):
            db.session.query(WorkOrderDependency).filter(
                or_(
                    WorkOrderDependency.work_order_id.in_(work_order_ids),
                    WorkOrderDependency.depends_on_work_order_id.in_(work_order_ids),
                )
            ).delete(synchronize_session=False)

    if _has("work_order_event_log"):
        db.session.query(WorkOrderEventLog).filter_by(org_id=oid).delete(synchronize_session=False)
    if _has("work_order_event_rule"):
        db.session.query(WorkOrderEventRule).filter_by(org_id=oid).delete(synchronize_session=False)
    template_ids = [
        row[0]
        for row in db.session.query(WorkOrderTemplate.id)
        .filter_by(org_id=oid)
        .all()
    ] if _has("work_order_template") else []
    if template_ids and _has("work_order_template_checklist_item"):
        db.session.query(WorkOrderTemplateChecklistItem).filter(
            WorkOrderTemplateChecklistItem.template_id.in_(template_ids)
        ).delete(synchronize_session=False)
    if _has("work_order_template"):
        db.session.query(WorkOrderTemplate).filter_by(org_id=oid).delete(synchronize_session=False)
    if _has("work_order_type"):
        db.session.query(WorkOrderType).filter_by(org_id=oid).delete(synchronize_session=False)
    if _has("work_order"):
        db.session.query(WorkOrder).filter_by(org_id=oid).delete(synchronize_session=False)
    if _has("payment"):
        db.session.query(Payment).filter_by(org_id=oid).delete(synchronize_session=False)
    if _has("tasa_mantenimiento_ticket"):
        db.session.query(TasaMantenimientoTicket).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("invoice"):
        db.session.query(Invoice).filter_by(org_id=oid).delete(synchronize_session=False)
    if _has("beneficiario"):
        db.session.query(Beneficiario).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("ownership_record"):
        db.session.query(OwnershipRecord).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("derecho_funerario_contrato"):
        db.session.query(DerechoFunerarioContrato).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("sepultura_ubicacion"):
        db.session.query(SepulturaUbicacion).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("sepultura_difunto"):
        db.session.query(SepulturaDifunto).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("movimiento_sepultura"):
        db.session.query(MovimientoSepultura).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("lapida_stock"):
        db.session.query(LapidaStock).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("sepultura"):
        db.session.query(Sepultura).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("person"):
        db.session.query(Person).filter_by(org_id=oid).delete(
            synchronize_session=False
        )
    if _has("operation_status_log", "operation_case"):
        db.session.execute(
            text(
                "DELETE FROM operation_status_log "
                "WHERE operation_case_id IN (SELECT id FROM operation_case WHERE org_id = :oid)"
            ),
            {"oid": oid},
        )
    if _has("operation_document", "operation_case"):
        db.session.execute(
            text(
                "DELETE FROM operation_document "
                "WHERE operation_case_id IN (SELECT id FROM operation_case WHERE org_id = :oid)"
            ),
            {"oid": oid},
        )
    if _has("operation_permit", "operation_case"):
        db.session.execute(
            text(
                "DELETE FROM operation_permit "
                "WHERE operation_case_id IN (SELECT id FROM operation_case WHERE org_id = :oid)"
            ),
            {"oid": oid},
        )
    if _has("operation_case"):
        db.session.execute(
            text("DELETE FROM operation_case WHERE org_id = :oid"),
            {"oid": oid},
        )
    db.session.commit()


def _ensure_demo_cemetery(oid: int) -> Cemetery:
    cemetery = Cemetery.query.filter_by(org_id=oid).order_by(Cemetery.id.asc()).first()
    if cemetery:
        return cemetery
    cemetery = Cemetery(
        org_id=oid,
        name="Cementeri Demo",
        location="Terrassa",
        municipality="Terrassa",
    )
    db.session.add(cemetery)
    db.session.flush()
    return cemetery


def reset_demo_org_data_to_zero() -> dict[str, int]:
    _purge_org_operational_data()
    oid = org_id()
    _ensure_demo_cemetery(oid)
    db.session.commit()
    return _demo_operational_counts(oid)


def _demo_case_document_status(
    case_status: OwnershipTransferStatus,
    required: bool,
    case_type: OwnershipTransferType,
    doc_type: str,
    case_index: int,
) -> CaseDocumentStatus:
    if case_status in {
        OwnershipTransferStatus.APPROVED,
        OwnershipTransferStatus.CLOSED,
    }:
        if required:
            return CaseDocumentStatus.VERIFIED
        return (
            CaseDocumentStatus.PROVIDED
            if case_index % 2 == 0
            else CaseDocumentStatus.MISSING
        )
    if case_status == OwnershipTransferStatus.DRAFT:
        return CaseDocumentStatus.MISSING
    if case_status == OwnershipTransferStatus.DOCS_PENDING:
        if not required:
            return CaseDocumentStatus.MISSING
        if (case_index + len(doc_type)) % 2 == 0:
            return CaseDocumentStatus.PROVIDED
        return CaseDocumentStatus.MISSING
    if case_status == OwnershipTransferStatus.UNDER_REVIEW:
        if not required:
            return (
                CaseDocumentStatus.PROVIDED
                if case_index % 3 == 0
                else CaseDocumentStatus.MISSING
            )
        if (
            case_type == OwnershipTransferType.INTER_VIVOS
            and doc_type == "ACREDITACION_PARENTESCO_2_GRADO"
        ):
            return (
                CaseDocumentStatus.VERIFIED
                if case_index % 2 == 0
                else CaseDocumentStatus.PROVIDED
            )
        return (
            CaseDocumentStatus.VERIFIED
            if (case_index + len(doc_type)) % 3 == 0
            else CaseDocumentStatus.PROVIDED
        )
    if case_status == OwnershipTransferStatus.REJECTED:
        if required:
            return (
                CaseDocumentStatus.REJECTED
                if (case_index + len(doc_type)) % 2 == 0
                else CaseDocumentStatus.PROVIDED
            )
        return CaseDocumentStatus.MISSING
    return CaseDocumentStatus.MISSING


DEMO_DNI_LETTERS = "TRWAGMYFPDXBNJZSQVHLCKE"
DEMO_CITIES = (
    "Terrassa",
    "Terrassa",
    "Terrassa",
    "Terrassa",
    "Terrassa",
    "Terrassa",
    "Terrassa",
    "Terrassa",
    "Sabadell",
    "Rubi",
    "Castellar del Valles",
    "Matadepera",
)
DEMO_STREETS = (
    "Carrer de la Rasa",
    "Carrer de Sant Pere",
    "Passeig del Vint-i-dos de Juliol",
    "Carrer de Topete",
    "Avinguda de Barcelona",
    "Carrer de Volta",
    "Carrer de Galileu",
    "Carrer de la Font Vella",
    "Carrer de Colom",
    "Carrer de Baldrich",
)
DEMO_OPERATION_PERMITS: dict[OperationType, tuple[str, ...]] = {
    OperationType.INHUMACION: ("LICENCIA_ENTERRAMIENTO", "PERMISO_SANITARIO"),
    OperationType.EXHUMACION: ("AUTORIZACION_EXHUMACION", "PERMISO_SANITARIO"),
    OperationType.TRASLADO_CORTO: ("AUTORIZACION_TRASLADO", "PERMISO_SANITARIO"),
    OperationType.TRASLADO_LARGO: ("AUTORIZACION_TRASLADO", "PERMISO_SANITARIO"),
    OperationType.RESCATE: ("AUTORIZACION_RETIRO_RESTOS", "PERMISO_SANITARIO"),
}


def _demo_spanish_dni(seed: int) -> str:
    numeric = 10_000_000 + (seed % 80_000_000)
    letter = DEMO_DNI_LETTERS[numeric % 23]
    return f"{numeric:08d}{letter}"


def _demo_phone(seed: int, mobile: bool = True) -> str:
    prefix = "6" if mobile else "9"
    return f"{prefix}{(10_000_000 + seed) % 100_000_000:08d}"


def _demo_person_address(seed: int) -> dict[str, str]:
    city = DEMO_CITIES[(seed - 1) % len(DEMO_CITIES)]
    street = DEMO_STREETS[(seed - 1) % len(DEMO_STREETS)]
    number = ((seed * 7) % 120) + 1
    cp_base = 8200 if city == "Terrassa" else 8100
    postal_code = f"{cp_base + ((seed - 1) % 30):05d}"
    line = f"{street}, {number}"
    return {
        "line": line,
        "postal_code": postal_code,
        "city": city,
        "province": "Barcelona",
        "country": "ES",
        "legacy": f"{line}, {postal_code} {city}",
    }


def _seed_demo_work_order_catalog(oid: int) -> dict[str, WorkOrderType]:
    specs = [
        (
            "INHUMACION",
            "Inhumacion",
            WorkOrderCategory.FUNERARIA,
            True,
            24,
        ),
        (
            "EXHUMACION",
            "Exhumacion",
            WorkOrderCategory.FUNERARIA,
            True,
            24,
        ),
        (
            "TRASLADO_CORTO",
            "Traslado corto",
            WorkOrderCategory.FUNERARIA,
            False,
            36,
        ),
        (
            "TRASLADO_LARGO",
            "Traslado largo",
            WorkOrderCategory.FUNERARIA,
            False,
            48,
        ),
        (
            "RESCATE",
            "Retirada de restos",
            WorkOrderCategory.FUNERARIA,
            True,
            36,
        ),
        (
            "DOCUMENTACION",
            "Documentacion",
            WorkOrderCategory.ADMINISTRATIVA,
            False,
            72,
        ),
        (
            "MANTENIMIENTO",
            "Mantenimiento",
            WorkOrderCategory.MANTENIMIENTO,
            False,
            96,
        ),
    ]
    rows: dict[str, WorkOrderType] = {}
    for code, name, category, critical, _sla in specs:
        row = WorkOrderType(
            org_id=oid,
            code=code,
            name=name,
            category=category,
            is_critical=critical,
            active=True,
        )
        db.session.add(row)
        rows[code] = row
    db.session.flush()
    for idx, (code, name, category, _critical, sla_hours) in enumerate(specs, start=1):
        db.session.add(
            WorkOrderTemplate(
                org_id=oid,
                code=f"TPL-{code}",
                name=f"Plantilla {name}",
                type_id=rows[code].id,
                default_priority=(
                    WorkOrderPriority.ALTA
                    if category == WorkOrderCategory.FUNERARIA
                    else WorkOrderPriority.MEDIA
                ),
                sla_hours=sla_hours,
                auto_create=False,
                requires_sepultura=True,
                allows_area=True,
                active=True,
                created_at=datetime(2026, 1, min(idx, 28), tzinfo=timezone.utc),
            )
        )
    return rows


def load_demo_org_initial_dataset(user_id: int | None = None) -> dict[str, int]:
    _purge_org_operational_data()
    oid = org_id()
    cemetery = _ensure_demo_cemetery(oid)
    wo_types = _seed_demo_work_order_catalog(oid)

    org_users = (
        User.query.join(Membership, Membership.user_id == User.id)
        .filter(Membership.org_id == oid)
        .order_by(User.id.asc())
        .all()
    )
    user_cycle = [row.id for row in org_users if row.id]
    default_operator_id = user_cycle[0] if user_cycle else user_id
    secondary_operator_id = (
        user_cycle[1] if len(user_cycle) > 1 else default_operator_id
    )

    holder_names = generate_demo_names(300, offset=0)
    extra_names = generate_demo_names(180, offset=97)

    holders: list[Person] = []
    extras: list[Person] = []
    for idx in range(1, 301):
        first_name, last_name = holder_names[idx - 1]
        if is_generic_demo_name(first_name, last_name):
            raise ValueError(f"Invalid generic holder name generated: {first_name} {last_name}")
        address = _demo_person_address(idx)
        holders.append(
            Person(
                org_id=oid,
                first_name=first_name,
                last_name=last_name,
                dni_nif=_demo_spanish_dni(idx),
                telefono=_demo_phone(idx, mobile=True),
                telefono2=_demo_phone(8000 + idx, mobile=True),
                email=f"titular{idx:03d}@terrassa.demo",
                email2=f"familia{idx:03d}@mail.demo" if idx % 4 == 0 else "",
                direccion=address["legacy"],
                direccion_linea=address["line"],
                codigo_postal=address["postal_code"],
                poblacion=address["city"],
                provincia=address["province"],
                pais=address["country"],
                notas=(
                    "Titular con expediente pendiente"
                    if idx <= 80
                    else "Titular demo"
                ),
            )
        )
    for idx in range(1, 181):
        first_name, last_name = extra_names[idx - 1]
        if is_generic_demo_name(first_name, last_name):
            raise ValueError(f"Invalid generic related-person name generated: {first_name} {last_name}")
        address = _demo_person_address(300 + idx)
        extras.append(
            Person(
                org_id=oid,
                first_name=first_name,
                last_name=last_name,
                dni_nif=_demo_spanish_dni(4000 + idx),
                telefono=_demo_phone(12000 + idx, mobile=True),
                telefono2=_demo_phone(22000 + idx, mobile=False),
                email=f"persona{idx:03d}@mail.demo",
                email2=f"alterno{idx:03d}@mail.demo" if idx % 5 == 0 else "",
                direccion=address["legacy"],
                direccion_linea=address["line"],
                codigo_postal=address["postal_code"],
                poblacion=address["city"],
                provincia=address["province"],
                pais=address["country"],
                notas=(
                    "Difunto con casuistica pendiente"
                    if idx <= 90
                    else "Persona relacionada con tramites"
                ),
            )
        )
    db.session.add_all([*holders, *extras])
    db.session.flush()

    modalidades = ("Ninxol", "Columbario", "Panteon", "Fosa")
    tipos_bloque = {
        "Ninxol": "Ninxols",
        "Columbario": "Columbaris",
        "Panteon": "Panteons",
        "Fosa": "Fosses",
    }
    tipo_lapidas = ("Resina", "Marmol", "Granito", "Sin lapida")
    orientaciones = ("Nord", "Sud", "Est", "Oest")
    sepulturas: list[Sepultura] = []
    for idx in range(1, 351):
        block_index = ((idx - 1) // 25) + 1
        local_index = (idx - 1) % 25
        fila = (local_index // 5) + 1
        columna = (local_index % 5) + 1
        if idx <= 300:
            estado = SepulturaEstado.DISPONIBLE
        elif idx <= 320:
            estado = SepulturaEstado.LLIURE
        elif idx <= 335:
            estado = SepulturaEstado.DISPONIBLE
        elif idx <= 345:
            estado = SepulturaEstado.INACTIVA
        else:
            estado = SepulturaEstado.PROPIA

        modalidad = modalidades[(idx - 1) % len(modalidades)]
        if estado == SepulturaEstado.PROPIA:
            modalidad = "Fosa"

        sepulturas.append(
            Sepultura(
                org_id=oid,
                cemetery_id=cemetery.id,
                bloque=f"B-{block_index:02d}",
                fila=fila,
                columna=columna,
                via=f"V-{((block_index - 1) % 8) + 1}",
                numero=1000 + idx,
                modalidad=modalidad,
                estado=estado,
                tipo_bloque=tipos_bloque[modalidad],
                tipo_lapida=tipo_lapidas[(idx - 1) % len(tipo_lapidas)],
                orientacion=orientaciones[(idx - 1) % len(orientaciones)],
            )
        )
    db.session.add_all(sepulturas)
    db.session.flush()

    contracts: list[DerechoFunerarioContrato] = []
    for idx, sep in enumerate(sepulturas[:300], start=1):
        contract_type = (
            DerechoTipo.CONCESION if idx <= 240 else DerechoTipo.USO_INMEDIATO
        )
        start_year = 1998 + (idx % 22)
        fecha_inicio = date(start_year, ((idx - 1) % 12) + 1, ((idx - 1) % 28) + 1)
        duration_years = (
            30 + (idx % 20)
            if contract_type == DerechoTipo.CONCESION
            else 10 + (idx % 15)
        )
        contracts.append(
            DerechoFunerarioContrato(
                org_id=oid,
                sepultura_id=sep.id,
                tipo=contract_type,
                fecha_inicio=fecha_inicio,
                fecha_fin=_add_years(fecha_inicio, duration_years),
                annual_fee_amount=Decimal(35 + (idx % 25)).quantize(Decimal("0.01")),
                estado="ACTIVO",
            )
        )
    db.session.add_all(contracts)
    db.session.flush()

    ownership_records: list[OwnershipRecord] = []
    owner_person_by_contract_id: dict[int, int] = {}
    for idx, contract in enumerate(contracts, start=1):
        is_pensioner = idx <= 72
        is_provisional = idx <= 18
        record = OwnershipRecord(
            org_id=oid,
            contract_id=contract.id,
            person_id=holders[idx - 1].id,
            start_date=contract.fecha_inicio,
            is_pensioner=is_pensioner,
            pensioner_since_date=date(2024, 1, 1) if is_pensioner else None,
            is_provisional=is_provisional,
            provisional_until=date(2036, 1, 1) if is_provisional else None,
            notes="Titularidad demo",
        )
        ownership_records.append(record)
        owner_person_by_contract_id[contract.id] = record.person_id
    db.session.add_all(ownership_records)

    active_beneficiaries: list[Beneficiario] = []
    active_beneficiary_person_by_contract_id: dict[int, int] = {}
    for idx, contract in enumerate(contracts[:180], start=1):
        beneficiary = Beneficiario(
            org_id=oid,
            contrato_id=contract.id,
            person_id=extras[idx - 1].id,
            activo_desde=_add_years(contract.fecha_inicio, 1),
        )
        active_beneficiaries.append(beneficiary)
        active_beneficiary_person_by_contract_id[contract.id] = beneficiary.person_id
    db.session.add_all(active_beneficiaries)

    historical_beneficiaries: list[Beneficiario] = []
    for idx, contract in enumerate(contracts[180:210], start=1):
        started_at = date(2016 + (idx % 5), ((idx - 1) % 12) + 1, ((idx - 1) % 28) + 1)
        historical_beneficiaries.append(
            Beneficiario(
                org_id=oid,
                contrato_id=contract.id,
                person_id=extras[(idx + 89) % len(extras)].id,
                activo_desde=started_at,
                activo_hasta=_add_years(started_at, 3),
            )
        )
    db.session.add_all(historical_beneficiaries)

    remains: list[SepulturaDifunto] = []
    for idx, sep in enumerate(sepulturas[:180], start=1):
        remains.append(
            SepulturaDifunto(
                org_id=oid,
                sepultura_id=sep.id,
                person_id=extras[(idx - 1) % len(extras)].id,
                notes=f"Restos previos demo {idx:03d}",
            )
        )
    db.session.add_all(remains)

    work_order_states = (
        [WorkOrderStatus.BORRADOR] * 20
        + [WorkOrderStatus.PENDIENTE_PLANIFICACION] * 70
        + [WorkOrderStatus.PLANIFICADA] * 40
        + [WorkOrderStatus.ASIGNADA] * 45
        + [WorkOrderStatus.EN_CURSO] * 30
        + [WorkOrderStatus.EN_VALIDACION] * 10
        + [WorkOrderStatus.COMPLETADA] * 35
    )
    type_codes = [
        "INHUMACION",
        "EXHUMACION",
        "TRASLADO_CORTO",
        "TRASLADO_LARGO",
        "RESCATE",
        "DOCUMENTACION",
        "MANTENIMIENTO",
    ]
    work_orders: list[WorkOrder] = []
    for idx in range(1, 251):
        status = work_order_states[idx - 1]
        created_at = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=idx)
        type_code = type_codes[(idx - 1) % len(type_codes)]
        category = (
            wo_types[type_code].category
            if type_code in wo_types
            else WorkOrderCategory.FUNERARIA
        )
        due_hours = 48
        if type_code in {"INHUMACION", "EXHUMACION"}:
            due_hours = 24
        elif type_code in {"TRASLADO_CORTO", "RESCATE"}:
            due_hours = 36
        elif type_code == "TRASLADO_LARGO":
            due_hours = 48
        elif type_code == "DOCUMENTACION":
            due_hours = 72
        elif type_code == "MANTENIMIENTO":
            due_hours = 96
        due_at = created_at + timedelta(hours=due_hours)
        started_at = (
            created_at + timedelta(hours=8)
            if status
            in {
                WorkOrderStatus.EN_CURSO,
                WorkOrderStatus.EN_VALIDACION,
                WorkOrderStatus.COMPLETADA,
            }
            else None
        )
        completed_at = (
            started_at + timedelta(hours=16)
            if started_at and status == WorkOrderStatus.COMPLETADA
            else None
        )
        work_orders.append(
            WorkOrder(
                org_id=oid,
                code=f"OT-2026-{idx:06d}",
                title=f"OT DEMO {idx:04d}",
                description="Orden de trabajo demo",
                category=category,
                type_code=type_code,
                priority=WorkOrderPriority.MEDIA if idx % 4 else WorkOrderPriority.ALTA,
                status=status,
                sepultura_id=sepulturas[(idx - 1) % 300].id,
                area_type=None,
                area_code=None,
                location_text=None,
                assigned_user_id=(
                    default_operator_id
                    if idx % 3 == 0
                    else (secondary_operator_id if idx % 3 == 1 else None)
                ),
                planned_start_at=created_at + timedelta(hours=4),
                planned_end_at=created_at + timedelta(hours=20),
                due_at=due_at,
                started_at=started_at,
                completed_at=completed_at,
                cancelled_at=None,
                block_reason="",
                cancel_reason="",
                close_notes="",
                created_by_user_id=default_operator_id,
                updated_by_user_id=default_operator_id,
                created_at=created_at,
                updated_at=created_at,
            )
        )
    db.session.add_all(work_orders)
    db.session.flush()

    operation_status_cycle = (
        [OperationStatus.BORRADOR] * 24
        + [OperationStatus.DOCS_PENDIENTES] * 36
        + [OperationStatus.PROGRAMADA] * 24
        + [OperationStatus.EN_EJECUCION] * 20
        + [OperationStatus.EN_VALIDACION] * 18
        + [OperationStatus.CERRADA] * 16
        + [OperationStatus.CANCELADA] * 2
    )
    operation_types = (
        OperationType.INHUMACION,
        OperationType.EXHUMACION,
        OperationType.TRASLADO_CORTO,
        OperationType.TRASLADO_LARGO,
        OperationType.RESCATE,
    )
    operation_cases: list[OperationCase] = []
    for idx in range(1, 141):
        op_type = operation_types[(idx - 1) % len(operation_types)]
        status = operation_status_cycle[idx - 1]
        source_sep = sepulturas[(idx - 1) % 300]
        target_sep = sepulturas[(idx + 14) % 300] if op_type in {
            OperationType.TRASLADO_CORTO,
            OperationType.TRASLADO_LARGO,
        } else None
        created_at = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=idx)
        scheduled_at = created_at + timedelta(days=2) if status != OperationStatus.BORRADOR else None
        executed_at = (
            created_at + timedelta(days=4)
            if status
            in {
                OperationStatus.EN_EJECUCION,
                OperationStatus.EN_VALIDACION,
                OperationStatus.CERRADA,
            }
            else None
        )
        closed_at = (
            created_at + timedelta(days=8)
            if status == OperationStatus.CERRADA
            else None
        )
        operation_cases.append(
            OperationCase(
                org_id=oid,
                code=f"OP-2026-{idx:04d}",
                type=op_type,
                status=status,
                source_sepultura_id=source_sep.id,
                target_sepultura_id=target_sep.id if target_sep else None,
                deceased_person_id=extras[(idx * 2) % len(extras)].id,
                declarant_person_id=holders[(idx * 3) % len(holders)].id,
                scheduled_at=scheduled_at,
                executed_at=executed_at,
                closed_at=closed_at,
                destination_cemetery_id=None,
                destination_name=(
                    ""
                    if op_type != OperationType.TRASLADO_LARGO
                    else "Cementerio municipal destino"
                ),
                destination_municipality=(
                    "Terrassa"
                    if op_type != OperationType.TRASLADO_LARGO
                    else ("Sabadell" if idx % 5 else "Barcelona")
                ),
                destination_region="Catalunya",
                destination_country="ES" if idx % 12 else "FR",
                cross_border=bool(op_type == OperationType.TRASLADO_LARGO and idx % 12 == 0),
                notes=(
                    "Inhumacion pendiente de documentacion"
                    if op_type == OperationType.INHUMACION
                    and status in {OperationStatus.BORRADOR, OperationStatus.DOCS_PENDIENTES}
                    else "Operacion demo"
                ),
                created_by_user_id=default_operator_id,
                managed_by_user_id=secondary_operator_id,
                created_at=created_at,
            )
        )
    db.session.add_all(operation_cases)
    db.session.flush()

    operation_permits: list[OperationPermit] = []
    operation_documents: list[OperationDocument] = []
    operation_logs: list[OperationStatusLog] = []
    for idx, case in enumerate(operation_cases, start=1):
        for permit_type in DEMO_OPERATION_PERMITS[case.type]:
            permit_status = OperationPermitStatus.MISSING
            if case.status in {
                OperationStatus.PROGRAMADA,
                OperationStatus.EN_EJECUCION,
                OperationStatus.EN_VALIDACION,
                OperationStatus.CERRADA,
            }:
                permit_status = OperationPermitStatus.VERIFIED
            elif case.status == OperationStatus.DOCS_PENDIENTES:
                permit_status = (
                    OperationPermitStatus.PROVIDED
                    if idx % 2 == 0
                    else OperationPermitStatus.MISSING
                )
            elif case.status == OperationStatus.CANCELADA:
                permit_status = OperationPermitStatus.REJECTED
            operation_permits.append(
                OperationPermit(
                    operation_case_id=case.id,
                    permit_type=permit_type,
                    required=True,
                    status=permit_status,
                    reference_number=f"PERM-{case.code}-{permit_type[:4]}",
                    issued_at=(
                        case.created_at + timedelta(days=1)
                        if permit_status != OperationPermitStatus.MISSING
                        else None
                    ),
                    verified_at=(
                        case.created_at + timedelta(days=2)
                        if permit_status == OperationPermitStatus.VERIFIED
                        else None
                    ),
                    verified_by_user_id=default_operator_id
                    if permit_status == OperationPermitStatus.VERIFIED
                    else None,
                    notes="Permiso demo",
                )
            )

        acta_status = (
            OperationPermitStatus.VERIFIED
            if case.status == OperationStatus.CERRADA
            else (
                OperationPermitStatus.PROVIDED
                if case.status
                in {
                    OperationStatus.EN_EJECUCION,
                    OperationStatus.EN_VALIDACION,
                }
                else OperationPermitStatus.MISSING
            )
        )
        operation_documents.append(
            OperationDocument(
                operation_case_id=case.id,
                doc_type="ACTA_OPERACION",
                file_path="",
                required=True,
                status=acta_status,
                uploaded_at=(
                    case.created_at + timedelta(days=4)
                    if acta_status != OperationPermitStatus.MISSING
                    else None
                ),
                verified_at=(
                    case.created_at + timedelta(days=6)
                    if acta_status == OperationPermitStatus.VERIFIED
                    else None
                ),
                verified_by_user_id=default_operator_id
                if acta_status == OperationPermitStatus.VERIFIED
                else None,
                notes="Acta demo",
            )
        )
        operation_documents.append(
            OperationDocument(
                operation_case_id=case.id,
                doc_type="OTROS",
                file_path="",
                required=False,
                status=(
                    OperationPermitStatus.PROVIDED
                    if idx % 3 == 0
                    else OperationPermitStatus.MISSING
                ),
                uploaded_at=case.created_at + timedelta(days=2) if idx % 3 == 0 else None,
                verified_at=None,
                verified_by_user_id=None,
                notes="Documento adicional demo",
            )
        )

        operation_logs.append(
            OperationStatusLog(
                operation_case_id=case.id,
                from_status="",
                to_status=OperationStatus.BORRADOR.value,
                changed_at=case.created_at,
                changed_by_user_id=default_operator_id,
                reason="Alta demo",
            )
        )
        if case.status != OperationStatus.BORRADOR:
            operation_logs.append(
                OperationStatusLog(
                    operation_case_id=case.id,
                    from_status=OperationStatus.BORRADOR.value,
                    to_status=case.status.value,
                    changed_at=case.created_at + timedelta(days=1),
                    changed_by_user_id=secondary_operator_id,
                    reason="Evolucion demo",
                )
            )

    db.session.add_all(operation_permits)
    db.session.add_all(operation_documents)
    db.session.add_all(operation_logs)

    for idx, case in enumerate(operation_cases[:140], start=1):
        work_order = work_orders[(idx - 1) % len(work_orders)]
        work_order.operation_case_id = case.id
        if case.status == OperationStatus.CERRADA:
            work_order.status = WorkOrderStatus.COMPLETADA
            work_order.completed_at = case.closed_at or (
                case.created_at + timedelta(days=7)
            )
        elif case.status == OperationStatus.EN_VALIDACION:
            work_order.status = WorkOrderStatus.EN_VALIDACION
        elif case.status == OperationStatus.EN_EJECUCION:
            work_order.status = WorkOrderStatus.EN_CURSO
        elif case.status == OperationStatus.PROGRAMADA:
            work_order.status = WorkOrderStatus.PLANIFICADA

    case_types = (
        OwnershipTransferType.MORTIS_CAUSA_TESTAMENTO,
        OwnershipTransferType.MORTIS_CAUSA_SIN_TESTAMENTO,
        OwnershipTransferType.MORTIS_CAUSA_CON_BENEFICIARIO,
        OwnershipTransferType.INTER_VIVOS,
        OwnershipTransferType.PROVISIONAL,
    )
    case_status_cycle = (
        OwnershipTransferStatus.DRAFT,
        OwnershipTransferStatus.DOCS_PENDING,
        OwnershipTransferStatus.UNDER_REVIEW,
        OwnershipTransferStatus.APPROVED,
        OwnershipTransferStatus.REJECTED,
        OwnershipTransferStatus.CLOSED,
    )
    ownership_cases: list[OwnershipTransferCase] = []
    resolution_counter = 1
    for idx in range(1, 91):
        transfer_type = case_types[(idx - 1) % len(case_types)]
        status = case_status_cycle[(idx - 1) % len(case_status_cycle)]
        if transfer_type == OwnershipTransferType.MORTIS_CAUSA_CON_BENEFICIARIO:
            contract = contracts[(idx - 1) % 180]
        else:
            contract = contracts[(idx - 1) % 300]
        opened_at = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=idx)
        resolution_number = None
        if status in {OwnershipTransferStatus.APPROVED, OwnershipTransferStatus.CLOSED}:
            resolution_number = f"RES-2026-{resolution_counter:04d}"
            resolution_counter += 1
        case = OwnershipTransferCase(
            org_id=oid,
            case_number=f"TR-2026-{idx:04d}",
            contract_id=contract.id,
            type=transfer_type,
            status=status,
            opened_at=opened_at,
            closed_at=(
                opened_at + timedelta(days=21)
                if status == OwnershipTransferStatus.CLOSED
                else None
            ),
            created_by_user_id=user_id,
            assigned_to_user_id=user_id if user_id and idx % 4 == 0 else None,
            resolution_number=resolution_number,
            rejection_reason=(
                f"Falta documentacion demo {idx:03d}"
                if status == OwnershipTransferStatus.REJECTED
                else None
            ),
            notes=f"Caso demo {idx:03d}",
            internal_notes="Dataset de demostracion",
        )
        if transfer_type == OwnershipTransferType.PROVISIONAL:
            provisional_start = date(2025, ((idx - 1) % 12) + 1, 1)
            case.provisional_start_date = provisional_start
            case.provisional_until = _add_years(provisional_start, 10)
        ownership_cases.append(case)
    db.session.add_all(ownership_cases)
    db.session.flush()

    case_parties: list[OwnershipTransferParty] = []
    for idx, case in enumerate(ownership_cases, start=1):
        previous_holder_person_id = owner_person_by_contract_id.get(case.contract_id)
        if previous_holder_person_id:
            case_parties.append(
                OwnershipTransferParty(
                    org_id=oid,
                    case_id=case.id,
                    role=OwnershipPartyRole.ANTERIOR_TITULAR,
                    person_id=previous_holder_person_id,
                )
            )
        if case.type == OwnershipTransferType.MORTIS_CAUSA_CON_BENEFICIARIO:
            new_holder_person_id = active_beneficiary_person_by_contract_id[
                case.contract_id
            ]
        else:
            new_holder_person_id = extras[(idx + 41) % len(extras)].id
        case_parties.append(
            OwnershipTransferParty(
                org_id=oid,
                case_id=case.id,
                role=OwnershipPartyRole.NUEVO_TITULAR,
                person_id=new_holder_person_id,
            )
        )
        if idx % 10 == 0:
            case_parties.append(
                OwnershipTransferParty(
                    org_id=oid,
                    case_id=case.id,
                    role=OwnershipPartyRole.REPRESENTANTE,
                    person_id=holders[(idx + 5) % len(holders)].id,
                )
            )
    db.session.add_all(case_parties)

    case_documents: list[CaseDocument] = []
    for idx, case in enumerate(ownership_cases, start=1):
        for doc_type, required in CASE_CHECKLIST[case.type]:
            doc_status = _demo_case_document_status(
                case.status, required, case.type, doc_type, idx
            )
            uploaded_at = None
            verified_at = None
            verified_by_user_id = None
            if doc_status in {
                CaseDocumentStatus.PROVIDED,
                CaseDocumentStatus.VERIFIED,
                CaseDocumentStatus.REJECTED,
            }:
                uploaded_at = case.opened_at + timedelta(days=1)
            if doc_status == CaseDocumentStatus.VERIFIED:
                verified_at = case.opened_at + timedelta(days=2)
                verified_by_user_id = user_id
            case_documents.append(
                CaseDocument(
                    org_id=oid,
                    case_id=case.id,
                    doc_type=doc_type,
                    required=required,
                    status=doc_status,
                    uploaded_at=uploaded_at,
                    verified_at=verified_at,
                    verified_by_user_id=verified_by_user_id,
                    notes="Documento demo",
                )
            )
    db.session.add_all(case_documents)

    publications: list[Publication] = []
    provisional_cases = [
        case
        for case in ownership_cases
        if case.type == OwnershipTransferType.PROVISIONAL
    ]
    for idx, case in enumerate(provisional_cases, start=1):
        published_date = date(2026, ((idx - 1) % 12) + 1, ((idx - 1) % 27) + 1)
        mode = idx % 3
        if mode in {0, 1}:
            publications.append(
                Publication(
                    org_id=oid,
                    case_id=case.id,
                    published_at=published_date,
                    channel="BOP",
                    reference_text=f"BOP-2026-{idx:04d}",
                    notes="Publicacion demo",
                )
            )
        if mode in {0, 2}:
            publications.append(
                Publication(
                    org_id=oid,
                    case_id=case.id,
                    published_at=published_date + timedelta(days=2),
                    channel="DIARIO",
                    reference_text=f"DIARIO-2026-{idx:04d}",
                    notes="Publicacion demo",
                )
            )
    db.session.add_all(publications)

    contract_by_id = {contract.id: contract for contract in contracts}
    contract_events: list[ContractEvent] = []
    movements: list[MovimientoSepultura] = []
    for idx, case in enumerate(ownership_cases, start=1):
        contract_events.append(
            ContractEvent(
                org_id=oid,
                contract_id=case.contract_id,
                case_id=case.id,
                event_type="INICIO_TRANSMISION",
                event_at=case.opened_at,
                details=f"Inicio de caso {case.case_number}",
                user_id=user_id,
            )
        )
        if case.status in {
            OwnershipTransferStatus.APPROVED,
            OwnershipTransferStatus.CLOSED,
        }:
            contract_events.append(
                ContractEvent(
                    org_id=oid,
                    contract_id=case.contract_id,
                    case_id=case.id,
                    event_type="APROBACION",
                    event_at=case.opened_at + timedelta(days=10),
                    details=f"Aprobado {case.case_number}",
                    user_id=user_id,
                )
            )
        if case.status == OwnershipTransferStatus.REJECTED:
            contract_events.append(
                ContractEvent(
                    org_id=oid,
                    contract_id=case.contract_id,
                    case_id=case.id,
                    event_type="RECHAZO",
                    event_at=case.opened_at + timedelta(days=7),
                    details=f"Rechazado {case.case_number}",
                    user_id=user_id,
                )
            )
        contract = contract_by_id.get(case.contract_id)
        if contract:
            movements.append(
                MovimientoSepultura(
                    org_id=oid,
                    sepultura_id=contract.sepultura_id,
                    tipo=MovimientoTipo.INICIO_TRANSMISION,
                    fecha=case.opened_at + timedelta(days=1),
                    detalle=f"Seguimiento {case.case_number}",
                    user_id=user_id,
                )
            )
    for idx, contract in enumerate(contracts[:30], start=1):
        movements.append(
            MovimientoSepultura(
                org_id=oid,
                sepultura_id=contract.sepultura_id,
                tipo=MovimientoTipo.BENEFICIARIO,
                fecha=datetime(2026, 6, 1, tzinfo=timezone.utc) + timedelta(days=idx),
                detalle=f"Revision beneficiario contrato {contract.id}",
                user_id=user_id,
            )
        )
    for idx, contract in enumerate(contracts[:20], start=1):
        movements.append(
            MovimientoSepultura(
                org_id=oid,
                sepultura_id=contract.sepultura_id,
                tipo=MovimientoTipo.PENSIONISTA,
                fecha=datetime(2026, 7, 1, tzinfo=timezone.utc) + timedelta(days=idx),
                detalle=f"Revision pensionista contrato {contract.id}",
                user_id=user_id,
            )
        )
    closed_cases = [
        case
        for case in ownership_cases
        if case.status == OwnershipTransferStatus.CLOSED
    ]
    for idx, case in enumerate(closed_cases[:20], start=1):
        contract = contract_by_id.get(case.contract_id)
        if not contract:
            continue
        movements.append(
            MovimientoSepultura(
                org_id=oid,
                sepultura_id=contract.sepultura_id,
                tipo=MovimientoTipo.CAMBIO_TITULARIDAD,
                fecha=case.opened_at + timedelta(days=22),
                detalle=f"Cierre titularidad {case.case_number}",
                user_id=user_id,
            )
        )
    operation_to_movement = {
        OperationType.INHUMACION: MovimientoTipo.INHUMACION,
        OperationType.EXHUMACION: MovimientoTipo.EXHUMACION,
        OperationType.TRASLADO_CORTO: MovimientoTipo.TRASLADO_CORTO,
        OperationType.TRASLADO_LARGO: MovimientoTipo.TRASLADO_LARGO,
        OperationType.RESCATE: MovimientoTipo.RESCATE,
    }
    for case in operation_cases:
        movement_type = operation_to_movement.get(case.type)
        if not movement_type:
            continue
        movement_date = (
            case.executed_at
            or case.scheduled_at
            or case.created_at
            or datetime.now(timezone.utc)
        )
        movements.append(
            MovimientoSepultura(
                org_id=oid,
                sepultura_id=case.source_sepultura_id,
                tipo=movement_type,
                fecha=movement_date,
                detalle=f"Operacion {case.code} en estado {case.status.value}",
                user_id=default_operator_id,
            )
        )
    db.session.add_all(contract_events)
    db.session.add_all(movements)

    ticket_years = (2020, 2021, 2022, 2023, 2024, 2025, 2026)
    discount_pct = Decimal("10.00")
    invoice_counter = 1
    receipt_counter = 1
    for contract_index, contract in enumerate(contracts[:170], start=1):
        holder = ownership_records[contract_index - 1]
        for year in ticket_years:
            amount = Decimal(contract.annual_fee_amount or Decimal("0.00")).quantize(
                Decimal("0.01")
            )
            discount_type = TicketDescuentoTipo.NONE
            if (
                holder.is_pensioner
                and holder.pensioner_since_date
                and year >= holder.pensioner_since_date.year
            ):
                amount = _apply_discount(amount, discount_pct)
                discount_type = TicketDescuentoTipo.PENSIONISTA

            state_bucket = (contract_index + year) % 3
            if state_bucket == 0:
                ticket_state = TicketEstado.PENDIENTE
            elif state_bucket == 1:
                ticket_state = TicketEstado.FACTURADO
            else:
                ticket_state = TicketEstado.COBRADO

            invoice_id = None
            if ticket_state in {TicketEstado.FACTURADO, TicketEstado.COBRADO}:
                invoice = Invoice(
                    org_id=oid,
                    contrato_id=contract.id,
                    sepultura_id=contract.sepultura_id,
                    numero=f"F-DEMO-2026-{invoice_counter:06d}",
                    estado=(
                        InvoiceEstado.IMPAGADA
                        if ticket_state == TicketEstado.FACTURADO
                        else InvoiceEstado.PAGADA
                    ),
                    total_amount=amount,
                    issued_at=datetime(
                        year, ((contract_index - 1) % 12) + 1, 15, tzinfo=timezone.utc
                    ),
                )
                db.session.add(invoice)
                db.session.flush()
                invoice_id = invoice.id
                invoice_counter += 1
                if ticket_state == TicketEstado.COBRADO:
                    db.session.add(
                        Payment(
                            org_id=oid,
                            invoice_id=invoice.id,
                            user_id=user_id,
                            amount=amount,
                            method="EFECTIVO",
                            receipt_number=f"R-DEMO-2026-{receipt_counter:06d}",
                            paid_at=invoice.issued_at + timedelta(days=5),
                        )
                    )
                    receipt_counter += 1

            db.session.add(
                TasaMantenimientoTicket(
                    org_id=oid,
                    contrato_id=contract.id,
                    invoice_id=invoice_id,
                    anio=year,
                    importe=amount,
                    descuento_tipo=discount_type,
                    estado=ticket_state,
                )
            )

    lapida_stocks: list[LapidaStock] = []
    for idx in range(1, 9):
        lapida_stocks.append(
            LapidaStock(
                org_id=oid,
                codigo=f"LAP-DEMO-{idx:02d}",
                descripcion=f"Modelo lapida demo {idx:02d}",
                estado="ACTIVO",
                available_qty=25 + idx,
            )
        )
    db.session.add_all(lapida_stocks)
    db.session.flush()

    lapida_movements: list[LapidaStockMovimiento] = []
    for idx in range(1, 31):
        stock = lapida_stocks[(idx - 1) % len(lapida_stocks)]
        quantity = (idx % 4) + 1
        stock.available_qty += quantity
        lapida_movements.append(
            LapidaStockMovimiento(
                org_id=oid,
                lapida_stock_id=stock.id,
                movimiento="ENTRADA",
                quantity=quantity,
                notes=f"Entrada demo {idx:03d}",
            )
        )
    for idx in range(1, 31):
        stock = lapida_stocks[(idx + 2) % len(lapida_stocks)]
        quantity = (idx % 3) + 1
        stock.available_qty -= quantity
        lapida_movements.append(
            LapidaStockMovimiento(
                org_id=oid,
                lapida_stock_id=stock.id,
                movimiento="SALIDA",
                quantity=quantity,
                sepultura_id=sepulturas[(idx * 3) % 300].id,
                notes=f"Salida demo {idx:03d}",
            )
        )
    db.session.add_all(lapida_movements)

    inscripcion_states = (
        ["PENDIENTE_GRABAR"] * 20
        + ["PENDIENTE_COLOCAR"] * 20
        + ["PENDIENTE_NOTIFICAR"] * 20
        + ["NOTIFICADA"] * 10
    )
    inscripciones: list[InscripcionLateral] = []
    for idx in range(1, 71):
        inscripciones.append(
            InscripcionLateral(
                org_id=oid,
                sepultura_id=sepulturas[(idx - 1) % 300].id,
                texto=f"Inscripcion demo {idx:03d}",
                estado=inscripcion_states[idx - 1],
            )
        )
    db.session.add_all(inscripciones)

    db.session.commit()
    return _demo_operational_counts(oid)


def _get_case_or_404(case_id: int) -> OwnershipTransferCase:
    case = (
        OwnershipTransferCase.query.options(
            joinedload(OwnershipTransferCase.contract).joinedload(
                DerechoFunerarioContrato.sepultura
            ),
            joinedload(OwnershipTransferCase.parties).joinedload(
                OwnershipTransferParty.person
            ),
            joinedload(OwnershipTransferCase.documents),
            joinedload(OwnershipTransferCase.publications),
            joinedload(OwnershipTransferCase.assigned_to),
        )
        .filter_by(org_id=org_id(), id=case_id)
        .first()
    )
    if not case:
        raise ValueError("Caso no encontrado")
    return case


def _case_party(
    case: OwnershipTransferCase, role: OwnershipPartyRole
) -> OwnershipTransferParty | None:
    return next((p for p in case.parties if p.role == role), None)


def _log_contract_event(
    contract_id: int,
    case_id: int | None,
    event_type: str,
    details: str,
    user_id: int | None,
) -> None:
    db.session.add(
        ContractEvent(
            org_id=org_id(),
            contract_id=contract_id,
            case_id=case_id,
            event_type=event_type,
            details=details,
            user_id=user_id,
        )
    )


def _log_activity_event(
    action_type: str,
    details: str,
    user_id: int | None,
    sepultura_id: int | None = None,
) -> None:
    db.session.add(
        ActivityLog(
            org_id=org_id(),
            sepultura_id=sepultura_id,
            action_type=str(action_type),
            details=(details or "").strip(),
            user_id=user_id,
        )
    )


def _log_case_movement(
    contract: DerechoFunerarioContrato,
    movement_type: MovimientoTipo,
    detail: str,
    user_id: int | None,
) -> None:
    _log_sepultura_movement(contract.sepultura_id, movement_type, detail, user_id)


def _next_transfer_number(prefix: str, year: int) -> str:
    value_prefix = f"{prefix}-{year}-"
    count = (
        db.session.query(func.count(OwnershipTransferCase.id))
        .filter(OwnershipTransferCase.org_id == org_id())
        .filter(OwnershipTransferCase.case_number.like(f"{value_prefix}%"))
        .scalar()
    )
    return f"{value_prefix}{count + 1:04d}"


def _next_resolution_number(year: int) -> str:
    value_prefix = f"RES-{year}-"
    count = (
        db.session.query(func.count(OwnershipTransferCase.id))
        .filter(OwnershipTransferCase.org_id == org_id())
        .filter(OwnershipTransferCase.resolution_number.like(f"{value_prefix}%"))
        .scalar()
    )
    return f"{value_prefix}{count + 1:04d}"


def _parse_transfer_type(value: str) -> OwnershipTransferType:
    raw = (value or "").strip().upper()
    try:
        return OwnershipTransferType[raw]
    except KeyError as exc:
        raise ValueError("Tipo de transmision invalido") from exc


def _parse_transfer_status(value: str) -> OwnershipTransferStatus:
    raw = (value or "").strip().upper()
    try:
        return OwnershipTransferStatus[raw]
    except KeyError as exc:
        raise ValueError("Estado de caso invalido") from exc


def _transition_case_status(
    case: OwnershipTransferCase, new_status: OwnershipTransferStatus
) -> None:
    allowed = CASE_STATUS_TRANSITIONS.get(case.status, set())
    if new_status not in allowed:
        raise ValueError(
            f"Transicion invalida: {case.status.value} -> {new_status.value}"
        )
    case.status = new_status
    db.session.add(case)


def _seed_case_documents(case: OwnershipTransferCase) -> None:
    checklist = CASE_CHECKLIST.get(case.type, [])
    for doc_type, required in checklist:
        db.session.add(
            CaseDocument(
                org_id=case.org_id,
                case_id=case.id,
                doc_type=doc_type,
                required=required,
                status=CaseDocumentStatus.MISSING,
            )
        )


def _case_storage_root(case: OwnershipTransferCase) -> Path:
    return (
        Path(current_app.instance_path)
        / "storage"
        / "cemetery"
        / "ownership_cases"
        / str(case.org_id)
        / str(case.id)
    )


def _ensure_resolution_pdf(case: OwnershipTransferCase) -> None:
    if not case.resolution_number:
        case.resolution_number = _next_resolution_number(
            datetime.now(timezone.utc).year
        )
    previous_holder = _case_party(case, OwnershipPartyRole.ANTERIOR_TITULAR)
    new_holder = _case_party(case, OwnershipPartyRole.NUEVO_TITULAR)
    contract = case.contract
    sepultura = contract.sepultura if contract else None

    lines = [
        "Resolucion de transmision de titularidad",
        f"Numero resolucion: {case.resolution_number}",
        f"Caso: {case.case_number}",
        f"Tipo: {case.type.value}",
        f"Fecha: {datetime.now(timezone.utc).date().isoformat()}",
        f"Contrato: {contract.id if contract else '-'}",
        f"Sepultura: {sepultura.location_label if sepultura else '-'}",
        f"Titular anterior: {previous_holder.person.full_name if previous_holder else '-'}",
        f"Nuevo titular: {new_holder.person.full_name if new_holder else '-'}",
    ]
    pdf = _simple_pdf(lines)
    root = _case_storage_root(case)
    root.mkdir(parents=True, exist_ok=True)
    filename = f"resolucion-{case.resolution_number}.pdf"
    absolute = root / filename
    absolute.write_bytes(pdf)
    case.resolution_pdf_path = absolute.relative_to(
        Path(current_app.instance_path)
    ).as_posix()
    db.session.add(case)


def list_ownership_cases(filters: dict[str, str]) -> list[OwnershipTransferCase]:
    query = (
        OwnershipTransferCase.query.options(
            joinedload(OwnershipTransferCase.contract).joinedload(
                DerechoFunerarioContrato.sepultura
            ),
            joinedload(OwnershipTransferCase.parties).joinedload(
                OwnershipTransferParty.person
            ),
            joinedload(OwnershipTransferCase.assigned_to),
        )
        .filter(OwnershipTransferCase.org_id == org_id())
        .order_by(
            OwnershipTransferCase.opened_at.desc(), OwnershipTransferCase.id.desc()
        )
    )
    type_raw = (filters.get("type") or "").strip().upper()
    if type_raw:
        try:
            query = query.filter(
                OwnershipTransferCase.type == OwnershipTransferType[type_raw]
            )
        except KeyError:
            return []
    status_raw = (filters.get("status") or "").strip().upper()
    if status_raw:
        try:
            query = query.filter(
                OwnershipTransferCase.status == OwnershipTransferStatus[status_raw]
            )
        except KeyError:
            return []
    contract_id = (filters.get("contract_id") or "").strip()
    if contract_id.isdigit():
        query = query.filter(OwnershipTransferCase.contract_id == int(contract_id))
    sepultura_id = (filters.get("sepultura_id") or "").strip()
    if sepultura_id.isdigit():
        query = query.join(
            DerechoFunerarioContrato,
            DerechoFunerarioContrato.id == OwnershipTransferCase.contract_id,
        )
        query = query.filter(DerechoFunerarioContrato.sepultura_id == int(sepultura_id))
    opened_from = (filters.get("opened_from") or "").strip()
    if opened_from:
        try:
            query = query.filter(
                OwnershipTransferCase.opened_at
                >= datetime.fromisoformat(f"{opened_from}T00:00:00")
            )
        except ValueError:
            return []
    opened_to = (filters.get("opened_to") or "").strip()
    if opened_to:
        try:
            query = query.filter(
                OwnershipTransferCase.opened_at
                <= datetime.fromisoformat(f"{opened_to}T23:59:59")
            )
        except ValueError:
            return []

    cases = query.all()
    name_filter = (filters.get("party_name") or "").strip().lower()
    if not name_filter:
        return cases
    rows: list[OwnershipTransferCase] = []
    for case in cases:
        names = " ".join([party.person.full_name.lower() for party in case.parties])
        if name_filter in names:
            rows.append(case)
    return rows


def create_ownership_case(
    payload: dict[str, str], user_id: int | None
) -> OwnershipTransferCase:
    raw_contract = str(payload.get("contract_id") or "").strip()
    if not raw_contract.isdigit():
        raise ValueError("Contrato invalido")
    contract_id = int(raw_contract)
    contract = contract_by_id(contract_id)

    transfer_type = _parse_transfer_type(payload.get("type", ""))
    beneficiary_for_new_holder = None
    if transfer_type == OwnershipTransferType.MORTIS_CAUSA_CON_BENEFICIARIO:
        beneficiary_for_new_holder = active_beneficiario_for_contract(contract.id)
        if not beneficiary_for_new_holder:
            raise ValueError(
                translate(
                    "validation.transfer.beneficiary_required_for_mortis_with_beneficiary"
                )
            )
    case = OwnershipTransferCase(
        org_id=org_id(),
        case_number=_next_transfer_number("TR", datetime.now(timezone.utc).year),
        contract_id=contract.id,
        type=transfer_type,
        status=OwnershipTransferStatus.DRAFT,
        created_by_user_id=user_id,
        assigned_to_user_id=(payload.get("assigned_to_user_id") or None),
        notes=(payload.get("notes") or "").strip(),
        internal_notes=(payload.get("internal_notes") or "").strip(),
    )
    if case.assigned_to_user_id and str(case.assigned_to_user_id).isdigit():
        case.assigned_to_user_id = int(case.assigned_to_user_id)
    else:
        case.assigned_to_user_id = None

    if transfer_type == OwnershipTransferType.PROVISIONAL:
        provisional_start = (
            _parse_optional_iso_date(payload.get("provisional_start_date"))
            or date.today()
        )
        case.provisional_start_date = provisional_start
        case.provisional_until = _add_years(provisional_start, 10)

    db.session.add(case)
    db.session.flush()
    _seed_case_documents(case)

    active_owner = active_titular_for_contract(contract.id)
    if active_owner:
        db.session.add(
            OwnershipTransferParty(
                org_id=org_id(),
                case_id=case.id,
                role=OwnershipPartyRole.ANTERIOR_TITULAR,
                person_id=active_owner.person_id,
            )
        )
    if transfer_type == OwnershipTransferType.MORTIS_CAUSA_CON_BENEFICIARIO:
        db.session.add(
            OwnershipTransferParty(
                org_id=org_id(),
                case_id=case.id,
                role=OwnershipPartyRole.NUEVO_TITULAR,
                person_id=beneficiary_for_new_holder.person_id,
            )
        )
    _log_case_movement(
        contract,
        MovimientoTipo.INICIO_TRANSMISION,
        f"Inicio de transmision {case.case_number}",
        user_id,
    )
    _log_contract_event(
        contract.id,
        case.id,
        "INICIO_TRANSMISION",
        f"Caso {case.case_number} creado",
        user_id,
    )
    db.session.commit()
    return case


def ownership_case_detail(case_id: int) -> dict[str, object]:
    case = _get_case_or_404(case_id)
    current_owner = active_titular_for_contract(case.contract_id)
    active_beneficiary = active_beneficiario_for_contract(case.contract_id)
    required_pending = [
        d
        for d in case.documents
        if d.required and d.status != CaseDocumentStatus.VERIFIED
    ]
    return {
        "case": case,
        "contract": case.contract,
        "sepultura": case.contract.sepultura,
        "current_owner": current_owner,
        "active_beneficiary": active_beneficiary,
        "required_pending": required_pending,
    }


def add_case_party(case_id: int, payload: dict[str, str]) -> OwnershipTransferParty:
    # Spec Cementiri: ver cementerio_extract.md (9.1.5)
    case = _get_case_or_404(case_id)
    role_raw = (payload.get("role") or "").strip().upper()
    try:
        role = OwnershipPartyRole[role_raw]
    except KeyError as exc:
        raise ValueError("Rol de parte invalido") from exc

    person_id_raw = (payload.get("person_id") or "").strip()
    if person_id_raw.isdigit():
        person = _person_by_org(int(person_id_raw))
    else:
        first_name = (payload.get("first_name") or payload.get("nombre") or "").strip()
        if not first_name:
            raise ValueError("Debes seleccionar o crear una persona")
        person = _create_or_reuse_person(
            first_name,
            payload.get("last_name") or payload.get("apellidos") or "",
            payload.get("dni_nif") or payload.get("document_id"),
        )

    if role != OwnershipPartyRole.OTRO:
        OwnershipTransferParty.query.filter_by(
            org_id=org_id(), case_id=case.id, role=role
        ).delete()

    percentage_raw = (payload.get("percentage") or "").strip().replace(",", ".")
    percentage = Decimal(percentage_raw) if percentage_raw else None
    party = OwnershipTransferParty(
        org_id=org_id(),
        case_id=case.id,
        role=role,
        person_id=person.id,
        percentage=percentage,
        notes=(payload.get("notes") or "").strip(),
    )
    db.session.add(party)
    db.session.commit()
    return party


def add_case_publication(case_id: int, payload: dict[str, str]) -> Publication:
    case = _get_case_or_404(case_id)
    if case.type != OwnershipTransferType.PROVISIONAL:
        raise ValueError("Solo los casos provisionales admiten publicaciones")
    published_at_raw = (payload.get("published_at") or "").strip()
    if not published_at_raw:
        raise ValueError("Fecha de publicacion obligatoria")
    publication = Publication(
        org_id=org_id(),
        case_id=case.id,
        published_at=date.fromisoformat(published_at_raw),
        channel=(payload.get("channel") or "").strip().upper(),
        reference_text=(payload.get("reference_text") or "").strip(),
        notes=(payload.get("notes") or "").strip(),
    )
    if not publication.channel:
        raise ValueError("Canal de publicacion obligatorio")
    db.session.add(publication)
    db.session.commit()
    return publication


def upload_case_document(
    case_id: int, doc_id: int, file_obj: FileStorage, user_id: int | None
) -> CaseDocument:
    case = _get_case_or_404(case_id)
    document = CaseDocument.query.filter_by(
        org_id=org_id(), case_id=case.id, id=doc_id
    ).first()
    if not document:
        raise ValueError("Documento no encontrado")
    if not file_obj or not file_obj.filename:
        raise ValueError("Debes seleccionar un fichero")

    filename = secure_filename(file_obj.filename) or f"document-{document.id}.bin"
    root = _case_storage_root(case) / "documents" / str(document.id)
    root.mkdir(parents=True, exist_ok=True)
    absolute = root / filename
    file_obj.save(absolute)
    document.file_path = absolute.relative_to(
        Path(current_app.instance_path)
    ).as_posix()
    document.uploaded_at = datetime.now(timezone.utc)
    document.status = CaseDocumentStatus.PROVIDED
    db.session.add(document)
    _log_case_movement(
        case.contract,
        MovimientoTipo.DOCUMENTO_SUBIDO,
        f"Documento {document.doc_type} subido",
        user_id,
    )
    _log_contract_event(
        case.contract_id,
        case.id,
        "DOCUMENTO_SUBIDO",
        f"{document.doc_type}: {document.file_path}",
        user_id,
    )
    db.session.commit()
    return document


def verify_case_document(
    case_id: int, doc_id: int, action: str, notes: str, user_id: int | None
) -> CaseDocument:
    case = _get_case_or_404(case_id)
    document = CaseDocument.query.filter_by(
        org_id=org_id(), case_id=case.id, id=doc_id
    ).first()
    if not document:
        raise ValueError("Documento no encontrado")
    normalized = (action or "").strip().lower()
    if normalized not in {"verify", "reject"}:
        raise ValueError("Accion invalida")
    if normalized == "verify":
        document.status = CaseDocumentStatus.VERIFIED
        document.verified_at = datetime.now(timezone.utc)
        document.verified_by_user_id = user_id
    else:
        document.status = CaseDocumentStatus.REJECTED
        document.verified_at = None
        document.verified_by_user_id = None
    if notes:
        document.notes = notes.strip()
    db.session.add(document)
    db.session.commit()
    return document


def ownership_case_document_download(case_id: int, doc_id: int) -> tuple[bytes, str]:
    case = _get_case_or_404(case_id)
    document = CaseDocument.query.filter_by(
        org_id=org_id(), case_id=case.id, id=doc_id
    ).first()
    if not document:
        raise ValueError("Documento no encontrado")
    if not document.file_path:
        raise ValueError("Documento sin fichero asociado")

    absolute = Path(current_app.instance_path) / document.file_path
    if not absolute.exists():
        raise ValueError("Fichero de documento no encontrado")
    return absolute.read_bytes(), absolute.name


def change_ownership_case_status(
    case_id: int, new_status_raw: str, user_id: int | None
) -> OwnershipTransferCase:
    case = _get_case_or_404(case_id)
    new_status = _parse_transfer_status(new_status_raw)
    _transition_case_status(case, new_status)
    db.session.commit()
    return case


def approve_ownership_case(case_id: int, user_id: int | None) -> OwnershipTransferCase:
    case = _get_case_or_404(case_id)
    _transition_case_status(case, OwnershipTransferStatus.APPROVED)
    _ensure_resolution_pdf(case)
    _log_case_movement(
        case.contract,
        MovimientoTipo.APROBACION,
        f"Caso {case.case_number} aprobado",
        user_id,
    )
    _log_contract_event(
        case.contract_id,
        case.id,
        "APROBACION",
        f"Caso {case.case_number} aprobado",
        user_id,
    )
    db.session.commit()
    emit_work_order_event(
        "OWNERSHIP_CASE_APPROVED",
        {
            "case_id": case.id,
            "case_number": case.case_number,
            "contract_id": case.contract_id,
            "sepultura_id": case.contract.sepultura_id if case.contract else None,
            "category": WorkOrderCategory.ADMINISTRATIVA.value,
            "title": f"Actualizar documental por caso {case.case_number}",
        },
        user_id=user_id,
    )
    return case


def reject_ownership_case(
    case_id: int, reason: str, user_id: int | None
) -> OwnershipTransferCase:
    case = _get_case_or_404(case_id)
    _transition_case_status(case, OwnershipTransferStatus.REJECTED)
    case.rejection_reason = (reason or "").strip()
    if not case.rejection_reason:
        raise ValueError("Motivo de rechazo obligatorio")
    _log_case_movement(
        case.contract,
        MovimientoTipo.RECHAZO,
        f"Caso {case.case_number} rechazado",
        user_id,
    )
    _log_contract_event(
        case.contract_id,
        case.id,
        "RECHAZO",
        f"Caso {case.case_number} rechazado: {case.rejection_reason}",
        user_id,
    )
    db.session.commit()
    return case


def _validate_case_ready_to_close(
    case: OwnershipTransferCase, payload: dict[str, str]
) -> None:
    if case.status != OwnershipTransferStatus.APPROVED:
        raise ValueError("Solo se pueden cerrar casos en estado APPROVED")
    pending_required = [
        d
        for d in case.documents
        if d.required and d.status != CaseDocumentStatus.VERIFIED
    ]
    if pending_required:
        raise ValueError("Faltan documentos obligatorios verificados")
    new_owner = _case_party(case, OwnershipPartyRole.NUEVO_TITULAR)
    if not new_owner:
        raise ValueError("Debes informar la parte NUEVO_TITULAR")
    if case.type == OwnershipTransferType.PROVISIONAL:
        has_bop = any((pub.channel or "").upper() == "BOP" for pub in case.publications)
        has_other = any(
            (pub.channel or "").upper() != "BOP" for pub in case.publications
        )
        if not (has_bop and has_other):
            raise ValueError(
                "El caso provisional requiere publicacion en BOP y en otro canal"
            )
    decision_raw = (payload.get("beneficiary_close_decision") or "").strip().upper()
    if decision_raw == BeneficiaryCloseDecision.REPLACE.value:
        for doc_type in BENEFICIARY_REPLACE_REQUIRED_DOC_TYPES:
            document = next(
                (doc for doc in case.documents if doc.doc_type == doc_type), None
            )
            if not document or document.status != CaseDocumentStatus.VERIFIED:
                raise ValueError(
                    translate("validation.transfer.beneficiary_replace_docs_missing")
                )
    if case.type == OwnershipTransferType.INTER_VIVOS:
        relation_doc = next(
            (
                doc
                for doc in case.documents
                if doc.doc_type == "ACREDITACION_PARENTESCO_2_GRADO"
            ),
            None,
        )
        if not relation_doc or relation_doc.status != CaseDocumentStatus.VERIFIED:
            raise ValueError(
                translate("validation.transfer.intervivos_requires_second_degree_doc")
            )


def close_ownership_case(
    case_id: int, payload: dict[str, str], user_id: int | None
) -> OwnershipTransferCase:
    # Spec Cementiri: ver cementerio_extract.md (9.1.5)
    case = _get_case_or_404(case_id)
    if case.type == OwnershipTransferType.MORTIS_CAUSA_CON_BENEFICIARIO:
        new_holder = _case_party(case, OwnershipPartyRole.NUEVO_TITULAR)
        if not new_holder:
            active_beneficiary = active_beneficiario_for_contract(case.contract_id)
            if not active_beneficiary:
                raise ValueError(
                    translate(
                        "validation.transfer.beneficiary_required_for_mortis_with_beneficiary"
                    )
                )
            db.session.add(
                OwnershipTransferParty(
                    org_id=org_id(),
                    case_id=case.id,
                    role=OwnershipPartyRole.NUEVO_TITULAR,
                    person_id=active_beneficiary.person_id,
                )
            )
            db.session.flush()
    _validate_case_ready_to_close(case, payload)

    _transition_case_status(case, OwnershipTransferStatus.CLOSED)
    today = date.today()
    now = datetime.now(timezone.utc)
    previous_owner = active_titular_for_contract(case.contract_id)
    if previous_owner:
        previous_owner.end_date = today
        db.session.add(previous_owner)

    new_owner_party = _case_party(case, OwnershipPartyRole.NUEVO_TITULAR)
    is_pensioner = (payload.get("is_pensioner") or "").lower() in {
        "1",
        "on",
        "true",
        "yes",
    }
    pensioner_since_date = _parse_optional_iso_date(payload.get("pensioner_since_date"))
    new_record = OwnershipRecord(
        org_id=org_id(),
        contract_id=case.contract_id,
        person_id=new_owner_party.person_id,
        start_date=today,
        is_pensioner=is_pensioner,
        pensioner_since_date=pensioner_since_date,
        is_provisional=case.type == OwnershipTransferType.PROVISIONAL,
        provisional_until=(
            case.provisional_until
            if case.type == OwnershipTransferType.PROVISIONAL
            else None
        ),
    )
    db.session.add(new_record)

    active_beneficiary = active_beneficiario_for_contract(case.contract_id)
    decision_raw = (payload.get("beneficiary_close_decision") or "").strip().upper()
    if active_beneficiary and not decision_raw:
        raise ValueError("Debes indicar si mantienes o sustituyes beneficiario")
    if decision_raw:
        try:
            decision = BeneficiaryCloseDecision[decision_raw]
        except KeyError as exc:
            raise ValueError("Decision de beneficiario invalida") from exc
        case.beneficiary_close_decision = decision
        if decision == BeneficiaryCloseDecision.REPLACE:
            beneficiary_person_id_raw = (
                payload.get("beneficiary_person_id") or ""
            ).strip()
            if beneficiary_person_id_raw.isdigit():
                new_beneficiary_person = _person_by_org(
                    int(beneficiary_person_id_raw), "beneficiario"
                )
            else:
                new_beneficiary_person = _create_or_reuse_person(
                    payload.get("beneficiary_first_name", ""),
                    payload.get("beneficiary_last_name", ""),
                    payload.get("beneficiary_dni_nif")
                    or payload.get("beneficiary_document_id"),
                )
            if not new_beneficiary_person:
                raise ValueError("Debes indicar el nuevo beneficiario")
            if active_beneficiary:
                active_beneficiary.activo_hasta = today
                db.session.add(active_beneficiary)
            db.session.add(
                Beneficiario(
                    org_id=org_id(),
                    contrato_id=case.contract_id,
                    person_id=new_beneficiary_person.id,
                    activo_desde=today,
                )
            )
    case.closed_at = now
    if not case.resolution_pdf_path:
        _ensure_resolution_pdf(case)
    previous_name = previous_owner.person.full_name if previous_owner else "-"
    new_name = new_owner_party.person.full_name
    detail = f"Cambio titularidad {previous_name} -> {new_name} ({case.case_number})"
    _log_case_movement(
        case.contract, MovimientoTipo.CAMBIO_TITULARIDAD, detail, user_id
    )
    _log_contract_event(
        case.contract_id, case.id, "CAMBIO_TITULARIDAD", detail, user_id
    )
    db.session.commit()
    return case


def ownership_case_resolution_pdf(case_id: int) -> tuple[bytes, str]:
    case = _get_case_or_404(case_id)
    if not case.resolution_pdf_path:
        raise ValueError("El caso no tiene resolucion")
    absolute = Path(current_app.instance_path) / case.resolution_pdf_path
    if not absolute.exists():
        _ensure_resolution_pdf(case)
        db.session.commit()
        absolute = Path(current_app.instance_path) / case.resolution_pdf_path
    return absolute.read_bytes(), absolute.name
