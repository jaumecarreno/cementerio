from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from io import BytesIO, StringIO
from pathlib import Path
import csv
import shutil

from flask import current_app, g
from sqlalchemy import func, or_
from sqlalchemy.orm import joinedload
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from app.core.extensions import db
from app.core.i18n import translate
from app.core.models import (
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
    MovimientoSepultura,
    MovimientoTipo,
    OrdenTrabajo,
    Organization,
    OWNERSHIP_CASE_CHECKLIST,
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
)


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


CASE_CHECKLIST: dict[OwnershipTransferType, list[tuple[str, bool]]] = OWNERSHIP_CASE_CHECKLIST
BENEFICIARY_REPLACE_REQUIRED_DOC_TYPES = ("SOLICITUD_BENEFICIARIO", "DNI_NUEVO_BENEFICIARIO")

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


def org_id() -> int:
    return g.org.id


def org_record() -> Organization:
    return Organization.query.filter_by(id=org_id()).first()


def org_cemetery() -> Cemetery:
    cemetery = Cemetery.query.filter_by(org_id=org_id()).order_by(Cemetery.id.asc()).first()
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
    expedientes_abiertos = (
        Expediente.query.filter_by(org_id=oid)
        .filter(Expediente.estado.notin_(["FINALIZADO", "CANCELADO"]))
        .count()
    )
    ot_pendientes = (
        OrdenTrabajo.query.filter_by(org_id=oid)
        .filter(OrdenTrabajo.estado.in_(["PENDIENTE", "EN_CURSO"]))
        .count()
    )
    tiquets_impagados = (
        TasaMantenimientoTicket.query.filter_by(org_id=oid)
        .filter(TasaMantenimientoTicket.estado != TicketEstado.COBRADO)
        .count()
    )
    pendientes_notificar = (
        InscripcionLateral.query.filter_by(org_id=oid, estado="PENDIENTE_NOTIFICAR").count()
    )

    recent_expedientes = (
        db.session.query(Expediente, Person)
        .outerjoin(Person, Person.id == Expediente.difunto_id)
        .filter(Expediente.org_id == oid)
        .order_by(Expediente.created_at.desc())
        .limit(5)
        .all()
    )
    recent_movements = (
        MovimientoSepultura.query.options(joinedload(MovimientoSepultura.sepultura))
        .filter_by(org_id=oid)
        .order_by(MovimientoSepultura.fecha.desc())
        .limit(30)
        .all()
    )

    lliures = Sepultura.query.filter_by(org_id=oid, estado=SepulturaEstado.LLIURE).count()
    alerts: list[str] = []
    pending_not_invoiced = (
        TasaMantenimientoTicket.query.filter_by(org_id=oid, estado=TicketEstado.PENDIENTE).count()
    )
    if pending_not_invoiced > 0:
        alerts.append(translate("dashboard.alert.pending_tickets").format(count=pending_not_invoiced))
    pending_lateral = InscripcionLateral.query.filter_by(org_id=oid, estado="PENDIENTE_COLOCAR").count()
    if pending_lateral > 0:
        alerts.append(translate("dashboard.alert.pending_lateral").format(count=pending_lateral))
    if lliures > 0:
        alerts.append(translate("dashboard.alert.lliures").format(count=lliures))
    if not alerts:
        alerts.append(translate("dashboard.alert.none"))

    return {
        "kpis": {
            "expedientes_abiertos": expedientes_abiertos,
            "ot_pendientes": ot_pendientes,
            "tiquets_impagados": tiquets_impagados,
            "pendientes_notificar": pendientes_notificar,
        },
        "recent_expedientes": recent_expedientes,
        "recent_activity_by_titular": _recent_activity_by_titular(oid, recent_movements),
        "alerts": alerts,
    }


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
            .filter(or_(OwnershipRecord.end_date.is_(None), OwnershipRecord.end_date >= date.today()))
            .order_by(OwnershipRecord.contract_id.asc(), OwnershipRecord.start_date.desc())
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
        movement_type = movement.tipo.value if hasattr(movement.tipo, "value") else str(movement.tipo)
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
        .filter(or_(OwnershipRecord.end_date.is_(None), OwnershipRecord.end_date >= today))
        .order_by(OwnershipRecord.start_date.desc())
        .first()
    )


def active_beneficiario_for_contract(contract_id: int) -> Beneficiario | None:
    today = date.today()
    return (
        Beneficiario.query.filter_by(org_id=org_id(), contrato_id=contract_id)
        .filter(or_(Beneficiario.activo_hasta.is_(None), Beneficiario.activo_hasta >= today))
        .order_by(Beneficiario.activo_desde.desc())
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


def _create_or_reuse_person(first_name: str, last_name: str, dni_nif: str | None) -> Person:
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
            )
        )
    return (
        query.order_by(Person.last_name.asc(), Person.first_name.asc(), Person.id.asc())
        .limit(max(1, min(limit, 500)))
        .all()
    )


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


def _person_payload(payload: dict[str, str]) -> dict[str, str | None]:
    return {
        "first_name": (payload.get("nombre") or payload.get("first_name") or "").strip(),
        "last_name": (payload.get("apellidos") or payload.get("last_name") or "").strip(),
        "dni_nif": _clean_dni_nif(payload.get("dni_nif") or payload.get("document_id")),
        "telefono": (payload.get("telefono") or payload.get("phone") or "").strip(),
        "email": _validate_email(payload.get("email") or ""),
        "direccion": (payload.get("direccion") or payload.get("address") or "").strip(),
        "notas": (payload.get("notas") or payload.get("notes") or "").strip(),
    }


def create_person(payload: dict[str, str]) -> Person:
    # Spec Cementiri: ver cementerio_extract.md (9.4.3 / 9.4.4 / 9.1.6)
    values = _person_payload(payload)
    if not values["first_name"]:
        raise ValueError("El nombre es obligatorio")
    if values["dni_nif"]:
        existing = Person.query.filter_by(org_id=org_id(), dni_nif=values["dni_nif"]).first()
        if existing:
            raise ValueError("Ya existe una persona con ese DNI/NIF")
    person = Person(
        org_id=org_id(),
        first_name=str(values["first_name"]),
        last_name=str(values["last_name"]),
        dni_nif=values["dni_nif"],
        telefono=str(values["telefono"]),
        email=str(values["email"]),
        direccion=str(values["direccion"]),
        notas=str(values["notas"]),
    )
    db.session.add(person)
    db.session.commit()
    return person


def update_person(person_id: int, payload: dict[str, str]) -> Person:
    # Spec Cementiri: ver cementerio_extract.md (9.4.3 / 9.4.4)
    person = person_by_id(person_id)
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
    person.email = str(values["email"])
    person.direccion = str(values["direccion"])
    person.notas = str(values["notas"])
    db.session.add(person)
    db.session.commit()
    return person


def create_funeral_right_contract(sepultura_id: int, payload: dict[str, str]) -> DerechoFunerarioContrato:
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
    annual_fee_amount = _parse_decimal(payload.get("annual_fee_amount", ""), "importe anual")
    legacy_99_years = (payload.get("legacy_99_years") or "").lower() in {"1", "on", "true", "yes"}

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

    pensionista = (payload.get("pensionista") or "").lower() in {"1", "on", "true", "yes"}
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
                payload.get("beneficiario_dni_nif") or payload.get("beneficiario_document_id"),
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
    contrato = DerechoFunerarioContrato.query.filter_by(org_id=org_id(), id=contract_id).first()
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


def set_contract_holder_pensioner(contract_id: int, payload: dict[str, str], user_id: int | None) -> OwnershipRecord:
    # Spec Cementiri 9.1.5 - marcar titular activo como pensionista no retroactivo por defecto
    contrato = contract_by_id(contract_id)
    titular = active_titular_for_contract(contrato.id)
    if not titular:
        raise ValueError("No hay titular activo")

    since_date = _parse_optional_iso_date(payload.get("since_date")) or date.today()
    allow_retroactive = (payload.get("allow_retroactive") or "").strip().lower() in {"1", "on", "true", "yes"}
    if since_date < date.today() and not allow_retroactive:
        raise ValueError("La pensionista se aplica desde hoy o fecha futura (no retroactivo por defecto)")

    titular.is_pensioner = True
    titular.pensioner_since_date = since_date
    db.session.add(titular)

    detail = f"Titular pensionista desde {since_date.isoformat()}: {titular.person.full_name}"
    _log_case_movement(contrato, MovimientoTipo.PENSIONISTA, detail, user_id)
    _log_contract_event(contrato.id, None, "PENSIONISTA", detail, user_id)
    db.session.commit()
    return titular


def remove_contract_beneficiary(contract_id: int, payload: dict[str, str], user_id: int | None) -> Beneficiario:
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

    detail = f"Beneficiario dado de baja: {active.person.full_name} ({end_date.isoformat()})"
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
        .filter(or_(OwnershipRecord.end_date.is_(None), OwnershipRecord.end_date >= reference_date))
        .order_by(OwnershipRecord.start_date.desc())
        .first()
    )


def _apply_discount(amount: Decimal, discount_pct: Decimal) -> Decimal:
    factor = (Decimal("100.00") - Decimal(discount_pct)) / Decimal("100.00")
    return (Decimal(amount) * factor).quantize(Decimal("0.01"))


def generate_maintenance_tickets_for_year(year: int, organization: Organization) -> TicketGenerationResult:
    # Spec 5.2.5.2.2 / 5.3.4 - generacion de tiquets el 1 de enero para concesiones
    jan_1 = date(year, 1, 1)
    result = TicketGenerationResult()
    contracts = (
        DerechoFunerarioContrato.query.join(Sepultura, Sepultura.id == DerechoFunerarioContrato.sepultura_id)
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
        amount = _apply_discount(base_amount, discount_pct) if apply_pensionista else base_amount
        discount_tipo = TicketDescuentoTipo.PENSIONISTA if apply_pensionista else TicketDescuentoTipo.NONE

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
    oid = org_id()
    query = Sepultura.query.filter_by(org_id=oid)

    if filters.get("bloque"):
        query = query.filter(Sepultura.bloque.ilike(f"%{filters['bloque']}%"))
    if filters.get("fila"):
        try:
            query = query.filter(Sepultura.fila == int(filters["fila"]))
        except ValueError:
            return []
    if filters.get("columna"):
        try:
            query = query.filter(Sepultura.columna == int(filters["columna"]))
        except ValueError:
            return []
    if filters.get("numero"):
        try:
            query = query.filter(Sepultura.numero == int(filters["numero"]))
        except ValueError:
            return []

    sepulturas = query.order_by(Sepultura.bloque, Sepultura.fila, Sepultura.columna, Sepultura.numero).all()
    if not sepulturas:
        return []

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
                db.session.query(func.coalesce(func.sum(TasaMantenimientoTicket.importe), 0))
                .filter_by(org_id=oid, contrato_id=contrato.id)
                .filter(TasaMantenimientoTicket.estado != TicketEstado.COBRADO)
                .scalar()
            )

        difuntos = [sd.person.full_name for sd in sep.difuntos]
        if titular_filter and titular_filter not in titular_name.lower():
            continue
        if difunto_filter and not any(difunto_filter in d.lower() for d in difuntos):
            continue

        rows.append(
            {
                "sepultura": sep,
                "titular_name": titular_name or "—",
                "beneficiario_name": beneficiario.person.full_name if beneficiario else "",
                "deuda": debt,
                "difuntos": difuntos,
            }
        )
    return rows


def list_sepultura_blocks() -> list[str]:
    rows = (
        db.session.query(Sepultura.bloque)
        .filter(Sepultura.org_id == org_id())
        .distinct()
        .order_by(Sepultura.bloque.asc())
        .all()
    )
    return [str(bloque) for (bloque,) in rows if bloque]


def sepultura_by_id(sepultura_id: int) -> Sepultura:
    sep = Sepultura.query.filter_by(org_id=org_id(), id=sepultura_id).first()
    if not sep:
        raise ValueError("Sepultura no encontrada")
    return sep


def change_sepultura_state(sepultura: Sepultura, new_state: SepulturaEstado) -> None:
    # Spec 9.4.2 - cambio de estado manual no permite asignar OCUPADA
    if new_state == SepulturaEstado.OCUPADA:
        raise ValueError("El estado Ocupada se asigna automáticamente al crear contrato")
    if sepultura.estado == SepulturaEstado.OCUPADA and new_state == SepulturaEstado.LLIURE:
        raise ValueError("No se puede pasar de Ocupada a Lliure manualmente")
    if sepultura.estado == SepulturaEstado.PROPIA and new_state == SepulturaEstado.OCUPADA:
        raise ValueError("Una sepultura Pròpia no puede contratarse")
    sepultura.estado = new_state
    db.session.add(sepultura)
    db.session.commit()


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


def validate_oldest_prefix_selection(tickets: list[TasaMantenimientoTicket], selected_ids: list[int]) -> None:
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


def _selected_pending_tickets(contract_id: int, selected_ids: list[int]) -> list[TasaMantenimientoTicket]:
    return (
        TasaMantenimientoTicket.query.filter_by(org_id=org_id(), contrato_id=contract_id, estado=TicketEstado.PENDIENTE)
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
    if not titularidad or not titularidad.is_pensioner or not titularidad.pensioner_since_date:
        return base_amount, TicketDescuentoTipo.NONE

    since_year = titularidad.pensioner_since_date.year
    should_apply = ticket.anio >= since_year or ticket.id in discount_ticket_ids
    if should_apply:
        return _apply_discount(base_amount, discount_pct), TicketDescuentoTipo.PENSIONISTA
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
    db.session.commit()
    return created


def sepultura_tabs_data(sepultura_id: int, tab: str, mov_filters: dict[str, str]) -> dict[str, object]:
    sep = sepultura_by_id(sepultura_id)
    contrato = active_contract_for_sepultura(sep.id)
    titulares = []
    beneficiarios = []
    tasas = []
    active_titular = None
    active_beneficiario = None
    if contrato:
        active_titular = active_titular_for_contract(contrato.id)
        active_beneficiario = active_beneficiario_for_contract(contrato.id)
        titulares = OwnershipRecord.query.filter_by(org_id=org_id(), contract_id=contrato.id).order_by(
            OwnershipRecord.start_date.desc()
        ).all()
        beneficiarios = Beneficiario.query.filter_by(org_id=org_id(), contrato_id=contrato.id).order_by(
            Beneficiario.activo_desde.desc()
        ).all()
        tasas = TasaMantenimientoTicket.query.filter_by(org_id=org_id(), contrato_id=contrato.id).order_by(
            TasaMantenimientoTicket.anio.desc()
        ).all()

    movements_query = MovimientoSepultura.query.filter_by(org_id=org_id(), sepultura_id=sep.id)
    if mov_filters.get("tipo"):
        try:
            mtype = MovimientoTipo[mov_filters["tipo"]]
            movements_query = movements_query.filter_by(tipo=mtype)
        except KeyError:
            pass
    if mov_filters.get("desde"):
        movements_query = movements_query.filter(MovimientoSepultura.fecha >= mov_filters["desde"])
    if mov_filters.get("hasta"):
        movements_query = movements_query.filter(MovimientoSepultura.fecha <= mov_filters["hasta"])
    movimientos = movements_query.order_by(MovimientoSepultura.fecha.desc()).all()

    return {
        "sepultura": sep,
        "contrato": contrato,
        "tab": tab,
        "active_titular": active_titular,
        "active_beneficiario": active_beneficiario,
        "titulares": titulares,
        "beneficiarios": beneficiarios,
        "movimientos": movimientos,
        "tasas": tasas,
    }


def _log_sepultura_movement(
    sepultura_id: int | None,
    movement_type: MovimientoTipo,
    detail: str,
    user_id: int | None,
) -> None:
    if not sepultura_id:
        return
    db.session.add(
        MovimientoSepultura(
            org_id=org_id(),
            sepultura_id=sepultura_id,
            tipo=movement_type,
            detalle=detail,
            user_id=user_id,
        )
    )


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
        Expediente.query.options(joinedload(Expediente.difunto), joinedload(Expediente.declarante))
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
        query = query.filter(Expediente.created_at >= datetime.combine(created_from, datetime.min.time()))
    if created_to:
        query = query.filter(Expediente.created_at <= datetime.combine(created_to, datetime.max.time()))
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
            active_owner = active_titular_for_contract(active_contract.id) if active_contract else None
            has_prior_remains = SepulturaDifunto.query.filter_by(org_id=org_id(), sepultura_id=sepultura_id).first()
            if active_owner and active_owner.is_provisional and has_prior_remains:
                raise ValueError(translate("validation.expediente.provisional_restriction"))

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


def transition_expediente_state(expediente_id: int, new_state: str, user_id: int | None) -> Expediente:
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


def create_expediente_ot(expediente_id: int, payload: dict[str, str], user_id: int | None) -> OrdenTrabajo:
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
    ot = OrdenTrabajo.query.filter_by(org_id=org_id(), expediente_id=expediente.id, id=ot_id).first()
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
    ot = OrdenTrabajo.query.filter_by(org_id=org_id(), expediente_id=expediente.id, id=ot_id).first()
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
        .order_by(LapidaStockMovimiento.created_at.desc(), LapidaStockMovimiento.id.desc())
        .limit(limit)
        .all()
    )


def _find_lapida_stock(stock_id_raw: str | None, codigo_raw: str | None) -> LapidaStock:
    stock = None
    if (stock_id_raw or "").strip().isdigit():
        stock = LapidaStock.query.filter_by(org_id=org_id(), id=int(stock_id_raw)).first()
    if not stock and (codigo_raw or "").strip():
        stock = LapidaStock.query.filter_by(org_id=org_id(), codigo=(codigo_raw or "").strip()).first()
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
        stock = LapidaStock.query.filter_by(org_id=org_id(), id=int(stock_id_raw)).first()

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

    expediente_id = None
    exp_raw = (payload.get("expediente_id") or "").strip()
    if exp_raw:
        if not exp_raw.isdigit():
            raise ValueError("Expediente invalido")
        expediente_id = int(exp_raw)
        expediente_by_id(expediente_id)

    stock.available_qty = current_qty - quantity
    db.session.add(stock)
    db.session.add(
        LapidaStockMovimiento(
            org_id=org_id(),
            lapida_stock_id=stock.id,
            movimiento="SALIDA",
            quantity=quantity,
            sepultura_id=sepultura_id,
            expediente_id=expediente_id,
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
    return stock


def list_inscripciones(filters: dict[str, str]) -> list[InscripcionLateral]:
    query = (
        InscripcionLateral.query.filter_by(org_id=org_id())
        .order_by(InscripcionLateral.created_at.desc(), InscripcionLateral.id.desc())
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


def create_inscripcion_lateral(payload: dict[str, str], user_id: int | None) -> InscripcionLateral:
    sepultura_id_raw = (payload.get("sepultura_id") or "").strip()
    if not sepultura_id_raw.isdigit():
        raise ValueError("Sepultura obligatoria")
    sepultura = sepultura_by_id(int(sepultura_id_raw))
    text = (payload.get("texto") or "").strip()
    if not text:
        raise ValueError("Texto de inscripcion obligatorio")

    expediente_id = None
    exp_raw = (payload.get("expediente_id") or "").strip()
    if exp_raw:
        if not exp_raw.isdigit():
            raise ValueError("Expediente invalido")
        expediente = expediente_by_id(int(exp_raw))
        expediente_id = expediente.id

    item = InscripcionLateral(
        org_id=org_id(),
        sepultura_id=sepultura.id,
        expediente_id=expediente_id,
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
    return item


def transition_inscripcion_estado(inscripcion_id: int, payload: dict[str, str], user_id: int | None) -> InscripcionLateral:
    item = InscripcionLateral.query.filter_by(org_id=org_id(), id=inscripcion_id).first()
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
    query = (
        DerechoFunerarioContrato.query.filter_by(org_id=org_id())
        .join(Sepultura, Sepultura.id == DerechoFunerarioContrato.sepultura_id)
        .order_by(DerechoFunerarioContrato.id.desc())
    )
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
            .filter(or_(OwnershipRecord.end_date.is_(None), OwnershipRecord.end_date >= today))
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
                "sepultura": contract.sepultura.location_label if contract.sepultura else "-",
            }
        )
    return rows


def reporting_deuda_rows(filters: dict[str, str]) -> list[dict[str, object]]:
    # Deuda consolidada por contrato: tiquets pendientes + facturas impagadas.
    query = (
        DerechoFunerarioContrato.query.filter_by(org_id=org_id())
        .join(Sepultura, Sepultura.id == DerechoFunerarioContrato.sepultura_id)
        .order_by(DerechoFunerarioContrato.id.desc())
    )
    contrato_id = (filters.get("contrato_id") or "").strip()
    if contrato_id:
        if not contrato_id.isdigit():
            return []
        query = query.filter(DerechoFunerarioContrato.id == int(contrato_id))
    rows = []
    for contract in query.all():
        pending_tickets = (
            TasaMantenimientoTicket.query.filter_by(org_id=org_id(), contrato_id=contract.id)
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
        ticket_amount = sum((Decimal(t.importe) for t in pending_tickets), Decimal("0.00"))
        invoice_amount = sum((Decimal(i.total_amount) for i in unpaid_invoices), Decimal("0.00"))
        rows.append(
            {
                "contrato_id": contract.id,
                "sepultura": contract.sepultura.location_label if contract.sepultura else "-",
                "tickets_pendientes": len(pending_tickets),
                "importe_tickets": ticket_amount,
                "facturas_impagadas": len(unpaid_invoices),
                "importe_facturas": invoice_amount,
                "deuda_total": ticket_amount + invoice_amount,
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
    raise ValueError("Informe invalido")


def paginate_rows(rows: list[dict[str, object]], page: int, page_size: int) -> dict[str, object]:
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


def reporting_csv_bytes(
    report_key: str,
    filters: dict[str, str],
    export_limit: int = 1000,
) -> bytes:
    rows = reporting_rows(report_key, filters)
    limited = rows[: max(1, min(export_limit, 5000))]
    if not limited:
        limited = []
    if report_key == "sepulturas":
        headers = ["id", "sepultura", "bloque", "modalidad", "estado"]
    elif report_key == "contratos":
        headers = ["id", "tipo", "vigencia", "titular", "sepultura"]
    else:
        headers = [
            "contrato_id",
            "sepultura",
            "tickets_pendientes",
            "importe_tickets",
            "facturas_impagadas",
            "importe_facturas",
            "deuda_total",
        ]
    stream = StringIO()
    writer = csv.DictWriter(stream, fieldnames=headers)
    writer.writeheader()
    for row in limited:
        normalized = {k: row.get(k, "") for k in headers}
        writer.writerow(normalized)
    return stream.getvalue().encode("utf-8")


def reset_demo_org_data(user_id: int | None = None) -> dict[str, int]:
    # Backward compatibility alias for older callers.
    return load_demo_org_initial_dataset(user_id)


def _demo_storage_root(oid: int) -> Path:
    return (
        Path(current_app.instance_path)
        / "storage"
        / "cemetery"
        / "ownership_cases"
        / str(oid)
    )


def _demo_operational_counts(oid: int) -> dict[str, int]:
    return {
        "persons": Person.query.filter_by(org_id=oid).count(),
        "sepulturas": Sepultura.query.filter_by(org_id=oid).count(),
        "contracts": DerechoFunerarioContrato.query.filter_by(org_id=oid).count(),
        "titulares_activos": OwnershipRecord.query.filter_by(org_id=oid)
        .filter(OwnershipRecord.end_date.is_(None))
        .count(),
        "beneficiarios_activos": Beneficiario.query.filter_by(org_id=oid)
        .filter(Beneficiario.activo_hasta.is_(None))
        .count(),
        "beneficiarios_historicos": Beneficiario.query.filter_by(org_id=oid)
        .filter(Beneficiario.activo_hasta.is_not(None))
        .count(),
        "difuntos": SepulturaDifunto.query.filter_by(org_id=oid).count(),
        "expedientes": Expediente.query.filter_by(org_id=oid).count(),
        "ots": OrdenTrabajo.query.filter_by(org_id=oid).count(),
        "casos": OwnershipTransferCase.query.filter_by(org_id=oid).count(),
        "documents": CaseDocument.query.filter_by(org_id=oid).count(),
        "publications": Publication.query.filter_by(org_id=oid).count(),
        "tickets": TasaMantenimientoTicket.query.filter_by(org_id=oid).count(),
        "invoices": Invoice.query.filter_by(org_id=oid).count(),
        "payments": Payment.query.filter_by(org_id=oid).count(),
        "lapida_stock": LapidaStock.query.filter_by(org_id=oid).count(),
        "lapida_movements": LapidaStockMovimiento.query.filter_by(org_id=oid).count(),
        "inscripciones": InscripcionLateral.query.filter_by(org_id=oid).count(),
    }


def _purge_org_operational_data() -> None:
    oid = org_id()
    storage_root = _demo_storage_root(oid)
    if storage_root.exists():
        shutil.rmtree(storage_root, ignore_errors=True)

    db.session.query(ContractEvent).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(Publication).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(CaseDocument).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(OwnershipTransferParty).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(OwnershipTransferCase).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(LapidaStockMovimiento).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(InscripcionLateral).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(OrdenTrabajo).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(Expediente).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(Payment).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(TasaMantenimientoTicket).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(Invoice).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(Beneficiario).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(OwnershipRecord).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(DerechoFunerarioContrato).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(SepulturaUbicacion).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(SepulturaDifunto).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(MovimientoSepultura).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(LapidaStock).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(Sepultura).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.query(Person).filter_by(org_id=oid).delete(synchronize_session=False)
    db.session.commit()


def _ensure_demo_cemetery(oid: int) -> Cemetery:
    cemetery = Cemetery.query.filter_by(org_id=oid).order_by(Cemetery.id.asc()).first()
    if cemetery:
        return cemetery
    cemetery = Cemetery(org_id=oid, name="Cementeri Demo", location="Terrassa")
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
    if case_status in {OwnershipTransferStatus.APPROVED, OwnershipTransferStatus.CLOSED}:
        if required:
            return CaseDocumentStatus.VERIFIED
        return CaseDocumentStatus.PROVIDED if case_index % 2 == 0 else CaseDocumentStatus.MISSING
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
            return CaseDocumentStatus.PROVIDED if case_index % 3 == 0 else CaseDocumentStatus.MISSING
        if case_type == OwnershipTransferType.INTER_VIVOS and doc_type == "ACREDITACION_PARENTESCO_2_GRADO":
            return CaseDocumentStatus.VERIFIED if case_index % 2 == 0 else CaseDocumentStatus.PROVIDED
        return CaseDocumentStatus.VERIFIED if (case_index + len(doc_type)) % 3 == 0 else CaseDocumentStatus.PROVIDED
    if case_status == OwnershipTransferStatus.REJECTED:
        if required:
            return CaseDocumentStatus.REJECTED if (case_index + len(doc_type)) % 2 == 0 else CaseDocumentStatus.PROVIDED
        return CaseDocumentStatus.MISSING
    return CaseDocumentStatus.MISSING


def load_demo_org_initial_dataset(user_id: int | None = None) -> dict[str, int]:
    _purge_org_operational_data()
    oid = org_id()
    cemetery = _ensure_demo_cemetery(oid)

    holder_first_names = (
        "Jose",
        "Antonio",
        "Manuel",
        "Francisco",
        "David",
        "Javier",
        "Juan",
        "Carlos",
        "Daniel",
        "Miguel",
        "Maria",
        "Carmen",
        "Ana",
        "Laura",
        "Isabel",
        "Marta",
        "Elena",
        "Rosa",
        "Silvia",
        "Lucia",
        "Jordi",
        "Pere",
        "Joan",
        "Montserrat",
        "Nuria",
        "Merce",
    )
    extra_first_names = (
        "Adria",
        "Aina",
        "Albert",
        "Aleix",
        "Alba",
        "Alex",
        "Amparo",
        "Andrea",
        "Arnau",
        "Berta",
        "Carla",
        "Celia",
        "Claudia",
        "Cristina",
        "Dolors",
        "Eloi",
        "Emma",
        "Eric",
        "Eva",
        "Felix",
        "Gemma",
        "Hector",
        "Irene",
        "Ivan",
        "Laia",
        "Lluis",
        "Lola",
        "Marc",
        "Mireia",
        "Noelia",
        "Oriol",
        "Paula",
        "Raul",
        "Ruben",
        "Sergi",
        "Sonia",
        "Teresa",
        "Victor",
    )
    last_names = (
        "Garcia",
        "Martinez",
        "Lopez",
        "Sanchez",
        "Perez",
        "Gonzalez",
        "Rodriguez",
        "Fernandez",
        "Alvarez",
        "Ruiz",
        "Moreno",
        "Romero",
        "Navarro",
        "Torres",
        "Dominguez",
        "Vidal",
        "Riera",
        "Pons",
        "Puig",
        "Soler",
        "Mora",
        "Serra",
        "Casals",
        "Costa",
        "Ferrer",
        "Prat",
        "Ribas",
        "Campos",
        "Ibanez",
        "Serrano",
        "Ortega",
        "Mendez",
    )

    holders: list[Person] = []
    extras: list[Person] = []
    for idx in range(1, 301):
        first_name = holder_first_names[(idx - 1) % len(holder_first_names)]
        last_name = (
            f"{last_names[(idx - 1) % len(last_names)]} {last_names[(idx + 7) % len(last_names)]}"
        )
        holders.append(
            Person(
                org_id=oid,
                first_name=first_name,
                last_name=last_name,
                dni_nif=f"HD{idx:07d}",
                telefono=f"600{idx:06d}",
                email=f"titular{idx:03d}@demo.local",
                direccion=f"Carrer Exemple {idx:03d}, Terrassa",
                notas=(
                    "Titular con expediente pendiente"
                    if idx <= 80
                    else "Titular demo"
                ),
            )
        )
    for idx in range(1, 181):
        first_name = extra_first_names[(idx - 1) % len(extra_first_names)]
        last_name = (
            f"{last_names[(idx + 3) % len(last_names)]} {last_names[(idx + 15) % len(last_names)]}"
        )
        extras.append(
            Person(
                org_id=oid,
                first_name=first_name,
                last_name=last_name,
                dni_nif=f"EX{idx:07d}" if idx <= 120 else None,
                telefono=f"700{idx:06d}" if idx <= 120 else "",
                email=f"extra{idx:03d}@demo.local" if idx <= 90 else "",
                direccion=f"Avinguda Prova {idx:03d}, Terrassa" if idx <= 60 else "",
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
        contract_type = DerechoTipo.CONCESION if idx <= 240 else DerechoTipo.USO_INMEDIATO
        start_year = 1998 + (idx % 22)
        fecha_inicio = date(start_year, ((idx - 1) % 12) + 1, ((idx - 1) % 28) + 1)
        duration_years = 30 + (idx % 20) if contract_type == DerechoTipo.CONCESION else 10 + (idx % 15)
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

    expediente_states = (
        ["ABIERTO"] * 40
        + ["EN_TRAMITE"] * 40
        + ["FINALIZADO"] * 35
        + ["CANCELADO"] * 25
    )
    expediente_types = ("INHUMACION", "EXHUMACION", "RESCATE", "FINALIZACION")
    expedientes: list[Expediente] = []
    for idx in range(1, 141):
        expedientes.append(
            Expediente(
                org_id=oid,
                numero=f"C-2026-{idx:04d}",
                tipo=expediente_types[(idx - 1) % len(expediente_types)],
                estado=expediente_states[idx - 1],
                sepultura_id=sepulturas[(idx - 1) % 300].id,
                difunto_id=extras[(idx - 1) % len(extras)].id if idx % 2 == 0 else None,
                declarante_id=(
                    holders[(idx + 37) % len(holders)].id
                    if idx % 3 == 0
                    else extras[(idx + 23) % len(extras)].id
                ),
                fecha_prevista=date(2026, ((idx - 1) % 12) + 1, ((idx - 1) % 28) + 1),
                notas=f"Expediente demo {idx:04d}",
            )
        )
    db.session.add_all(expedientes)
    db.session.flush()

    ot_states = ["PENDIENTE"] * 100 + ["EN_CURSO"] * 70 + ["COMPLETADA"] * 50
    ots: list[OrdenTrabajo] = []
    for idx in range(1, 221):
        state = ot_states[idx - 1]
        completed_at = (
            datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=idx)
            if state == "COMPLETADA"
            else None
        )
        ots.append(
            OrdenTrabajo(
                org_id=oid,
                expediente_id=expedientes[(idx - 1) % len(expedientes)].id,
                titulo=f"OT DEMO {idx:04d}",
                estado=state,
                completed_at=completed_at,
                notes="Orden de trabajo demo",
            )
        )
    db.session.add_all(ots)

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
            closed_at=opened_at + timedelta(days=21) if status == OwnershipTransferStatus.CLOSED else None,
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
            new_holder_person_id = active_beneficiary_person_by_contract_id[case.contract_id]
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
            doc_status = _demo_case_document_status(case.status, required, case.type, doc_type, idx)
            uploaded_at = None
            verified_at = None
            verified_by_user_id = None
            if doc_status in {CaseDocumentStatus.PROVIDED, CaseDocumentStatus.VERIFIED, CaseDocumentStatus.REJECTED}:
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
    provisional_cases = [case for case in ownership_cases if case.type == OwnershipTransferType.PROVISIONAL]
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
        if case.status in {OwnershipTransferStatus.APPROVED, OwnershipTransferStatus.CLOSED}:
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
    closed_cases = [case for case in ownership_cases if case.status == OwnershipTransferStatus.CLOSED]
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
    db.session.add_all(contract_events)
    db.session.add_all(movements)

    ticket_years = (2024, 2025, 2026)
    discount_pct = Decimal("10.00")
    invoice_counter = 1
    receipt_counter = 1
    for contract_index, contract in enumerate(contracts[:120], start=1):
        holder = ownership_records[contract_index - 1]
        for year in ticket_years:
            amount = Decimal(contract.annual_fee_amount or Decimal("0.00")).quantize(Decimal("0.01"))
            discount_type = TicketDescuentoTipo.NONE
            if holder.is_pensioner and holder.pensioner_since_date and year >= holder.pensioner_since_date.year:
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
                    estado=InvoiceEstado.IMPAGADA if ticket_state == TicketEstado.FACTURADO else InvoiceEstado.PAGADA,
                    total_amount=amount,
                    issued_at=datetime(year, ((contract_index - 1) % 12) + 1, 15, tzinfo=timezone.utc),
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
                expediente_id=expedientes[(idx * 2) % len(expedientes)].id if idx % 2 == 0 else None,
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
                expediente_id=expedientes[(idx - 1) % len(expedientes)].id if idx % 2 == 0 else None,
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
            joinedload(OwnershipTransferCase.contract).joinedload(DerechoFunerarioContrato.sepultura),
            joinedload(OwnershipTransferCase.parties).joinedload(OwnershipTransferParty.person),
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


def _case_party(case: OwnershipTransferCase, role: OwnershipPartyRole) -> OwnershipTransferParty | None:
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


def _log_case_movement(
    contract: DerechoFunerarioContrato,
    movement_type: MovimientoTipo,
    detail: str,
    user_id: int | None,
) -> None:
    db.session.add(
        MovimientoSepultura(
            org_id=org_id(),
            sepultura_id=contract.sepultura_id,
            tipo=movement_type,
            detalle=detail,
            user_id=user_id,
        )
    )


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


def _transition_case_status(case: OwnershipTransferCase, new_status: OwnershipTransferStatus) -> None:
    allowed = CASE_STATUS_TRANSITIONS.get(case.status, set())
    if new_status not in allowed:
        raise ValueError(f"Transicion invalida: {case.status.value} -> {new_status.value}")
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
        case.resolution_number = _next_resolution_number(datetime.now(timezone.utc).year)
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
    case.resolution_pdf_path = absolute.relative_to(Path(current_app.instance_path)).as_posix()
    db.session.add(case)


def list_ownership_cases(filters: dict[str, str]) -> list[OwnershipTransferCase]:
    query = (
        OwnershipTransferCase.query.options(
            joinedload(OwnershipTransferCase.contract).joinedload(DerechoFunerarioContrato.sepultura),
            joinedload(OwnershipTransferCase.parties).joinedload(OwnershipTransferParty.person),
            joinedload(OwnershipTransferCase.assigned_to),
        )
        .filter(OwnershipTransferCase.org_id == org_id())
        .order_by(OwnershipTransferCase.opened_at.desc(), OwnershipTransferCase.id.desc())
    )
    type_raw = (filters.get("type") or "").strip().upper()
    if type_raw:
        try:
            query = query.filter(OwnershipTransferCase.type == OwnershipTransferType[type_raw])
        except KeyError:
            return []
    status_raw = (filters.get("status") or "").strip().upper()
    if status_raw:
        try:
            query = query.filter(OwnershipTransferCase.status == OwnershipTransferStatus[status_raw])
        except KeyError:
            return []
    contract_id = (filters.get("contract_id") or "").strip()
    if contract_id.isdigit():
        query = query.filter(OwnershipTransferCase.contract_id == int(contract_id))
    sepultura_id = (filters.get("sepultura_id") or "").strip()
    if sepultura_id.isdigit():
        query = query.join(DerechoFunerarioContrato, DerechoFunerarioContrato.id == OwnershipTransferCase.contract_id)
        query = query.filter(DerechoFunerarioContrato.sepultura_id == int(sepultura_id))
    opened_from = (filters.get("opened_from") or "").strip()
    if opened_from:
        try:
            query = query.filter(
                OwnershipTransferCase.opened_at >= datetime.fromisoformat(f"{opened_from}T00:00:00")
            )
        except ValueError:
            return []
    opened_to = (filters.get("opened_to") or "").strip()
    if opened_to:
        try:
            query = query.filter(
                OwnershipTransferCase.opened_at <= datetime.fromisoformat(f"{opened_to}T23:59:59")
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


def create_ownership_case(payload: dict[str, str], user_id: int | None) -> OwnershipTransferCase:
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
            raise ValueError(translate("validation.transfer.beneficiary_required_for_mortis_with_beneficiary"))
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
        provisional_start = _parse_optional_iso_date(payload.get("provisional_start_date")) or date.today()
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
    _log_case_movement(contract, MovimientoTipo.INICIO_TRANSMISION, f"Inicio de transmision {case.case_number}", user_id)
    _log_contract_event(contract.id, case.id, "INICIO_TRANSMISION", f"Caso {case.case_number} creado", user_id)
    db.session.commit()
    return case


def ownership_case_detail(case_id: int) -> dict[str, object]:
    case = _get_case_or_404(case_id)
    current_owner = active_titular_for_contract(case.contract_id)
    active_beneficiary = active_beneficiario_for_contract(case.contract_id)
    required_pending = [d for d in case.documents if d.required and d.status != CaseDocumentStatus.VERIFIED]
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
        OwnershipTransferParty.query.filter_by(org_id=org_id(), case_id=case.id, role=role).delete()

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


def upload_case_document(case_id: int, doc_id: int, file_obj: FileStorage, user_id: int | None) -> CaseDocument:
    case = _get_case_or_404(case_id)
    document = CaseDocument.query.filter_by(org_id=org_id(), case_id=case.id, id=doc_id).first()
    if not document:
        raise ValueError("Documento no encontrado")
    if not file_obj or not file_obj.filename:
        raise ValueError("Debes seleccionar un fichero")

    filename = secure_filename(file_obj.filename) or f"document-{document.id}.bin"
    root = _case_storage_root(case) / "documents" / str(document.id)
    root.mkdir(parents=True, exist_ok=True)
    absolute = root / filename
    file_obj.save(absolute)
    document.file_path = absolute.relative_to(Path(current_app.instance_path)).as_posix()
    document.uploaded_at = datetime.now(timezone.utc)
    document.status = CaseDocumentStatus.PROVIDED
    db.session.add(document)
    _log_case_movement(case.contract, MovimientoTipo.DOCUMENTO_SUBIDO, f"Documento {document.doc_type} subido", user_id)
    _log_contract_event(
        case.contract_id,
        case.id,
        "DOCUMENTO_SUBIDO",
        f"{document.doc_type}: {document.file_path}",
        user_id,
    )
    db.session.commit()
    return document


def verify_case_document(case_id: int, doc_id: int, action: str, notes: str, user_id: int | None) -> CaseDocument:
    case = _get_case_or_404(case_id)
    document = CaseDocument.query.filter_by(org_id=org_id(), case_id=case.id, id=doc_id).first()
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
    document = CaseDocument.query.filter_by(org_id=org_id(), case_id=case.id, id=doc_id).first()
    if not document:
        raise ValueError("Documento no encontrado")
    if not document.file_path:
        raise ValueError("Documento sin fichero asociado")

    absolute = Path(current_app.instance_path) / document.file_path
    if not absolute.exists():
        raise ValueError("Fichero de documento no encontrado")
    return absolute.read_bytes(), absolute.name


def change_ownership_case_status(case_id: int, new_status_raw: str, user_id: int | None) -> OwnershipTransferCase:
    case = _get_case_or_404(case_id)
    new_status = _parse_transfer_status(new_status_raw)
    _transition_case_status(case, new_status)
    db.session.commit()
    return case


def approve_ownership_case(case_id: int, user_id: int | None) -> OwnershipTransferCase:
    case = _get_case_or_404(case_id)
    _transition_case_status(case, OwnershipTransferStatus.APPROVED)
    _ensure_resolution_pdf(case)
    _log_case_movement(case.contract, MovimientoTipo.APROBACION, f"Caso {case.case_number} aprobado", user_id)
    _log_contract_event(case.contract_id, case.id, "APROBACION", f"Caso {case.case_number} aprobado", user_id)
    db.session.commit()
    return case


def reject_ownership_case(case_id: int, reason: str, user_id: int | None) -> OwnershipTransferCase:
    case = _get_case_or_404(case_id)
    _transition_case_status(case, OwnershipTransferStatus.REJECTED)
    case.rejection_reason = (reason or "").strip()
    if not case.rejection_reason:
        raise ValueError("Motivo de rechazo obligatorio")
    _log_case_movement(case.contract, MovimientoTipo.RECHAZO, f"Caso {case.case_number} rechazado", user_id)
    _log_contract_event(
        case.contract_id,
        case.id,
        "RECHAZO",
        f"Caso {case.case_number} rechazado: {case.rejection_reason}",
        user_id,
    )
    db.session.commit()
    return case


def _validate_case_ready_to_close(case: OwnershipTransferCase, payload: dict[str, str]) -> None:
    if case.status != OwnershipTransferStatus.APPROVED:
        raise ValueError("Solo se pueden cerrar casos en estado APPROVED")
    pending_required = [d for d in case.documents if d.required and d.status != CaseDocumentStatus.VERIFIED]
    if pending_required:
        raise ValueError("Faltan documentos obligatorios verificados")
    new_owner = _case_party(case, OwnershipPartyRole.NUEVO_TITULAR)
    if not new_owner:
        raise ValueError("Debes informar la parte NUEVO_TITULAR")
    if case.type == OwnershipTransferType.PROVISIONAL:
        has_bop = any((pub.channel or "").upper() == "BOP" for pub in case.publications)
        has_other = any((pub.channel or "").upper() != "BOP" for pub in case.publications)
        if not (has_bop and has_other):
            raise ValueError("El caso provisional requiere publicacion en BOP y en otro canal")
    decision_raw = (payload.get("beneficiary_close_decision") or "").strip().upper()
    if decision_raw == BeneficiaryCloseDecision.REPLACE.value:
        for doc_type in BENEFICIARY_REPLACE_REQUIRED_DOC_TYPES:
            document = next((doc for doc in case.documents if doc.doc_type == doc_type), None)
            if not document or document.status != CaseDocumentStatus.VERIFIED:
                raise ValueError(translate("validation.transfer.beneficiary_replace_docs_missing"))
    if case.type == OwnershipTransferType.INTER_VIVOS:
        relation_doc = next((doc for doc in case.documents if doc.doc_type == "ACREDITACION_PARENTESCO_2_GRADO"), None)
        if not relation_doc or relation_doc.status != CaseDocumentStatus.VERIFIED:
            raise ValueError(translate("validation.transfer.intervivos_requires_second_degree_doc"))


def close_ownership_case(case_id: int, payload: dict[str, str], user_id: int | None) -> OwnershipTransferCase:
    # Spec Cementiri: ver cementerio_extract.md (9.1.5)
    case = _get_case_or_404(case_id)
    if case.type == OwnershipTransferType.MORTIS_CAUSA_CON_BENEFICIARIO:
        new_holder = _case_party(case, OwnershipPartyRole.NUEVO_TITULAR)
        if not new_holder:
            active_beneficiary = active_beneficiario_for_contract(case.contract_id)
            if not active_beneficiary:
                raise ValueError(translate("validation.transfer.beneficiary_required_for_mortis_with_beneficiary"))
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
    is_pensioner = (payload.get("is_pensioner") or "").lower() in {"1", "on", "true", "yes"}
    pensioner_since_date = _parse_optional_iso_date(payload.get("pensioner_since_date"))
    new_record = OwnershipRecord(
        org_id=org_id(),
        contract_id=case.contract_id,
        person_id=new_owner_party.person_id,
        start_date=today,
        is_pensioner=is_pensioner,
        pensioner_since_date=pensioner_since_date,
        is_provisional=case.type == OwnershipTransferType.PROVISIONAL,
        provisional_until=case.provisional_until if case.type == OwnershipTransferType.PROVISIONAL else None,
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
            beneficiary_person_id_raw = (payload.get("beneficiary_person_id") or "").strip()
            if beneficiary_person_id_raw.isdigit():
                new_beneficiary_person = _person_by_org(int(beneficiary_person_id_raw), "beneficiario")
            else:
                new_beneficiary_person = _create_or_reuse_person(
                    payload.get("beneficiary_first_name", ""),
                    payload.get("beneficiary_last_name", ""),
                    payload.get("beneficiary_dni_nif") or payload.get("beneficiary_document_id"),
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
    _log_case_movement(case.contract, MovimientoTipo.CAMBIO_TITULARIDAD, detail, user_id)
    _log_contract_event(case.contract_id, case.id, "CAMBIO_TITULARIDAD", detail, user_id)
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

