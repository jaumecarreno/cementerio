from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from io import BytesIO
from pathlib import Path

from flask import current_app, g
from sqlalchemy import func, or_
from sqlalchemy.orm import joinedload
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from app.core.extensions import db
from app.core.models import (
    Beneficiario,
    Cemetery,
    DerechoTipo,
    DerechoFunerarioContrato,
    Expediente,
    InscripcionLateral,
    Invoice,
    InvoiceEstado,
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
    Payment,
    Person,
    Publication,
    Sepultura,
    SepulturaDifunto,
    SepulturaEstado,
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


CASE_CHECKLIST: dict[OwnershipTransferType, list[tuple[str, bool]]] = {
    OwnershipTransferType.MORTIS_CAUSA_TESTAMENTO: [
        ("CERT_DEFUNCION", True),
        ("TITULO_SEPULTURA", True),
        ("SOLICITUD_CAMBIO_TITULARIDAD", True),
        ("CERT_ULTIMAS_VOLUNTADES", True),
        ("TESTAMENTO_O_ACEPTACION_HERENCIA", True),
        ("CESION_DERECHOS", False),
        ("SOLICITUD_BENEFICIARIO", False),
        ("DNI_NUEVO_BENEFICIARIO", False),
    ],
    OwnershipTransferType.MORTIS_CAUSA_SIN_TESTAMENTO: [
        ("CERT_DEFUNCION", True),
        ("TITULO_SEPULTURA", True),
        ("SOLICITUD_CAMBIO_TITULARIDAD", True),
        ("CERT_ULTIMAS_VOLUNTADES", True),
        ("LIBRO_FAMILIA_O_TESTIGOS", False),
        ("CESION_DERECHOS", False),
        ("SOLICITUD_BENEFICIARIO", False),
        ("DNI_NUEVO_BENEFICIARIO", False),
    ],
    OwnershipTransferType.INTER_VIVOS: [
        ("SOLICITUD_CAMBIO_TITULARIDAD", True),
        ("TITULO_SEPULTURA", True),
        ("DNI_TITULAR_ACTUAL", True),
        ("DNI_NUEVO_TITULAR", True),
        ("SOLICITUD_BENEFICIARIO", False),
        ("DNI_NUEVO_BENEFICIARIO", False),
    ],
    OwnershipTransferType.PROVISIONAL: [
        ("SOLICITUD_CAMBIO_TITULARIDAD", True),
        ("ACEPTACION_SMSFT", True),
        ("PUBLICACION_BOP", True),
        ("PUBLICACION_DIARIO", True),
        ("SOLICITUD_BENEFICIARIO", False),
        ("DNI_NUEVO_BENEFICIARIO", False),
    ],
}


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
        .filter(Expediente.estado.notin_(["CERRADO", "FINALIZADO"]))
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

    lliures = Sepultura.query.filter_by(org_id=oid, estado=SepulturaEstado.LLIURE).count()
    alerts: list[str] = []
    pending_not_invoiced = (
        TasaMantenimientoTicket.query.filter_by(org_id=oid, estado=TicketEstado.PENDIENTE).count()
    )
    if pending_not_invoiced > 0:
        alerts.append(
            f"Hay tiquets de contribución pendientes (no facturados): {pending_not_invoiced}"
        )
    pending_lateral = InscripcionLateral.query.filter_by(org_id=oid, estado="PENDIENTE_COLOCAR").count()
    if pending_lateral > 0:
        alerts.append(f"Inscripciones laterales en estado pendiente de colocar: {pending_lateral}")
    if lliures > 0:
        alerts.append(f"Sepulturas en estado Lliure pendientes de revisión/vaciado: {lliures}")
    if not alerts:
        alerts.append("Sin alertas activas")

    return {
        "kpis": {
            "expedientes_abiertos": expedientes_abiertos,
            "ot_pendientes": ot_pendientes,
            "tiquets_impagados": tiquets_impagados,
            "pendientes_notificar": pendientes_notificar,
        },
        "recent_expedientes": recent_expedientes,
        "alerts": alerts,
    }


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


def _create_or_reuse_person(first_name: str, last_name: str, document_id: str | None) -> Person:
    # Spec Cementiri 9.1.5 / 9.1.6 - reutilizacion de persona por documento
    first_name = (first_name or "").strip()
    last_name = (last_name or "").strip()
    document_id = (document_id or "").strip() or None
    if not first_name:
        raise ValueError("El nombre de la persona es obligatorio")
    if document_id:
        existing = Person.query.filter_by(org_id=org_id(), document_id=document_id).first()
        if existing:
            return existing
    person = Person(
        org_id=org_id(),
        first_name=first_name,
        last_name=last_name,
        document_id=document_id,
    )
    db.session.add(person)
    db.session.flush()
    return person


def create_funeral_right_contract(sepultura_id: int, payload: dict[str, str]) -> DerechoFunerarioContrato:
    # Spec Cementiri 9.1.7.x - contratacion del derecho funerario
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

    titular = _create_or_reuse_person(
        payload.get("titular_first_name", ""),
        payload.get("titular_last_name", ""),
        payload.get("titular_document_id"),
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

    beneficiario_first_name = (payload.get("beneficiario_first_name") or "").strip()
    if beneficiario_first_name:
        beneficiario = _create_or_reuse_person(
            beneficiario_first_name,
            payload.get("beneficiario_last_name", ""),
            payload.get("beneficiario_document_id"),
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


def nominate_contract_beneficiary(contract_id: int, payload: dict[str, str]) -> Beneficiario:
    # Spec Cementiri 9.1.6 - nombramiento de beneficiario
    contrato = contract_by_id(contract_id)
    first_name = (payload.get("first_name") or "").strip()
    if not first_name:
        raise ValueError("El nombre del beneficiario es obligatorio")
    person = _create_or_reuse_person(
        first_name,
        payload.get("last_name", ""),
        payload.get("document_id"),
    )

    active = active_beneficiario_for_contract(contrato.id)
    if active and active.person_id == person.id:
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
    db.session.commit()
    return beneficiary


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
    case = _get_case_or_404(case_id)
    role_raw = (payload.get("role") or "").strip().upper()
    try:
        role = OwnershipPartyRole[role_raw]
    except KeyError as exc:
        raise ValueError("Rol de parte invalido") from exc

    person_id_raw = (payload.get("person_id") or "").strip()
    if person_id_raw.isdigit():
        person = Person.query.filter_by(id=int(person_id_raw), org_id=org_id()).first()
        if not person:
            raise ValueError("Persona no encontrada")
    else:
        person = _create_or_reuse_person(
            payload.get("first_name", ""),
            payload.get("last_name", ""),
            payload.get("document_id"),
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


def _validate_case_ready_to_close(case: OwnershipTransferCase) -> None:
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


def close_ownership_case(case_id: int, payload: dict[str, str], user_id: int | None) -> OwnershipTransferCase:
    case = _get_case_or_404(case_id)
    _validate_case_ready_to_close(case)

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
                new_beneficiary_person = Person.query.filter_by(
                    org_id=org_id(),
                    id=int(beneficiary_person_id_raw),
                ).first()
            else:
                new_beneficiary_person = _create_or_reuse_person(
                    payload.get("beneficiary_first_name", ""),
                    payload.get("beneficiary_last_name", ""),
                    payload.get("beneficiary_document_id"),
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
