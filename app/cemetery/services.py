from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from io import BytesIO

from flask import g
from sqlalchemy import func, or_

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
    MovimientoSepultura,
    MovimientoTipo,
    OrdenTrabajo,
    Organization,
    Payment,
    Person,
    Sepultura,
    SepulturaDifunto,
    SepulturaEstado,
    TasaMantenimientoTicket,
    TicketDescuentoTipo,
    TicketEstado,
    Titularidad,
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


def active_titular_for_contract(contract_id: int) -> Titularidad | None:
    today = date.today()
    return (
        Titularidad.query.filter_by(org_id=org_id(), contrato_id=contract_id)
        .filter(or_(Titularidad.activo_hasta.is_(None), Titularidad.activo_hasta >= today))
        .order_by(Titularidad.activo_desde.desc())
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
        fecha_fin=fecha_fin,
        legacy_99_years=legacy_99_years,
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
        Titularidad(
            org_id=org_id(),
            contrato_id=contrato.id,
            person_id=titular.id,
            activo_desde=fecha_inicio,
            pensionista=pensionista,
            pensionista_desde=pensionista_desde_date,
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
) -> Titularidad | None:
    return (
        Titularidad.query.filter_by(org_id=organization_id, contrato_id=contract_id)
        .filter(Titularidad.activo_desde <= reference_date)
        .filter(or_(Titularidad.activo_hasta.is_(None), Titularidad.activo_hasta >= reference_date))
        .order_by(Titularidad.activo_desde.desc())
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
            and titular.pensionista
            and titular.pensionista_desde
            and year >= titular.pensionista_desde.year
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
    data = sepultura_tickets_and_invoices(sepultura_id)
    contrato = data["contrato"]
    if contrato is None:
        raise ValueError("La sepultura no tiene contrato activo")
    if data["sepultura"].estado == SepulturaEstado.PROPIA:
        raise ValueError("Las sepulturas Pròpia no generan tiquets de contribución")
    selected = _selected_pending_tickets(contrato.id, selected_ids)
    validate_oldest_prefix_selection(data["pending_tickets"], selected_ids)
    total = sum((ticket.importe for ticket in selected), Decimal("0.00"))
    invoice = Invoice(
        org_id=org_id(),
        contrato_id=contrato.id,
        sepultura_id=sepultura_id,
        numero=_next_invoice_number(),
        estado=InvoiceEstado.IMPAGADA,
        total_amount=total,
        issued_at=datetime.now(timezone.utc),
    )
    db.session.add(invoice)
    db.session.flush()

    for ticket in selected:
        ticket.estado = TicketEstado.FACTURADO
        ticket.invoice_id = invoice.id
        db.session.add(ticket)
    db.session.commit()
    return invoice


def collect_tickets(sepultura_id: int, selected_ids: list[int], method: str = "EFECTIVO") -> tuple[Invoice, Payment]:
    data = sepultura_tickets_and_invoices(sepultura_id)
    contrato = data["contrato"]
    if contrato is None:
        raise ValueError("La sepultura no tiene contrato activo")
    if data["sepultura"].estado == SepulturaEstado.PROPIA:
        raise ValueError("Las sepulturas Pròpia no generan tiquets de contribución")
    selected = _selected_pending_tickets(contrato.id, selected_ids)
    validate_oldest_prefix_selection(data["pending_tickets"], selected_ids)
    total = sum((ticket.importe for ticket in selected), Decimal("0.00"))
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
        titulares = Titularidad.query.filter_by(org_id=org_id(), contrato_id=contrato.id).order_by(
            Titularidad.activo_desde.desc()
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
