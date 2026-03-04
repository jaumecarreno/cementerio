from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

from flask import g
from sqlalchemy.exc import IntegrityError

from app.core.extensions import db
from app.core.models import (
    BillingDocumentStatus,
    BillingDocumentType,
    BillingDocumentV2,
    BillingLineV2,
    BillingSequenceV2,
    FiscalSubmissionStatus,
    FiscalSubmissionV2,
    IdempotencyRequestV2,
    PaymentMethod,
    PaymentV2,
    PaymentAllocationV2,
)


@dataclass
class FiscalSubmissionResult:
    status: FiscalSubmissionStatus
    external_submission_id: str = ""
    response_payload_json: str = "{}"
    error_message: str = ""


class FiscalProviderAdapter:
    provider_name = "undefined"

    def send_document(self, document: BillingDocumentV2) -> FiscalSubmissionResult:  # pragma: no cover - interface
        raise NotImplementedError


class BlockedFiscalProviderAdapter(FiscalProviderAdapter):
    provider_name = "blocked_no_provider"

    def send_document(self, document: BillingDocumentV2) -> FiscalSubmissionResult:
        raise RuntimeError(
            "Integracion fiscal bloqueada: proveedor no definido para envio VeriFactu"
        )


def _org_id() -> int:
    return g.org.id


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _safe_decimal(value: Decimal | int | float | str | None) -> Decimal:
    if value is None:
        return Decimal("0.00")
    return Decimal(value).quantize(Decimal("0.01"))


def _parse_decimal(value: str | Decimal | None, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value.quantize(Decimal("0.01"))
    raw = str(value or "").strip().replace(",", ".")
    if not raw:
        raise ValueError(f"Falta {field_name}")
    try:
        amount = Decimal(raw).quantize(Decimal("0.01"))
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"Importe invalido en {field_name}") from exc
    return amount


def _parse_optional_int(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    if not raw.isdigit():
        raise ValueError("Id invalido")
    return int(raw)


def _canonical_payload_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _next_sequence_number(sequence_key: str) -> str:
    year = date.today().year
    sequence = (
        BillingSequenceV2.query.filter_by(org_id=_org_id(), sequence_key=sequence_key, year=year)
        .with_for_update()
        .first()
    )
    if not sequence:
        sequence = BillingSequenceV2(
            org_id=_org_id(),
            sequence_key=sequence_key,
            year=year,
            current_value=0,
        )
        db.session.add(sequence)
        db.session.flush()

    sequence.current_value = int(sequence.current_value) + 1
    db.session.add(sequence)

    prefix_map = {
        "INVOICE": "F2-CEM",
        "CREDIT_NOTE": "NC-CEM",
        "RECEIPT": "R2-CEM",
    }
    prefix = prefix_map.get(sequence_key, "SEQ")
    return f"{prefix}-{year}-{sequence.current_value:06d}"


def _fetch_document(document_id: int) -> BillingDocumentV2:
    row = BillingDocumentV2.query.filter_by(org_id=_org_id(), id=document_id).first()
    if not row:
        raise ValueError("Documento de facturacion no encontrado")
    return row


def _load_or_create_submission(document: BillingDocumentV2) -> FiscalSubmissionV2:
    submission = (
        FiscalSubmissionV2.query.filter_by(org_id=_org_id(), document_id=document.id)
        .order_by(FiscalSubmissionV2.id.desc())
        .first()
    )
    if submission:
        return submission
    submission = FiscalSubmissionV2(
        org_id=_org_id(),
        document_id=document.id,
        status=FiscalSubmissionStatus.PENDING,
        provider_name=BlockedFiscalProviderAdapter.provider_name,
    )
    db.session.add(submission)
    db.session.flush()
    return submission


def _provider_adapter() -> FiscalProviderAdapter:
    return BlockedFiscalProviderAdapter()


def submit_document_fiscally(document: BillingDocumentV2, submission: FiscalSubmissionV2) -> FiscalSubmissionV2:
    adapter = _provider_adapter()
    submission.provider_name = adapter.provider_name
    submission.attempt_count = int(submission.attempt_count or 0) + 1
    submission.last_attempt_at = _utcnow()
    submission.status = FiscalSubmissionStatus.SENT
    db.session.add(submission)
    db.session.flush()
    try:
        result = adapter.send_document(document)
    except Exception as exc:
        submission.status = FiscalSubmissionStatus.RETRYING
        submission.error_message = str(exc)
        document.fiscal_status = FiscalSubmissionStatus.RETRYING
        db.session.add_all([submission, document])
        return submission

    submission.status = result.status
    submission.external_submission_id = result.external_submission_id
    submission.response_payload_json = result.response_payload_json or "{}"
    submission.error_message = result.error_message or ""
    if result.status == FiscalSubmissionStatus.ACCEPTED:
        submission.accepted_at = _utcnow()
    document.fiscal_status = result.status
    db.session.add_all([submission, document])
    return submission


def parse_invoice_lines(payload: dict[str, str]) -> list[dict[str, Decimal | str]]:
    concepts = payload.get("line_concept", "")
    quantities = payload.get("line_quantity", "")
    unit_prices = payload.get("line_unit_price", "")
    tax_rates = payload.get("line_tax_rate", "")

    concept_items = [part.strip() for part in concepts.split("|") if part.strip()]
    qty_items = [part.strip() for part in quantities.split("|") if part.strip()]
    price_items = [part.strip() for part in unit_prices.split("|") if part.strip()]
    tax_items = [part.strip() for part in tax_rates.split("|") if part.strip()]

    if not concept_items:
        raise ValueError("Debes indicar al menos una linea de factura")
    if not (len(concept_items) == len(qty_items) == len(price_items) == len(tax_items)):
        raise ValueError("Las lineas de factura estan incompletas")

    lines: list[dict[str, Decimal | str]] = []
    for idx, concept in enumerate(concept_items):
        qty = _parse_decimal(qty_items[idx], f"cantidad linea {idx + 1}")
        unit_price = _parse_decimal(price_items[idx], f"precio linea {idx + 1}")
        tax_rate = _parse_decimal(tax_items[idx], f"iva linea {idx + 1}")
        if qty <= 0:
            raise ValueError("La cantidad debe ser mayor que cero")
        if unit_price < 0:
            raise ValueError("El precio unitario no puede ser negativo")
        if tax_rate < 0:
            raise ValueError("El IVA no puede ser negativo")
        lines.append(
            {
                "concept": concept,
                "quantity": qty,
                "unit_price": unit_price,
                "tax_rate": tax_rate,
            }
        )
    return lines


def create_invoice_draft(payload: dict[str, str], user_id: int | None) -> BillingDocumentV2:
    contract_id = _parse_optional_int(payload.get("contract_id"))
    sepultura_id = _parse_optional_int(payload.get("sepultura_id"))
    lines = parse_invoice_lines(payload)

    document = BillingDocumentV2(
        org_id=_org_id(),
        contract_id=contract_id,
        sepultura_id=sepultura_id,
        document_type=BillingDocumentType.INVOICE,
        status=BillingDocumentStatus.DRAFT,
        fiscal_status=FiscalSubmissionStatus.PENDING,
        currency="EUR",
        total_amount=Decimal("0.00"),
        residual_amount=Decimal("0.00"),
        created_by_user_id=user_id,
    )
    db.session.add(document)
    db.session.flush()

    total = Decimal("0.00")
    for idx, line in enumerate(lines, start=1):
        quantity = _safe_decimal(line["quantity"])
        unit_price = _safe_decimal(line["unit_price"])
        tax_rate = _safe_decimal(line["tax_rate"])
        net = (quantity * unit_price).quantize(Decimal("0.01"))
        tax = (net * tax_rate / Decimal("100.00")).quantize(Decimal("0.01"))
        amount = (net + tax).quantize(Decimal("0.01"))
        total += amount
        db.session.add(
            BillingLineV2(
                org_id=_org_id(),
                document_id=document.id,
                line_no=idx,
                concept=str(line["concept"]),
                quantity=quantity,
                unit_price=unit_price,
                tax_rate=tax_rate,
                net_amount=net,
                tax_amount=tax,
                total_amount=amount,
            )
        )

    document.total_amount = total
    document.residual_amount = total
    db.session.add(document)
    db.session.commit()
    return document


def issue_invoice(document_id: int, user_id: int | None) -> BillingDocumentV2:
    document = _fetch_document(document_id)
    if document.document_type != BillingDocumentType.INVOICE:
        raise ValueError("Solo se pueden emitir facturas")
    if document.status != BillingDocumentStatus.DRAFT:
        raise ValueError("Solo se pueden emitir facturas en borrador")

    document.number = _next_sequence_number("INVOICE")
    document.status = BillingDocumentStatus.ISSUED
    document.issued_at = _utcnow()
    document.updated_at = _utcnow()
    db.session.add(document)

    submission = _load_or_create_submission(document)
    submit_document_fiscally(document, submission)

    db.session.commit()
    return document


def _apply_document_paid_state(document: BillingDocumentV2) -> None:
    residual = _safe_decimal(document.residual_amount)
    if residual == Decimal("0.00"):
        document.status = BillingDocumentStatus.PAID
    elif residual < _safe_decimal(document.total_amount):
        document.status = BillingDocumentStatus.PARTIALLY_PAID
    else:
        document.status = BillingDocumentStatus.ISSUED


def register_payment(
    document_id: int,
    payload: dict[str, str],
    user_id: int | None,
    idempotency_key: str,
    endpoint: str,
) -> tuple[PaymentV2, bool]:
    if not idempotency_key:
        raise ValueError("Falta cabecera Idempotency-Key")

    method_raw = (payload.get("method") or "").strip().upper() or PaymentMethod.EFECTIVO.value
    try:
        method = PaymentMethod[method_raw]
    except KeyError as exc:
        raise ValueError("Metodo de pago invalido") from exc

    amount = _parse_decimal(payload.get("amount"), "importe")
    if amount <= 0:
        raise ValueError("El importe debe ser mayor que cero")

    request_payload = {
        "document_id": document_id,
        "amount": str(amount),
        "method": method.value,
        "external_reference": (payload.get("external_reference") or "").strip(),
    }
    request_hash = _canonical_payload_hash(request_payload)

    existing_key = IdempotencyRequestV2.query.filter_by(
        org_id=_org_id(), endpoint=endpoint, idempotency_key=idempotency_key
    ).first()
    if existing_key:
        if existing_key.request_hash != request_hash:
            raise ValueError("Idempotency-Key reutilizada con payload distinto")
        if existing_key.response_json:
            response = json.loads(existing_key.response_json)
            payment_id = int(response.get("payment_id", 0) or 0)
            payment = PaymentV2.query.filter_by(org_id=_org_id(), id=payment_id).first()
            if payment:
                return payment, True

    if not existing_key:
        existing_key = IdempotencyRequestV2(
            org_id=_org_id(),
            endpoint=endpoint,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            response_status=0,
            response_json="{}",
        )
        db.session.add(existing_key)
        try:
            db.session.flush()
        except IntegrityError:
            db.session.rollback()
            collision = IdempotencyRequestV2.query.filter_by(
                org_id=_org_id(), endpoint=endpoint, idempotency_key=idempotency_key
            ).first()
            if collision and collision.request_hash == request_hash:
                response = json.loads(collision.response_json or "{}")
                payment_id = int(response.get("payment_id", 0) or 0)
                payment = PaymentV2.query.filter_by(org_id=_org_id(), id=payment_id).first()
                if payment:
                    return payment, True
            raise ValueError("No se pudo asegurar idempotencia")

    document = _fetch_document(document_id)
    if document.status not in {
        BillingDocumentStatus.ISSUED,
        BillingDocumentStatus.PARTIALLY_PAID,
    }:
        raise ValueError("Solo se pueden cobrar documentos emitidos")

    residual = _safe_decimal(document.residual_amount)
    if amount > residual:
        raise ValueError("El importe supera el saldo pendiente")

    payment = PaymentV2(
        org_id=_org_id(),
        document_id=document.id,
        amount=amount,
        method=method,
        receipt_number=_next_sequence_number("RECEIPT"),
        external_reference=(payload.get("external_reference") or "").strip(),
        created_by_user_id=user_id,
    )
    db.session.add(payment)
    db.session.flush()

    allocation = PaymentAllocationV2(
        org_id=_org_id(),
        payment_id=payment.id,
        document_id=document.id,
        amount=amount,
    )
    db.session.add(allocation)

    document.residual_amount = (residual - amount).quantize(Decimal("0.01"))
    _apply_document_paid_state(document)
    db.session.add(document)

    existing_key.response_status = 201
    existing_key.response_json = json.dumps(
        {
            "payment_id": payment.id,
            "receipt_number": payment.receipt_number,
            "document_id": document.id,
            "document_status": document.status.value,
            "residual_amount": str(document.residual_amount),
        },
        ensure_ascii=True,
        sort_keys=True,
    )
    db.session.add(existing_key)
    db.session.commit()
    return payment, False


def create_credit_note(
    document_id: int,
    payload: dict[str, str],
    user_id: int | None,
) -> BillingDocumentV2:
    original = _fetch_document(document_id)
    if original.document_type != BillingDocumentType.INVOICE:
        raise ValueError("Solo se puede rectificar sobre una factura")
    if original.status == BillingDocumentStatus.DRAFT:
        raise ValueError("Primero debes emitir la factura antes de rectificar")

    amount = _parse_decimal(payload.get("amount"), "importe rectificativa")
    if amount <= 0:
        raise ValueError("El importe rectificativo debe ser mayor que cero")
    if amount > _safe_decimal(original.total_amount):
        raise ValueError("La rectificativa no puede superar el total de la factura")

    concept = (payload.get("concept") or "Rectificacion").strip() or "Rectificacion"

    credit_note = BillingDocumentV2(
        org_id=_org_id(),
        contract_id=original.contract_id,
        sepultura_id=original.sepultura_id,
        original_document_id=original.id,
        document_type=BillingDocumentType.CREDIT_NOTE,
        status=BillingDocumentStatus.ISSUED,
        fiscal_status=FiscalSubmissionStatus.PENDING,
        number=_next_sequence_number("CREDIT_NOTE"),
        currency=original.currency,
        total_amount=amount,
        residual_amount=Decimal("0.00"),
        issued_at=_utcnow(),
        created_by_user_id=user_id,
    )
    db.session.add(credit_note)
    db.session.flush()

    db.session.add(
        BillingLineV2(
            org_id=_org_id(),
            document_id=credit_note.id,
            line_no=1,
            concept=concept,
            quantity=Decimal("1.00"),
            unit_price=amount,
            tax_rate=Decimal("0.00"),
            net_amount=amount,
            tax_amount=Decimal("0.00"),
            total_amount=amount,
        )
    )

    if (
        amount == _safe_decimal(original.total_amount)
        and original.status == BillingDocumentStatus.ISSUED
        and _safe_decimal(original.residual_amount) == _safe_decimal(original.total_amount)
    ):
        original.status = BillingDocumentStatus.CANCELLED
        original.cancelled_at = _utcnow()
        original.residual_amount = Decimal("0.00")
        db.session.add(original)

    submission = _load_or_create_submission(credit_note)
    submit_document_fiscally(credit_note, submission)

    db.session.commit()
    return credit_note


def payment_receipt_by_id(payment_id: int) -> PaymentV2:
    payment = PaymentV2.query.filter_by(org_id=_org_id(), id=payment_id).first()
    if not payment:
        raise ValueError("Recibo no encontrado")
    return payment


def retry_fiscal_submission(submission_id: int, user_id: int | None) -> FiscalSubmissionV2:
    submission = FiscalSubmissionV2.query.filter_by(org_id=_org_id(), id=submission_id).first()
    if not submission:
        raise ValueError("Envio fiscal no encontrado")
    document = _fetch_document(submission.document_id)
    submit_document_fiscally(document, submission)
    db.session.commit()
    return submission


def workspace_data(filters: dict[str, str]) -> dict[str, object]:
    query = BillingDocumentV2.query.filter_by(org_id=_org_id())

    status_raw = (filters.get("status") or "").strip().upper()
    if status_raw:
        try:
            query = query.filter(BillingDocumentV2.status == BillingDocumentStatus[status_raw])
        except KeyError:
            return {
                "documents": [],
                "pending_total": Decimal("0.00"),
                "recent_submissions": [],
            }

    contract_id_raw = (filters.get("contract_id") or "").strip()
    if contract_id_raw:
        if not contract_id_raw.isdigit():
            return {
                "documents": [],
                "pending_total": Decimal("0.00"),
                "recent_submissions": [],
            }
        query = query.filter(BillingDocumentV2.contract_id == int(contract_id_raw))

    sepultura_id_raw = (filters.get("sepultura_id") or "").strip()
    if sepultura_id_raw:
        if not sepultura_id_raw.isdigit():
            return {
                "documents": [],
                "pending_total": Decimal("0.00"),
                "recent_submissions": [],
            }
        query = query.filter(BillingDocumentV2.sepultura_id == int(sepultura_id_raw))

    documents = (
        query.order_by(BillingDocumentV2.created_at.desc(), BillingDocumentV2.id.desc())
        .limit(120)
        .all()
    )

    pending_total = _safe_decimal(
        sum(
            (
                _safe_decimal(item.residual_amount)
                for item in documents
                if item.status in {BillingDocumentStatus.ISSUED, BillingDocumentStatus.PARTIALLY_PAID}
            ),
            Decimal("0.00"),
        )
    )
    recent_submissions = (
        FiscalSubmissionV2.query.filter_by(org_id=_org_id())
        .order_by(FiscalSubmissionV2.updated_at.desc(), FiscalSubmissionV2.id.desc())
        .limit(20)
        .all()
    )

    return {
        "documents": documents,
        "pending_total": pending_total,
        "recent_submissions": recent_submissions,
    }
