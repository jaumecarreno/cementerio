from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from app.core.extensions import db
from app.core.models import (
    BillingDocumentStatus,
    BillingDocumentType,
    BillingDocumentV2,
    DerechoFunerarioContrato,
    DerechoTipo,
    FiscalSubmissionV2,
    FiscalSubmissionStatus,
    Invoice,
    InvoiceEstado,
    Payment,
    PaymentMethod,
    PaymentV2,
    Sepultura,
    TasaMantenimientoTicket,
    TicketEstado,
)


def _create_invoice_v2(client, contract_id: int, sepultura_id: int):
    return client.post(
        "/cementerio/facturacion/invoices",
        data={
            "contract_id": str(contract_id),
            "sepultura_id": str(sepultura_id),
            "line_concept": "Tasa mantenimiento",
            "line_quantity": "1",
            "line_unit_price": "100.00",
            "line_tax_rate": "0",
        },
        follow_redirects=True,
    )


def _issue_invoice_v2(client, document_id: int):
    return client.post(
        f"/cementerio/facturacion/invoices/{document_id}/issue",
        follow_redirects=True,
    )


def test_legacy_receipt_route_is_org_scoped(app, client, login_admin, second_org_sepultura):
    login_admin()
    with app.app_context():
        sep2 = Sepultura.query.filter_by(id=second_org_sepultura).first()
        contract = DerechoFunerarioContrato(
            org_id=sep2.org_id,
            sepultura_id=sep2.id,
            tipo=DerechoTipo.CONCESION,
            fecha_inicio=date(2020, 1, 1),
            fecha_fin=date(2030, 1, 1),
            annual_fee_amount=Decimal("20.00"),
            estado="ACTIVO",
        )
        db.session.add(contract)
        db.session.flush()
        invoice = Invoice(
            org_id=sep2.org_id,
            contrato_id=contract.id,
            sepultura_id=sep2.id,
            numero="F-ORG2-0001",
            estado=InvoiceEstado.PAGADA,
            total_amount=Decimal("20.00"),
            issued_at=datetime.now(timezone.utc),
        )
        db.session.add(invoice)
        db.session.flush()
        payment = Payment(
            org_id=sep2.org_id,
            invoice_id=invoice.id,
            amount=Decimal("20.00"),
            method="EFECTIVO",
            receipt_number="R-ORG2-0001",
        )
        db.session.add(payment)
        db.session.commit()
        payment_id = payment.id

    response = client.get(f"/cementerio/tasas/recibo/{payment_id}")
    assert response.status_code == 404


def test_billing_v2_idempotent_payment_endpoint(app, client, login_admin):
    login_admin()
    with app.app_context():
        contract = (
            DerechoFunerarioContrato.query.join(Sepultura)
            .filter(Sepultura.bloque == "B-12", Sepultura.numero == 127)
            .first()
        )
        assert contract is not None

    create_resp = _create_invoice_v2(client, contract.id, contract.sepultura_id)
    assert create_resp.status_code == 200

    with app.app_context():
        document = BillingDocumentV2.query.order_by(BillingDocumentV2.id.desc()).first()
        assert document is not None
        document_id = document.id

    issue_resp = _issue_invoice_v2(client, document_id)
    assert issue_resp.status_code == 200

    first = client.post(
        f"/cementerio/facturacion/invoices/{document_id}/payments",
        data={"amount": "40.00", "method": "TARJETA"},
        headers={"Idempotency-Key": "idemp-test-1"},
        follow_redirects=False,
    )
    assert first.status_code == 302

    second = client.post(
        f"/cementerio/facturacion/invoices/{document_id}/payments",
        data={"amount": "40.00", "method": "TARJETA"},
        headers={"Idempotency-Key": "idemp-test-1"},
        follow_redirects=False,
    )
    assert second.status_code == 302

    with app.app_context():
        payments = PaymentV2.query.filter_by(document_id=document_id).all()
        assert len(payments) == 1


def test_billing_v2_idempotency_rejects_payload_mismatch(app, client, login_admin):
    login_admin()
    with app.app_context():
        contract = (
            DerechoFunerarioContrato.query.join(Sepultura)
            .filter(Sepultura.bloque == "B-12", Sepultura.numero == 127)
            .first()
        )
        assert contract is not None

    _create_invoice_v2(client, contract.id, contract.sepultura_id)
    with app.app_context():
        document = BillingDocumentV2.query.order_by(BillingDocumentV2.id.desc()).first()
        document_id = document.id
    _issue_invoice_v2(client, document_id)

    first = client.post(
        f"/cementerio/facturacion/invoices/{document_id}/payments",
        data={"amount": "10.00", "method": "EFECTIVO"},
        headers={"Idempotency-Key": "same-key"},
        follow_redirects=True,
    )
    assert first.status_code == 200

    second = client.post(
        f"/cementerio/facturacion/invoices/{document_id}/payments",
        data={"amount": "15.00", "method": "EFECTIVO"},
        headers={"Idempotency-Key": "same-key"},
        follow_redirects=True,
    )
    assert second.status_code == 200
    assert b"payload distinto" in second.data


def test_billing_v2_partial_then_full_payment(app, client, login_admin):
    login_admin()
    with app.app_context():
        contract = (
            DerechoFunerarioContrato.query.join(Sepultura)
            .filter(Sepultura.bloque == "B-20", Sepultura.numero == 210)
            .first()
        )
        assert contract is not None

    _create_invoice_v2(client, contract.id, contract.sepultura_id)
    with app.app_context():
        document = BillingDocumentV2.query.order_by(BillingDocumentV2.id.desc()).first()
        document_id = document.id

    _issue_invoice_v2(client, document_id)

    pay_1 = client.post(
        f"/cementerio/facturacion/invoices/{document_id}/payments",
        data={"amount": "30.00", "method": "EFECTIVO"},
        headers={"Idempotency-Key": "partial-1"},
        follow_redirects=True,
    )
    assert pay_1.status_code == 200

    with app.app_context():
        document = BillingDocumentV2.query.filter_by(id=document_id).first()
        assert document.status == BillingDocumentStatus.PARTIALLY_PAID
        assert document.residual_amount == Decimal("70.00")

    pay_2 = client.post(
        f"/cementerio/facturacion/invoices/{document_id}/payments",
        data={"amount": "70.00", "method": "EFECTIVO"},
        headers={"Idempotency-Key": "partial-2"},
        follow_redirects=True,
    )
    assert pay_2.status_code == 200

    with app.app_context():
        document = BillingDocumentV2.query.filter_by(id=document_id).first()
        assert document.status == BillingDocumentStatus.PAID
        assert document.residual_amount == Decimal("0.00")


def test_billing_v2_credit_note_created_for_issued_invoice(app, client, login_admin):
    login_admin()
    with app.app_context():
        contract = (
            DerechoFunerarioContrato.query.join(Sepultura)
            .filter(Sepultura.bloque == "B-12", Sepultura.numero == 128)
            .first()
        )
        assert contract is not None

    _create_invoice_v2(client, contract.id, contract.sepultura_id)
    with app.app_context():
        document = BillingDocumentV2.query.order_by(BillingDocumentV2.id.desc()).first()
        document_id = document.id

    _issue_invoice_v2(client, document_id)
    credit = client.post(
        f"/cementerio/facturacion/invoices/{document_id}/credit-note",
        data={"amount": "100.00", "concept": "Rectificacion total"},
        follow_redirects=True,
    )
    assert credit.status_code == 200

    with app.app_context():
        note = (
            BillingDocumentV2.query.filter_by(original_document_id=document_id)
            .order_by(BillingDocumentV2.id.desc())
            .first()
        )
        original = BillingDocumentV2.query.filter_by(id=document_id).first()
        assert note is not None
        assert note.document_type == BillingDocumentType.CREDIT_NOTE
        assert note.status == BillingDocumentStatus.ISSUED
        assert original.status == BillingDocumentStatus.CANCELLED


def test_billing_v2_fiscal_retry_keeps_retrying_without_provider(app, client, login_admin):
    login_admin()
    with app.app_context():
        contract = (
            DerechoFunerarioContrato.query.join(Sepultura)
            .filter(Sepultura.bloque == "B-12", Sepultura.numero == 127)
            .first()
        )
        assert contract is not None

    _create_invoice_v2(client, contract.id, contract.sepultura_id)
    with app.app_context():
        document = BillingDocumentV2.query.order_by(BillingDocumentV2.id.desc()).first()
        document_id = document.id

    _issue_invoice_v2(client, document_id)

    with app.app_context():
        submission = FiscalSubmissionV2.query.filter_by(document_id=document_id).first()
        assert submission is not None
        submission_id = submission.id

    response = client.post(
        f"/cementerio/facturacion/fiscal/submissions/{submission_id}/retry",
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"RETRYING" in response.data


def test_billing_v2_receipt_route_is_org_scoped(app, client, login_admin, second_org_sepultura):
    login_admin()
    with app.app_context():
        sep2 = Sepultura.query.filter_by(id=second_org_sepultura).first()
        contract = DerechoFunerarioContrato(
            org_id=sep2.org_id,
            sepultura_id=sep2.id,
            tipo=DerechoTipo.CONCESION,
            fecha_inicio=date(2020, 1, 1),
            fecha_fin=date(2030, 1, 1),
            annual_fee_amount=Decimal("20.00"),
            estado="ACTIVO",
        )
        db.session.add(contract)
        db.session.flush()
        document = BillingDocumentV2(
            org_id=sep2.org_id,
            contract_id=contract.id,
            sepultura_id=sep2.id,
            document_type=BillingDocumentType.INVOICE,
            status=BillingDocumentStatus.PAID,
            fiscal_status=FiscalSubmissionStatus.PENDING,
            number="F2-ORG2-0001",
            currency="EUR",
            total_amount=Decimal("20.00"),
            residual_amount=Decimal("0.00"),
            issued_at=datetime.now(timezone.utc),
        )
        db.session.add(document)
        db.session.flush()
        payment = PaymentV2(
            org_id=sep2.org_id,
            document_id=document.id,
            amount=Decimal("20.00"),
            method=PaymentMethod.EFECTIVO,
            receipt_number="R2-ORG2-0001",
        )
        db.session.add(payment)
        db.session.commit()
        payment_id = payment.id

    response = client.get(f"/cementerio/facturacion/receipts/{payment_id}")
    assert response.status_code == 404


def test_legacy_collect_is_blocked_after_cutover_date(app, client, login_admin):
    login_admin()
    app.config["BILLING_V2_CUTOVER_DATE"] = date.today().isoformat()

    with app.app_context():
        sep = Sepultura.query.filter_by(bloque="B-12", numero=127).first()
        pending = (
            TasaMantenimientoTicket.query.join(
                DerechoFunerarioContrato,
                TasaMantenimientoTicket.contrato_id == DerechoFunerarioContrato.id,
            )
            .filter(DerechoFunerarioContrato.sepultura_id == sep.id)
            .filter(TasaMantenimientoTicket.estado == TicketEstado.PENDIENTE)
            .order_by(TasaMantenimientoTicket.anio.asc())
            .limit(1)
            .all()
        )
        ticket_ids = [str(t.id) for t in pending]

    blocked = client.post(
        "/cementerio/tasas/cobro/cobrar",
        data={
            "sepultura_id": sep.id,
            "ticket_ids": ticket_ids,
            "payment_method": "EFECTIVO",
        },
        follow_redirects=True,
    )
    assert blocked.status_code == 200
    assert b"Cobro legacy deshabilitado" in blocked.data
    assert b"Facturacion V2" in blocked.data
