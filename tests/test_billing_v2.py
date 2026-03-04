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
    FiscalSubmissionStatus,
    FiscalSubmissionV2,
    PaymentMethod,
    PaymentV2,
    Sepultura,
)


def _create_invoice(client, contract_id: int, sepultura_id: int):
    return client.post(
        "/cementerio/facturacion/invoices",
        data={
            "contract_id": str(contract_id),
            "sepultura_id": str(sepultura_id),
            "line_concept": "Mantenimiento anual",
            "line_quantity": "1",
            "line_unit_price": "100.00",
            "line_tax_rate": "0",
        },
        follow_redirects=True,
    )


def _issue_invoice(client, document_id: int):
    return client.post(
        f"/cementerio/facturacion/invoices/{document_id}/issue",
        follow_redirects=True,
    )


def _new_demo_contract_for_org2(app, second_org_sepultura: int) -> tuple[int, int, int]:
    with app.app_context():
        sep = Sepultura.query.filter_by(id=second_org_sepultura).first()
        contract = DerechoFunerarioContrato(
            org_id=sep.org_id,
            sepultura_id=sep.id,
            tipo=DerechoTipo.CONCESION,
            fecha_inicio=date(2020, 1, 1),
            fecha_fin=date(2030, 1, 1),
            annual_fee_amount=Decimal("20.00"),
            estado="ACTIVO",
        )
        db.session.add(contract)
        db.session.commit()
        return sep.org_id, sep.id, contract.id


def _default_contract(app):
    with app.app_context():
        contract = (
            DerechoFunerarioContrato.query.join(Sepultura)
            .filter(Sepultura.bloque == "B-12", Sepultura.numero == 127)
            .first()
        )
        assert contract is not None
        return contract.id, contract.sepultura_id


def test_legacy_tasas_routes_return_404(client, login_admin):
    login_admin()
    assert client.get("/cementerio/tasas").status_code == 404
    assert client.get("/cementerio/tasas/cobro").status_code == 404
    assert client.get("/cementerio/tasas/recibo/1").status_code == 404
    assert client.post("/cementerio/tasas/cobro/cobrar").status_code == 404


def test_billing_idempotent_payment_endpoint(app, client, login_admin):
    login_admin()
    contract_id, sepultura_id = _default_contract(app)

    create_resp = _create_invoice(client, contract_id, sepultura_id)
    assert create_resp.status_code == 200

    with app.app_context():
        document = BillingDocumentV2.query.order_by(BillingDocumentV2.id.desc()).first()
        assert document is not None
        document_id = document.id

    issue_resp = _issue_invoice(client, document_id)
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


def test_billing_idempotency_rejects_payload_mismatch(app, client, login_admin):
    login_admin()
    contract_id, sepultura_id = _default_contract(app)

    _create_invoice(client, contract_id, sepultura_id)
    with app.app_context():
        document = BillingDocumentV2.query.order_by(BillingDocumentV2.id.desc()).first()
        document_id = document.id
    _issue_invoice(client, document_id)

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


def test_billing_partial_then_full_payment(app, client, login_admin):
    login_admin()
    contract_id, sepultura_id = _default_contract(app)

    _create_invoice(client, contract_id, sepultura_id)
    with app.app_context():
        document = BillingDocumentV2.query.order_by(BillingDocumentV2.id.desc()).first()
        document_id = document.id

    _issue_invoice(client, document_id)

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


def test_billing_credit_note_created_for_issued_invoice(app, client, login_admin):
    login_admin()
    contract_id, sepultura_id = _default_contract(app)

    _create_invoice(client, contract_id, sepultura_id)
    with app.app_context():
        document = BillingDocumentV2.query.order_by(BillingDocumentV2.id.desc()).first()
        document_id = document.id

    _issue_invoice(client, document_id)
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


def test_billing_fiscal_retry_keeps_retrying_without_provider(app, client, login_admin):
    login_admin()
    contract_id, sepultura_id = _default_contract(app)

    _create_invoice(client, contract_id, sepultura_id)
    with app.app_context():
        document = BillingDocumentV2.query.order_by(BillingDocumentV2.id.desc()).first()
        document_id = document.id

    _issue_invoice(client, document_id)

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


def test_billing_receipt_route_is_org_scoped(app, client, login_admin, second_org_sepultura):
    login_admin()
    org_id, sep_id, contract_id = _new_demo_contract_for_org2(app, second_org_sepultura)

    with app.app_context():
        document = BillingDocumentV2(
            org_id=org_id,
            contract_id=contract_id,
            sepultura_id=sep_id,
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
            org_id=org_id,
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


def test_billing_workspace_uses_folder_picker_for_contract_and_sepultura_ids(client, login_admin):
    login_admin()
    response = client.get("/cementerio/facturacion")
    assert response.status_code == 200
    assert b"billing_create_contract_id" in response.data
    assert b"billing_create_sepultura_id" in response.data
    assert b"value_mode=contract_id" in response.data
    assert b"value_mode=sepultura_id" in response.data


def test_sepultura_picker_contract_mode_returns_active_contract_value(app, client, login_admin):
    login_admin()
    contract_id, sep_id = _default_contract(app)

    response = client.get(
        f"/cementerio/expedientes/picker/sepulturas?target_field=billing_create_contract_id&label_field=billing_create_contract_selected&value_mode=contract_id&sepultura_id={sep_id}"
    )
    assert response.status_code == 200
    assert f'data-value="{contract_id}"'.encode() in response.data
    assert f"C{contract_id}".encode() in response.data
