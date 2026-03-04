from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from flask import g

from app.cemetery.billing_v2_service import (
    create_invoice_draft,
    issue_invoice,
    register_payment,
)
from app.cemetery.services import change_sepultura_state
from app.core.extensions import db
from app.core.models import (
    BillingDocumentStatus,
    Cemetery,
    DerechoFunerarioContrato,
    DerechoTipo,
    Organization,
    Sepultura,
    SepulturaEstado,
    User,
)


def test_contract_limit_validation_standard_legacy_and_lloguer(app):
    with app.app_context():
        org = Organization.query.filter_by(code="SMSFT").first()
        cemetery = Cemetery.query.filter_by(org_id=org.id).first()
        sep = Sepultura(
            org_id=org.id,
            cemetery_id=cemetery.id,
            bloque="B-LIMIT",
            fila=1,
            columna=1,
            via="V-1",
            numero=999,
            modalidad="Ninxol",
            estado=SepulturaEstado.DISPONIBLE,
            tipo_bloque="Ninxols",
            tipo_lapida="Resina",
            orientacion="Nord",
        )
        db.session.add(sep)
        db.session.flush()

        with pytest.raises(ValueError):
            DerechoFunerarioContrato(
                org_id=org.id,
                sepultura_id=sep.id,
                tipo=DerechoTipo.CONCESION,
                fecha_inicio=date(2000, 1, 1),
                fecha_fin=date(2052, 1, 1),
            )

        legacy_ok = DerechoFunerarioContrato(
            org_id=org.id,
            sepultura_id=sep.id,
            tipo=DerechoTipo.CONCESION,
            legacy_99_years=True,
            fecha_inicio=date(1980, 1, 1),
            fecha_fin=date(2079, 1, 1),
            annual_fee_amount=Decimal("10.00"),
        )
        db.session.add(legacy_ok)
        db.session.flush()

        with pytest.raises(ValueError):
            DerechoFunerarioContrato(
                org_id=org.id,
                sepultura_id=sep.id,
                tipo=DerechoTipo.CONCESION,
                legacy_99_years=True,
                fecha_inicio=date(1980, 1, 1),
                fecha_fin=date(2081, 1, 1),
            )

        with pytest.raises(ValueError):
            DerechoFunerarioContrato(
                org_id=org.id,
                sepultura_id=sep.id,
                tipo=DerechoTipo.USO_INMEDIATO,
                fecha_inicio=date(2000, 1, 1),
                fecha_fin=date(2027, 1, 1),
            )


def test_manual_set_ocupada_is_blocked(app):
    with app.app_context():
        org = Organization.query.filter_by(code="SMSFT").first()
        sep = Sepultura.query.filter_by(org_id=org.id, bloque="B-12", numero=128).first()
        with app.test_request_context("/"):
            g.org = org
            with pytest.raises(ValueError):
                change_sepultura_state(sep, SepulturaEstado.OCUPADA)


def test_payment_rejects_over_collection_and_updates_status(app):
    with app.app_context():
        org = Organization.query.filter_by(code="SMSFT").first()
        admin = User.query.filter_by(email="admin@smsft.local").first()
        contract = (
            DerechoFunerarioContrato.query.join(Sepultura)
            .filter(Sepultura.bloque == "B-12", Sepultura.numero == 127)
            .first()
        )

        with app.test_request_context("/"):
            g.org = org
            draft = create_invoice_draft(
                {
                    "contract_id": str(contract.id),
                    "sepultura_id": str(contract.sepultura_id),
                    "line_concept": "Mantenimiento",
                    "line_quantity": "1",
                    "line_unit_price": "100.00",
                    "line_tax_rate": "0",
                },
                admin.id,
            )
            issued = issue_invoice(draft.id, admin.id)

            with pytest.raises(ValueError):
                register_payment(
                    issued.id,
                    {"amount": "120.00", "method": "EFECTIVO"},
                    admin.id,
                    idempotency_key="overpay-test",
                    endpoint=f"/cementerio/facturacion/invoices/{issued.id}/payments",
                )

            payment, _ = register_payment(
                issued.id,
                {"amount": "30.00", "method": "EFECTIVO"},
                admin.id,
                idempotency_key="pay-partial",
                endpoint=f"/cementerio/facturacion/invoices/{issued.id}/payments",
            )
            assert payment.amount == Decimal("30.00")

        refreshed = db.session.get(type(issued), issued.id)
        assert refreshed.status == BillingDocumentStatus.PARTIALLY_PAID
        assert refreshed.residual_amount == Decimal("70.00")


def test_issue_invoice_requires_draft_status(app):
    with app.app_context():
        org = Organization.query.filter_by(code="SMSFT").first()
        admin = User.query.filter_by(email="admin@smsft.local").first()
        contract = (
            DerechoFunerarioContrato.query.join(Sepultura)
            .filter(Sepultura.bloque == "B-12", Sepultura.numero == 127)
            .first()
        )

        with app.test_request_context("/"):
            g.org = org
            draft = create_invoice_draft(
                {
                    "contract_id": str(contract.id),
                    "sepultura_id": str(contract.sepultura_id),
                    "line_concept": "Mantenimiento",
                    "line_quantity": "1",
                    "line_unit_price": "45.00",
                    "line_tax_rate": "0",
                },
                admin.id,
            )
            issue_invoice(draft.id, admin.id)

            with pytest.raises(ValueError):
                issue_invoice(draft.id, admin.id)
