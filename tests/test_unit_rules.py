from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from flask import g

from app.cemetery.services import (
    change_sepultura_state,
    collect_tickets,
    generate_maintenance_tickets_for_year,
    validate_oldest_prefix_selection,
)
from app.core.extensions import db
from app.core.models import (
    Cemetery,
    DerechoFunerarioContrato,
    DerechoTipo,
    InvoiceEstado,
    Organization,
    Sepultura,
    SepulturaEstado,
    TasaMantenimientoTicket,
    TicketDescuentoTipo,
    TicketEstado,
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


def test_generate_tickets_only_concession_and_idempotent(app):
    with app.app_context():
        org = Organization.query.filter_by(code="SMSFT").first()
        year = 2028

        first = generate_maintenance_tickets_for_year(year, org)
        second = generate_maintenance_tickets_for_year(year, org)

        assert first.created >= 1
        assert second.created == 0
        assert second.existing >= first.created

        lloguer_contracts = (
            DerechoFunerarioContrato.query.filter_by(org_id=org.id, tipo=DerechoTipo.USO_INMEDIATO).all()
        )
        for contract in lloguer_contracts:
            ticket = TasaMantenimientoTicket.query.filter_by(
                org_id=org.id,
                contrato_id=contract.id,
                anio=year,
            ).first()
            assert ticket is None


def test_prefix_rule_rejects_non_contiguous_selection(app):
    with app.app_context():
        tickets = (
            TasaMantenimientoTicket.query.filter_by(org_id=1, estado=TicketEstado.PENDIENTE)
            .order_by(TasaMantenimientoTicket.anio.asc())
            .limit(4)
            .all()
        )
        with pytest.raises(ValueError):
            validate_oldest_prefix_selection(tickets, [tickets[1].id, tickets[2].id])


def test_pensionista_non_retroactive_seed(app):
    with app.app_context():
        tickets = (
            TasaMantenimientoTicket.query.filter_by(org_id=1)
            .filter(TasaMantenimientoTicket.anio.in_([2024, 2025]))
            .order_by(TasaMantenimientoTicket.anio.asc())
            .all()
        )
        assert tickets[0].descuento_tipo == TicketDescuentoTipo.NONE
        assert tickets[1].descuento_tipo == TicketDescuentoTipo.PENSIONISTA


def test_collect_creates_paid_invoice_and_payment_user(app):
    with app.app_context():
        org = Organization.query.filter_by(code="SMSFT").first()
        admin = User.query.filter_by(email="admin@smsft.local").first()
        sep = Sepultura.query.filter_by(org_id=org.id, bloque="B-12", numero=127).first()
        pending = (
            TasaMantenimientoTicket.query.join(DerechoFunerarioContrato)
            .filter(DerechoFunerarioContrato.sepultura_id == sep.id)
            .filter(TasaMantenimientoTicket.estado == TicketEstado.PENDIENTE)
            .order_by(TasaMantenimientoTicket.anio.asc())
            .limit(2)
            .all()
        )
        with app.test_request_context("/"):
            g.org = org
            invoice, payment = collect_tickets(
                sepultura_id=sep.id,
                selected_ids=[pending[0].id, pending[1].id],
                method="EFECTIVO",
                user_id=admin.id,
            )

        assert invoice.estado == InvoiceEstado.PAGADA
        assert payment.user_id == admin.id
        updated = TasaMantenimientoTicket.query.filter(
            TasaMantenimientoTicket.id.in_([pending[0].id, pending[1].id])
        ).all()
        assert all(t.estado == TicketEstado.COBRADO for t in updated)


def test_manual_set_ocupada_is_blocked(app):
    with app.app_context():
        org = Organization.query.filter_by(code="SMSFT").first()
        sep = Sepultura.query.filter_by(org_id=org.id, bloque="B-12", numero=128).first()
        with app.test_request_context("/"):
            g.org = org
            with pytest.raises(ValueError):
                change_sepultura_state(sep, SepulturaEstado.OCUPADA)


def test_propia_cannot_collect_tickets(app):
    with app.app_context():
        org = Organization.query.filter_by(code="SMSFT").first()
        sep = Sepultura.query.filter_by(org_id=org.id, bloque="B-12", numero=127).first()
        sep.estado = SepulturaEstado.PROPIA
        db.session.add(sep)
        db.session.commit()
        ticket = (
            TasaMantenimientoTicket.query.join(DerechoFunerarioContrato)
            .filter(DerechoFunerarioContrato.sepultura_id == sep.id)
            .filter(TasaMantenimientoTicket.estado == TicketEstado.PENDIENTE)
            .order_by(TasaMantenimientoTicket.anio.asc())
            .first()
        )
        with app.test_request_context("/"):
            g.org = org
            with pytest.raises(ValueError):
                collect_tickets(sep.id, [ticket.id], method="EFECTIVO", user_id=None)
