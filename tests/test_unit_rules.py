from __future__ import annotations

from datetime import date

import pytest
from flask import g

from app.cemetery.services import (
    change_sepultura_state,
    generate_invoice_for_tickets,
    validate_oldest_prefix_selection,
)
from app.core.extensions import db
from app.core.models import (
    Cemetery,
    DerechoFunerarioContrato,
    DerechoTipo,
    Organization,
    Sepultura,
    SepulturaEstado,
    TasaMantenimientoTicket,
    TicketDescuentoTipo,
    TicketEstado,
)


def test_contract_limit_validation(app):
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
            modalidad="Nínxol",
            estado=SepulturaEstado.DISPONIBLE,
            tipo_bloque="Nínxols",
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


def test_prefix_rule_rejects_non_contiguous_selection(app):
    with app.app_context():
        tickets = (
            TasaMantenimientoTicket.query.filter_by(org_id=1)
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


def test_manual_set_ocupada_is_blocked(app):
    with app.app_context():
        org = Organization.query.filter_by(code="SMSFT").first()
        sep = Sepultura.query.filter_by(org_id=org.id, bloque="B-12", numero=128).first()
        with app.test_request_context("/"):
            g.org = org
            with pytest.raises(ValueError):
                change_sepultura_state(sep, SepulturaEstado.OCUPADA)


def test_propia_cannot_generate_tickets(app):
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
                generate_invoice_for_tickets(sep.id, [ticket.id])
