from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.core.extensions import db
from app.core.models import (
    Beneficiario,
    DerechoFunerarioContrato,
    DerechoTipo,
    Sepultura,
    SepulturaEstado,
    TasaMantenimientoTicket,
    TicketEstado,
)


def test_tenant_isolation_on_sepultura_detail(app, client, login_admin, second_org_sepultura):
    login_admin()
    response = client.get(f"/cementerio/sepulturas/{second_org_sepultura}")
    assert response.status_code == 404


def test_vertical_flow_search_to_collect(app, client, login_admin):
    login_admin()

    search_response = client.post(
        "/cementerio/sepulturas/buscar",
        data={"bloque": "B-12"},
    )
    assert search_response.status_code == 200
    assert b"B-12" in search_response.data

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
            .limit(2)
            .all()
        )
        ticket_ids = [str(t.id) for t in pending]

    detail_response = client.get(f"/cementerio/sepulturas/{sep.id}")
    assert detail_response.status_code == 200
    assert b"Sepultura" in detail_response.data

    fees_response = client.get(f"/cementerio/tasas/cobro?sepultura_id={sep.id}")
    assert fees_response.status_code == 200

    collect_response = client.post(
        "/cementerio/tasas/cobro/cobrar",
        data={
            "sepultura_id": sep.id,
            "ticket_ids": ticket_ids,
            "payment_method": "EFECTIVO",
        },
        follow_redirects=True,
    )
    assert collect_response.status_code == 200
    assert b"Recibo de cobro" in collect_response.data

    with app.app_context():
        updated = TasaMantenimientoTicket.query.filter(
            TasaMantenimientoTicket.id.in_([int(t) for t in ticket_ids])
        ).all()
        assert all(t.estado == TicketEstado.COBRADO for t in updated)


def test_contract_creation_and_pdf_title(app, client, login_admin):
    login_admin()
    with app.app_context():
        sep = Sepultura.query.filter_by(bloque="B-30", numero=510).first()

    create_response = client.post(
        f"/cementerio/sepulturas/{sep.id}/derecho/contratar",
        data={
            "tipo": "CONCESION",
            "fecha_inicio": "2026-01-01",
            "fecha_fin": "2050-01-01",
            "annual_fee_amount": "48.00",
            "titular_first_name": "Laura",
            "titular_last_name": "Prat",
            "titular_document_id": "55555555E",
            "pensionista": "on",
            "pensionista_desde": "2027-01-01",
        },
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    assert b"Contrato creado correctamente" in create_response.data

    with app.app_context():
        contrato = DerechoFunerarioContrato.query.filter_by(sepultura_id=sep.id).first()
        assert contrato is not None
        assert contrato.annual_fee_amount == Decimal("48.00")

    pdf_response = client.get(f"/cementerio/contratos/{contrato.id}/titulo.pdf")
    assert pdf_response.status_code == 200
    assert pdf_response.headers["Content-Type"].startswith("application/pdf")


def test_fees_warning_without_beneficiary_and_quick_nomination(app, client, login_admin):
    login_admin()
    with app.app_context():
        sep = Sepultura.query.filter_by(bloque="B-12", numero=127).first()
        contrato = DerechoFunerarioContrato.query.filter_by(sepultura_id=sep.id, estado="ACTIVO").first()
        existing = Beneficiario.query.filter_by(contrato_id=contrato.id).first()
        if existing:
            db.session.delete(existing)
            db.session.commit()

    fees_response = client.get(f"/cementerio/tasas/cobro?sepultura_id={sep.id}")
    assert fees_response.status_code == 200
    assert b"no tiene beneficiario" in fees_response.data

    nominate_response = client.post(
        f"/cementerio/contratos/{contrato.id}/beneficiario/nombrar",
        data={
            "sepultura_id": sep.id,
            "first_name": "Nuria",
            "last_name": "Arenas",
            "document_id": "77777777G",
        },
        follow_redirects=True,
    )
    assert nominate_response.status_code == 200
    assert b"Beneficiario guardado" in nominate_response.data


def test_admin_ticket_generation_endpoint(app, client, login_admin):
    login_admin()
    response = client.post(
        "/cementerio/admin/tickets/generar",
        data={"year": "2031"},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"Tiquets generados 2031" in response.data


def test_tenant_isolation_for_contract_pdf(app, client, login_admin, second_org_sepultura):
    with app.app_context():
        sep2 = Sepultura.query.filter_by(id=second_org_sepultura).first()
        contract = DerechoFunerarioContrato(
            org_id=sep2.org_id,
            sepultura_id=sep2.id,
            tipo=DerechoTipo.CONCESION,
            fecha_inicio=date(2020, 1, 1),
            fecha_fin=date(2030, 1, 1),
            annual_fee_amount=Decimal("10.00"),
            estado="ACTIVO",
        )
        db.session.add(contract)
        db.session.commit()
        contract_id = contract.id

    login_admin()
    response = client.get(f"/cementerio/contratos/{contract_id}/titulo.pdf")
    assert response.status_code == 404


def test_mass_create_creates_lliure(app, client, login_admin):
    login_admin()
    response = client.post(
        "/cementerio/sepulturas/alta-masiva",
        data={
            "bloque": "B-99",
            "via": "V-9",
            "tipo_bloque": "Ninxols",
            "modalidad": "Ninxol nou",
            "tipo_lapida": "Resina fenolica",
            "orientacion": "Nord",
            "filas": "1-2",
            "columnas": "1-2",
            "action": "create",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"Sepulturas creadas" in response.data

    with app.app_context():
        rows = Sepultura.query.filter_by(bloque="B-99", estado=SepulturaEstado.LLIURE).all()
        assert len(rows) == 4


def test_layout_includes_theme_toggle(app, client, login_admin):
    login_admin()
    response = client.get("/cementerio/panel")
    assert response.status_code == 200
    assert b'id="theme-toggle"' in response.data
    assert b"gsf-theme" in response.data
