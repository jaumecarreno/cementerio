from __future__ import annotations

from app.core.models import (
    DerechoFunerarioContrato,
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
        updated = TasaMantenimientoTicket.query.filter(TasaMantenimientoTicket.id.in_([int(t) for t in ticket_ids])).all()
        assert all(t.estado == TicketEstado.COBRADO for t in updated)


def test_mass_create_creates_lliure(app, client, login_admin):
    login_admin()
    response = client.post(
        "/cementerio/sepulturas/alta-masiva",
        data={
            "bloque": "B-99",
            "via": "V-9",
            "tipo_bloque": "Nínxols",
            "modalidad": "Nínxol nou",
            "tipo_lapida": "Resina fenòlica",
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
