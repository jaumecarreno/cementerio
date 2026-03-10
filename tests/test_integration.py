from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from app.core.extensions import db
from app.core.models import (
    Beneficiario,
    Cemetery,
    DerechoFunerarioContrato,
    DerechoTipo,
    OwnershipRecord,
    OwnershipTransferCase,
    OwnershipPartyRole,
    OwnershipTransferParty,
    OwnershipTransferStatus,
    OwnershipTransferType,
    Person,
    Sepultura,
    SepulturaEstado,
)


def test_tenant_isolation_on_sepultura_detail(app, client, login_admin, second_org_sepultura):
    login_admin()
    response = client.get(f"/cementerio/sepulturas/{second_org_sepultura}")
    assert response.status_code == 404


def test_search_graves_filters_by_modalidad_estado_deuda_and_shows_sepultura_id(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        cemetery = Cemetery.query.order_by(Cemetery.id.asc()).first()
        sep = Sepultura(
            org_id=cemetery.org_id,
            cemetery_id=cemetery.id,
            bloque="ZZ-TST",
            fila=1,
            columna=1,
            via="V-TEST",
            numero=9901,
            modalidad="Modalitat Test",
            estado=SepulturaEstado.DISPONIBLE,
            tipo_bloque="Ninxols",
            tipo_lapida="Resina",
            orientacion="Nord",
        )
        titular = Person(
            org_id=cemetery.org_id,
            first_name="Titular",
            last_name="Busqueda ZZ",
            dni_nif="11112222Z",
        )
        db.session.add(sep)
        db.session.add(titular)
        db.session.flush()
        contract = DerechoFunerarioContrato(
            org_id=cemetery.org_id,
            sepultura_id=sep.id,
            tipo=DerechoTipo.CONCESION,
            fecha_inicio=date(date.today().year - 1, 1, 1),
            fecha_fin=date(date.today().year + 2, 12, 31),
            annual_fee_amount=Decimal("10.00"),
            estado="ACTIVO",
        )
        db.session.add(contract)
        db.session.flush()
        owner = OwnershipRecord(
            org_id=cemetery.org_id,
            contract_id=contract.id,
            person_id=titular.id,
            start_date=date(date.today().year - 1, 1, 1),
        )
        db.session.add(owner)
        db.session.commit()
        expected_location = f"ZZ-TST / F1 C1 / N9901 - {sep.id}".encode()

    base = client.post("/cementerio/sepulturas/buscar", data={"bloque": "ZZ-TST"})
    assert base.status_code == 200
    assert expected_location in base.data

    by_modalidad = client.post(
        "/cementerio/sepulturas/buscar", data={"modalidad": "Modalitat Test"}
    )
    assert by_modalidad.status_code == 200
    assert expected_location in by_modalidad.data

    wrong_state = client.post(
        "/cementerio/sepulturas/buscar",
        data={"bloque": "ZZ-TST", "estado": SepulturaEstado.LLIURE.value},
    )
    assert wrong_state.status_code == 200
    assert expected_location not in wrong_state.data

    only_deuda = client.post(
        "/cementerio/sepulturas/buscar",
        data={"bloque": "ZZ-TST", "con_deuda": "1"},
    )
    assert only_deuda.status_code == 200
    assert expected_location not in only_deuda.data

    by_titular_name = client.post(
        "/cementerio/sepulturas/buscar",
        data={"titular": "Busqueda ZZ"},
    )
    assert by_titular_name.status_code == 200
    assert expected_location in by_titular_name.data

    by_titular_dni = client.post(
        "/cementerio/sepulturas/buscar",
        data={"titular": "11112222z"},
    )
    assert by_titular_dni.status_code == 200
    assert expected_location in by_titular_dni.data

    by_prefixed_location = client.post(
        "/cementerio/sepulturas/buscar",
        data={"bloque": "ZZ-TST", "fila": "F1", "columna": "C1", "numero": "N9901"},
    )
    assert by_prefixed_location.status_code == 200
    assert expected_location in by_prefixed_location.data


def test_grave_detail_shows_contract_number_and_facturacion_tab(app, client, login_admin):
    login_admin()
    with app.app_context():
        contract = (
            DerechoFunerarioContrato.query.join(Sepultura)
            .filter(Sepultura.bloque == "B-12", Sepultura.numero == 127)
            .first()
        )
        assert contract is not None
        sep_id = contract.sepultura_id

    response = client.get(f"/cementerio/sepulturas/{sep_id}")
    assert response.status_code == 200
    assert f"C{contract.id}".encode() in response.data
    assert b"Facturacion" in response.data
    assert b"tasas" not in response.data.lower()


def test_grave_detail_principal_uses_latest_representative_and_shows_cards(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        contract = (
            DerechoFunerarioContrato.query.join(Sepultura)
            .filter(Sepultura.bloque == "B-12", Sepultura.numero == 127)
            .first()
        )
        contract_id = contract.id
        sep = contract.sepultura
        sep_id = sep.id
        old_rep = Person(
            org_id=sep.org_id,
            first_name="Representante",
            last_name="Antiguo",
            dni_nif="REP-OLD-127",
            telefono="900111000",
            email="old.rep@example.test",
            direccion="Calle Antigua 1",
        )
        new_rep = Person(
            org_id=sep.org_id,
            first_name="Representante",
            last_name="Reciente",
            dni_nif="REP-NEW-127",
            telefono="900222000",
            email="new.rep@example.test",
            direccion="Calle Nueva 2",
        )
        db.session.add_all([old_rep, new_rep])
        db.session.flush()
        old_case = OwnershipTransferCase(
            org_id=sep.org_id,
            case_number="TR-2026-4101",
            contract_id=contract.id,
            type=OwnershipTransferType.INTER_VIVOS,
            status=OwnershipTransferStatus.DRAFT,
            opened_at=datetime(2026, 1, 10, tzinfo=timezone.utc),
        )
        new_case = OwnershipTransferCase(
            org_id=sep.org_id,
            case_number="TR-2026-4102",
            contract_id=contract.id,
            type=OwnershipTransferType.INTER_VIVOS,
            status=OwnershipTransferStatus.DRAFT,
            opened_at=datetime(2026, 2, 10, tzinfo=timezone.utc),
        )
        db.session.add_all([old_case, new_case])
        db.session.flush()
        db.session.add_all(
            [
                OwnershipTransferParty(
                    org_id=sep.org_id,
                    case_id=old_case.id,
                    role=OwnershipPartyRole.REPRESENTANTE,
                    person_id=old_rep.id,
                ),
                OwnershipTransferParty(
                    org_id=sep.org_id,
                    case_id=new_case.id,
                    role=OwnershipPartyRole.REPRESENTANTE,
                    person_id=new_rep.id,
                ),
            ]
        )
        active_owner = (
            OwnershipRecord.query.filter_by(contract_id=contract.id)
            .filter(OwnershipRecord.end_date.is_(None))
            .first()
        )
        active_owner.is_pensioner = True
        db.session.add(active_owner)
        db.session.commit()

    response = client.get(f"/cementerio/sepulturas/{sep_id}")
    assert response.status_code == 200
    assert b"Principal" in response.data
    assert f"prefill_contract_id={contract_id}".encode() in response.data
    assert b"Representante Reciente" in response.data
    assert b"Representante Antiguo" not in response.data


def test_change_holder_direct_redirects_with_prefill_and_does_not_create_case(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        contract = (
            DerechoFunerarioContrato.query.join(Sepultura)
            .filter(Sepultura.bloque == "B-12", Sepultura.numero == 127)
            .first()
        )
        contract_id = contract.id
        sep_id = contract.sepultura_id
        before_count = OwnershipTransferCase.query.filter_by(contract_id=contract_id).count()

    response = client.post(
        f"/cementerio/sepulturas/{sep_id}/cambiar-titular",
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert f"/cementerio/titularidad/casos?prefill_contract_id={contract_id}" in response.headers[
        "Location"
    ]

    with app.app_context():
        after_count = OwnershipTransferCase.query.filter_by(contract_id=contract_id).count()
        assert after_count == before_count


def test_ownership_cases_prefill_contract_validation(
    app, client, login_admin, second_org_sepultura
):
    login_admin()
    with app.app_context():
        own_contract = (
            DerechoFunerarioContrato.query.join(Sepultura)
            .filter(Sepultura.bloque == "B-12", Sepultura.numero == 127)
            .first()
        )
        sep_other_org = Sepultura.query.filter_by(id=second_org_sepultura).first()
        foreign_contract = DerechoFunerarioContrato(
            org_id=sep_other_org.org_id,
            sepultura_id=second_org_sepultura,
            tipo=DerechoTipo.CONCESION,
            fecha_inicio=date(2020, 1, 1),
            fecha_fin=date(2030, 1, 1),
            annual_fee_amount=Decimal("10.00"),
            estado="ACTIVO",
        )
        db.session.add(foreign_contract)
        db.session.commit()
        own_contract_id = own_contract.id
        foreign_contract_id = foreign_contract.id

    valid = client.get(f"/cementerio/titularidad/casos?prefill_contract_id={own_contract_id}")
    assert valid.status_code == 200
    assert (
        f'<input type="number" name="contract_id" min="1" value="{own_contract_id}" required>'.encode()
        in valid.data
    )

    invalid_text = client.get("/cementerio/titularidad/casos?prefill_contract_id=abc")
    assert invalid_text.status_code == 200
    assert b'<input type="number" name="contract_id" min="1" value="" required>' in invalid_text.data

    invalid_foreign = client.get(
        f"/cementerio/titularidad/casos?prefill_contract_id={foreign_contract_id}"
    )
    assert invalid_foreign.status_code == 200
    assert b'<input type="number" name="contract_id" min="1" value="" required>' in invalid_foreign.data


def test_grave_detail_without_contract_hides_change_holder_button(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        cemetery = Cemetery.query.order_by(Cemetery.id.asc()).first()
        sep_without_contract = Sepultura(
            org_id=cemetery.org_id,
            cemetery_id=cemetery.id,
            bloque="B-NOC-BTN",
            fila=8,
            columna=8,
            via="V-8",
            numero=8080,
            modalidad="Ninxol nou",
            estado=SepulturaEstado.DISPONIBLE,
            tipo_bloque="Ninxols",
            tipo_lapida="Resina",
            orientacion="Nord",
        )
        db.session.add(sep_without_contract)
        db.session.commit()
        sep_id = sep_without_contract.id

    response = client.get(f"/cementerio/sepulturas/{sep_id}")
    assert response.status_code == 200
    assert b"Cambiar titular" not in response.data
    assert b"Sin contrato activo" in response.data


def test_sidebar_menu_contains_facturacion_and_not_tasas(app, client, login_admin):
    login_admin()
    response = client.get("/dashboard")
    assert response.status_code == 200

    html = response.data
    start = html.find(b'<aside class="sidebar">')
    end = html.find(b"</aside>", start)
    assert start != -1
    assert end != -1
    sidebar = html[start:end]

    assert b'href="/cementerio/facturacion"' in sidebar
    assert b'href="/cementerio/tasas"' not in sidebar


def test_dashboard_shows_inhumacion_button_before_resumen(app, client, login_admin):
    login_admin()
    response = client.get("/dashboard")
    assert response.status_code == 200
    html = response.get_data(as_text=True)

    assert "/cementerio/inhumaciones/asistente" in html
    assert "Inhumación" in html

    inhumacion_pos = html.find("Inhumación")
    resumen_pos = html.find("Resumen")
    assert inhumacion_pos != -1
    assert resumen_pos != -1
    assert inhumacion_pos < resumen_pos


def test_inhumation_assistant_requires_login(client):
    response = client.get("/cementerio/inhumaciones/asistente", follow_redirects=False)
    assert response.status_code == 302
    assert "/auth/login" in response.headers.get("Location", "")


def test_inhumation_assistant_page_renders_certificate_layout(app, client, login_admin):
    login_admin()
    response = client.get("/cementerio/inhumaciones/asistente")
    assert response.status_code == 200

    html = response.get_data(as_text=True)
    assert "Asistente para crear una inhumación" in html
    assert "Certificación médica" in html
    assert "Datos del difunto y documento" in html
    assert "Número de certificado" in html
    assert "Hora de la defunción (hora:minutos)" in html
    assert "Incineración condicionada por" in html
    assert "Continuar (próximamente)" in html
    assert 'type="button" disabled' in html

    cert_pos = html.find("Certificación médica")
    doctor_pos = html.find('name="doctor_name"')
    difunto_pos = html.find("Datos del difunto y documento")
    defuncion_pos = html.find("Datos de defunción")
    assert cert_pos != -1
    assert doctor_pos != -1
    assert difunto_pos != -1
    assert defuncion_pos != -1
    assert cert_pos < doctor_pos < difunto_pos < defuncion_pos

    assert html.count('name="doctor_name"') == 1
    assert html.count('name="doctor_registered_in"') == 1
    assert html.count('name="doctor_registration_number"') == 1
    assert html.count('name="doctor_professional_practice"') == 1
