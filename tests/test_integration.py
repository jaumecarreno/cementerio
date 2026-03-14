from __future__ import annotations

from io import BytesIO
from datetime import date, datetime, timezone
from decimal import Decimal

from app.core.extensions import db
from app.core.models import (
    Beneficiario,
    Cemetery,
    DerechoFunerarioContrato,
    DerechoTipo,
    OperationCase,
    OperationStatus,
    OperationType,
    OwnershipRecord,
    OwnershipTransferCase,
    OwnershipPartyRole,
    OwnershipTransferParty,
    OwnershipTransferStatus,
    OwnershipTransferType,
    Person,
    Sepultura,
    SepulturaEstado,
    WorkOrder,
    WorkOrderPriority,
    WorkOrderStatus,
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


def test_gestor_senda_supports_individual_and_bulk_grave_creation(
    app, client, login_admin
):
    login_admin()
    page = client.get("/cementerio/sepulturas/gestor-senda")
    assert page.status_code == 200
    assert b"Gestor Senda" in page.data
    assert b"Alta individual" in page.data
    assert b"Alta masiva" in page.data

    single_response = client.post(
        "/cementerio/sepulturas/gestor-senda",
        data={
            "section": "individual",
            "bloque": "GS-UNIT",
            "fila": "9",
            "columna": "3",
            "numero": "903",
            "via": "V-GS",
            "tipo_bloque": "Ninxols",
            "modalidad": "Ninxol nou",
            "tipo_lapida": "Resina",
            "orientacion": "Nord",
            "estado": SepulturaEstado.LLIURE.value,
        },
        follow_redirects=True,
    )
    assert single_response.status_code == 200
    assert b"Sepultura creada" in single_response.data

    with app.app_context():
        created_single = Sepultura.query.filter_by(
            bloque="GS-UNIT",
            fila=9,
            columna=3,
            numero=903,
        ).first()
        assert created_single is not None
        assert created_single.via == "V-GS"

        before_bulk = Sepultura.query.filter_by(bloque="GS-BULK").count()

    preview_response = client.post(
        "/cementerio/sepulturas/gestor-senda",
        data={
            "section": "bulk",
            "bloque": "GS-BULK",
            "via": "V-BULK",
            "tipo_bloque": "Ninxols",
            "modalidad": "Ninxol nou",
            "tipo_lapida": "Resina",
            "orientacion": "Nord",
            "filas": "1-2",
            "columnas": "1-2",
            "action": "preview",
        },
        follow_redirects=True,
    )
    assert preview_response.status_code == 200
    assert b"GS-BULK" in preview_response.data

    with app.app_context():
        after_preview = Sepultura.query.filter_by(bloque="GS-BULK").count()
        assert after_preview == before_bulk

    create_bulk_response = client.post(
        "/cementerio/sepulturas/gestor-senda",
        data={
            "section": "bulk",
            "bloque": "GS-BULK",
            "via": "V-BULK",
            "tipo_bloque": "Ninxols",
            "modalidad": "Ninxol nou",
            "tipo_lapida": "Resina",
            "orientacion": "Nord",
            "filas": "1-2",
            "columnas": "1-2",
            "action": "create",
        },
        follow_redirects=True,
    )
    assert create_bulk_response.status_code == 200
    assert b"Sepulturas creadas en estado Lliure" in create_bulk_response.data

    with app.app_context():
        bulk_rows = Sepultura.query.filter_by(bloque="GS-BULK").all()
        assert len(bulk_rows) == before_bulk + 4


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
    assert "Inhum" in html

    inhumacion_pos = html.find("Inhum")
    resumen_pos = html.find("Resumen")
    assert inhumacion_pos != -1
    assert resumen_pos != -1
    assert inhumacion_pos < resumen_pos

def test_inhumation_assistant_requires_login(client):
    response = client.get("/cementerio/inhumaciones/asistente", follow_redirects=False)
    assert response.status_code == 302
    assert "/auth/login" in response.headers.get("Location", "")


def test_inhumation_assistant_page_renders_contract_assistant_layout(
    app, client, login_admin
):
    login_admin()
    response = client.get("/cementerio/inhumaciones/asistente")
    assert response.status_code == 200

    html = response.get_data(as_text=True)
    assert "Asistente de alta de contrato" in html
    assert "Documentacion" in html
    assert "Datos del titular" in html
    assert "Datos del difunto" in html
    assert "Seleccion de sepultura" in html
    assert "Datos del beneficiario" in html
    assert "Certificado medico de defuncion" not in html
    assert "Crear Expediente" in html
    assert 'id="contract-continue-btn"' in html
    assert 'id="contract-doc-toggle"' in html
    assert 'class="contract-doc-summary"' in html
    assert 'class="contract-doc-row"' in html
    assert html.count('class="contract-doc-row"') == 6
    assert 'class="contract-doc-label"' in html

    assert 'id="holder-document-upload"' in html
    assert 'id="beneficiary-document-upload"' in html
    assert 'id="billing-document-upload"' in html
    assert 'id="burial-license-upload"' in html
    assert 'id="death-certificate-upload"' in html
    assert 'id="burial-title-upload"' in html

    assert 'id="holder-ai-extract-btn"' in html
    assert 'id="beneficiary-ai-extract-btn"' in html
    assert 'id="billing-ai-extract-btn"' in html
    assert 'id="license-ai-extract-btn"' in html
    assert 'id="death-ai-extract-btn"' in html
    assert 'id="title-ai-extract-btn"' in html

    assert 'id="holder-dni-lookup-btn"' in html
    assert 'id="beneficiary-dni-lookup-btn"' in html
    assert 'id="holder-dni-status"' in html
    assert 'id="beneficiary-dni-status"' in html
    assert 'id="holder-active-contracts-block"' in html
    assert 'id="holder-contracts-status"' in html
    assert 'id="holder-active-contracts-body"' in html
    assert "Contratos activos del titular" in html

    assert 'name="deceased_first_name"' in html
    assert 'name="deceased_last_name"' in html
    assert 'name="deceased_second_last_name"' in html
    assert 'name="deceased_document_number"' in html
    assert 'name="deceased_sex"' in html
    assert 'name="deceased_birth_date"' in html
    assert 'name="deceased_death_date"' in html
    assert 'name="deceased_death_time"' in html
    assert 'name="deceased_death_place"' in html

    assert 'id="assistant_sepultura_id"' in html
    assert 'id="assistant-sepultura-picker-btn"' in html
    assert 'id="assistant_sepultura_selected"' in html
    assert 'id="sepultura-lookup-status"' in html
    assert "estado=LLIURE" in html
    assert 'name="sepultura_bloque" readonly' in html
    assert 'name="sepultura_fila" readonly' in html
    assert 'name="sepultura_columna" readonly' in html
    assert 'name="sepultura_numero" readonly' in html
    assert 'name="sepultura_modalidad" readonly' in html
    assert 'name="sepultura_estado" readonly' in html

    assert 'name="holder_first_name"' in html
    assert 'name="holder_last_name"' in html
    assert 'name="holder_second_last_name"' in html
    assert 'name="holder_document_number"' in html
    assert 'name="holder_person_id"' in html
    assert 'name="holder_lookup_dni"' in html
    assert 'name="holder_sex"' in html
    assert 'name="holder_birth_date"' in html
    assert 'name="holder_phone_1"' in html
    assert 'name="holder_phone_2"' in html
    assert 'name="holder_email_1"' in html
    assert 'name="holder_email_2"' in html
    assert 'name="holder_address"' in html
    assert 'name="holder_postal_code"' in html
    assert 'name="holder_city"' in html
    assert 'name="holder_country"' in html
    assert 'name="holder_observations"' in html

    assert 'name="beneficiary_first_name"' in html
    assert 'name="beneficiary_last_name"' in html
    assert 'name="beneficiary_second_last_name"' in html
    assert 'name="beneficiary_document_number"' in html
    assert 'name="beneficiary_person_id"' in html
    assert 'name="beneficiary_lookup_dni"' in html
    assert 'name="beneficiary_sex"' in html
    assert 'name="beneficiary_birth_date"' in html
    assert 'name="beneficiary_phone_1"' in html
    assert 'name="beneficiary_phone_2"' in html
    assert 'name="beneficiary_email_1"' in html
    assert 'name="beneficiary_email_2"' in html
    assert 'name="beneficiary_address"' in html
    assert 'name="beneficiary_postal_code"' in html
    assert 'name="beneficiary_city"' in html
    assert 'name="beneficiary_country"' in html
    assert 'name="beneficiary_observations"' in html

    assert 'value="Terrassa"' in html
    assert 'value="Espana"' in html
    assert "Extraer con IA" in html

    docs_pos = html.find("Paso 1")
    holder_pos = html.find("Paso 2")
    deceased_pos = html.find("Paso 3")
    sepultura_pos = html.find("Paso 4")
    beneficiary_pos = html.find("Paso 5")
    assert docs_pos != -1
    assert holder_pos != -1
    assert deceased_pos != -1
    assert sepultura_pos != -1
    assert beneficiary_pos != -1
    assert docs_pos < holder_pos < deceased_pos < sepultura_pos < beneficiary_pos

def test_inhumation_assistant_person_lookup_requires_login(client):
    response = client.get(
        "/cementerio/inhumaciones/asistente/persona-por-dni?dni=12345678Z",
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert "/auth/login" in response.headers.get("Location", "")


def test_inhumation_assistant_person_lookup_returns_400_without_dni(
    client, login_admin
):
    login_admin()
    response = client.get("/cementerio/inhumaciones/asistente/persona-por-dni")
    assert response.status_code == 400
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is False
    assert payload["found"] is False
    assert payload["person"] is None
    assert payload["active_contracts"] == []


def test_inhumation_assistant_person_lookup_returns_person_when_found(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        person = Person.query.filter(Person.dni_nif.isnot(None)).order_by(Person.id.asc()).first()
        assert person is not None
        dni = str(person.dni_nif)
        first_name = str(person.first_name)

    response = client.get(
        f"/cementerio/inhumaciones/asistente/persona-por-dni?dni={dni}"
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is True
    assert payload["found"] is True
    assert payload["person"]["dni_nif"] == dni
    assert payload["person"]["first_name"] == first_name
    assert isinstance(payload["active_contracts"], list)


def test_inhumation_assistant_person_lookup_returns_not_found_with_empty_contracts(
    client, login_admin
):
    login_admin()
    response = client.get(
        "/cementerio/inhumaciones/asistente/persona-por-dni?dni=ZZZ-NO-EXISTE-001"
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is True
    assert payload["found"] is False
    assert payload["person"] is None
    assert payload["active_contracts"] == []


def test_inhumation_assistant_person_lookup_returns_active_contracts_for_holder(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        cemetery = Cemetery.query.order_by(Cemetery.id.asc()).first()
        assert cemetery is not None
        today = date.today()
        holder = Person(
            org_id=cemetery.org_id,
            first_name="Titular",
            last_name="Con Contrato",
            dni_nif="HOLDER-CONTRACT-001",
        )
        sepultura = Sepultura(
            org_id=cemetery.org_id,
            cemetery_id=cemetery.id,
            bloque="B-HOLDER",
            fila=3,
            columna=4,
            via="V-HOLDER",
            numero=321,
            modalidad="Ninxol",
            estado=SepulturaEstado.DISPONIBLE,
            tipo_bloque="Ninxols",
            tipo_lapida="Resina",
            orientacion="Nord",
        )
        db.session.add_all([holder, sepultura])
        db.session.flush()
        contract = DerechoFunerarioContrato(
            org_id=cemetery.org_id,
            sepultura_id=sepultura.id,
            tipo=DerechoTipo.CONCESION,
            fecha_inicio=date(today.year - 1, 1, 1),
            fecha_fin=date(today.year + 1, 12, 31),
            annual_fee_amount=Decimal("20.00"),
            estado="ACTIVO",
        )
        db.session.add(contract)
        db.session.flush()
        ownership = OwnershipRecord(
            org_id=cemetery.org_id,
            contract_id=contract.id,
            person_id=holder.id,
            start_date=date(today.year - 1, 1, 1),
        )
        db.session.add(ownership)
        db.session.commit()
        holder_dni = str(holder.dni_nif)
        contract_id = contract.id
        sepultura_id = sepultura.id

    response = client.get(
        f"/cementerio/inhumaciones/asistente/persona-por-dni?dni={holder_dni}"
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is True
    assert payload["found"] is True
    assert isinstance(payload["active_contracts"], list)
    assert payload["active_contracts"]
    row = payload["active_contracts"][0]
    assert row["contract_id"] == contract_id
    assert row["sepultura_id"] == sepultura_id
    assert row["bloque"] == "B-HOLDER"


def test_inhumation_assistant_sepultura_lookup_requires_login(client):
    response = client.get(
        "/cementerio/inhumaciones/asistente/sepultura-por-id?sepultura_id=1",
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert "/auth/login" in response.headers.get("Location", "")


def test_inhumation_assistant_sepultura_lookup_returns_400_without_id(client, login_admin):
    login_admin()
    response = client.get("/cementerio/inhumaciones/asistente/sepultura-por-id")
    assert response.status_code == 400
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is False
    assert payload["found"] is False
    assert payload["sepultura"] is None


def test_inhumation_assistant_sepultura_lookup_returns_400_for_invalid_id(
    client, login_admin
):
    login_admin()
    response = client.get(
        "/cementerio/inhumaciones/asistente/sepultura-por-id?sepultura_id=ABC123"
    )
    assert response.status_code == 400
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is False
    assert payload["found"] is False
    assert payload["sepultura"] is None


def test_inhumation_assistant_sepultura_lookup_returns_not_found(client, login_admin):
    login_admin()
    response = client.get(
        "/cementerio/inhumaciones/asistente/sepultura-por-id?sepultura_id=999999999"
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is True
    assert payload["found"] is False
    assert payload["sepultura"] is None


def test_inhumation_assistant_sepultura_lookup_returns_sepultura_when_found(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        sepultura = Sepultura.query.order_by(Sepultura.id.asc()).first()
        assert sepultura is not None
        sepultura_id = sepultura.id
        expected_block = sepultura.bloque

    response = client.get(
        f"/cementerio/inhumaciones/asistente/sepultura-por-id?sepultura_id={sepultura_id}"
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is True
    assert payload["found"] is True
    assert payload["sepultura"]["id"] == sepultura_id
    assert payload["sepultura"]["bloque"] == expected_block


def test_inhumation_assistant_reserve_sepultura_requires_login(client):
    response = client.post(
        "/cementerio/inhumaciones/asistente/reservar-sepultura",
        data={
            "holder_document_number": "12345678Z",
            "sepultura_id": "1",
            "holder_document_upload": (BytesIO(b"holder"), "holder.pdf"),
            "burial_license_upload": (BytesIO(b"license"), "license.pdf"),
        },
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert "/auth/login" in response.headers.get("Location", "")


def test_inhumation_assistant_reserve_sepultura_returns_400_when_missing_requirements(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        sepultura = Sepultura.query.order_by(Sepultura.id.asc()).first()
        assert sepultura is not None
        sepultura_id = sepultura.id

    cases = [
        {"sepultura_id": str(sepultura_id), "holder_document_upload": (BytesIO(b"holder"), "holder.pdf"), "burial_license_upload": (BytesIO(b"license"), "license.pdf")},
        {"holder_document_number": "12345678Z", "holder_document_upload": (BytesIO(b"holder"), "holder.pdf"), "burial_license_upload": (BytesIO(b"license"), "license.pdf")},
        {"holder_document_number": "12345678Z", "sepultura_id": str(sepultura_id), "burial_license_upload": (BytesIO(b"license"), "license.pdf")},
        {"holder_document_number": "12345678Z", "sepultura_id": str(sepultura_id), "holder_document_upload": (BytesIO(b"holder"), "holder.pdf")},
    ]
    for data in cases:
        response = client.post(
            "/cementerio/inhumaciones/asistente/reservar-sepultura",
            data=data,
            content_type="multipart/form-data",
        )
        assert response.status_code == 400
        payload = response.get_json()
        assert payload is not None
        assert payload["success"] is False
        assert "message" in payload


def test_inhumation_assistant_reserve_sepultura_creates_reservation_ot(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        sepultura = Sepultura.query.order_by(Sepultura.id.asc()).first()
        assert sepultura is not None
        sepultura_id = sepultura.id

    response = client.post(
        "/cementerio/inhumaciones/asistente/reservar-sepultura",
        data={
            "holder_document_number": "12345678Z",
            "holder_first_name": "Titular",
            "beneficiary_document_number": "87654321X",
            "beneficiary_first_name": "Beneficiario",
            "deceased_document_number": "44556677H",
            "deceased_first_name": "Difunto",
            "deceased_last_name": "Reserva",
            "sepultura_id": str(sepultura_id),
            "holder_document_upload": (BytesIO(b"holder"), "holder.pdf"),
            "burial_license_upload": (BytesIO(b"license"), "license.pdf"),
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 201
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is True
    assert payload["expediente_id"] > 0
    assert payload["expediente_code"].startswith("OP-")
    assert payload["work_order_id"] > 0
    assert payload["work_order_code"].startswith("OT-")

    with app.app_context():
        case = db.session.get(OperationCase, int(payload["expediente_id"]))
        assert case is not None
        assert case.code == payload["expediente_code"]
        assert case.type == OperationType.INHUMACION
        assert case.status == OperationStatus.BORRADOR
        assert case.source_sepultura_id == sepultura_id
        assert case.deceased_person_id is not None

        holder = Person.query.filter_by(dni_nif="12345678Z").first()
        beneficiary = Person.query.filter_by(dni_nif="87654321X").first()
        deceased = Person.query.filter_by(dni_nif="44556677H").first()
        assert holder is not None
        assert beneficiary is not None
        assert deceased is not None
        assert case.deceased_person_id == deceased.id

        row = db.session.get(WorkOrder, int(payload["work_order_id"]))
        assert row is not None
        assert row.title == "RESERVA"
        assert row.description == f"Reserva 12345678Z para {case.code}"
        assert row.type_code == "INHUMACION"
        assert row.priority == WorkOrderPriority.MEDIA
        assert row.status == WorkOrderStatus.PENDIENTE_PLANIFICACION
        assert row.operation_case_id == case.id
        assert row.sepultura_id == sepultura_id
        assert row.planned_start_at is not None
        assert row.planned_end_at is not None
        assert int((row.planned_end_at - row.planned_start_at).total_seconds()) == 7200

    listing = client.get("/cementerio/expedientes")
    assert listing.status_code == 200
    assert payload["expediente_code"].encode() in listing.data


def test_inhumation_assistant_reserve_sepultura_updates_people_when_lookup_dni_matches(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        sepultura = Sepultura.query.order_by(Sepultura.id.asc()).first()
        assert sepultura is not None
        sepultura_id = sepultura.id
        holder = Person(
            org_id=sepultura.org_id,
            first_name="Holder",
            last_name="Original",
            dni_nif="10000000H",
            telefono="600000000",
            provincia="Barcelona",
        )
        beneficiary = Person(
            org_id=sepultura.org_id,
            first_name="Benef",
            last_name="Original",
            dni_nif="20000000B",
            telefono="611111111",
        )
        deceased = Person(
            org_id=sepultura.org_id,
            first_name="Deceased",
            last_name="Original",
            dni_nif="30000000D",
        )
        db.session.add_all([holder, beneficiary, deceased])
        db.session.commit()
        holder_id = holder.id
        beneficiary_id = beneficiary.id
        deceased_id = deceased.id

    response = client.post(
        "/cementerio/inhumaciones/asistente/reservar-sepultura",
        data={
            "holder_document_number": "10000000H",
            "holder_first_name": "Holder",
            "holder_last_name": "Actualizado",
            "holder_phone_1": "699000111",
            "holder_person_id": str(holder_id),
            "holder_lookup_dni": "10000000H",
            "beneficiary_document_number": "20000000B",
            "beneficiary_first_name": "Benef",
            "beneficiary_last_name": "Actualizado",
            "beneficiary_phone_1": "688000222",
            "beneficiary_person_id": str(beneficiary_id),
            "beneficiary_lookup_dni": "20000000B",
            "deceased_document_number": "30000000D",
            "deceased_first_name": "Deceased",
            "deceased_last_name": "Actualizado",
            "sepultura_id": str(sepultura_id),
            "holder_document_upload": (BytesIO(b"holder"), "holder.pdf"),
            "burial_license_upload": (BytesIO(b"license"), "license.pdf"),
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 201

    with app.app_context():
        holder_after = db.session.get(Person, holder_id)
        beneficiary_after = db.session.get(Person, beneficiary_id)
        deceased_after = db.session.get(Person, deceased_id)
        assert holder_after is not None
        assert beneficiary_after is not None
        assert deceased_after is not None
        assert holder_after.last_name == "Actualizado"
        assert holder_after.telefono == "699000111"
        assert holder_after.provincia == "Barcelona"
        assert beneficiary_after.last_name == "Actualizado"
        assert beneficiary_after.telefono == "688000222"
        assert deceased_after.last_name == "Actualizado"


def test_inhumation_assistant_reserve_sepultura_creates_new_person_when_lookup_dni_changes(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        sepultura = Sepultura.query.order_by(Sepultura.id.asc()).first()
        assert sepultura is not None
        sepultura_id = sepultura.id
        holder = Person(
            org_id=sepultura.org_id,
            first_name="Holder",
            last_name="Original",
            dni_nif="40000000H",
        )
        beneficiary = Person(
            org_id=sepultura.org_id,
            first_name="Benef",
            last_name="Original",
            dni_nif="50000000B",
        )
        db.session.add_all([holder, beneficiary])
        db.session.commit()
        holder_id = holder.id
        beneficiary_id = beneficiary.id

    response = client.post(
        "/cementerio/inhumaciones/asistente/reservar-sepultura",
        data={
            "holder_document_number": "40000001H",
            "holder_first_name": "Holder Nuevo",
            "holder_last_name": "Nuevo",
            "holder_person_id": str(holder_id),
            "holder_lookup_dni": "40000000H",
            "beneficiary_document_number": "50000001B",
            "beneficiary_first_name": "Benef Nuevo",
            "beneficiary_last_name": "Nuevo",
            "beneficiary_person_id": str(beneficiary_id),
            "beneficiary_lookup_dni": "50000000B",
            "deceased_document_number": "60000000D",
            "deceased_first_name": "Deceased",
            "deceased_last_name": "Nuevo",
            "sepultura_id": str(sepultura_id),
            "holder_document_upload": (BytesIO(b"holder"), "holder.pdf"),
            "burial_license_upload": (BytesIO(b"license"), "license.pdf"),
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 201

    with app.app_context():
        old_holder = db.session.get(Person, holder_id)
        old_beneficiary = db.session.get(Person, beneficiary_id)
        new_holder = Person.query.filter_by(dni_nif="40000001H").first()
        new_beneficiary = Person.query.filter_by(dni_nif="50000001B").first()
        assert old_holder is not None
        assert old_beneficiary is not None
        assert new_holder is not None
        assert new_beneficiary is not None
        assert old_holder.first_name == "Holder"
        assert old_beneficiary.first_name == "Benef"
        assert new_holder.id != holder_id
        assert new_beneficiary.id != beneficiary_id


def test_inhumation_assistant_reserve_sepultura_validates_required_people_data(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        sepultura = Sepultura.query.order_by(Sepultura.id.asc()).first()
        assert sepultura is not None
        sepultura_id = sepultura.id

    response = client.post(
        "/cementerio/inhumaciones/asistente/reservar-sepultura",
        data={
            "holder_document_number": "70000000H",
            "holder_first_name": "Titular",
            "beneficiary_first_name": "Beneficiario sin DNI",
            "sepultura_id": str(sepultura_id),
            "holder_document_upload": (BytesIO(b"holder"), "holder.pdf"),
            "burial_license_upload": (BytesIO(b"license"), "license.pdf"),
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is False
    assert "DNI del beneficiario" in payload["message"]

    response_deceased = client.post(
        "/cementerio/inhumaciones/asistente/reservar-sepultura",
        data={
            "holder_document_number": "70000000H",
            "holder_first_name": "Titular",
            "beneficiary_document_number": "70000001B",
            "beneficiary_first_name": "Beneficiario",
            "sepultura_id": str(sepultura_id),
            "holder_document_upload": (BytesIO(b"holder"), "holder.pdf"),
            "burial_license_upload": (BytesIO(b"license"), "license.pdf"),
        },
        content_type="multipart/form-data",
    )
    assert response_deceased.status_code == 400
    deceased_payload = response_deceased.get_json()
    assert deceased_payload is not None
    assert deceased_payload["success"] is False
    assert "DNI del difunto" in deceased_payload["message"] or "difunto" in deceased_payload["message"].lower()


def test_inhumation_assistant_reserve_sepultura_fails_when_new_dni_collides(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        sepultura = Sepultura.query.order_by(Sepultura.id.asc()).first()
        assert sepultura is not None
        sepultura_id = sepultura.id
        holder = Person(
            org_id=sepultura.org_id,
            first_name="Holder",
            last_name="Original",
            dni_nif="80000000H",
        )
        existing_collision = Person(
            org_id=sepultura.org_id,
            first_name="Otra",
            last_name="Persona",
            dni_nif="80000001H",
        )
        db.session.add_all([holder, existing_collision])
        db.session.commit()
        holder_id = holder.id

    response = client.post(
        "/cementerio/inhumaciones/asistente/reservar-sepultura",
        data={
            "holder_document_number": "80000001H",
            "holder_first_name": "Holder Nuevo",
            "holder_person_id": str(holder_id),
            "holder_lookup_dni": "80000000H",
            "beneficiary_document_number": "80000002B",
            "beneficiary_first_name": "Beneficiario",
            "deceased_document_number": "80000003D",
            "deceased_first_name": "Difunto",
            "deceased_last_name": "Apellido",
            "sepultura_id": str(sepultura_id),
            "holder_document_upload": (BytesIO(b"holder"), "holder.pdf"),
            "burial_license_upload": (BytesIO(b"license"), "license.pdf"),
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is False
    assert "DNI/NIF" in payload["message"]


def test_inhumation_assistant_reserve_sepultura_returns_409_for_duplicate_open_reservation(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        sepultura = Sepultura.query.order_by(Sepultura.id.asc()).first()
        assert sepultura is not None
        sepultura_id = sepultura.id
        baseline_count = (
            WorkOrder.query.filter_by(sepultura_id=sepultura_id, title="RESERVA", type_code="INHUMACION")
            .filter(WorkOrder.status.notin_([WorkOrderStatus.COMPLETADA, WorkOrderStatus.CANCELADA]))
            .count()
        )

    first = client.post(
        "/cementerio/inhumaciones/asistente/reservar-sepultura",
        data={
            "holder_document_number": "12345678Z",
            "holder_first_name": "Titular Uno",
            "beneficiary_document_number": "77777777T",
            "beneficiary_first_name": "Beneficiario Uno",
            "deceased_document_number": "90000001A",
            "deceased_first_name": "Difunto",
            "deceased_last_name": "Uno",
            "sepultura_id": str(sepultura_id),
            "holder_document_upload": (BytesIO(b"holder"), "holder.pdf"),
            "burial_license_upload": (BytesIO(b"license"), "license.pdf"),
        },
        content_type="multipart/form-data",
    )
    assert first.status_code == 201

    second = client.post(
        "/cementerio/inhumaciones/asistente/reservar-sepultura",
        data={
            "holder_document_number": "87654321X",
            "holder_first_name": "Titular Dos",
            "beneficiary_document_number": "88888888R",
            "beneficiary_first_name": "Beneficiario Dos",
            "deceased_document_number": "90000002B",
            "deceased_first_name": "Difunto",
            "deceased_last_name": "Dos",
            "sepultura_id": str(sepultura_id),
            "holder_document_upload": (BytesIO(b"holder2"), "holder2.pdf"),
            "burial_license_upload": (BytesIO(b"license2"), "license2.pdf"),
        },
        content_type="multipart/form-data",
    )
    assert second.status_code == 409
    second_payload = second.get_json()
    assert second_payload is not None
    assert second_payload["success"] is False
    assert "reserva activa" in second_payload["message"].lower()

    with app.app_context():
        open_reservations = (
            WorkOrder.query.filter_by(sepultura_id=sepultura_id, title="RESERVA", type_code="INHUMACION")
            .filter(WorkOrder.status.notin_([WorkOrderStatus.COMPLETADA, WorkOrderStatus.CANCELADA]))
            .count()
        )
        assert open_reservations == baseline_count + 1


def test_inhumation_assistant_extract_document_requires_login(client):
    response = client.post(
        "/cementerio/inhumaciones/asistente/extraer-documento",
        data={},
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert "/auth/login" in response.headers.get("Location", "")


def test_inhumation_assistant_extract_document_returns_400_without_file(
    app, client, login_admin
):
    login_admin()
    response = client.post(
        "/cementerio/inhumaciones/asistente/extraer-documento",
        data={},
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is False
    assert isinstance(payload["warnings"], list)
    assert payload["normalized_data"] == {}


def test_inhumation_assistant_extract_document_returns_400_for_bad_extension(
    app, client, login_admin
):
    login_admin()
    response = client.post(
        "/cementerio/inhumaciones/asistente/extraer-documento",
        data={"document": (BytesIO(b"dummy"), "certificado.txt")},
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is False
    assert payload["normalized_data"] == {}
    assert "formato" in " ".join(payload["warnings"]).lower()


def test_inhumation_assistant_extract_document_returns_200_with_mocked_service(
    app, client, login_admin, monkeypatch
):
    login_admin()
    import app.cemetery.routes as cemetery_routes

    def fake_extract(_file_obj):
        return {
            "success": True,
            "raw_text": "texto extraido",
            "fields_extracted": {"nombre_difunto": "Juan"},
            "normalized_data": {"first_name": "Juan"},
            "confidence": {"first_name": 0.93},
            "needs_review": True,
            "warnings": [],
        }

    monkeypatch.setattr(cemetery_routes, "extract_inhumation_document", fake_extract)

    response = client.post(
        "/cementerio/inhumaciones/asistente/extraer-documento",
        data={"document": (BytesIO(b"%PDF-1.4"), "certificado.pdf")},
        content_type="multipart/form-data",
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload is not None
    assert payload["success"] is True
    assert payload["raw_text"] == "texto extraido"
    assert payload["fields_extracted"]["nombre_difunto"] == "Juan"
    assert payload["normalized_data"]["first_name"] == "Juan"
    assert "confidence" in payload
    assert "needs_review" in payload
    assert "warnings" in payload


