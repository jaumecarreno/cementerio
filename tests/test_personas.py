from __future__ import annotations

from app.core.extensions import db
from app.core.models import Beneficiario, DerechoFunerarioContrato, OwnershipRecord, Person, Sepultura


def test_person_crud_list_and_search(app, client, login_admin):
    login_admin()

    list_response = client.get("/cementerio/personas")
    assert list_response.status_code == 200
    assert b"Personas" in list_response.data

    create_response = client.post(
        "/cementerio/personas/nueva",
        data={
            "nombre": "Eva",
            "apellidos": "Romero",
            "dni_nif": "55667788Z",
            "telefono": "600555444",
            "email": "eva.romero@example.com",
            "direccion": "Carrer Nou 15, Terrassa",
            "notas": "Alta desde test",
        },
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    assert b"Persona Eva Romero creada" in create_response.data

    with app.app_context():
        person = Person.query.filter_by(dni_nif="55667788Z").first()
        assert person is not None
        person_id = person.id

    search_response = client.get("/cementerio/personas?q=Eva")
    assert search_response.status_code == 200
    assert b"Eva Romero" in search_response.data

    edit_response = client.post(
        f"/cementerio/personas/{person_id}/editar",
        data={
            "nombre": "Eva",
            "apellidos": "Romero",
            "dni_nif": "55667788Z",
            "telefono": "600000000",
            "email": "eva.romero@example.com",
            "direccion": "Carrer Nou 15, Terrassa",
            "notas": "Actualizada",
        },
        follow_redirects=True,
    )
    assert edit_response.status_code == 200
    assert b"Persona actualizada" in edit_response.data

    with app.app_context():
        updated = db.session.get(Person, person_id)
        assert updated.telefono == "600000000"
        assert updated.notas == "Actualizada"


def test_person_picker_happy_path_contract_creation(app, client, login_admin):
    login_admin()

    with app.app_context():
        sep = Sepultura.query.filter_by(bloque="B-30", numero=510).first()
        assert sep is not None

    picker_create = client.post(
        "/cementerio/personas/picker/create",
        data={
            "picker_id": "contract-titular-test",
            "field_name": "titular_person_id",
            "nombre": "Nora",
            "apellidos": "Costa",
            "dni_nif": "88776655X",
        },
        headers={"HX-Request": "true"},
    )
    assert picker_create.status_code == 200
    assert b"Persona creada y seleccionada" in picker_create.data

    with app.app_context():
        titular = Person.query.filter_by(dni_nif="88776655X").first()
        assert titular is not None

    create_contract = client.post(
        f"/cementerio/sepulturas/{sep.id}/derecho/contratar",
        data={
            "tipo": "CONCESION",
            "fecha_inicio": "2026-01-01",
            "fecha_fin": "2040-01-01",
            "annual_fee_amount": "47.00",
            "titular_person_id": str(titular.id),
        },
        follow_redirects=True,
    )
    assert create_contract.status_code == 200
    assert b"Contrato creado correctamente" in create_contract.data

    with app.app_context():
        contract = DerechoFunerarioContrato.query.filter_by(sepultura_id=sep.id, estado="ACTIVO").first()
        assert contract is not None
        owner = OwnershipRecord.query.filter_by(contract_id=contract.id).order_by(OwnershipRecord.id.desc()).first()
        assert owner is not None
        assert owner.person_id == titular.id


def test_nominate_beneficiary_with_picker_person_id(app, client, login_admin):
    login_admin()

    with app.app_context():
        sep = Sepultura.query.filter_by(bloque="B-12", numero=127).first()
        contract = DerechoFunerarioContrato.query.filter_by(sepultura_id=sep.id, estado="ACTIVO").first()
        person = Person.query.filter_by(first_name="Lucia", last_name="Navarro").first()
        assert contract is not None
        assert person is not None

    response = client.post(
        f"/cementerio/contratos/{contract.id}/beneficiario/nombrar",
        data={
            "sepultura_id": str(sep.id),
            "person_id": str(person.id),
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"Beneficiario guardado" in response.data

    with app.app_context():
        active = (
            Beneficiario.query.filter_by(contrato_id=contract.id)
            .filter(Beneficiario.activo_hasta.is_(None))
            .order_by(Beneficiario.id.desc())
            .first()
        )
        assert active is not None
        assert active.person_id == person.id
