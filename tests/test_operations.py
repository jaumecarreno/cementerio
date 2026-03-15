from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.core.extensions import db
from app.core.models import (
    Beneficiario,
    Cemetery,
    DerechoFunerarioContrato,
    DerechoTipo,
    OperationCase,
    OperationDocument,
    OperationPermit,
    OperationPermitStatus,
    OperationStatus,
    OperationType,
    OwnershipRecord,
    Person,
    Sepultura,
    SepulturaEstado,
    SepulturaDifunto,
    WorkOrder,
    WorkOrderStatus,
)


def _move_ot_to_completed(client, ot_id: int) -> None:
    for status in [
        "PLANIFICADA",
        "ASIGNADA",
        "EN_CURSO",
        "EN_VALIDACION",
        "COMPLETADA",
    ]:
        response = client.post(
            f"/cementerio/ot/{ot_id}/estado",
            data={"status": status},
            follow_redirects=True,
        )
        assert response.status_code == 200


def _create_test_sepultura(
    org_id: int,
    cemetery_id: int,
    bloque: str,
    numero: int,
    estado: SepulturaEstado = SepulturaEstado.DISPONIBLE,
) -> Sepultura:
    sep = Sepultura(
        org_id=org_id,
        cemetery_id=cemetery_id,
        bloque=bloque,
        fila=1,
        columna=1,
        via="V-TEST",
        numero=numero,
        modalidad="Ninxol",
        estado=estado,
        tipo_bloque="Ninxols",
        tipo_lapida="Resina",
        orientacion="Nord",
    )
    db.session.add(sep)
    db.session.flush()
    return sep


def _prepare_case_for_close(app, client, case_id: int) -> None:
    response = client.post(
        f"/cementerio/expedientes/{case_id}/estado",
        data={"status": "DOCS_PENDIENTES"},
        follow_redirects=True,
    )
    assert response.status_code == 200

    with app.app_context():
        permits = OperationPermit.query.filter_by(operation_case_id=case_id).all()
    for permit in permits:
        verify_response = client.post(
            f"/cementerio/expedientes/{case_id}/permisos/{permit.id}/verify",
            data={"action": "verify"},
            follow_redirects=True,
        )
        assert verify_response.status_code == 200

    for status in ["PROGRAMADA", "EN_EJECUCION", "EN_VALIDACION"]:
        transition_response = client.post(
            f"/cementerio/expedientes/{case_id}/estado",
            data={"status": status},
            follow_redirects=True,
        )
        assert transition_response.status_code == 200

    with app.app_context():
        ot = WorkOrder.query.filter_by(operation_case_id=case_id).order_by(WorkOrder.id.desc()).first()
        assert ot is not None
        ot_id = ot.id
    _move_ot_to_completed(client, ot_id)
    with app.app_context():
        ot = db.session.get(WorkOrder, ot_id)
        assert ot is not None
        if ot.status != WorkOrderStatus.COMPLETADA:
            ot.status = WorkOrderStatus.COMPLETADA
            db.session.add(ot)
            db.session.commit()


def test_operations_new_case_box_is_collapsed_by_default(client, login_admin):
    login_admin()
    response = client.get("/cementerio/expedientes")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'id="expediente-create-toggle"' in html
    assert 'id="expediente-create-toggle" open' not in html


def test_operations_page_and_create_case(app, client, login_admin):
    login_admin()
    with app.app_context():
        sep = Sepultura.query.filter_by(bloque="B-12", numero=127).first()
        deceased = Person.query.filter_by(first_name="Antoni", last_name="Ferrer").first()
        assert sep is not None
        assert deceased is not None

    response = client.post(
        "/cementerio/expedientes",
        data={
            "type": "INHUMACION",
            "source_sepultura_id": str(sep.id),
            "deceased_person_id": str(deceased.id),
            "notes": "Expediente de prueba",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"Expediente OP-" in response.data

    with app.app_context():
        created = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert created is not None
        assert created.type == OperationType.INHUMACION
        permits = OperationPermit.query.filter_by(operation_case_id=created.id).all()
        permit_types = {row.permit_type for row in permits}
        assert {
            "DNI_TITULAR",
            "DNI_BENEFICIARIO",
            "DNI_DIFUNTO",
            "LICENCIA_ENTERRAMIENTO",
            "CERTIFICADO_DEFUNCION",
            "CERTIFICADO_MEDICO_DEFUNCION",
        } <= permit_types
        permit_required = {row.permit_type: row.required for row in permits}
        assert permit_required.get("DNI_TITULAR") is True
        assert permit_required.get("LICENCIA_ENTERRAMIENTO") is True
        assert permit_required.get("DNI_BENEFICIARIO") is False
        assert permit_required.get("DNI_DIFUNTO") is False
        assert permit_required.get("CERTIFICADO_DEFUNCION") is False
        assert permit_required.get("CERTIFICADO_MEDICO_DEFUNCION") is False
        docs = OperationDocument.query.filter_by(operation_case_id=created.id).all()
        acta = next((row for row in docs if row.doc_type == "ACTA_OPERACION"), None)
        assert acta is not None
        assert acta.required is True
        assert acta.status == OperationPermitStatus.MISSING


def test_traslado_type_mismatch_is_blocked(app, client, login_admin):
    login_admin()
    with app.app_context():
        sep = Sepultura.query.filter_by(bloque="B-12", numero=127).first()
        assert sep is not None

    response = client.post(
        "/cementerio/expedientes",
        data={
            "type": "TRASLADO_LARGO",
            "source_sepultura_id": str(sep.id),
            "destination_municipality": "Terrassa",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"Tipo de traslado invalido" in response.data


def test_programada_requires_verified_permits(app, client, login_admin):
    login_admin()
    with app.app_context():
        sep = Sepultura.query.filter_by(bloque="B-12", numero=127).first()
        assert sep is not None

    create = client.post(
        "/cementerio/expedientes",
        data={
            "type": "TRASLADO_CORTO",
            "source_sepultura_id": str(sep.id),
            "destination_municipality": "Terrassa",
        },
        follow_redirects=True,
    )
    assert create.status_code == 200

    with app.app_context():
        case = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert case is not None
        case_id = case.id

    status_docs = client.post(
        f"/cementerio/expedientes/{case_id}/estado",
        data={"status": "DOCS_PENDIENTES"},
        follow_redirects=True,
    )
    assert status_docs.status_code == 200

    status_programada = client.post(
        f"/cementerio/expedientes/{case_id}/estado",
        data={"status": "PROGRAMADA"},
        follow_redirects=True,
    )
    assert status_programada.status_code == 200
    assert b"faltan permisos verificados" in status_programada.data

    with app.app_context():
        case = db.session.get(OperationCase, case_id)
        assert case.status == OperationStatus.DOCS_PENDIENTES


def test_expediente_detail_select_reflects_current_status(app, client, login_admin):
    login_admin()
    with app.app_context():
        sep = Sepultura.query.filter_by(bloque="B-12", numero=127).first()
        assert sep is not None

    create = client.post(
        "/cementerio/expedientes",
        data={
            "type": "TRASLADO_CORTO",
            "source_sepultura_id": str(sep.id),
            "destination_municipality": "Terrassa",
        },
        follow_redirects=True,
    )
    assert create.status_code == 200

    with app.app_context():
        case = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert case is not None
        case_id = case.id

    update = client.post(
        f"/cementerio/expedientes/{case_id}/estado",
        data={"status": "DOCS_PENDIENTES"},
        follow_redirects=True,
    )
    assert update.status_code == 200

    detail = client.get(f"/cementerio/expedientes/{case_id}")
    assert detail.status_code == 200
    assert b'value="DOCS_PENDIENTES" selected' in detail.data


def test_expediente_inhumacion_summary_update_with_pickers(app, client, login_admin):
    login_admin()
    with app.app_context():
        source = Sepultura.query.filter_by(bloque="B-12", numero=127).first()
        replacement = Sepultura.query.filter_by(bloque="B-12", numero=128).first()
        deceased = Person.query.filter_by(first_name="Antoni", last_name="Ferrer").first()
        holder = Person.query.filter_by(first_name="Marta", last_name="Soler").first()
        beneficiary = Person.query.filter_by(first_name="Joan", last_name="Riera").first()
        assert source is not None
        assert replacement is not None
        assert deceased is not None
        assert holder is not None
        assert beneficiary is not None

    create = client.post(
        "/cementerio/expedientes",
        data={
            "type": "INHUMACION",
            "source_sepultura_id": str(source.id),
            "deceased_person_id": str(deceased.id),
        },
        follow_redirects=True,
    )
    assert create.status_code == 200

    with app.app_context():
        case = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert case is not None
        case_id = case.id

    update = client.post(
        f"/cementerio/expedientes/{case_id}/resumen",
        data={
            "source_sepultura_id": str(replacement.id),
            "deceased_person_id": str(deceased.id),
            "holder_person_id": str(holder.id),
            "beneficiary_person_id": str(beneficiary.id),
        },
        follow_redirects=True,
    )
    assert update.status_code == 200
    assert b"Resumen del expediente actualizado" in update.data

    with app.app_context():
        refreshed = db.session.get(OperationCase, case_id)
        assert refreshed is not None
        assert refreshed.source_sepultura_id == replacement.id
        assert refreshed.deceased_person_id == deceased.id
        assert refreshed.holder_person_id == holder.id
        assert refreshed.beneficiary_person_id == beneficiary.id
        assert refreshed.declarant_person_id == holder.id

    detail = client.get(f"/cementerio/expedientes/{case_id}")
    assert detail.status_code == 200
    html = detail.get_data(as_text=True)
    assert "Documentación" in html
    assert "DNI titular" in html
    assert "estado=LLIURE" in html


def test_expediente_summary_requires_holder_for_inhumacion(app, client, login_admin):
    login_admin()
    with app.app_context():
        source = Sepultura.query.filter_by(bloque="B-12", numero=127).first()
        deceased = Person.query.filter_by(first_name="Antoni", last_name="Ferrer").first()
        assert source is not None
        assert deceased is not None

    create = client.post(
        "/cementerio/expedientes",
        data={
            "type": "INHUMACION",
            "source_sepultura_id": str(source.id),
            "deceased_person_id": str(deceased.id),
        },
        follow_redirects=True,
    )
    assert create.status_code == 200

    with app.app_context():
        case = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert case is not None
        case_id = case.id
        previous_holder = case.holder_person_id

    update = client.post(
        f"/cementerio/expedientes/{case_id}/resumen",
        data={
            "source_sepultura_id": str(source.id),
            "deceased_person_id": str(deceased.id),
            "holder_person_id": "",
            "beneficiary_person_id": "",
        },
        follow_redirects=True,
    )
    assert update.status_code == 200
    assert b"Debes seleccionar un titular" in update.data

    with app.app_context():
        refreshed = db.session.get(OperationCase, case_id)
        assert refreshed is not None
        assert refreshed.holder_person_id == previous_holder


def test_close_traslado_flow_requires_completed_ot_and_generates_acta(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        source = Sepultura.query.filter_by(bloque="B-12", numero=127).first()
        target = Sepultura.query.filter_by(bloque="B-12", numero=128).first()
        assert source is not None
        assert target is not None
        source_before = SepulturaDifunto.query.filter_by(sepultura_id=source.id).count()
        target_before = SepulturaDifunto.query.filter_by(sepultura_id=target.id).count()

    create = client.post(
        "/cementerio/expedientes",
        data={
            "type": "TRASLADO_CORTO",
            "source_sepultura_id": str(source.id),
            "target_sepultura_id": str(target.id),
            "destination_municipality": "Terrassa",
        },
        follow_redirects=True,
    )
    assert create.status_code == 200
    with app.app_context():
        case = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert case is not None
        case_id = case.id

    client.post(
        f"/cementerio/expedientes/{case_id}/estado",
        data={"status": "DOCS_PENDIENTES"},
        follow_redirects=True,
    )
    with app.app_context():
        permits = OperationPermit.query.filter_by(operation_case_id=case_id).all()
    for permit in permits:
        response = client.post(
            f"/cementerio/expedientes/{case_id}/permisos/{permit.id}/verify",
            data={"action": "verify"},
            follow_redirects=True,
        )
        assert response.status_code == 200

    client.post(
        f"/cementerio/expedientes/{case_id}/estado",
        data={"status": "PROGRAMADA"},
        follow_redirects=True,
    )
    client.post(
        f"/cementerio/expedientes/{case_id}/estado",
        data={"status": "EN_EJECUCION"},
        follow_redirects=True,
    )
    client.post(
        f"/cementerio/expedientes/{case_id}/estado",
        data={"status": "EN_VALIDACION"},
        follow_redirects=True,
    )

    close_without_ot = client.post(
        f"/cementerio/expedientes/{case_id}/cerrar",
        data={"reason": "intento sin ot"},
        follow_redirects=True,
    )
    assert close_without_ot.status_code == 200
    assert b"no hay OT completada" in close_without_ot.data

    with app.app_context():
        ot = WorkOrder.query.filter_by(operation_case_id=case_id).order_by(WorkOrder.id.desc()).first()
        assert ot is not None
        ot_id = ot.id
    _move_ot_to_completed(client, ot_id)

    close_ok = client.post(
        f"/cementerio/expedientes/{case_id}/cerrar",
        data={"reason": "cierre operativo"},
        follow_redirects=True,
    )
    assert close_ok.status_code == 200
    assert b"Expediente cerrado" in close_ok.data

    with app.app_context():
        refreshed = db.session.get(OperationCase, case_id)
        assert refreshed.status == OperationStatus.CERRADA
        acta = (
            OperationDocument.query.filter_by(operation_case_id=case_id, doc_type="ACTA_OPERACION")
            .order_by(OperationDocument.id.asc())
            .first()
        )
        assert acta is not None
        assert acta.status == OperationPermitStatus.VERIFIED
        assert acta.file_path
        source_after = SepulturaDifunto.query.filter_by(sepultura_id=source.id).count()
        target_after = SepulturaDifunto.query.filter_by(sepultura_id=target.id).count()
        assert source_after == source_before - 1
        assert target_after == target_before + 1


def test_rescate_close_is_blocked_without_remains(app, client, login_admin):
    login_admin()
    with app.app_context():
        source = Sepultura.query.filter_by(bloque="B-20", numero=210).first()
        assert source is not None
        assert SepulturaDifunto.query.filter_by(sepultura_id=source.id).count() == 0

    create = client.post(
        "/cementerio/expedientes",
        data={
            "type": "RESCATE",
            "source_sepultura_id": str(source.id),
        },
        follow_redirects=True,
    )
    assert create.status_code == 200
    with app.app_context():
        case = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert case is not None
        case_id = case.id

    client.post(
        f"/cementerio/expedientes/{case_id}/estado",
        data={"status": "DOCS_PENDIENTES"},
        follow_redirects=True,
    )
    with app.app_context():
        permits = OperationPermit.query.filter_by(operation_case_id=case_id).all()
    for permit in permits:
        client.post(
            f"/cementerio/expedientes/{case_id}/permisos/{permit.id}/verify",
            data={"action": "verify"},
            follow_redirects=True,
        )

    client.post(
        f"/cementerio/expedientes/{case_id}/estado",
        data={"status": "PROGRAMADA"},
        follow_redirects=True,
    )
    client.post(
        f"/cementerio/expedientes/{case_id}/estado",
        data={"status": "EN_EJECUCION"},
        follow_redirects=True,
    )
    client.post(
        f"/cementerio/expedientes/{case_id}/estado",
        data={"status": "EN_VALIDACION"},
        follow_redirects=True,
    )

    with app.app_context():
        ot = WorkOrder.query.filter_by(operation_case_id=case_id).order_by(WorkOrder.id.desc()).first()
        assert ot is not None
        ot_id = ot.id
    _move_ot_to_completed(client, ot_id)

    close_response = client.post(
        f"/cementerio/expedientes/{case_id}/cerrar",
        data={"reason": "cierre rescate"},
        follow_redirects=True,
    )
    assert close_response.status_code == 200
    assert b"no hay restos previos" in close_response.data

    with app.app_context():
        refreshed = db.session.get(OperationCase, case_id)
        assert refreshed.status == OperationStatus.EN_VALIDACION
        ot = db.session.get(WorkOrder, ot_id)
        assert ot.status == WorkOrderStatus.COMPLETADA


def test_inhumacion_detail_shows_documentos_2_and_concesion_defaults(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        cemetery = Cemetery.query.order_by(Cemetery.id.asc()).first()
        assert cemetery is not None
        sep = _create_test_sepultura(
            cemetery.org_id,
            cemetery.id,
            bloque="OP-CONC-DET",
            numero=9001,
            estado=SepulturaEstado.DISPONIBLE,
        )
        deceased = Person.query.filter_by(first_name="Antoni", last_name="Ferrer").first()
        assert deceased is not None
        sep_id = sep.id
        deceased_id = deceased.id
        db.session.commit()

    create_response = client.post(
        "/cementerio/expedientes",
        data={
            "type": "INHUMACION",
            "source_sepultura_id": str(sep_id),
            "deceased_person_id": str(deceased_id),
        },
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    assert b"Documentos 2" in create_response.data
    assert b"Concesion" in create_response.data

    with app.app_context():
        case = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert case is not None
        assert case.type == OperationType.INHUMACION
        today = date.today()
        try:
            expected_end = today.replace(year=today.year + 25)
        except ValueError:
            expected_end = today.replace(month=2, day=28, year=today.year + 25)
        assert case.concession_start_date == today
        assert case.concession_end_date == expected_end
        assert case.concession_duration_years == 25


def test_inhumacion_concession_update_persists_dates(app, client, login_admin):
    login_admin()
    with app.app_context():
        cemetery = Cemetery.query.order_by(Cemetery.id.asc()).first()
        assert cemetery is not None
        sep = _create_test_sepultura(
            cemetery.org_id,
            cemetery.id,
            bloque="OP-CONC-UPD",
            numero=9002,
            estado=SepulturaEstado.DISPONIBLE,
        )
        deceased = Person.query.filter_by(first_name="Antoni", last_name="Ferrer").first()
        assert deceased is not None
        sep_id = sep.id
        deceased_id = deceased.id
        db.session.commit()

    client.post(
        "/cementerio/expedientes",
        data={
            "type": "INHUMACION",
            "source_sepultura_id": str(sep_id),
            "deceased_person_id": str(deceased_id),
        },
        follow_redirects=True,
    )
    with app.app_context():
        case = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert case is not None
        case_id = case.id

    update_response = client.post(
        f"/cementerio/expedientes/{case_id}/concesion",
        data={
            "concession_start_date": "2026-01-01",
            "concession_end_date": "2051-01-01",
        },
        follow_redirects=True,
    )
    assert update_response.status_code == 200
    assert b"Concesion actualizada" in update_response.data

    with app.app_context():
        refreshed = db.session.get(OperationCase, case_id)
        assert refreshed is not None
        assert refreshed.concession_start_date == date(2026, 1, 1)
        assert refreshed.concession_end_date == date(2051, 1, 1)
        assert refreshed.concession_duration_years == 25


def test_inhumacion_with_active_contract_shows_readonly_concession_and_rejects_edit(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        active_contract = (
            DerechoFunerarioContrato.query.filter_by(estado="ACTIVO")
            .order_by(DerechoFunerarioContrato.id.asc())
            .first()
        )
        deceased = Person.query.filter_by(first_name="Antoni", last_name="Ferrer").first()
        assert active_contract is not None
        assert deceased is not None
        contract_id = active_contract.id
        sepultura_id = active_contract.sepultura_id
        deceased_id = deceased.id

    client.post(
        "/cementerio/expedientes",
        data={
            "type": "INHUMACION",
            "source_sepultura_id": str(sepultura_id),
            "deceased_person_id": str(deceased_id),
        },
        follow_redirects=True,
    )
    with app.app_context():
        case = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert case is not None
        case_id = case.id

    detail_response = client.get(f"/cementerio/expedientes/{case_id}")
    assert detail_response.status_code == 200
    html = detail_response.get_data(as_text=True)
    assert f"Contrato activo reutilizado: C{contract_id}" in html
    assert 'name="concession_start_date"' in html
    assert "disabled" in html

    update_response = client.post(
        f"/cementerio/expedientes/{case_id}/concesion",
        data={
            "concession_start_date": "2028-01-01",
            "concession_end_date": "2053-01-01",
        },
        follow_redirects=True,
    )
    assert update_response.status_code == 200
    assert b"no puede editarse" in update_response.data

    with app.app_context():
        refreshed = db.session.get(OperationCase, case_id)
        assert refreshed is not None
        assert refreshed.concession_start_date != date(2028, 1, 1)
        assert refreshed.concession_end_date != date(2053, 1, 1)


def test_close_inhumacion_without_contract_creates_and_links_concession(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        cemetery = Cemetery.query.order_by(Cemetery.id.asc()).first()
        deceased = Person.query.filter_by(first_name="Antoni", last_name="Ferrer").first()
        holder = Person.query.filter_by(first_name="Marta", last_name="Soler").first()
        beneficiary = Person.query.filter_by(first_name="Joan", last_name="Riera").first()
        assert cemetery is not None
        assert deceased is not None
        assert holder is not None
        assert beneficiary is not None
        sep = _create_test_sepultura(
            cemetery.org_id,
            cemetery.id,
            bloque="OP-CONC-CLOSE-N",
            numero=9003,
            estado=SepulturaEstado.DISPONIBLE,
        )
        sep_id = sep.id
        deceased_id = deceased.id
        holder_id = holder.id
        beneficiary_id = beneficiary.id
        db.session.commit()

    create_response = client.post(
        "/cementerio/expedientes",
        data={
            "type": "INHUMACION",
            "source_sepultura_id": str(sep_id),
            "deceased_person_id": str(deceased_id),
            "holder_person_id": str(holder_id),
            "beneficiary_person_id": str(beneficiary_id),
        },
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    with app.app_context():
        case = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert case is not None
        case_id = case.id

    _prepare_case_for_close(app, client, case_id)
    close_response = client.post(
        f"/cementerio/expedientes/{case_id}/cerrar",
        data={"reason": "cierre inhumacion"},
        follow_redirects=True,
    )
    assert close_response.status_code == 200
    assert b"Expediente cerrado" in close_response.data

    with app.app_context():
        refreshed = db.session.get(OperationCase, case_id)
        assert refreshed is not None
        assert refreshed.status == OperationStatus.CERRADA
        assert refreshed.contract_id is not None

        contract = db.session.get(DerechoFunerarioContrato, refreshed.contract_id)
        assert contract is not None
        assert contract.tipo == DerechoTipo.CONCESION
        assert contract.annual_fee_amount == Decimal("0.00")
        assert contract.fecha_inicio == refreshed.concession_start_date
        assert contract.fecha_fin == refreshed.concession_end_date

        owner = OwnershipRecord.query.filter_by(contract_id=contract.id, person_id=holder_id).first()
        assert owner is not None
        beneficiary_row = Beneficiario.query.filter_by(contrato_id=contract.id, person_id=beneficiary_id).first()
        assert beneficiary_row is not None


def test_close_inhumacion_with_active_contract_reuses_existing_contract(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        active_contract = (
            DerechoFunerarioContrato.query.filter_by(estado="ACTIVO")
            .order_by(DerechoFunerarioContrato.id.asc())
            .first()
        )
        deceased = Person.query.filter_by(first_name="Antoni", last_name="Ferrer").first()
        holder = Person.query.filter_by(first_name="Marta", last_name="Soler").first()
        assert active_contract is not None
        assert deceased is not None
        assert holder is not None
        contract_id = active_contract.id
        sepultura_id = active_contract.sepultura_id
        deceased_id = deceased.id
        holder_id = holder.id
        before_count = DerechoFunerarioContrato.query.filter_by(
            sepultura_id=sepultura_id
        ).count()

    create_response = client.post(
        "/cementerio/expedientes",
        data={
            "type": "INHUMACION",
            "source_sepultura_id": str(sepultura_id),
            "deceased_person_id": str(deceased_id),
            "holder_person_id": str(holder_id),
        },
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    with app.app_context():
        case = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert case is not None
        case_id = case.id

    _prepare_case_for_close(app, client, case_id)
    close_response = client.post(
        f"/cementerio/expedientes/{case_id}/cerrar",
        data={"reason": "cierre con contrato existente"},
        follow_redirects=True,
    )
    assert close_response.status_code == 200

    with app.app_context():
        refreshed = db.session.get(OperationCase, case_id)
        assert refreshed is not None
        assert refreshed.status == OperationStatus.CERRADA
        assert refreshed.contract_id == contract_id
        after_count = DerechoFunerarioContrato.query.filter_by(
            sepultura_id=sepultura_id
        ).count()
        assert after_count == before_count


def test_non_inhumacion_has_no_concession_card_and_concession_endpoint_fails(
    app, client, login_admin
):
    login_admin()
    with app.app_context():
        source = Sepultura.query.filter_by(bloque="B-12", numero=127).first()
        target = Sepultura.query.filter_by(bloque="B-12", numero=128).first()
        assert source is not None
        assert target is not None

    create_response = client.post(
        "/cementerio/expedientes",
        data={
            "type": "TRASLADO_CORTO",
            "source_sepultura_id": str(source.id),
            "target_sepultura_id": str(target.id),
            "destination_municipality": "Terrassa",
        },
        follow_redirects=True,
    )
    assert create_response.status_code == 200
    with app.app_context():
        case = OperationCase.query.order_by(OperationCase.id.desc()).first()
        assert case is not None
        case_id = case.id

    detail_response = client.get(f"/cementerio/expedientes/{case_id}")
    assert detail_response.status_code == 200
    assert b"Concesion" not in detail_response.data

    update_response = client.post(
        f"/cementerio/expedientes/{case_id}/concesion",
        data={
            "concession_start_date": "2026-01-01",
            "concession_end_date": "2051-01-01",
        },
        follow_redirects=True,
    )
    assert update_response.status_code == 200
    assert b"Solo INHUMACION permite editar la concesion" in update_response.data

    with app.app_context():
        refreshed = db.session.get(OperationCase, case_id)
        assert refreshed is not None
        assert refreshed.concession_start_date is None
        assert refreshed.concession_end_date is None
