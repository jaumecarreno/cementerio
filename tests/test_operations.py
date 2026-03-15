from __future__ import annotations

from app.core.extensions import db
from app.core.models import (
    OperationCase,
    OperationDocument,
    OperationPermit,
    OperationPermitStatus,
    OperationStatus,
    OperationType,
    Person,
    Sepultura,
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
