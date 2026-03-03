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
        assert {"LICENCIA_ENTERRAMIENTO", "PERMISO_SANITARIO"} <= permit_types
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
