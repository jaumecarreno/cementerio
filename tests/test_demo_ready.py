from __future__ import annotations

from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
import subprocess
import sys

from app.core.extensions import db
from app.core.models import (
    Beneficiario,
    CaseDocument,
    CaseDocumentStatus,
    DerechoFunerarioContrato,
    Expediente,
    Invoice,
    InscripcionLateral,
    LapidaStock,
    MovimientoSepultura,
    MovimientoTipo,
    OrdenTrabajo,
    OwnershipRecord,
    OwnershipTransferCase,
    OwnershipTransferStatus,
    OwnershipTransferType,
    Payment,
    Person,
    Publication,
    Sepultura,
    SepulturaDifunto,
    TasaMantenimientoTicket,
    TicketEstado,
)


def _contract_for_sepultura_128(app):
    with app.app_context():
        sep = Sepultura.query.filter_by(bloque="B-12", numero=128).first()
        contract = DerechoFunerarioContrato.query.filter_by(sepultura_id=sep.id, estado="ACTIVO").first()
        return sep.id, contract.id


def _create_case(client, contract_id: int):
    response = client.post(
        "/cementerio/titularidad/casos",
        data={"contract_id": str(contract_id), "type": "INTER_VIVOS"},
        follow_redirects=True,
    )
    assert response.status_code == 200


def test_navigation_main_menu_routes_no_404(app, client, login_admin):
    login_admin()
    paths = [
        "/cementerio/panel",
        "/cementerio/sepulturas/buscar",
        "/cementerio/expedientes",
        "/cementerio/titularidad",
        "/cementerio/personas",
        "/cementerio/lapidas",
        "/cementerio/reporting",
        "/config",
        "/demo",
        "/modulo/servicios-funerarios",
    ]
    for path in paths:
        response = client.get(path, follow_redirects=True)
        assert response.status_code == 200, path


def test_ui_audit_script_passes():
    repo_root = Path(__file__).resolve().parents[1]
    cmd = [sys.executable, "scripts/dev_ui_audit.py"]
    result = subprocess.run(cmd, cwd=repo_root, check=False, capture_output=True, text=True)
    assert result.returncode == 0
    assert "UI audit passed" in result.stdout


def test_pensionista_non_retroactive_default(app, client, login_admin):
    login_admin()
    sep_id, contract_id = _contract_for_sepultura_128(app)
    past_day = (date.today() - timedelta(days=7)).isoformat()

    response = client.post(
        f"/cementerio/contratos/{contract_id}/titular/pensionista",
        data={"sepultura_id": sep_id, "since_date": past_day},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"no retroactivo por defecto" in response.data

    with app.app_context():
        holder = (
            OwnershipRecord.query.filter_by(contract_id=contract_id)
            .filter(OwnershipRecord.end_date.is_(None))
            .first()
        )
        assert holder.is_pensioner is False


def test_beneficiario_single_active_crud(app, client, login_admin):
    login_admin()
    sep_id, contract_id = _contract_for_sepultura_128(app)

    client.post(
        f"/cementerio/contratos/{contract_id}/beneficiario/nombrar",
        data={
            "sepultura_id": sep_id,
            "first_name": "Primero",
            "last_name": "Benef",
            "document_id": "BEN-1",
        },
        follow_redirects=True,
    )
    client.post(
        f"/cementerio/contratos/{contract_id}/beneficiario/nombrar",
        data={
            "sepultura_id": sep_id,
            "first_name": "Segundo",
            "last_name": "Benef",
            "document_id": "BEN-2",
        },
        follow_redirects=True,
    )

    with app.app_context():
        active = (
            Beneficiario.query.filter_by(contrato_id=contract_id)
            .filter(Beneficiario.activo_hasta.is_(None))
            .all()
        )
        assert len(active) == 1

    client.post(
        f"/cementerio/contratos/{contract_id}/beneficiario/eliminar",
        data={"sepultura_id": sep_id},
        follow_redirects=True,
    )

    with app.app_context():
        active = (
            Beneficiario.query.filter_by(contrato_id=contract_id)
            .filter(Beneficiario.activo_hasta.is_(None))
            .all()
        )
        assert len(active) == 0


def test_movements_created_for_pensioner_and_beneficiary_actions(app, client, login_admin):
    login_admin()
    sep_id, contract_id = _contract_for_sepultura_128(app)
    future_day = (date.today() + timedelta(days=1)).isoformat()

    client.post(
        f"/cementerio/contratos/{contract_id}/beneficiario/nombrar",
        data={
            "sepultura_id": sep_id,
            "first_name": "Alba",
            "last_name": "Mora",
            "document_id": "BEN-MOV-1",
        },
        follow_redirects=True,
    )
    client.post(
        f"/cementerio/contratos/{contract_id}/titular/pensionista",
        data={"sepultura_id": sep_id, "since_date": future_day},
        follow_redirects=True,
    )
    client.post(
        f"/cementerio/contratos/{contract_id}/beneficiario/eliminar",
        data={"sepultura_id": sep_id},
        follow_redirects=True,
    )

    with app.app_context():
        movement_types = {
            row.tipo
            for row in MovimientoSepultura.query.filter_by(sepultura_id=sep_id).order_by(MovimientoSepultura.id.desc()).limit(10)
        }
        assert MovimientoTipo.BENEFICIARIO in movement_types
        assert MovimientoTipo.PENSIONISTA in movement_types


def test_ownership_case_document_download(app, client, login_admin):
    login_admin()
    with app.app_context():
        contract = DerechoFunerarioContrato.query.join(Sepultura).filter(Sepultura.numero == 127).first()

    _create_case(client, contract.id)

    with app.app_context():
        case = (
            OwnershipTransferCase.query.filter_by(contract_id=contract.id, type=OwnershipTransferType.INTER_VIVOS)
            .order_by(OwnershipTransferCase.id.desc())
            .first()
        )
        doc = CaseDocument.query.filter_by(case_id=case.id).order_by(CaseDocument.id.asc()).first()

    upload = client.post(
        f"/cementerio/titularidad/casos/{case.id}/documents/{doc.id}/upload",
        data={"file": (BytesIO(b"doc-content"), "certificado.txt")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert upload.status_code == 200

    response = client.get(f"/cementerio/titularidad/casos/{case.id}/documents/{doc.id}/download")
    assert response.status_code == 200
    assert response.data == b"doc-content"


def test_expediente_create_transition_ot_and_pdf(app, client, login_admin):
    login_admin()
    with app.app_context():
        sep = Sepultura.query.filter_by(bloque="B-12", numero=127).first()

    create = client.post(
        "/cementerio/expedientes",
        data={"tipo": "INHUMACION", "sepultura_id": sep.id, "notas": "demo"},
        follow_redirects=True,
    )
    assert create.status_code == 200
    assert b"Expediente" in create.data

    with app.app_context():
        expediente = Expediente.query.filter_by(sepultura_id=sep.id, tipo="INHUMACION").order_by(Expediente.id.desc()).first()

    ok_transition = client.post(
        f"/cementerio/expedientes/{expediente.id}/estado",
        data={"estado": "EN_TRAMITE"},
        follow_redirects=True,
    )
    assert ok_transition.status_code == 200

    bad_transition = client.post(
        f"/cementerio/expedientes/{expediente.id}/estado",
        data={"estado": "ABIERTO"},
        follow_redirects=True,
    )
    assert bad_transition.status_code == 200
    assert b"Transicion invalida" in bad_transition.data

    create_ot = client.post(
        f"/cementerio/expedientes/{expediente.id}/ot",
        data={"titulo": "OT demo"},
        follow_redirects=True,
    )
    assert create_ot.status_code == 200

    with app.app_context():
        ot = OrdenTrabajo.query.filter_by(expediente_id=expediente.id).order_by(OrdenTrabajo.id.desc()).first()

    complete_ot = client.post(
        f"/cementerio/expedientes/{expediente.id}/ot/{ot.id}/completar",
        data={"notes": "done"},
        follow_redirects=True,
    )
    assert complete_ot.status_code == 200

    pdf = client.get(f"/cementerio/expedientes/{expediente.id}/ot/{ot.id}/orden.pdf")
    assert pdf.status_code == 200
    assert pdf.headers["Content-Type"].startswith("application/pdf")


def test_lapidas_flow_and_stock_non_negative(app, client, login_admin, second_org_sepultura):
    login_admin()

    entry = client.post(
        "/cementerio/lapidas/stock/entrada",
        data={"codigo": "LAP-TST", "descripcion": "Test", "quantity": "2"},
        follow_redirects=True,
    )
    assert entry.status_code == 200

    with app.app_context():
        stock = LapidaStock.query.filter_by(codigo="LAP-TST").first()
        assert stock.available_qty == 2

    bad_exit = client.post(
        "/cementerio/lapidas/stock/salida",
        data={"stock_id": str(stock.id), "quantity": "5"},
        follow_redirects=True,
    )
    assert bad_exit.status_code == 200
    assert b"No hay stock suficiente" in bad_exit.data

    isolate_exit = client.post(
        "/cementerio/lapidas/stock/salida",
        data={"stock_id": str(stock.id), "quantity": "1", "sepultura_id": str(second_org_sepultura)},
        follow_redirects=True,
    )
    assert isolate_exit.status_code == 200
    assert b"Sepultura no encontrada" in isolate_exit.data

    with app.app_context():
        sep = Sepultura.query.filter_by(bloque="B-12", numero=127).first()

    create_inscripcion = client.post(
        "/cementerio/lapidas/inscripciones",
        data={"sepultura_id": str(sep.id), "texto": "Nueva placa"},
        follow_redirects=True,
    )
    assert create_inscripcion.status_code == 200

    with app.app_context():
        item = InscripcionLateral.query.filter_by(sepultura_id=sep.id, texto="Nueva placa").first()

    for expected in ["PENDIENTE_COLOCAR", "PENDIENTE_NOTIFICAR", "NOTIFICADA"]:
        response = client.post(
            f"/cementerio/lapidas/inscripciones/{item.id}/estado",
            data={},
            follow_redirects=True,
        )
        assert response.status_code == 200
        with app.app_context():
            refreshed = db.session.get(InscripcionLateral, item.id)
            assert refreshed.estado == expected


def test_reporting_csv_filters_and_pagination_tenant_isolation(app, client, login_admin, second_org_sepultura):
    login_admin()

    for report_key, expected_header in [
        ("sepulturas", "sepultura"),
        ("contratos", "vigencia"),
        ("deuda", "deuda_total"),
    ]:
        response = client.get(f"/cementerio/reporting/export.csv?report={report_key}")
        assert response.status_code == 200
        assert response.headers["Content-Type"].startswith("text/csv")
        assert expected_header.encode() in response.data.splitlines()[0]

    filtered = client.get("/cementerio/reporting/export.csv?report=sepulturas&bloque=B-12")
    assert filtered.status_code == 200
    assert b"B-12" in filtered.data
    assert b"ORG2" not in filtered.data

    paged = client.get("/cementerio/reporting?report=sepulturas&page_size=1")
    assert paged.status_code == 200
    assert b"Pagina:" in paged.data or "P\u00e1gina:".encode() in paged.data


def test_operator_rbac_readonly_titularidad_and_config_but_expedientes_allowed(app, client, login_operator):
    login_operator()

    with app.app_context():
        contract = DerechoFunerarioContrato.query.order_by(DerechoFunerarioContrato.id.asc()).first()
        sep = Sepultura.query.order_by(Sepultura.id.asc()).first()

    read_titularidad = client.get("/cementerio/titularidad/casos")
    assert read_titularidad.status_code == 200

    mutate_case = client.post(
        "/cementerio/titularidad/casos",
        data={"contract_id": str(contract.id), "type": "INTER_VIVOS"},
    )
    assert mutate_case.status_code == 403

    mutate_beneficiary = client.post(
        f"/cementerio/contratos/{contract.id}/beneficiario/nombrar",
        data={"sepultura_id": str(sep.id), "first_name": "NoPerm"},
    )
    assert mutate_beneficiary.status_code == 403

    config_read = client.get("/config")
    assert config_read.status_code == 200

    expediente_flow = client.post(
        "/cementerio/expedientes",
        data={"tipo": "INHUMACION", "sepultura_id": str(sep.id), "notas": "operator"},
        follow_redirects=True,
    )
    assert expediente_flow.status_code == 200


def test_sidebar_menu_contains_only_expected_links_and_no_top_tabs(app, client, login_admin):
    login_admin()
    response = client.get("/dashboard")
    assert response.status_code == 200

    html = response.data
    start = html.find(b'<aside class="sidebar">')
    end = html.find(b"</aside>", start)
    assert start != -1
    assert end != -1
    sidebar = html[start:end]

    expected_hrefs = [
        b'href="/dashboard"',
        b'href="/cementerio/sepulturas/buscar"',
        b'href="/cementerio/expedientes"',
        b'href="/cementerio/tasas"',
        b'href="/cementerio/titularidad"',
        b'href="/cementerio/reporting"',
        b'href="/demo"',
    ]
    positions = [sidebar.find(href) for href in expected_hrefs]
    assert all(pos != -1 for pos in positions)
    assert positions == sorted(positions)

    assert b'href="/config"' not in sidebar
    assert b'href="/modulo/servicios-funerarios"' not in sidebar
    assert b"cem-submenu" not in html


def test_demo_page_access_for_admin_and_operator(app, client, login_admin, login_operator):
    login_admin()
    admin_response = client.get("/demo")
    assert admin_response.status_code == 200

    client.post("/auth/logout", follow_redirects=True)
    login_operator()
    operator_response = client.get("/demo")
    assert operator_response.status_code == 200


def test_demo_operator_cannot_execute_actions(app, client, login_operator):
    login_operator()
    reset_response = client.post("/demo/reset", follow_redirects=False)
    assert reset_response.status_code == 403

    load_response = client.post("/demo/load-initial", follow_redirects=False)
    assert load_response.status_code == 403


def test_demo_page_uses_native_confirm_for_both_actions(app, client, login_admin):
    login_admin()
    response = client.get("/demo")
    assert response.status_code == 200
    assert b"return confirm(" in response.data
    assert b'action="/demo/reset"' in response.data
    assert b'action="/demo/load-initial"' in response.data


def test_demo_admin_can_load_initial_dataset_and_reset_to_zero(app, client, login_admin):
    login_admin()

    load_response = client.post("/demo/load-initial", follow_redirects=True)
    assert load_response.status_code == 200

    with app.app_context():
        assert Person.query.count() == 480
        assert Sepultura.query.count() == 350
        assert DerechoFunerarioContrato.query.count() == 300
        assert OwnershipRecord.query.filter(OwnershipRecord.end_date.is_(None)).count() == 300
        assert Beneficiario.query.filter(Beneficiario.activo_hasta.is_(None)).count() == 180
        assert Beneficiario.query.filter(Beneficiario.activo_hasta.is_not(None)).count() == 30
        assert SepulturaDifunto.query.count() == 180
        assert Expediente.query.count() == 140
        assert OrdenTrabajo.query.count() == 220
        assert OwnershipTransferCase.query.count() == 90
        assert TasaMantenimientoTicket.query.count() == 360
        assert Invoice.query.count() > 0
        assert Payment.query.count() > 0
        assert CaseDocument.query.count() > 0
        assert Publication.query.count() > 0
        assert (
            Expediente.query.filter(Expediente.declarante_id.is_not(None)).count() > 0
        )
        case_types = {row.type for row in OwnershipTransferCase.query.all()}
        assert OwnershipTransferType.MORTIS_CAUSA_CON_BENEFICIARIO in case_types
        blocked_scenarios = (
            db.session.query(OwnershipRecord)
            .join(DerechoFunerarioContrato, DerechoFunerarioContrato.id == OwnershipRecord.contract_id)
            .join(SepulturaDifunto, SepulturaDifunto.sepultura_id == DerechoFunerarioContrato.sepultura_id)
            .filter(OwnershipRecord.end_date.is_(None))
            .filter(OwnershipRecord.is_provisional.is_(True))
            .count()
        )
        assert blocked_scenarios > 0

    reset_response = client.post("/demo/reset", follow_redirects=True)
    assert reset_response.status_code == 200

    with app.app_context():
        assert Person.query.count() == 0
        assert Sepultura.query.count() == 0
        assert DerechoFunerarioContrato.query.count() == 0
        assert OwnershipRecord.query.count() == 0
        assert Beneficiario.query.count() == 0
        assert SepulturaDifunto.query.count() == 0
        assert Expediente.query.count() == 0
        assert OrdenTrabajo.query.count() == 0
        assert OwnershipTransferCase.query.count() == 0
        assert CaseDocument.query.count() == 0
        assert Publication.query.count() == 0
        assert TasaMantenimientoTicket.query.count() == 0
        assert Invoice.query.count() == 0
        assert Payment.query.count() == 0
        assert LapidaStock.query.count() == 0
        assert InscripcionLateral.query.count() == 0


def test_demo_actions_can_be_disabled_by_config(app, client, login_admin):
    login_admin()
    prev_demo_flag = app.config.get("DEMO_ACTIONS_ENABLED")
    app.config["DEMO_ACTIONS_ENABLED"] = False
    try:
        response = client.post("/demo/reset", follow_redirects=False)
    finally:
        app.config["DEMO_ACTIONS_ENABLED"] = prev_demo_flag
    assert response.status_code == 403
