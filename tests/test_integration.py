from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from io import BytesIO

from app.core.extensions import db
from app.core.models import (
    Beneficiario,
    CaseDocument,
    CaseDocumentStatus,
    ContractEvent,
    DerechoFunerarioContrato,
    DerechoTipo,
    MovimientoSepultura,
    MovimientoTipo,
    OwnershipRecord,
    OwnershipTransferCase,
    OwnershipTransferParty,
    OwnershipTransferStatus,
    OwnershipTransferType,
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


def test_tenant_isolation_for_ownership_case_detail(app, client, login_admin, second_org_sepultura):
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
        db.session.flush()
        case = OwnershipTransferCase(
            org_id=sep2.org_id,
            case_number="TR-2026-0001",
            contract_id=contract.id,
            type=OwnershipTransferType.INTER_VIVOS,
            status=OwnershipTransferStatus.DRAFT,
            opened_at=datetime.now(timezone.utc),
        )
        db.session.add(case)
        db.session.commit()
        case_id = case.id

    login_admin()
    response = client.get(f"/cementerio/titularidad/casos/{case_id}")
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


def _create_case(app, client, contract_id: int, transfer_type: str = "INTER_VIVOS"):
    response = client.post(
        "/cementerio/titularidad/casos",
        data={"contract_id": str(contract_id), "type": transfer_type},
        follow_redirects=True,
    )
    assert response.status_code == 200
    with app.app_context():
        case = (
            OwnershipTransferCase.query.filter_by(contract_id=contract_id, type=OwnershipTransferType[transfer_type])
            .order_by(OwnershipTransferCase.id.desc())
            .first()
        )
    assert case is not None
    return case


def _move_case_to_approved(client, case_id: int):
    for status in ["DOCS_PENDING", "UNDER_REVIEW"]:
        response = client.post(
            f"/cementerio/titularidad/casos/{case_id}/status",
            data={"status": status},
            follow_redirects=True,
        )
        assert response.status_code == 200
    response = client.post(
        f"/cementerio/titularidad/casos/{case_id}/approve",
        follow_redirects=True,
    )
    assert response.status_code == 200


def _add_new_holder_party(client, case_id: int, first_name: str = "Nuevo", last_name: str = "Titular"):
    response = client.post(
        f"/cementerio/titularidad/casos/{case_id}/parties",
        data={
            "role": "NUEVO_TITULAR",
            "first_name": first_name,
            "last_name": last_name,
            "document_id": f"DOC-{case_id}",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200


def test_ownership_case_create_upload_and_verify_document(app, client, login_admin):
    login_admin()
    with app.app_context():
        contrato = DerechoFunerarioContrato.query.join(Sepultura).filter(Sepultura.numero == 127).first()

    case = _create_case(app, client, contrato.id, "INTER_VIVOS")

    with app.app_context():
        created = db.session.get(OwnershipTransferCase, case.id)
        assert created.case_number.startswith("TR-")
        assert created.status == OwnershipTransferStatus.DRAFT
        assert any(p.role.value == "ANTERIOR_TITULAR" for p in created.parties)
        doc = CaseDocument.query.filter_by(case_id=created.id).order_by(CaseDocument.id.asc()).first()
        assert doc is not None
        doc_id = doc.id

    upload_response = client.post(
        f"/cementerio/titularidad/casos/{case.id}/documents/{doc_id}/upload",
        data={"file": (BytesIO(b"demo-content"), "certificado.txt")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert upload_response.status_code == 200

    verify_response = client.post(
        f"/cementerio/titularidad/casos/{case.id}/documents/{doc_id}/verify",
        data={"action": "verify"},
        follow_redirects=True,
    )
    assert verify_response.status_code == 200

    with app.app_context():
        uploaded = db.session.get(CaseDocument, doc_id)
        assert uploaded.file_path
        assert uploaded.status == CaseDocumentStatus.VERIFIED


def test_ownership_case_cannot_close_without_required_verified(app, client, login_admin):
    login_admin()
    with app.app_context():
        contrato = DerechoFunerarioContrato.query.join(Sepultura).filter(Sepultura.numero == 127).first()
    case = _create_case(app, client, contrato.id, "INTER_VIVOS")
    _add_new_holder_party(client, case.id)
    _move_case_to_approved(client, case.id)

    close_response = client.post(
        f"/cementerio/titularidad/casos/{case.id}/close",
        data={},
        follow_redirects=True,
    )
    assert close_response.status_code == 200
    assert b"Faltan documentos obligatorios verificados" in close_response.data

    with app.app_context():
        refreshed = db.session.get(OwnershipTransferCase, case.id)
        assert refreshed.status == OwnershipTransferStatus.APPROVED


def test_ownership_case_close_changes_holder_and_registers_audit(app, client, login_admin):
    login_admin()
    with app.app_context():
        contrato = DerechoFunerarioContrato.query.join(Sepultura).filter(Sepultura.numero == 127).first()
        sepultura_id = contrato.sepultura_id
        old_active = (
            OwnershipRecord.query.filter_by(contract_id=contrato.id)
            .filter(OwnershipRecord.end_date.is_(None))
            .first()
        )
        old_holder_id = old_active.person_id
    case = _create_case(app, client, contrato.id, "INTER_VIVOS")
    _add_new_holder_party(client, case.id, first_name="Carla", last_name="Mora")

    with app.app_context():
        docs = CaseDocument.query.filter_by(case_id=case.id, required=True).all()
        doc_ids = [doc.id for doc in docs]
    for doc_id in doc_ids:
        client.post(
            f"/cementerio/titularidad/casos/{case.id}/documents/{doc_id}/verify",
            data={"action": "verify"},
            follow_redirects=True,
        )
    _move_case_to_approved(client, case.id)

    close_response = client.post(
        f"/cementerio/titularidad/casos/{case.id}/close",
        data={},
        follow_redirects=True,
    )
    assert close_response.status_code == 200
    assert b"Caso cerrado y titularidad aplicada" in close_response.data

    with app.app_context():
        refreshed = db.session.get(OwnershipTransferCase, case.id)
        assert refreshed.status == OwnershipTransferStatus.CLOSED
        active = (
            OwnershipRecord.query.filter_by(contract_id=contrato.id)
            .filter(OwnershipRecord.end_date.is_(None))
            .first()
        )
        assert active is not None
        assert active.person_id != old_holder_id
        old_record = OwnershipRecord.query.filter_by(contract_id=contrato.id, person_id=old_holder_id).first()
        assert old_record.end_date is not None
        movement = (
            MovimientoSepultura.query.filter_by(sepultura_id=sepultura_id, tipo=MovimientoTipo.CAMBIO_TITULARIDAD)
            .order_by(MovimientoSepultura.id.desc())
            .first()
        )
        assert movement is not None
        event = (
            ContractEvent.query.filter_by(contract_id=contrato.id, event_type="CAMBIO_TITULARIDAD")
            .order_by(ContractEvent.id.desc())
            .first()
        )
        assert event is not None


def test_ownership_case_provisional_requires_publications_and_sets_until(app, client, login_admin):
    login_admin()
    with app.app_context():
        contrato = DerechoFunerarioContrato.query.join(Sepultura).filter(Sepultura.numero == 128).first()
    response = client.post(
        "/cementerio/titularidad/casos",
        data={
            "contract_id": str(contrato.id),
            "type": "PROVISIONAL",
            "provisional_start_date": "2026-01-01",
        },
        follow_redirects=True,
    )
    assert response.status_code == 200
    with app.app_context():
        case = (
            OwnershipTransferCase.query.filter_by(contract_id=contrato.id, type=OwnershipTransferType.PROVISIONAL)
            .order_by(OwnershipTransferCase.id.desc())
            .first()
        )
        assert case is not None
    _add_new_holder_party(client, case.id, first_name="Sonia", last_name="Pons")
    with app.app_context():
        required_docs = CaseDocument.query.filter_by(case_id=case.id, required=True).all()
    for doc in required_docs:
        client.post(
            f"/cementerio/titularidad/casos/{case.id}/documents/{doc.id}/verify",
            data={"action": "verify"},
            follow_redirects=True,
        )
    _move_case_to_approved(client, case.id)

    close_fail = client.post(
        f"/cementerio/titularidad/casos/{case.id}/close",
        data={},
        follow_redirects=True,
    )
    assert close_fail.status_code == 200
    assert b"requiere publicacion en BOP y en otro canal" in close_fail.data

    client.post(
        f"/cementerio/titularidad/casos/{case.id}/publications",
        data={"published_at": "2026-02-01", "channel": "BOP", "reference_text": "BOP ref"},
        follow_redirects=True,
    )
    client.post(
        f"/cementerio/titularidad/casos/{case.id}/publications",
        data={"published_at": "2026-02-05", "channel": "DIARIO", "reference_text": "Diario ref"},
        follow_redirects=True,
    )

    close_ok = client.post(
        f"/cementerio/titularidad/casos/{case.id}/close",
        data={},
        follow_redirects=True,
    )
    assert close_ok.status_code == 200

    with app.app_context():
        active = (
            OwnershipRecord.query.filter_by(contract_id=contrato.id)
            .filter(OwnershipRecord.end_date.is_(None))
            .first()
        )
        assert active.is_provisional is True
        assert active.provisional_until == date(2036, 1, 1)
