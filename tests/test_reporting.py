from __future__ import annotations

from datetime import datetime

from app.core.extensions import db
from app.core.models import (
    Membership,
    Organization,
    ReportDeliveryLog,
    ReportSchedule,
    User,
)


def test_reporting_new_report_keys_render(app, client, login_admin):
    login_admin()
    keys = [
        "ot_carga_equipos",
        "ot_sla_cumplimiento",
        "ot_calendario_faenas",
        "deuda_aging",
        "deuda_recaudacion",
    ]
    for key in keys:
        response = client.get(f"/cementerio/reporting?report={key}&cadence_preset=weekly")
        assert response.status_code == 200
        assert b"Resultados" in response.data


def test_reporting_exports_csv_and_pdf_for_new_report(app, client, login_admin):
    login_admin()

    csv_response = client.get(
        "/cementerio/reporting/export.csv?report=ot_carga_equipos&cadence_preset=weekly"
    )
    assert csv_response.status_code == 200
    assert csv_response.headers["Content-Type"].startswith("text/csv")

    pdf_response = client.get(
        "/cementerio/reporting/export.pdf?report=ot_sla_cumplimiento&cadence_preset=weekly"
    )
    assert pdf_response.status_code == 200
    assert pdf_response.headers["Content-Type"].startswith("application/pdf")


def test_reporting_schedules_create_and_run_now(app, client, login_admin):
    login_admin()
    create = client.post(
        "/cementerio/reporting/schedules",
        data={
            "name": "Mandos semanal",
            "report_key": "ot_carga_equipos",
            "cadence": "WEEKLY",
            "day_of_week": "0",
            "run_time": "07:00",
            "timezone": "Europe/Madrid",
            "recipients": "",
            "filters_json": '{"cadence_preset":"weekly"}',
            "formats": ["CSV", "PDF"],
            "active": "1",
        },
        follow_redirects=True,
    )
    assert create.status_code == 200
    assert b"Programacion" in create.data

    with app.app_context():
        schedule = ReportSchedule.query.filter_by(name="Mandos semanal").first()
        assert schedule is not None
        schedule_id = schedule.id

    run_now = client.post(
        f"/cementerio/reporting/schedules/{schedule_id}/run-now",
        follow_redirects=True,
    )
    assert run_now.status_code == 200

    with app.app_context():
        delivery = (
            ReportDeliveryLog.query.filter_by(schedule_id=schedule_id)
            .order_by(ReportDeliveryLog.id.desc())
            .first()
        )
        assert delivery is not None
        assert delivery.rows_count >= 0


def test_reporting_schedules_requires_admin(app, client, login_operator):
    login_operator()
    response = client.get("/cementerio/reporting/schedules")
    assert response.status_code == 403


def test_reporting_schedules_are_org_scoped(
    app, client, login_admin, second_org_sepultura
):
    login_admin()
    with app.app_context():
        org1 = Organization.query.filter_by(code="SMSFT").first()
        org2 = Organization.query.filter_by(code="ORG2").first()
        assert org1 is not None
        assert org2 is not None
        own_schedule = ReportSchedule(
            org_id=org1.id,
            name="Org1 schedule",
            report_key="ot_carga_equipos",
            cadence="WEEKLY",
            day_of_week=0,
            run_time="07:00",
            timezone="Europe/Madrid",
            recipients="",
            filters_json="{}",
            formats="CSV",
            active=True,
        )
        other_schedule = ReportSchedule(
            org_id=org2.id,
            name="Org2 schedule hidden",
            report_key="ot_carga_equipos",
            cadence="WEEKLY",
            day_of_week=0,
            run_time="07:00",
            timezone="Europe/Madrid",
            recipients="",
            filters_json="{}",
            formats="CSV",
            active=True,
        )
        db.session.add_all([own_schedule, other_schedule])
        db.session.commit()

    response = client.get("/cementerio/reporting/schedules")
    assert response.status_code == 200
    assert b"Org1 schedule" in response.data
    assert b"Org2 schedule hidden" not in response.data


def test_reporting_run_due_cli_runs_due_schedules(app):
    with app.app_context():
        org = Organization.query.filter_by(code="SMSFT").first()
        admin_user = User.query.filter_by(email="admin@smsft.local").first()
        assert org is not None
        assert admin_user is not None
        assert (
            Membership.query.filter_by(user_id=admin_user.id, org_id=org.id).first()
            is not None
        )
        schedule = ReportSchedule(
            org_id=org.id,
            name="CLI due schedule",
            report_key="ot_carga_equipos",
            cadence="WEEKLY",
            day_of_week=datetime.now().weekday(),
            run_time="00:00",
            timezone="Europe/Madrid",
            recipients="",
            filters_json='{"cadence_preset":"weekly"}',
            formats="CSV",
            active=True,
        )
        db.session.add(schedule)
        db.session.commit()
        schedule_id = schedule.id

    runner = app.test_cli_runner()
    result = runner.invoke(args=["reporting", "run-due", "--org-code", "SMSFT"])
    assert result.exit_code == 0
    assert "executed=" in result.output

    with app.app_context():
        log = (
            ReportDeliveryLog.query.filter_by(schedule_id=schedule_id)
            .order_by(ReportDeliveryLog.id.desc())
            .first()
        )
        assert log is not None
