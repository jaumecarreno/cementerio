"""OT big-bang without expediente

Revision ID: b7c8d9e0f1a2
Revises: a9b8c7d6e5f4
Create Date: 2026-03-02 14:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "b7c8d9e0f1a2"
down_revision = "a9b8c7d6e5f4"
branch_labels = None
depends_on = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_exists(bind, table_name: str, column_name: str) -> bool:
    cols = inspect(bind).get_columns(table_name)
    return any(col.get("name") == column_name for col in cols)


def _index_exists(bind, table_name: str, index_name: str) -> bool:
    indexes = inspect(bind).get_indexes(table_name)
    return any(idx.get("name") == index_name for idx in indexes)


def _drop_index_if_exists(bind, table_name: str, index_name: str) -> None:
    if not _table_exists(bind, table_name):
        return
    if not _index_exists(bind, table_name, index_name):
        return
    with op.batch_alter_table(table_name, schema=None) as batch_op:
        batch_op.drop_index(index_name)


def _create_legacy_tables_if_needed(bind) -> None:
    if not _table_exists(bind, "legacy_expediente"):
        op.create_table(
            "legacy_expediente",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("numero", sa.String(length=40), nullable=False),
            sa.Column("tipo", sa.String(length=40), nullable=False),
            sa.Column("estado", sa.String(length=40), nullable=False),
            sa.Column("sepultura_id", sa.Integer(), nullable=True),
            sa.Column("difunto_id", sa.Integer(), nullable=True),
            sa.Column("declarante_id", sa.Integer(), nullable=True),
            sa.Column("fecha_prevista", sa.Date(), nullable=True),
            sa.Column("notas", sa.Text(), nullable=False, server_default=""),
            sa.Column("created_at", sa.DateTime(), nullable=False),
        )
    if not _table_exists(bind, "legacy_orden_trabajo"):
        op.create_table(
            "legacy_orden_trabajo",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("expediente_id", sa.Integer(), nullable=True),
            sa.Column("titulo", sa.String(length=120), nullable=False),
            sa.Column("estado", sa.String(length=40), nullable=False),
            sa.Column("completed_at", sa.DateTime(), nullable=True),
            sa.Column("notes", sa.String(length=255), nullable=False, server_default=""),
            sa.Column("created_at", sa.DateTime(), nullable=False),
        )


def _backup_legacy_data(bind) -> None:
    if _table_exists(bind, "expediente"):
        op.execute(
            sa.text(
                """
                INSERT INTO legacy_expediente
                (id, org_id, numero, tipo, estado, sepultura_id, difunto_id, declarante_id, fecha_prevista, notas, created_at)
                SELECT id, org_id, numero, tipo, estado, sepultura_id, difunto_id, declarante_id, fecha_prevista, notas, created_at
                FROM expediente
                WHERE id NOT IN (SELECT id FROM legacy_expediente)
                """
            )
        )
    if _table_exists(bind, "orden_trabajo"):
        op.execute(
            sa.text(
                """
                INSERT INTO legacy_orden_trabajo
                (id, org_id, expediente_id, titulo, estado, completed_at, notes, created_at)
                SELECT id, org_id, expediente_id, titulo, estado, completed_at, notes, created_at
                FROM orden_trabajo
                WHERE id NOT IN (SELECT id FROM legacy_orden_trabajo)
                """
            )
        )


def _create_work_order_tables(bind) -> None:
    if not _table_exists(bind, "work_order_type"):
        op.create_table(
            "work_order_type",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("code", sa.String(length=40), nullable=False),
            sa.Column("name", sa.String(length=120), nullable=False),
            sa.Column("category", sa.String(length=30), nullable=False),
            sa.Column("is_critical", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("org_id", "code", name="uq_work_order_type_org_code"),
        )
        op.create_index("ix_work_order_type_org_active", "work_order_type", ["org_id", "active"])

    if not _table_exists(bind, "work_order"):
        op.create_table(
            "work_order",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("code", sa.String(length=30), nullable=False),
            sa.Column("title", sa.String(length=140), nullable=False),
            sa.Column("description", sa.String(length=500), nullable=False, server_default=""),
            sa.Column("category", sa.String(length=30), nullable=False),
            sa.Column("type_code", sa.String(length=40), nullable=True),
            sa.Column("priority", sa.String(length=20), nullable=False),
            sa.Column("status", sa.String(length=40), nullable=False),
            sa.Column("sepultura_id", sa.Integer(), nullable=True),
            sa.Column("area_type", sa.String(length=20), nullable=True),
            sa.Column("area_code", sa.String(length=60), nullable=True),
            sa.Column("location_text", sa.String(length=255), nullable=True),
            sa.Column("assigned_user_id", sa.Integer(), nullable=True),
            sa.Column("planned_start_at", sa.DateTime(), nullable=True),
            sa.Column("planned_end_at", sa.DateTime(), nullable=True),
            sa.Column("due_at", sa.DateTime(), nullable=True),
            sa.Column("started_at", sa.DateTime(), nullable=True),
            sa.Column("completed_at", sa.DateTime(), nullable=True),
            sa.Column("cancelled_at", sa.DateTime(), nullable=True),
            sa.Column("block_reason", sa.String(length=255), nullable=False, server_default=""),
            sa.Column("cancel_reason", sa.String(length=255), nullable=False, server_default=""),
            sa.Column("close_notes", sa.String(length=500), nullable=False, server_default=""),
            sa.Column("created_by_user_id", sa.Integer(), nullable=True),
            sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("org_id", "code", name="uq_work_order_org_code"),
            sa.CheckConstraint(
                "(sepultura_id IS NOT NULL) OR (area_type IS NOT NULL AND (area_code IS NOT NULL OR location_text IS NOT NULL))",
                name="ck_work_order_location",
            ),
        )
        op.create_index("ix_work_order_org_status_due", "work_order", ["org_id", "status", "due_at"])

    if not _table_exists(bind, "work_order_template"):
        op.create_table(
            "work_order_template",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("code", sa.String(length=40), nullable=False),
            sa.Column("name", sa.String(length=120), nullable=False),
            sa.Column("type_id", sa.Integer(), nullable=True),
            sa.Column("default_priority", sa.String(length=20), nullable=False),
            sa.Column("sla_hours", sa.Integer(), nullable=True),
            sa.Column("auto_create", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("requires_sepultura", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("allows_area", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("org_id", "code", name="uq_work_order_template_org_code"),
        )
        op.create_index("ix_work_order_template_org_active", "work_order_template", ["org_id", "active"])

    if not _table_exists(bind, "work_order_template_checklist_item"):
        op.create_table(
            "work_order_template_checklist_item",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("template_id", sa.Integer(), nullable=False),
            sa.Column("label", sa.String(length=255), nullable=False),
            sa.Column("required", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        )
        op.create_index("ix_wo_template_checklist_template", "work_order_template_checklist_item", ["template_id", "sort_order"])

    if not _table_exists(bind, "work_order_checklist_item"):
        op.create_table(
            "work_order_checklist_item",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("work_order_id", sa.Integer(), nullable=False),
            sa.Column("label", sa.String(length=255), nullable=False),
            sa.Column("required", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("done", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("done_by_user_id", sa.Integer(), nullable=True),
            sa.Column("done_at", sa.DateTime(), nullable=True),
            sa.Column("notes", sa.String(length=500), nullable=False, server_default=""),
            sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        )
        op.create_index("ix_wo_checklist_work_order", "work_order_checklist_item", ["work_order_id", "sort_order"])

    if not _table_exists(bind, "work_order_evidence"):
        op.create_table(
            "work_order_evidence",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("work_order_id", sa.Integer(), nullable=False),
            sa.Column("file_path", sa.String(length=255), nullable=False),
            sa.Column("file_name", sa.String(length=120), nullable=False),
            sa.Column("mime_type", sa.String(length=120), nullable=False, server_default="application/octet-stream"),
            sa.Column("uploaded_by_user_id", sa.Integer(), nullable=True),
            sa.Column("uploaded_at", sa.DateTime(), nullable=False),
            sa.Column("notes", sa.String(length=500), nullable=False, server_default=""),
        )
        op.create_index("ix_wo_evidence_work_order", "work_order_evidence", ["work_order_id", "uploaded_at"])

    if not _table_exists(bind, "work_order_dependency"):
        op.create_table(
            "work_order_dependency",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("work_order_id", sa.Integer(), nullable=False),
            sa.Column("depends_on_work_order_id", sa.Integer(), nullable=False),
            sa.Column("dependency_type", sa.String(length=30), nullable=False, server_default="FINISH_TO_START"),
            sa.UniqueConstraint("work_order_id", "depends_on_work_order_id", name="uq_wo_dependency_pair"),
        )

    if not _table_exists(bind, "work_order_event_rule"):
        op.create_table(
            "work_order_event_rule",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("event_type", sa.String(length=60), nullable=False),
            sa.Column("template_id", sa.Integer(), nullable=False),
            sa.Column("conditions_json", sa.Text(), nullable=False, server_default="{}"),
            sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("priority", sa.Integer(), nullable=False, server_default="100"),
            sa.Column("created_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_wo_event_rule_org_event_active", "work_order_event_rule", ["org_id", "event_type", "active"])

    if not _table_exists(bind, "work_order_event_log"):
        op.create_table(
            "work_order_event_log",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("event_type", sa.String(length=60), nullable=False),
            sa.Column("payload_json", sa.Text(), nullable=False, server_default="{}"),
            sa.Column("processed_at", sa.DateTime(), nullable=False),
            sa.Column("result", sa.String(length=255), nullable=False, server_default=""),
        )
        op.create_index("ix_wo_event_log_org_event", "work_order_event_log", ["org_id", "event_type", "processed_at"])

    if not _table_exists(bind, "work_order_status_log"):
        op.create_table(
            "work_order_status_log",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("work_order_id", sa.Integer(), nullable=False),
            sa.Column("from_status", sa.String(length=40), nullable=False, server_default=""),
            sa.Column("to_status", sa.String(length=40), nullable=False),
            sa.Column("changed_by_user_id", sa.Integer(), nullable=True),
            sa.Column("changed_at", sa.DateTime(), nullable=False),
            sa.Column("reason", sa.String(length=500), nullable=False, server_default=""),
        )
        op.create_index("ix_wo_status_log_work_order_at", "work_order_status_log", ["work_order_id", "changed_at"])


def _drop_expediente_operational_tables(bind) -> None:
    if _table_exists(bind, "lapida_stock_movimiento") and _column_exists(bind, "lapida_stock_movimiento", "expediente_id"):
        _drop_index_if_exists(bind, "lapida_stock_movimiento", "ix_lapida_stock_movimiento_expediente_id")
        with op.batch_alter_table("lapida_stock_movimiento", schema=None) as batch_op:
            batch_op.drop_column("expediente_id")
    if _table_exists(bind, "inscripcion_lateral") and _column_exists(bind, "inscripcion_lateral", "expediente_id"):
        _drop_index_if_exists(bind, "inscripcion_lateral", "ix_inscripcion_lateral_expediente_id")
        with op.batch_alter_table("inscripcion_lateral", schema=None) as batch_op:
            batch_op.drop_column("expediente_id")
    if _table_exists(bind, "orden_trabajo"):
        op.drop_table("orden_trabajo")
    if _table_exists(bind, "expediente"):
        op.drop_table("expediente")


def upgrade():
    bind = op.get_bind()
    _create_legacy_tables_if_needed(bind)
    _backup_legacy_data(bind)
    _create_work_order_tables(bind)
    _drop_expediente_operational_tables(bind)


def downgrade():
    bind = op.get_bind()

    if not _table_exists(bind, "expediente"):
        op.create_table(
            "expediente",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("numero", sa.String(length=40), nullable=False),
            sa.Column("tipo", sa.String(length=40), nullable=False),
            sa.Column("estado", sa.String(length=40), nullable=False),
            sa.Column("sepultura_id", sa.Integer(), nullable=True),
            sa.Column("difunto_id", sa.Integer(), nullable=True),
            sa.Column("declarante_id", sa.Integer(), nullable=True),
            sa.Column("fecha_prevista", sa.Date(), nullable=True),
            sa.Column("notas", sa.Text(), nullable=False, server_default=""),
            sa.Column("created_at", sa.DateTime(), nullable=False),
        )

    if not _table_exists(bind, "orden_trabajo"):
        op.create_table(
            "orden_trabajo",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("expediente_id", sa.Integer(), nullable=True),
            sa.Column("titulo", sa.String(length=120), nullable=False),
            sa.Column("estado", sa.String(length=40), nullable=False),
            sa.Column("completed_at", sa.DateTime(), nullable=True),
            sa.Column("notes", sa.String(length=255), nullable=False, server_default=""),
            sa.Column("created_at", sa.DateTime(), nullable=False),
        )

    if _table_exists(bind, "inscripcion_lateral") and not _column_exists(bind, "inscripcion_lateral", "expediente_id"):
        with op.batch_alter_table("inscripcion_lateral", schema=None) as batch_op:
            batch_op.add_column(sa.Column("expediente_id", sa.Integer(), nullable=True))
    if _table_exists(bind, "lapida_stock_movimiento") and not _column_exists(bind, "lapida_stock_movimiento", "expediente_id"):
        with op.batch_alter_table("lapida_stock_movimiento", schema=None) as batch_op:
            batch_op.add_column(sa.Column("expediente_id", sa.Integer(), nullable=True))
