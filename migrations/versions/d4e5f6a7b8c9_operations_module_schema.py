"""operations module schema

Revision ID: d4e5f6a7b8c9
Revises: b7c8d9e0f1a2
Create Date: 2026-03-03 11:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "d4e5f6a7b8c9"
down_revision = "b7c8d9e0f1a2"
branch_labels = None
depends_on = None


OLD_MOVIMIENTO_VALUES = (
    "INHUMACION",
    "EXHUMACION",
    "TASAS",
    "LAPIDA",
    "CAMBIO_ESTADO",
    "CONTRATO",
    "INSCRIPCION_LATERAL",
    "INICIO_TRANSMISION",
    "DOCUMENTO_SUBIDO",
    "APROBACION",
    "RECHAZO",
    "CAMBIO_TITULARIDAD",
    "ALTA_EXPEDIENTE",
    "CAMBIO_ESTADO_EXPEDIENTE",
    "OT_EXPEDIENTE",
    "BENEFICIARIO",
    "PENSIONISTA",
)
NEW_MOVIMIENTO_VALUES = OLD_MOVIMIENTO_VALUES[:2] + (
    "TRASLADO_CORTO",
    "TRASLADO_LARGO",
    "RESCATE",
) + OLD_MOVIMIENTO_VALUES[2:]


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_exists(bind, table_name: str, column_name: str) -> bool:
    cols = inspect(bind).get_columns(table_name)
    return any(col.get("name") == column_name for col in cols)


def _index_exists(bind, table_name: str, index_name: str) -> bool:
    indexes = inspect(bind).get_indexes(table_name)
    return any(idx.get("name") == index_name for idx in indexes)


def _expand_movimiento_tipo_enum() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(sa.text("ALTER TYPE movimiento_tipo ADD VALUE IF NOT EXISTS 'TRASLADO_CORTO'"))
        op.execute(sa.text("ALTER TYPE movimiento_tipo ADD VALUE IF NOT EXISTS 'TRASLADO_LARGO'"))
        op.execute(sa.text("ALTER TYPE movimiento_tipo ADD VALUE IF NOT EXISTS 'RESCATE'"))
        return
    if bind.dialect.name == "sqlite":
        with op.batch_alter_table("movimiento_sepultura", schema=None) as batch_op:
            batch_op.alter_column(
                "tipo",
                existing_type=sa.Enum(*OLD_MOVIMIENTO_VALUES, name="movimiento_tipo"),
                type_=sa.Enum(*NEW_MOVIMIENTO_VALUES, name="movimiento_tipo"),
                existing_nullable=False,
            )


def _shrink_movimiento_tipo_enum_sqlite() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "sqlite":
        return
    with op.batch_alter_table("movimiento_sepultura", schema=None) as batch_op:
        batch_op.alter_column(
            "tipo",
            existing_type=sa.Enum(*NEW_MOVIMIENTO_VALUES, name="movimiento_tipo"),
            type_=sa.Enum(*OLD_MOVIMIENTO_VALUES, name="movimiento_tipo"),
            existing_nullable=False,
        )


def upgrade():
    bind = op.get_bind()
    _expand_movimiento_tipo_enum()

    if _table_exists(bind, "cemetery") and not _column_exists(bind, "cemetery", "municipality"):
        with op.batch_alter_table("cemetery", schema=None) as batch_op:
            batch_op.add_column(sa.Column("municipality", sa.String(length=120), nullable=False, server_default=""))

    if not _table_exists(bind, "operation_case"):
        op.create_table(
            "operation_case",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("code", sa.String(length=40), nullable=False),
            sa.Column(
                "type",
                sa.Enum(
                    "INHUMACION",
                    "EXHUMACION",
                    "TRASLADO_CORTO",
                    "TRASLADO_LARGO",
                    "RESCATE",
                    name="operation_type",
                ),
                nullable=False,
            ),
            sa.Column(
                "status",
                sa.Enum(
                    "BORRADOR",
                    "DOCS_PENDIENTES",
                    "PROGRAMADA",
                    "EN_EJECUCION",
                    "EN_VALIDACION",
                    "CERRADA",
                    "CANCELADA",
                    name="operation_status",
                ),
                nullable=False,
            ),
            sa.Column("source_sepultura_id", sa.Integer(), nullable=False, index=True),
            sa.Column("target_sepultura_id", sa.Integer(), nullable=True, index=True),
            sa.Column("deceased_person_id", sa.Integer(), nullable=True, index=True),
            sa.Column("declarant_person_id", sa.Integer(), nullable=True, index=True),
            sa.Column("scheduled_at", sa.DateTime(), nullable=True),
            sa.Column("executed_at", sa.DateTime(), nullable=True),
            sa.Column("closed_at", sa.DateTime(), nullable=True),
            sa.Column("destination_cemetery_id", sa.Integer(), nullable=True, index=True),
            sa.Column("destination_name", sa.String(length=255), nullable=False, server_default=""),
            sa.Column("destination_municipality", sa.String(length=120), nullable=False, server_default=""),
            sa.Column("destination_region", sa.String(length=120), nullable=False, server_default=""),
            sa.Column("destination_country", sa.String(length=120), nullable=False, server_default=""),
            sa.Column("cross_border", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column("notes", sa.Text(), nullable=False, server_default=""),
            sa.Column("created_by_user_id", sa.Integer(), nullable=True),
            sa.Column("managed_by_user_id", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.UniqueConstraint("org_id", "code", name="uq_operation_case_org_code"),
            sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
            sa.ForeignKeyConstraint(["source_sepultura_id"], ["sepultura.id"]),
            sa.ForeignKeyConstraint(["target_sepultura_id"], ["sepultura.id"]),
            sa.ForeignKeyConstraint(["deceased_person_id"], ["person.id"]),
            sa.ForeignKeyConstraint(["declarant_person_id"], ["person.id"]),
            sa.ForeignKeyConstraint(["destination_cemetery_id"], ["cemetery.id"]),
            sa.ForeignKeyConstraint(["created_by_user_id"], ["user_account.id"]),
            sa.ForeignKeyConstraint(["managed_by_user_id"], ["user_account.id"]),
        )
        op.create_index(
            "ix_operation_case_org_status_scheduled",
            "operation_case",
            ["org_id", "status", "scheduled_at"],
        )

    if not _table_exists(bind, "operation_permit"):
        op.create_table(
            "operation_permit",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("operation_case_id", sa.Integer(), nullable=False, index=True),
            sa.Column("permit_type", sa.String(length=80), nullable=False),
            sa.Column("required", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column(
                "status",
                sa.Enum("MISSING", "PROVIDED", "VERIFIED", "REJECTED", name="operation_permit_status"),
                nullable=False,
            ),
            sa.Column("reference_number", sa.String(length=80), nullable=False, server_default=""),
            sa.Column("issued_at", sa.DateTime(), nullable=True),
            sa.Column("verified_at", sa.DateTime(), nullable=True),
            sa.Column("verified_by_user_id", sa.Integer(), nullable=True),
            sa.Column("notes", sa.String(length=500), nullable=False, server_default=""),
            sa.ForeignKeyConstraint(["operation_case_id"], ["operation_case.id"]),
            sa.ForeignKeyConstraint(["verified_by_user_id"], ["user_account.id"]),
        )
        op.create_index("ix_operation_permit_case_type", "operation_permit", ["operation_case_id", "permit_type"])

    if not _table_exists(bind, "operation_document"):
        op.create_table(
            "operation_document",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("operation_case_id", sa.Integer(), nullable=False, index=True),
            sa.Column("doc_type", sa.String(length=80), nullable=False),
            sa.Column("file_path", sa.String(length=255), nullable=True),
            sa.Column("required", sa.Boolean(), nullable=False, server_default=sa.false()),
            sa.Column(
                "status",
                sa.Enum("MISSING", "PROVIDED", "VERIFIED", "REJECTED", name="operation_permit_status"),
                nullable=False,
            ),
            sa.Column("uploaded_at", sa.DateTime(), nullable=True),
            sa.Column("verified_at", sa.DateTime(), nullable=True),
            sa.Column("verified_by_user_id", sa.Integer(), nullable=True),
            sa.Column("notes", sa.String(length=500), nullable=False, server_default=""),
            sa.ForeignKeyConstraint(["operation_case_id"], ["operation_case.id"]),
            sa.ForeignKeyConstraint(["verified_by_user_id"], ["user_account.id"]),
        )
        op.create_index("ix_operation_document_case_type", "operation_document", ["operation_case_id", "doc_type"])

    if not _table_exists(bind, "operation_status_log"):
        op.create_table(
            "operation_status_log",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("operation_case_id", sa.Integer(), nullable=False, index=True),
            sa.Column("from_status", sa.String(length=40), nullable=False, server_default=""),
            sa.Column("to_status", sa.String(length=40), nullable=False),
            sa.Column("changed_at", sa.DateTime(), nullable=False),
            sa.Column("changed_by_user_id", sa.Integer(), nullable=True),
            sa.Column("reason", sa.String(length=500), nullable=False, server_default=""),
            sa.ForeignKeyConstraint(["operation_case_id"], ["operation_case.id"]),
            sa.ForeignKeyConstraint(["changed_by_user_id"], ["user_account.id"]),
        )
        op.create_index("ix_operation_status_log_case_changed", "operation_status_log", ["operation_case_id", "changed_at"])

    if _table_exists(bind, "work_order") and not _column_exists(bind, "work_order", "operation_case_id"):
        with op.batch_alter_table("work_order", schema=None) as batch_op:
            batch_op.add_column(sa.Column("operation_case_id", sa.Integer(), nullable=True))
            batch_op.create_foreign_key(
                "fk_work_order_operation_case_id",
                "operation_case",
                ["operation_case_id"],
                ["id"],
            )
        if not _index_exists(bind, "work_order", "ix_work_order_operation_case_id"):
            op.create_index("ix_work_order_operation_case_id", "work_order", ["operation_case_id"], unique=False)


def downgrade():
    bind = op.get_bind()

    if _table_exists(bind, "work_order") and _column_exists(bind, "work_order", "operation_case_id"):
        if _index_exists(bind, "work_order", "ix_work_order_operation_case_id"):
            op.drop_index("ix_work_order_operation_case_id", table_name="work_order")
        with op.batch_alter_table("work_order", schema=None) as batch_op:
            batch_op.drop_constraint("fk_work_order_operation_case_id", type_="foreignkey")
            batch_op.drop_column("operation_case_id")

    if _table_exists(bind, "operation_status_log"):
        op.drop_index("ix_operation_status_log_case_changed", table_name="operation_status_log")
        op.drop_table("operation_status_log")

    if _table_exists(bind, "operation_document"):
        op.drop_index("ix_operation_document_case_type", table_name="operation_document")
        op.drop_table("operation_document")

    if _table_exists(bind, "operation_permit"):
        op.drop_index("ix_operation_permit_case_type", table_name="operation_permit")
        op.drop_table("operation_permit")

    if _table_exists(bind, "operation_case"):
        op.drop_index("ix_operation_case_org_status_scheduled", table_name="operation_case")
        op.drop_table("operation_case")

    if _table_exists(bind, "cemetery") and _column_exists(bind, "cemetery", "municipality"):
        with op.batch_alter_table("cemetery", schema=None) as batch_op:
            batch_op.drop_column("municipality")

    _shrink_movimiento_tipo_enum_sqlite()
