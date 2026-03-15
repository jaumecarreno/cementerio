"""operation status flow refresh

Revision ID: 16ff78aa89bb
Revises: 15ee67ff78aa
Create Date: 2026-03-15 18:30:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "16ff78aa89bb"
down_revision = "15ee67ff78aa"
branch_labels = None
depends_on = None


OLD_OPERATION_STATUS_VALUES = (
    "BORRADOR",
    "DOCS_PENDIENTES",
    "PROGRAMADA",
    "EN_EJECUCION",
    "EN_VALIDACION",
    "CERRADA",
    "CANCELADA",
)

NEW_OPERATION_STATUS_VALUES = (
    "BORRADOR",
    "PDT_DOCUMENTACION",
    "PDT_DERECHO_FUNERARIO",
    "PDT_PAGO",
    "PDT_PROGRAMACION",
    "REALIZADO",
    "FINALIZADA",
    "CANCELADO",
)

UPGRADE_STATUS_MAPPING = {
    "BORRADOR": "PDT_DOCUMENTACION",
    "DOCS_PENDIENTES": "PDT_DOCUMENTACION",
    "PROGRAMADA": "PDT_DOCUMENTACION",
    "EN_EJECUCION": "PDT_DOCUMENTACION",
    "EN_VALIDACION": "PDT_DOCUMENTACION",
    "CERRADA": "FINALIZADA",
    "CANCELADA": "CANCELADO",
}

DOWNGRADE_STATUS_MAPPING = {
    "BORRADOR": "BORRADOR",
    "PDT_DOCUMENTACION": "DOCS_PENDIENTES",
    "PDT_DERECHO_FUNERARIO": "DOCS_PENDIENTES",
    "PDT_PAGO": "DOCS_PENDIENTES",
    "PDT_PROGRAMACION": "PROGRAMADA",
    "REALIZADO": "EN_VALIDACION",
    "FINALIZADA": "CERRADA",
    "CANCELADO": "CANCELADA",
}

COMBINED_OPERATION_STATUS_VALUES = tuple(
    dict.fromkeys(OLD_OPERATION_STATUS_VALUES + NEW_OPERATION_STATUS_VALUES)
)


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_exists(bind, table_name: str, column_name: str) -> bool:
    cols = inspect(bind).get_columns(table_name)
    return any(col.get("name") == column_name for col in cols)


def _update_status_values(table_name: str, column_name: str, mapping: dict[str, str]) -> None:
    bind = op.get_bind()
    if not _table_exists(bind, table_name) or not _column_exists(bind, table_name, column_name):
        return
    for source, target in mapping.items():
        op.execute(
            sa.text(
                f"UPDATE {table_name} SET {column_name} = :target WHERE {column_name} = :source"
            ),
            {"source": source, "target": target},
        )


def _expand_operation_status_enum() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, "operation_case") or not _column_exists(bind, "operation_case", "status"):
        return
    if bind.dialect.name == "postgresql":
        for value in NEW_OPERATION_STATUS_VALUES:
            op.execute(sa.text(f"ALTER TYPE operation_status ADD VALUE IF NOT EXISTS '{value}'"))
        return
    if bind.dialect.name == "sqlite":
        with op.batch_alter_table("operation_case", schema=None) as batch_op:
            batch_op.alter_column(
                "status",
                existing_type=sa.Enum(*OLD_OPERATION_STATUS_VALUES, name="operation_status"),
                type_=sa.Enum(*COMBINED_OPERATION_STATUS_VALUES, name="operation_status"),
                existing_nullable=False,
            )


def _contract_operation_status_enum_to_new() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, "operation_case") or not _column_exists(bind, "operation_case", "status"):
        return
    if bind.dialect.name == "postgresql":
        op.execute(
            sa.text(
                "ALTER TYPE operation_status RENAME TO operation_status_old"
            )
        )
        op.execute(
            sa.text(
                "CREATE TYPE operation_status AS ENUM "
                "('BORRADOR','PDT_DOCUMENTACION','PDT_DERECHO_FUNERARIO','PDT_PAGO',"
                "'PDT_PROGRAMACION','REALIZADO','FINALIZADA','CANCELADO')"
            )
        )
        op.execute(
            sa.text(
                "ALTER TABLE operation_case "
                "ALTER COLUMN status TYPE operation_status USING status::text::operation_status"
            )
        )
        op.execute(sa.text("DROP TYPE operation_status_old"))
        return
    if bind.dialect.name == "sqlite":
        with op.batch_alter_table("operation_case", schema=None) as batch_op:
            batch_op.alter_column(
                "status",
                existing_type=sa.Enum(*COMBINED_OPERATION_STATUS_VALUES, name="operation_status"),
                type_=sa.Enum(*NEW_OPERATION_STATUS_VALUES, name="operation_status"),
                existing_nullable=False,
            )


def _expand_operation_status_enum_for_downgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, "operation_case") or not _column_exists(bind, "operation_case", "status"):
        return
    if bind.dialect.name == "postgresql":
        for value in OLD_OPERATION_STATUS_VALUES:
            op.execute(sa.text(f"ALTER TYPE operation_status ADD VALUE IF NOT EXISTS '{value}'"))
        return
    if bind.dialect.name == "sqlite":
        with op.batch_alter_table("operation_case", schema=None) as batch_op:
            batch_op.alter_column(
                "status",
                existing_type=sa.Enum(*NEW_OPERATION_STATUS_VALUES, name="operation_status"),
                type_=sa.Enum(*COMBINED_OPERATION_STATUS_VALUES, name="operation_status"),
                existing_nullable=False,
            )


def _contract_operation_status_enum_to_old() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, "operation_case") or not _column_exists(bind, "operation_case", "status"):
        return
    if bind.dialect.name == "postgresql":
        op.execute(
            sa.text(
                "ALTER TYPE operation_status RENAME TO operation_status_new"
            )
        )
        op.execute(
            sa.text(
                "CREATE TYPE operation_status AS ENUM "
                "('BORRADOR','DOCS_PENDIENTES','PROGRAMADA','EN_EJECUCION',"
                "'EN_VALIDACION','CERRADA','CANCELADA')"
            )
        )
        op.execute(
            sa.text(
                "ALTER TABLE operation_case "
                "ALTER COLUMN status TYPE operation_status USING status::text::operation_status"
            )
        )
        op.execute(sa.text("DROP TYPE operation_status_new"))
        return
    if bind.dialect.name == "sqlite":
        with op.batch_alter_table("operation_case", schema=None) as batch_op:
            batch_op.alter_column(
                "status",
                existing_type=sa.Enum(*COMBINED_OPERATION_STATUS_VALUES, name="operation_status"),
                type_=sa.Enum(*OLD_OPERATION_STATUS_VALUES, name="operation_status"),
                existing_nullable=False,
            )


def upgrade():
    _expand_operation_status_enum()
    _update_status_values("operation_case", "status", UPGRADE_STATUS_MAPPING)
    _update_status_values("operation_status_log", "from_status", UPGRADE_STATUS_MAPPING)
    _update_status_values("operation_status_log", "to_status", UPGRADE_STATUS_MAPPING)
    _contract_operation_status_enum_to_new()


def downgrade():
    _expand_operation_status_enum_for_downgrade()
    _update_status_values("operation_case", "status", DOWNGRADE_STATUS_MAPPING)
    _update_status_values("operation_status_log", "from_status", DOWNGRADE_STATUS_MAPPING)
    _update_status_values("operation_status_log", "to_status", DOWNGRADE_STATUS_MAPPING)
    _contract_operation_status_enum_to_old()
