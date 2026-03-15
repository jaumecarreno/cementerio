"""operation case concession fields and contract link

Revision ID: 15ee67ff78aa
Revises: 14dd56ee67ff
Create Date: 2026-03-15 16:10:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "15ee67ff78aa"
down_revision = "14dd56ee67ff"
branch_labels = None
depends_on = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_exists(bind, table_name: str, column_name: str) -> bool:
    columns = inspect(bind).get_columns(table_name)
    return any(col.get("name") == column_name for col in columns)


def _index_exists(bind, table_name: str, index_name: str) -> bool:
    indexes = inspect(bind).get_indexes(table_name)
    return any(idx.get("name") == index_name for idx in indexes)


def _foreign_key_exists(bind, table_name: str, fk_name: str) -> bool:
    fks = inspect(bind).get_foreign_keys(table_name)
    return any((fk.get("name") or "") == fk_name for fk in fks)


def upgrade():
    bind = op.get_bind()
    if not _table_exists(bind, "operation_case"):
        return

    with op.batch_alter_table("operation_case", schema=None) as batch_op:
        if not _column_exists(bind, "operation_case", "contract_id"):
            batch_op.add_column(sa.Column("contract_id", sa.Integer(), nullable=True))
        if not _column_exists(bind, "operation_case", "concession_start_date"):
            batch_op.add_column(sa.Column("concession_start_date", sa.Date(), nullable=True))
        if not _column_exists(bind, "operation_case", "concession_end_date"):
            batch_op.add_column(sa.Column("concession_end_date", sa.Date(), nullable=True))

    with op.batch_alter_table("operation_case", schema=None) as batch_op:
        if (
            _column_exists(bind, "operation_case", "contract_id")
            and not _foreign_key_exists(bind, "operation_case", "fk_operation_case_contract_id")
        ):
            batch_op.create_foreign_key(
                "fk_operation_case_contract_id",
                "derecho_funerario_contrato",
                ["contract_id"],
                ["id"],
            )

    if (
        _column_exists(bind, "operation_case", "contract_id")
        and not _index_exists(bind, "operation_case", "ix_operation_case_contract_id")
    ):
        op.create_index(
            "ix_operation_case_contract_id",
            "operation_case",
            ["contract_id"],
            unique=False,
        )


def downgrade():
    bind = op.get_bind()
    if not _table_exists(bind, "operation_case"):
        return

    if _index_exists(bind, "operation_case", "ix_operation_case_contract_id"):
        op.drop_index("ix_operation_case_contract_id", table_name="operation_case")

    with op.batch_alter_table("operation_case", schema=None) as batch_op:
        if _foreign_key_exists(bind, "operation_case", "fk_operation_case_contract_id"):
            batch_op.drop_constraint("fk_operation_case_contract_id", type_="foreignkey")
        if _column_exists(bind, "operation_case", "concession_end_date"):
            batch_op.drop_column("concession_end_date")
        if _column_exists(bind, "operation_case", "concession_start_date"):
            batch_op.drop_column("concession_start_date")
        if _column_exists(bind, "operation_case", "contract_id"):
            batch_op.drop_column("contract_id")
