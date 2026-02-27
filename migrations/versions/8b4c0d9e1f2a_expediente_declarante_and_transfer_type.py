"""expediente declarante and ownership transfer type update

Revision ID: 8b4c0d9e1f2a
Revises: 5a7c9d1e2f3a
Create Date: 2026-02-27 10:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8b4c0d9e1f2a"
down_revision = "5a7c9d1e2f3a"
branch_labels = None
depends_on = None


OLD_OWNERSHIP_TRANSFER_VALUES = (
    "MORTIS_CAUSA_TESTAMENTO",
    "MORTIS_CAUSA_SIN_TESTAMENTO",
    "INTER_VIVOS",
    "PROVISIONAL",
)
NEW_OWNERSHIP_TRANSFER_VALUES = (
    "MORTIS_CAUSA_TESTAMENTO",
    "MORTIS_CAUSA_SIN_TESTAMENTO",
    "MORTIS_CAUSA_CON_BENEFICIARIO",
    "INTER_VIVOS",
    "PROVISIONAL",
)


def _expand_ownership_transfer_type_enum() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(sa.text("ALTER TYPE ownership_transfer_type ADD VALUE IF NOT EXISTS 'MORTIS_CAUSA_CON_BENEFICIARIO'"))
        return
    if bind.dialect.name == "sqlite":
        with op.batch_alter_table("ownership_transfer_case", schema=None) as batch_op:
            batch_op.alter_column(
                "type",
                existing_type=sa.Enum(*OLD_OWNERSHIP_TRANSFER_VALUES, name="ownership_transfer_type"),
                type_=sa.Enum(*NEW_OWNERSHIP_TRANSFER_VALUES, name="ownership_transfer_type"),
                existing_nullable=False,
            )


def _shrink_ownership_transfer_type_enum_sqlite() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "sqlite":
        return
    with op.batch_alter_table("ownership_transfer_case", schema=None) as batch_op:
        batch_op.alter_column(
            "type",
            existing_type=sa.Enum(*NEW_OWNERSHIP_TRANSFER_VALUES, name="ownership_transfer_type"),
            type_=sa.Enum(*OLD_OWNERSHIP_TRANSFER_VALUES, name="ownership_transfer_type"),
            existing_nullable=False,
        )


def upgrade():
    _expand_ownership_transfer_type_enum()

    with op.batch_alter_table("expediente", schema=None) as batch_op:
        batch_op.add_column(sa.Column("declarante_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            "fk_expediente_declarante_id",
            "person",
            ["declarante_id"],
            ["id"],
        )
        batch_op.create_index("ix_expediente_declarante_id", ["declarante_id"], unique=False)


def downgrade():
    with op.batch_alter_table("expediente", schema=None) as batch_op:
        batch_op.drop_index("ix_expediente_declarante_id")
        batch_op.drop_constraint("fk_expediente_declarante_id", type_="foreignkey")
        batch_op.drop_column("declarante_id")

    _shrink_ownership_transfer_type_enum_sqlite()

