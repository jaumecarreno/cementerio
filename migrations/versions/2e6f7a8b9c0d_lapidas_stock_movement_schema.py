"""lapida stock movement and inscription linkage

Revision ID: 2e6f7a8b9c0d
Revises: 1d4e5f6a7b8c
Create Date: 2026-02-26 23:45:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "2e6f7a8b9c0d"
down_revision = "1d4e5f6a7b8c"
branch_labels = None
depends_on = None


def _extend_movimiento_tipo_enum() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    for value in [
        "ALTA_EXPEDIENTE",
        "CAMBIO_ESTADO_EXPEDIENTE",
        "OT_EXPEDIENTE",
        "BENEFICIARIO",
        "PENSIONISTA",
    ]:
        op.execute(sa.text(f"ALTER TYPE movimiento_tipo ADD VALUE IF NOT EXISTS '{value}'"))


def upgrade():
    _extend_movimiento_tipo_enum()

    with op.batch_alter_table("lapida_stock", schema=None) as batch_op:
        batch_op.add_column(sa.Column("available_qty", sa.Integer(), nullable=False, server_default="0"))

    with op.batch_alter_table("inscripcion_lateral", schema=None) as batch_op:
        batch_op.add_column(sa.Column("expediente_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            "fk_inscripcion_lateral_expediente_id",
            "expediente",
            ["expediente_id"],
            ["id"],
        )
        batch_op.create_index("ix_inscripcion_lateral_expediente_id", ["expediente_id"], unique=False)

    op.create_table(
        "lapida_stock_movimiento",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("lapida_stock_id", sa.Integer(), nullable=False),
        sa.Column("movimiento", sa.String(length=20), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("sepultura_id", sa.Integer(), nullable=True),
        sa.Column("expediente_id", sa.Integer(), nullable=True),
        sa.Column("notes", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["expediente_id"], ["expediente.id"]),
        sa.ForeignKeyConstraint(["lapida_stock_id"], ["lapida_stock.id"]),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.ForeignKeyConstraint(["sepultura_id"], ["sepultura.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("lapida_stock_movimiento", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_lapida_stock_movimiento_org_id"), ["org_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_lapida_stock_movimiento_lapida_stock_id"), ["lapida_stock_id"], unique=False)


def downgrade():
    with op.batch_alter_table("lapida_stock_movimiento", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_lapida_stock_movimiento_lapida_stock_id"))
        batch_op.drop_index(batch_op.f("ix_lapida_stock_movimiento_org_id"))
    op.drop_table("lapida_stock_movimiento")

    with op.batch_alter_table("inscripcion_lateral", schema=None) as batch_op:
        batch_op.drop_index("ix_inscripcion_lateral_expediente_id")
        batch_op.drop_constraint("fk_inscripcion_lateral_expediente_id", type_="foreignkey")
        batch_op.drop_column("expediente_id")

    with op.batch_alter_table("lapida_stock", schema=None) as batch_op:
        batch_op.drop_column("available_qty")
