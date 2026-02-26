"""contracts fees schema updates

Revision ID: f3c1a2b9d101
Revises: 7fea0a113b9a
Create Date: 2026-02-26 22:20:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f3c1a2b9d101"
down_revision = "7fea0a113b9a"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("organization", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "pensionista_discount_pct",
                sa.Numeric(precision=5, scale=2),
                nullable=False,
                server_default="10.00",
            )
        )

    with op.batch_alter_table("derecho_funerario_contrato", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "legacy_99_years",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
        batch_op.add_column(
            sa.Column(
                "annual_fee_amount",
                sa.Numeric(precision=10, scale=2),
                nullable=False,
                server_default="0.00",
            )
        )
        batch_op.create_index(
            "ix_contract_org_tipo_estado_dates",
            ["org_id", "tipo", "estado", "fecha_inicio", "fecha_fin"],
            unique=False,
        )

    with op.batch_alter_table("payment", schema=None) as batch_op:
        batch_op.add_column(sa.Column("user_id", sa.Integer(), nullable=True))
        batch_op.create_index("ix_payment_org_user_id", ["org_id", "user_id"], unique=False)
        batch_op.create_foreign_key(
            "fk_payment_user_id_user_account",
            "user_account",
            ["user_id"],
            ["id"],
        )


def downgrade():
    with op.batch_alter_table("payment", schema=None) as batch_op:
        batch_op.drop_constraint("fk_payment_user_id_user_account", type_="foreignkey")
        batch_op.drop_index("ix_payment_org_user_id")
        batch_op.drop_column("user_id")

    with op.batch_alter_table("derecho_funerario_contrato", schema=None) as batch_op:
        batch_op.drop_index("ix_contract_org_tipo_estado_dates")
        batch_op.drop_column("annual_fee_amount")
        batch_op.drop_column("legacy_99_years")

    with op.batch_alter_table("organization", schema=None) as batch_op:
        batch_op.drop_column("pensionista_discount_pct")
