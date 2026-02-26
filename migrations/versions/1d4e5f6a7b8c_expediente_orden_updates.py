"""expediente and work order operational fields

Revision ID: 1d4e5f6a7b8c
Revises: 9c2f8d7e4a11
Create Date: 2026-02-26 23:40:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "1d4e5f6a7b8c"
down_revision = "9c2f8d7e4a11"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("expediente", schema=None) as batch_op:
        batch_op.add_column(sa.Column("fecha_prevista", sa.Date(), nullable=True))
        batch_op.add_column(sa.Column("notas", sa.Text(), nullable=False, server_default=""))

    with op.batch_alter_table("orden_trabajo", schema=None) as batch_op:
        batch_op.add_column(sa.Column("completed_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("notes", sa.String(length=255), nullable=False, server_default=""))


def downgrade():
    with op.batch_alter_table("orden_trabajo", schema=None) as batch_op:
        batch_op.drop_column("notes")
        batch_op.drop_column("completed_at")

    with op.batch_alter_table("expediente", schema=None) as batch_op:
        batch_op.drop_column("notas")
        batch_op.drop_column("fecha_prevista")
