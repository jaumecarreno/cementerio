"""sepultura notes and postit fields

Revision ID: e5f6a7b8c9d0
Revises: c1d2e3f4a5b6
Create Date: 2026-03-01 13:20:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e5f6a7b8c9d0"
down_revision = "c1d2e3f4a5b6"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("sepultura", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("postit", sa.String(length=255), nullable=False, server_default="")
        )
        batch_op.add_column(
            sa.Column("notas", sa.Text(), nullable=False, server_default="")
        )


def downgrade():
    with op.batch_alter_table("sepultura", schema=None) as batch_op:
        batch_op.drop_column("notas")
        batch_op.drop_column("postit")
