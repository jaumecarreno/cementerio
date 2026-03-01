"""person secondary contacts and structured address fields

Revision ID: c1d2e3f4a5b6
Revises: 8b4c0d9e1f2a
Create Date: 2026-03-01 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c1d2e3f4a5b6"
down_revision = "8b4c0d9e1f2a"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("person", schema=None) as batch_op:
        batch_op.add_column(sa.Column("telefono2", sa.String(length=40), nullable=False, server_default=""))
        batch_op.add_column(sa.Column("email2", sa.String(length=120), nullable=False, server_default=""))
        batch_op.add_column(sa.Column("direccion_linea", sa.String(length=255), nullable=False, server_default=""))
        batch_op.add_column(sa.Column("codigo_postal", sa.String(length=20), nullable=False, server_default=""))
        batch_op.add_column(sa.Column("poblacion", sa.String(length=120), nullable=False, server_default=""))
        batch_op.add_column(sa.Column("provincia", sa.String(length=120), nullable=False, server_default=""))
        batch_op.add_column(sa.Column("pais", sa.String(length=120), nullable=False, server_default=""))

    op.execute(
        sa.text(
            "UPDATE person "
            "SET direccion_linea = direccion "
            "WHERE (direccion_linea IS NULL OR direccion_linea = '') "
            "AND direccion IS NOT NULL AND direccion <> ''"
        )
    )


def downgrade():
    with op.batch_alter_table("person", schema=None) as batch_op:
        batch_op.drop_column("pais")
        batch_op.drop_column("provincia")
        batch_op.drop_column("poblacion")
        batch_op.drop_column("codigo_postal")
        batch_op.drop_column("direccion_linea")
        batch_op.drop_column("email2")
        batch_op.drop_column("telefono2")
