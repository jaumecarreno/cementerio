"""remove legacy billing schema

Revision ID: 13cc45dd56ee
Revises: 12bb34cc45dd
Create Date: 2026-03-04 23:40:00.000000

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "13cc45dd56ee"
down_revision = "12bb34cc45dd"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    op.execute("DROP TABLE IF EXISTS tasa_mantenimiento_ticket")
    op.execute("DROP TABLE IF EXISTS payment")
    op.execute("DROP TABLE IF EXISTS invoice")
    if bind.dialect.name == "postgresql":
        op.execute("DROP TYPE IF EXISTS ticket_descuento_tipo")
        op.execute("DROP TYPE IF EXISTS ticket_estado")
        op.execute("DROP TYPE IF EXISTS invoice_estado")


def downgrade():
    # Destructive migration: legacy billing schema is intentionally not restored.
    pass
