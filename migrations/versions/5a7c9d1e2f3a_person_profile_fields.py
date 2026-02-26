"""person profile fields and dni_nif partial unique index

Revision ID: 5a7c9d1e2f3a
Revises: 2e6f7a8b9c0d
Create Date: 2026-02-27 00:35:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "5a7c9d1e2f3a"
down_revision = "2e6f7a8b9c0d"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("person", schema=None) as batch_op:
        batch_op.add_column(sa.Column("dni_nif", sa.String(length=30), nullable=True))
        batch_op.add_column(sa.Column("telefono", sa.String(length=40), nullable=False, server_default=""))
        batch_op.add_column(sa.Column("email", sa.String(length=120), nullable=False, server_default=""))
        batch_op.add_column(sa.Column("direccion", sa.String(length=255), nullable=False, server_default=""))
        batch_op.add_column(sa.Column("notas", sa.String(length=500), nullable=False, server_default=""))

    op.execute(sa.text("UPDATE person SET dni_nif = document_id WHERE document_id IS NOT NULL"))

    with op.batch_alter_table("person", schema=None) as batch_op:
        batch_op.drop_constraint("uq_person_org_document", type_="unique")
        batch_op.drop_column("document_id")

    op.create_index(
        "ix_person_org_dni_nif_not_null",
        "person",
        ["org_id", "dni_nif"],
        unique=True,
        sqlite_where=sa.text("dni_nif IS NOT NULL"),
        postgresql_where=sa.text("dni_nif IS NOT NULL"),
    )


def downgrade():
    op.drop_index("ix_person_org_dni_nif_not_null", table_name="person")

    with op.batch_alter_table("person", schema=None) as batch_op:
        batch_op.add_column(sa.Column("document_id", sa.String(length=30), nullable=True))

    op.execute(sa.text("UPDATE person SET document_id = dni_nif WHERE dni_nif IS NOT NULL"))

    with op.batch_alter_table("person", schema=None) as batch_op:
        batch_op.create_unique_constraint("uq_person_org_document", ["org_id", "document_id"])
        batch_op.drop_column("notas")
        batch_op.drop_column("direccion")
        batch_op.drop_column("email")
        batch_op.drop_column("telefono")
        batch_op.drop_column("dni_nif")
