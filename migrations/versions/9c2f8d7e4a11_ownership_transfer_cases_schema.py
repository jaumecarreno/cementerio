"""ownership transfer cases schema

Revision ID: 9c2f8d7e4a11
Revises: f3c1a2b9d101
Create Date: 2026-02-26 23:15:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9c2f8d7e4a11"
down_revision = "f3c1a2b9d101"
branch_labels = None
depends_on = None


def _extend_movimiento_tipo_enum() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    for value in [
        "INICIO_TRANSMISION",
        "DOCUMENTO_SUBIDO",
        "APROBACION",
        "RECHAZO",
        "CAMBIO_TITULARIDAD",
    ]:
        op.execute(sa.text(f"ALTER TYPE movimiento_tipo ADD VALUE IF NOT EXISTS '{value}'"))


def upgrade():
    _extend_movimiento_tipo_enum()

    op.create_table(
        "ownership_record",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("contract_id", sa.Integer(), nullable=False),
        sa.Column("person_id", sa.Integer(), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("is_pensioner", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("pensioner_since_date", sa.Date(), nullable=True),
        sa.Column("is_provisional", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("provisional_until", sa.Date(), nullable=True),
        sa.Column("notes", sa.String(length=255), nullable=False, server_default=""),
        sa.CheckConstraint("end_date IS NULL OR end_date >= start_date", name="ck_ownership_dates"),
        sa.ForeignKeyConstraint(["contract_id"], ["derecho_funerario_contrato.id"]),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.ForeignKeyConstraint(["person_id"], ["person.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("ownership_record", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_ownership_record_org_id"), ["org_id"], unique=False)
    op.create_index(
        "ix_ownership_record_org_contract_start",
        "ownership_record",
        ["org_id", "contract_id", "start_date"],
        unique=False,
    )
    op.create_index(
        "ix_ownership_record_org_contract_current",
        "ownership_record",
        ["org_id", "contract_id"],
        unique=True,
        sqlite_where=sa.text("end_date IS NULL"),
        postgresql_where=sa.text("end_date IS NULL"),
    )

    op.execute(
        sa.text(
            """
            INSERT INTO ownership_record (
                org_id, contract_id, person_id, start_date, end_date, is_pensioner, pensioner_since_date, is_provisional, provisional_until, notes
            )
            SELECT
                org_id, contrato_id, person_id, activo_desde, activo_hasta, pensionista, pensionista_desde, 0, NULL, ''
            FROM titularidad
            """
        )
    )

    with op.batch_alter_table("titularidad", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_titularidad_org_id"))
    op.drop_table("titularidad")

    op.create_index(
        "ix_beneficiario_org_contract_current",
        "beneficiario",
        ["org_id", "contrato_id"],
        unique=True,
        sqlite_where=sa.text("activo_hasta IS NULL"),
        postgresql_where=sa.text("activo_hasta IS NULL"),
    )

    op.create_table(
        "ownership_transfer_case",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("case_number", sa.String(length=20), nullable=False),
        sa.Column("contract_id", sa.Integer(), nullable=False),
        sa.Column(
            "type",
            sa.Enum(
                "MORTIS_CAUSA_TESTAMENTO",
                "MORTIS_CAUSA_SIN_TESTAMENTO",
                "INTER_VIVOS",
                "PROVISIONAL",
                name="ownership_transfer_type",
            ),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Enum(
                "DRAFT",
                "DOCS_PENDING",
                "UNDER_REVIEW",
                "APPROVED",
                "REJECTED",
                "CLOSED",
                name="ownership_transfer_status",
            ),
            nullable=False,
        ),
        sa.Column("opened_at", sa.DateTime(), nullable=False),
        sa.Column("closed_at", sa.DateTime(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("assigned_to_user_id", sa.Integer(), nullable=True),
        sa.Column("resolution_number", sa.String(length=20), nullable=True),
        sa.Column("resolution_pdf_path", sa.String(length=255), nullable=True),
        sa.Column(
            "beneficiary_close_decision",
            sa.Enum("KEEP", "REPLACE", name="beneficiary_close_decision"),
            nullable=True,
        ),
        sa.Column("provisional_start_date", sa.Date(), nullable=True),
        sa.Column("provisional_until", sa.Date(), nullable=True),
        sa.Column("notes", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("internal_notes", sa.String(length=1000), nullable=False, server_default=""),
        sa.Column("rejection_reason", sa.String(length=500), nullable=True),
        sa.ForeignKeyConstraint(["assigned_to_user_id"], ["user_account.id"]),
        sa.ForeignKeyConstraint(["contract_id"], ["derecho_funerario_contrato.id"]),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["user_account.id"]),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "case_number", name="uq_ownership_case_org_number"),
        sa.UniqueConstraint("org_id", "resolution_number", name="uq_ownership_case_org_resolution"),
    )
    with op.batch_alter_table("ownership_transfer_case", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_ownership_transfer_case_org_id"), ["org_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_ownership_transfer_case_contract_id"), ["contract_id"], unique=False)
    op.create_index(
        "ix_ownership_case_org_status_opened",
        "ownership_transfer_case",
        ["org_id", "status", "opened_at"],
        unique=False,
    )
    op.create_index(
        "ix_ownership_case_org_type_status",
        "ownership_transfer_case",
        ["org_id", "type", "status"],
        unique=False,
    )

    op.create_table(
        "ownership_transfer_party",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("case_id", sa.Integer(), nullable=False),
        sa.Column(
            "role",
            sa.Enum(
                "CAUSANT",
                "ANTERIOR_TITULAR",
                "NUEVO_TITULAR",
                "REPRESENTANTE",
                "OTRO",
                name="ownership_party_role",
            ),
            nullable=False,
        ),
        sa.Column("person_id", sa.Integer(), nullable=False),
        sa.Column("percentage", sa.Numeric(precision=5, scale=2), nullable=True),
        sa.Column("notes", sa.String(length=500), nullable=False, server_default=""),
        sa.ForeignKeyConstraint(["case_id"], ["ownership_transfer_case.id"]),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.ForeignKeyConstraint(["person_id"], ["person.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("ownership_transfer_party", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_ownership_transfer_party_org_id"), ["org_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_ownership_transfer_party_case_id"), ["case_id"], unique=False)
    op.create_index(
        "ix_ownership_party_org_case_role",
        "ownership_transfer_party",
        ["org_id", "case_id", "role"],
        unique=False,
    )

    op.create_table(
        "case_document",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("case_id", sa.Integer(), nullable=False),
        sa.Column("doc_type", sa.String(length=50), nullable=False),
        sa.Column("required", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "status",
            sa.Enum("MISSING", "PROVIDED", "VERIFIED", "REJECTED", name="case_document_status"),
            nullable=False,
        ),
        sa.Column("file_path", sa.String(length=255), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(), nullable=True),
        sa.Column("verified_at", sa.DateTime(), nullable=True),
        sa.Column("verified_by_user_id", sa.Integer(), nullable=True),
        sa.Column("notes", sa.String(length=500), nullable=False, server_default=""),
        sa.ForeignKeyConstraint(["case_id"], ["ownership_transfer_case.id"]),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.ForeignKeyConstraint(["verified_by_user_id"], ["user_account.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("case_document", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_case_document_org_id"), ["org_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_case_document_case_id"), ["case_id"], unique=False)
    op.create_index(
        "ix_case_document_org_case_type",
        "case_document",
        ["org_id", "case_id", "doc_type"],
        unique=False,
    )
    op.create_index(
        "ix_case_document_org_case_required_status",
        "case_document",
        ["org_id", "case_id", "required", "status"],
        unique=False,
    )

    op.create_table(
        "publication",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("case_id", sa.Integer(), nullable=False),
        sa.Column("published_at", sa.Date(), nullable=False),
        sa.Column("channel", sa.String(length=50), nullable=False),
        sa.Column("reference_text", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("notes", sa.String(length=500), nullable=False, server_default=""),
        sa.ForeignKeyConstraint(["case_id"], ["ownership_transfer_case.id"]),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("publication", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_publication_org_id"), ["org_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_publication_case_id"), ["case_id"], unique=False)
    op.create_index(
        "ix_publication_org_case_published",
        "publication",
        ["org_id", "case_id", "published_at"],
        unique=False,
    )

    op.create_table(
        "contract_event",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("contract_id", sa.Integer(), nullable=False),
        sa.Column("case_id", sa.Integer(), nullable=True),
        sa.Column("event_type", sa.String(length=50), nullable=False),
        sa.Column("event_at", sa.DateTime(), nullable=False),
        sa.Column("details", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["case_id"], ["ownership_transfer_case.id"]),
        sa.ForeignKeyConstraint(["contract_id"], ["derecho_funerario_contrato.id"]),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["user_account.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("contract_event", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_contract_event_org_id"), ["org_id"], unique=False)
    op.create_index(
        "ix_contract_event_org_contract_at",
        "contract_event",
        ["org_id", "contract_id", "event_at"],
        unique=False,
    )
    op.create_index(
        "ix_contract_event_org_case_at",
        "contract_event",
        ["org_id", "case_id", "event_at"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_contract_event_org_case_at", table_name="contract_event")
    op.drop_index("ix_contract_event_org_contract_at", table_name="contract_event")
    with op.batch_alter_table("contract_event", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_contract_event_org_id"))
    op.drop_table("contract_event")

    op.drop_index("ix_publication_org_case_published", table_name="publication")
    with op.batch_alter_table("publication", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_publication_case_id"))
        batch_op.drop_index(batch_op.f("ix_publication_org_id"))
    op.drop_table("publication")

    op.drop_index("ix_case_document_org_case_required_status", table_name="case_document")
    op.drop_index("ix_case_document_org_case_type", table_name="case_document")
    with op.batch_alter_table("case_document", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_case_document_case_id"))
        batch_op.drop_index(batch_op.f("ix_case_document_org_id"))
    op.drop_table("case_document")

    op.drop_index("ix_ownership_party_org_case_role", table_name="ownership_transfer_party")
    with op.batch_alter_table("ownership_transfer_party", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_ownership_transfer_party_case_id"))
        batch_op.drop_index(batch_op.f("ix_ownership_transfer_party_org_id"))
    op.drop_table("ownership_transfer_party")

    op.drop_index("ix_ownership_case_org_type_status", table_name="ownership_transfer_case")
    op.drop_index("ix_ownership_case_org_status_opened", table_name="ownership_transfer_case")
    with op.batch_alter_table("ownership_transfer_case", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_ownership_transfer_case_contract_id"))
        batch_op.drop_index(batch_op.f("ix_ownership_transfer_case_org_id"))
    op.drop_table("ownership_transfer_case")

    op.drop_index("ix_beneficiario_org_contract_current", table_name="beneficiario")

    op.create_table(
        "titularidad",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("contrato_id", sa.Integer(), nullable=False),
        sa.Column("person_id", sa.Integer(), nullable=False),
        sa.Column("activo_desde", sa.Date(), nullable=False),
        sa.Column("activo_hasta", sa.Date(), nullable=True),
        sa.Column("pensionista", sa.Boolean(), nullable=False),
        sa.Column("pensionista_desde", sa.Date(), nullable=True),
        sa.ForeignKeyConstraint(["contrato_id"], ["derecho_funerario_contrato.id"]),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.ForeignKeyConstraint(["person_id"], ["person.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("titularidad", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_titularidad_org_id"), ["org_id"], unique=False)

    op.execute(
        sa.text(
            """
            INSERT INTO titularidad (
                org_id, contrato_id, person_id, activo_desde, activo_hasta, pensionista, pensionista_desde
            )
            SELECT
                org_id, contract_id, person_id, start_date, end_date, is_pensioner, pensioner_since_date
            FROM ownership_record
            """
        )
    )

    op.drop_index("ix_ownership_record_org_contract_current", table_name="ownership_record")
    op.drop_index("ix_ownership_record_org_contract_start", table_name="ownership_record")
    with op.batch_alter_table("ownership_record", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_ownership_record_org_id"))
    op.drop_table("ownership_record")
