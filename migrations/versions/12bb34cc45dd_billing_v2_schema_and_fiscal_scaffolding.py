"""billing v2 schema and fiscal scaffolding

Revision ID: 12bb34cc45dd
Revises: 11aa22bb33cc
Create Date: 2026-03-04 10:30:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "12bb34cc45dd"
down_revision = "11aa22bb33cc"
branch_labels = None
depends_on = None


billing_document_v2_type = sa.Enum(
    "INVOICE",
    "CREDIT_NOTE",
    name="billing_document_v2_type",
    create_type=False,
)
billing_document_v2_status = sa.Enum(
    "DRAFT",
    "ISSUED",
    "PARTIALLY_PAID",
    "PAID",
    "CANCELLED",
    name="billing_document_v2_status",
    create_type=False,
)
billing_document_v2_fiscal_status = sa.Enum(
    "PENDING",
    "SENT",
    "ACCEPTED",
    "REJECTED",
    "RETRYING",
    name="billing_document_v2_fiscal_status",
    create_type=False,
)
payment_method_v2 = sa.Enum(
    "EFECTIVO",
    "TARJETA",
    "TRANSFERENCIA",
    "BIZUM",
    name="payment_method_v2",
    create_type=False,
)
fiscal_submission_v2_status = sa.Enum(
    "PENDING",
    "SENT",
    "ACCEPTED",
    "REJECTED",
    "RETRYING",
    name="fiscal_submission_v2_status",
    create_type=False,
)


def upgrade():
    bind = op.get_bind()
    billing_document_v2_type.create(bind, checkfirst=True)
    billing_document_v2_status.create(bind, checkfirst=True)
    billing_document_v2_fiscal_status.create(bind, checkfirst=True)
    payment_method_v2.create(bind, checkfirst=True)
    fiscal_submission_v2_status.create(bind, checkfirst=True)

    op.create_table(
        "billing_document_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("contract_id", sa.Integer(), nullable=True),
        sa.Column("sepultura_id", sa.Integer(), nullable=True),
        sa.Column("original_document_id", sa.Integer(), nullable=True),
        sa.Column("document_type", billing_document_v2_type, nullable=False),
        sa.Column("status", billing_document_v2_status, nullable=False),
        sa.Column("fiscal_status", billing_document_v2_fiscal_status, nullable=False),
        sa.Column("number", sa.String(length=60), nullable=True),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("total_amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("residual_amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("issued_at", sa.DateTime(), nullable=True),
        sa.Column("cancelled_at", sa.DateTime(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("total_amount >= 0", name="ck_billing_document_v2_total_non_negative"),
        sa.CheckConstraint("residual_amount >= 0", name="ck_billing_document_v2_residual_non_negative"),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.ForeignKeyConstraint(["contract_id"], ["derecho_funerario_contrato.id"]),
        sa.ForeignKeyConstraint(["sepultura_id"], ["sepultura.id"]),
        sa.ForeignKeyConstraint(["original_document_id"], ["billing_document_v2.id"]),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["user_account.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "number", name="uq_billing_document_v2_org_number"),
    )
    op.create_index("ix_billing_document_v2_org_id", "billing_document_v2", ["org_id"], unique=False)
    op.create_index("ix_billing_document_v2_contract_id", "billing_document_v2", ["contract_id"], unique=False)
    op.create_index("ix_billing_document_v2_sepultura_id", "billing_document_v2", ["sepultura_id"], unique=False)
    op.create_index("ix_billing_document_v2_original_document_id", "billing_document_v2", ["original_document_id"], unique=False)
    op.create_index(
        "ix_billing_document_v2_org_status_issued_at",
        "billing_document_v2",
        ["org_id", "status", "issued_at"],
        unique=False,
    )
    op.create_index(
        "ix_billing_document_v2_org_contract_status",
        "billing_document_v2",
        ["org_id", "contract_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_billing_document_v2_org_fiscal_status",
        "billing_document_v2",
        ["org_id", "fiscal_status"],
        unique=False,
    )

    op.create_table(
        "billing_line_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("document_id", sa.Integer(), nullable=False),
        sa.Column("line_no", sa.Integer(), nullable=False),
        sa.Column("concept", sa.String(length=255), nullable=False),
        sa.Column("quantity", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("unit_price", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("tax_rate", sa.Numeric(precision=5, scale=2), nullable=False),
        sa.Column("net_amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("tax_amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("total_amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("quantity > 0", name="ck_billing_line_v2_qty_positive"),
        sa.CheckConstraint("unit_price >= 0", name="ck_billing_line_v2_price_non_negative"),
        sa.CheckConstraint("total_amount >= 0", name="ck_billing_line_v2_total_non_negative"),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.ForeignKeyConstraint(["document_id"], ["billing_document_v2.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "document_id", "line_no", name="uq_billing_line_v2_org_doc_line"),
    )
    op.create_index("ix_billing_line_v2_org_id", "billing_line_v2", ["org_id"], unique=False)
    op.create_index("ix_billing_line_v2_document_id", "billing_line_v2", ["document_id"], unique=False)
    op.create_index(
        "ix_billing_line_v2_org_document",
        "billing_line_v2",
        ["org_id", "document_id"],
        unique=False,
    )

    op.create_table(
        "payment_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("document_id", sa.Integer(), nullable=False),
        sa.Column("amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("method", payment_method_v2, nullable=False),
        sa.Column("receipt_number", sa.String(length=60), nullable=False),
        sa.Column("external_reference", sa.String(length=120), nullable=False),
        sa.Column("paid_at", sa.DateTime(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("amount > 0", name="ck_payment_v2_amount_positive"),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.ForeignKeyConstraint(["document_id"], ["billing_document_v2.id"]),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["user_account.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "receipt_number", name="uq_payment_v2_org_receipt"),
    )
    op.create_index("ix_payment_v2_org_id", "payment_v2", ["org_id"], unique=False)
    op.create_index("ix_payment_v2_document_id", "payment_v2", ["document_id"], unique=False)
    op.create_index(
        "ix_payment_v2_org_paid_at",
        "payment_v2",
        ["org_id", "paid_at"],
        unique=False,
    )

    op.create_table(
        "payment_allocation_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("payment_id", sa.Integer(), nullable=False),
        sa.Column("document_id", sa.Integer(), nullable=False),
        sa.Column("amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("amount > 0", name="ck_payment_allocation_v2_amount_positive"),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.ForeignKeyConstraint(["payment_id"], ["payment_v2.id"]),
        sa.ForeignKeyConstraint(["document_id"], ["billing_document_v2.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id",
            "payment_id",
            "document_id",
            name="uq_payment_allocation_v2_org_payment_document",
        ),
    )
    op.create_index("ix_payment_allocation_v2_org_id", "payment_allocation_v2", ["org_id"], unique=False)
    op.create_index("ix_payment_allocation_v2_payment_id", "payment_allocation_v2", ["payment_id"], unique=False)
    op.create_index("ix_payment_allocation_v2_document_id", "payment_allocation_v2", ["document_id"], unique=False)

    op.create_table(
        "fiscal_submission_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("document_id", sa.Integer(), nullable=False),
        sa.Column("status", fiscal_submission_v2_status, nullable=False),
        sa.Column("provider_name", sa.String(length=80), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("external_submission_id", sa.String(length=120), nullable=False),
        sa.Column("request_payload_json", sa.Text(), nullable=False),
        sa.Column("response_payload_json", sa.Text(), nullable=False),
        sa.Column("error_message", sa.String(length=255), nullable=False),
        sa.Column("last_attempt_at", sa.DateTime(), nullable=True),
        sa.Column("accepted_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.ForeignKeyConstraint(["document_id"], ["billing_document_v2.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_fiscal_submission_v2_org_id", "fiscal_submission_v2", ["org_id"], unique=False)
    op.create_index("ix_fiscal_submission_v2_document_id", "fiscal_submission_v2", ["document_id"], unique=False)
    op.create_index(
        "ix_fiscal_submission_v2_org_status",
        "fiscal_submission_v2",
        ["org_id", "status"],
        unique=False,
    )

    op.create_table(
        "billing_sequence_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("sequence_key", sa.String(length=30), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("current_value", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "sequence_key", "year", name="uq_billing_sequence_v2_org_key_year"),
    )
    op.create_index("ix_billing_sequence_v2_org_id", "billing_sequence_v2", ["org_id"], unique=False)

    op.create_table(
        "idempotency_request_v2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("endpoint", sa.String(length=120), nullable=False),
        sa.Column("idempotency_key", sa.String(length=120), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("response_status", sa.Integer(), nullable=False),
        sa.Column("response_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "org_id",
            "endpoint",
            "idempotency_key",
            name="uq_idempotency_request_v2_org_endpoint_key",
        ),
    )
    op.create_index("ix_idempotency_request_v2_org_id", "idempotency_request_v2", ["org_id"], unique=False)
    op.create_index(
        "ix_idempotency_request_v2_org_created",
        "idempotency_request_v2",
        ["org_id", "created_at"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_idempotency_request_v2_org_created", table_name="idempotency_request_v2")
    op.drop_index("ix_idempotency_request_v2_org_id", table_name="idempotency_request_v2")
    op.drop_table("idempotency_request_v2")

    op.drop_index("ix_billing_sequence_v2_org_id", table_name="billing_sequence_v2")
    op.drop_table("billing_sequence_v2")

    op.drop_index("ix_fiscal_submission_v2_org_status", table_name="fiscal_submission_v2")
    op.drop_index("ix_fiscal_submission_v2_document_id", table_name="fiscal_submission_v2")
    op.drop_index("ix_fiscal_submission_v2_org_id", table_name="fiscal_submission_v2")
    op.drop_table("fiscal_submission_v2")

    op.drop_index("ix_payment_allocation_v2_document_id", table_name="payment_allocation_v2")
    op.drop_index("ix_payment_allocation_v2_payment_id", table_name="payment_allocation_v2")
    op.drop_index("ix_payment_allocation_v2_org_id", table_name="payment_allocation_v2")
    op.drop_table("payment_allocation_v2")

    op.drop_index("ix_payment_v2_org_paid_at", table_name="payment_v2")
    op.drop_index("ix_payment_v2_document_id", table_name="payment_v2")
    op.drop_index("ix_payment_v2_org_id", table_name="payment_v2")
    op.drop_table("payment_v2")

    op.drop_index("ix_billing_line_v2_org_document", table_name="billing_line_v2")
    op.drop_index("ix_billing_line_v2_document_id", table_name="billing_line_v2")
    op.drop_index("ix_billing_line_v2_org_id", table_name="billing_line_v2")
    op.drop_table("billing_line_v2")

    op.drop_index("ix_billing_document_v2_org_fiscal_status", table_name="billing_document_v2")
    op.drop_index("ix_billing_document_v2_org_contract_status", table_name="billing_document_v2")
    op.drop_index("ix_billing_document_v2_org_status_issued_at", table_name="billing_document_v2")
    op.drop_index("ix_billing_document_v2_original_document_id", table_name="billing_document_v2")
    op.drop_index("ix_billing_document_v2_sepultura_id", table_name="billing_document_v2")
    op.drop_index("ix_billing_document_v2_contract_id", table_name="billing_document_v2")
    op.drop_index("ix_billing_document_v2_org_id", table_name="billing_document_v2")
    op.drop_table("billing_document_v2")

    bind = op.get_bind()
    fiscal_submission_v2_status.drop(bind, checkfirst=True)
    payment_method_v2.drop(bind, checkfirst=True)
    billing_document_v2_fiscal_status.drop(bind, checkfirst=True)
    billing_document_v2_status.drop(bind, checkfirst=True)
    billing_document_v2_type.drop(bind, checkfirst=True)
