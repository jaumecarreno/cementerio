"""activity log table for global recent activity

Revision ID: a9b8c7d6e5f4
Revises: e5f6a7b8c9d0
Create Date: 2026-03-02 12:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a9b8c7d6e5f4"
down_revision = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "activity_log",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("sepultura_id", sa.Integer(), nullable=True),
        sa.Column("action_type", sa.String(length=60), nullable=False),
        sa.Column("details", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
        sa.ForeignKeyConstraint(["sepultura_id"], ["sepultura.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["user_account.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_activity_log_org_id", "activity_log", ["org_id"], unique=False)
    op.create_index("ix_activity_log_sepultura_id", "activity_log", ["sepultura_id"], unique=False)
    op.create_index("ix_activity_log_user_id", "activity_log", ["user_id"], unique=False)
    op.create_index("ix_activity_log_created_at", "activity_log", ["created_at"], unique=False)
    op.create_index(
        "ix_activity_log_org_created_at",
        "activity_log",
        ["org_id", "created_at"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_activity_log_org_created_at", table_name="activity_log")
    op.drop_index("ix_activity_log_created_at", table_name="activity_log")
    op.drop_index("ix_activity_log_user_id", table_name="activity_log")
    op.drop_index("ix_activity_log_sepultura_id", table_name="activity_log")
    op.drop_index("ix_activity_log_org_id", table_name="activity_log")
    op.drop_table("activity_log")
