"""reporting schedules schema

Revision ID: 11aa22bb33cc
Revises: d4e5f6a7b8c9
Create Date: 2026-03-03 18:10:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "11aa22bb33cc"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _index_exists(bind, table_name: str, index_name: str) -> bool:
    indexes = inspect(bind).get_indexes(table_name)
    return any(idx.get("name") == index_name for idx in indexes)


def upgrade():
    bind = op.get_bind()

    if not _table_exists(bind, "report_schedule"):
        op.create_table(
            "report_schedule",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("name", sa.String(length=120), nullable=False),
            sa.Column("report_key", sa.String(length=80), nullable=False),
            sa.Column("cadence", sa.String(length=20), nullable=False),
            sa.Column("day_of_week", sa.Integer(), nullable=True),
            sa.Column("day_of_month", sa.Integer(), nullable=True),
            sa.Column("run_time", sa.String(length=5), nullable=False, server_default="07:00"),
            sa.Column("timezone", sa.String(length=64), nullable=False, server_default="Europe/Madrid"),
            sa.Column("recipients", sa.Text(), nullable=False, server_default=""),
            sa.Column("filters_json", sa.Text(), nullable=False, server_default="{}"),
            sa.Column("formats", sa.String(length=40), nullable=False, server_default="CSV"),
            sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("last_run_at", sa.DateTime(), nullable=True),
            sa.Column("created_by_user_id", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
            sa.ForeignKeyConstraint(["created_by_user_id"], ["user_account.id"]),
        )
    if not _index_exists(bind, "report_schedule", "ix_report_schedule_org_active_cadence"):
        op.create_index(
            "ix_report_schedule_org_active_cadence",
            "report_schedule",
            ["org_id", "active", "cadence"],
        )

    if not _table_exists(bind, "report_delivery_log"):
        op.create_table(
            "report_delivery_log",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), nullable=False, index=True),
            sa.Column("schedule_id", sa.Integer(), nullable=False, index=True),
            sa.Column("run_at", sa.DateTime(), nullable=False),
            sa.Column("status", sa.String(length=20), nullable=False, server_default="SUCCESS"),
            sa.Column("rows_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("artifacts_json", sa.Text(), nullable=False, server_default="[]"),
            sa.Column("error", sa.Text(), nullable=False, server_default=""),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.ForeignKeyConstraint(["org_id"], ["organization.id"]),
            sa.ForeignKeyConstraint(["schedule_id"], ["report_schedule.id"]),
        )
    if not _index_exists(bind, "report_delivery_log", "ix_report_delivery_schedule_run"):
        op.create_index(
            "ix_report_delivery_schedule_run",
            "report_delivery_log",
            ["schedule_id", "run_at"],
        )


def downgrade():
    bind = op.get_bind()
    if _table_exists(bind, "report_delivery_log"):
        if _index_exists(bind, "report_delivery_log", "ix_report_delivery_schedule_run"):
            op.drop_index("ix_report_delivery_schedule_run", table_name="report_delivery_log")
        op.drop_table("report_delivery_log")

    if _table_exists(bind, "report_schedule"):
        if _index_exists(bind, "report_schedule", "ix_report_schedule_org_active_cadence"):
            op.drop_index("ix_report_schedule_org_active_cadence", table_name="report_schedule")
        op.drop_table("report_schedule")
