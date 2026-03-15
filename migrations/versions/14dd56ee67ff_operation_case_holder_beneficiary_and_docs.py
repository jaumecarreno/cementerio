"""operation case holder/beneficiary and inhumation documentation update

Revision ID: 14dd56ee67ff
Revises: 13cc45dd56ee
Create Date: 2026-03-15 13:30:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "14dd56ee67ff"
down_revision = "13cc45dd56ee"
branch_labels = None
depends_on = None


INHUMACION_DOC_CODES: tuple[tuple[str, bool], ...] = (
    ("DNI_TITULAR", True),
    ("DNI_BENEFICIARIO", False),
    ("DNI_DIFUNTO", False),
    ("LICENCIA_ENTERRAMIENTO", True),
    ("CERTIFICADO_DEFUNCION", False),
    ("CERTIFICADO_MEDICO_DEFUNCION", False),
)


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _column_exists(bind, table_name: str, column_name: str) -> bool:
    columns = inspect(bind).get_columns(table_name)
    return any(col.get("name") == column_name for col in columns)


def _index_exists(bind, table_name: str, index_name: str) -> bool:
    indexes = inspect(bind).get_indexes(table_name)
    return any(idx.get("name") == index_name for idx in indexes)


def _foreign_key_exists(bind, table_name: str, fk_name: str) -> bool:
    fks = inspect(bind).get_foreign_keys(table_name)
    return any((fk.get("name") or "") == fk_name for fk in fks)


def _add_operation_case_columns(bind) -> None:
    if not _table_exists(bind, "operation_case"):
        return
    with op.batch_alter_table("operation_case", schema=None) as batch_op:
        if not _column_exists(bind, "operation_case", "holder_person_id"):
            batch_op.add_column(sa.Column("holder_person_id", sa.Integer(), nullable=True))
        if not _column_exists(bind, "operation_case", "beneficiary_person_id"):
            batch_op.add_column(sa.Column("beneficiary_person_id", sa.Integer(), nullable=True))

    with op.batch_alter_table("operation_case", schema=None) as batch_op:
        if not _foreign_key_exists(bind, "operation_case", "fk_operation_case_holder_person_id"):
            batch_op.create_foreign_key(
                "fk_operation_case_holder_person_id",
                "person",
                ["holder_person_id"],
                ["id"],
            )
        if not _foreign_key_exists(bind, "operation_case", "fk_operation_case_beneficiary_person_id"):
            batch_op.create_foreign_key(
                "fk_operation_case_beneficiary_person_id",
                "person",
                ["beneficiary_person_id"],
                ["id"],
            )

    if not _index_exists(bind, "operation_case", "ix_operation_case_holder_person_id"):
        op.create_index(
            "ix_operation_case_holder_person_id",
            "operation_case",
            ["holder_person_id"],
            unique=False,
        )
    if not _index_exists(bind, "operation_case", "ix_operation_case_beneficiary_person_id"):
        op.create_index(
            "ix_operation_case_beneficiary_person_id",
            "operation_case",
            ["beneficiary_person_id"],
            unique=False,
        )


def _backfill_holder_from_declarant(bind) -> None:
    if not _table_exists(bind, "operation_case"):
        return
    if not _column_exists(bind, "operation_case", "holder_person_id"):
        return
    if not _column_exists(bind, "operation_case", "declarant_person_id"):
        return
    bind.execute(
        sa.text(
            """
            UPDATE operation_case
               SET holder_person_id = declarant_person_id
             WHERE holder_person_id IS NULL
               AND declarant_person_id IS NOT NULL
            """
        )
    )


def _backfill_inhumacion_documentation(bind) -> None:
    if not _table_exists(bind, "operation_case") or not _table_exists(bind, "operation_permit"):
        return

    rows = bind.execute(
        sa.text("SELECT id FROM operation_case WHERE type = 'INHUMACION'")
    ).fetchall()
    if not rows:
        return

    expected = {code: required for code, required in INHUMACION_DOC_CODES}
    allowed_codes = set(expected.keys())

    for row in rows:
        case_id = int(row[0])
        permit_rows = bind.execute(
            sa.text(
                """
                SELECT id, permit_type
                  FROM operation_permit
                 WHERE operation_case_id = :case_id
                 ORDER BY id ASC
                """
            ),
            {"case_id": case_id},
        ).fetchall()

        by_type: dict[str, list[int]] = {}
        for permit_id, permit_type in permit_rows:
            by_type.setdefault(str(permit_type), []).append(int(permit_id))

        for permit_type, required in expected.items():
            ids = by_type.get(permit_type, [])
            if ids:
                keep_id = ids[0]
                bind.execute(
                    sa.text(
                        "UPDATE operation_permit SET required = :required WHERE id = :permit_id"
                    ),
                    {"required": required, "permit_id": keep_id},
                )
                for duplicate_id in ids[1:]:
                    bind.execute(
                        sa.text("DELETE FROM operation_permit WHERE id = :permit_id"),
                        {"permit_id": duplicate_id},
                    )
            else:
                bind.execute(
                    sa.text(
                        """
                        INSERT INTO operation_permit (
                            operation_case_id,
                            permit_type,
                            required,
                            status,
                            reference_number,
                            issued_at,
                            verified_at,
                            verified_by_user_id,
                            notes
                        ) VALUES (
                            :case_id,
                            :permit_type,
                            :required,
                            'MISSING',
                            '',
                            NULL,
                            NULL,
                            NULL,
                            ''
                        )
                        """
                    ),
                    {
                        "case_id": case_id,
                        "permit_type": permit_type,
                        "required": required,
                    },
                )

        for permit_type, ids in by_type.items():
            if permit_type in allowed_codes:
                continue
            for permit_id in ids:
                bind.execute(
                    sa.text("DELETE FROM operation_permit WHERE id = :permit_id"),
                    {"permit_id": permit_id},
                )


def _downgrade_inhumacion_documentation(bind) -> None:
    if not _table_exists(bind, "operation_case") or not _table_exists(bind, "operation_permit"):
        return

    rows = bind.execute(
        sa.text("SELECT id FROM operation_case WHERE type = 'INHUMACION'")
    ).fetchall()
    if not rows:
        return

    legacy_codes = {
        "LICENCIA_ENTERRAMIENTO": True,
        "PERMISO_SANITARIO": True,
    }
    allowed_legacy = set(legacy_codes.keys())

    for row in rows:
        case_id = int(row[0])
        permit_rows = bind.execute(
            sa.text(
                """
                SELECT id, permit_type
                  FROM operation_permit
                 WHERE operation_case_id = :case_id
                 ORDER BY id ASC
                """
            ),
            {"case_id": case_id},
        ).fetchall()
        by_type: dict[str, list[int]] = {}
        for permit_id, permit_type in permit_rows:
            by_type.setdefault(str(permit_type), []).append(int(permit_id))

        for permit_type, required in legacy_codes.items():
            ids = by_type.get(permit_type, [])
            if ids:
                keep_id = ids[0]
                bind.execute(
                    sa.text(
                        "UPDATE operation_permit SET required = :required WHERE id = :permit_id"
                    ),
                    {"required": required, "permit_id": keep_id},
                )
                for duplicate_id in ids[1:]:
                    bind.execute(
                        sa.text("DELETE FROM operation_permit WHERE id = :permit_id"),
                        {"permit_id": duplicate_id},
                    )
            else:
                bind.execute(
                    sa.text(
                        """
                        INSERT INTO operation_permit (
                            operation_case_id,
                            permit_type,
                            required,
                            status,
                            reference_number,
                            issued_at,
                            verified_at,
                            verified_by_user_id,
                            notes
                        ) VALUES (
                            :case_id,
                            :permit_type,
                            :required,
                            'MISSING',
                            '',
                            NULL,
                            NULL,
                            NULL,
                            ''
                        )
                        """
                    ),
                    {
                        "case_id": case_id,
                        "permit_type": permit_type,
                        "required": required,
                    },
                )

        for permit_type, ids in by_type.items():
            if permit_type in allowed_legacy:
                continue
            for permit_id in ids:
                bind.execute(
                    sa.text("DELETE FROM operation_permit WHERE id = :permit_id"),
                    {"permit_id": permit_id},
                )


def _drop_operation_case_columns(bind) -> None:
    if not _table_exists(bind, "operation_case"):
        return
    if _index_exists(bind, "operation_case", "ix_operation_case_holder_person_id"):
        op.drop_index("ix_operation_case_holder_person_id", table_name="operation_case")
    if _index_exists(bind, "operation_case", "ix_operation_case_beneficiary_person_id"):
        op.drop_index("ix_operation_case_beneficiary_person_id", table_name="operation_case")

    with op.batch_alter_table("operation_case", schema=None) as batch_op:
        if _foreign_key_exists(bind, "operation_case", "fk_operation_case_holder_person_id"):
            batch_op.drop_constraint("fk_operation_case_holder_person_id", type_="foreignkey")
        if _foreign_key_exists(bind, "operation_case", "fk_operation_case_beneficiary_person_id"):
            batch_op.drop_constraint(
                "fk_operation_case_beneficiary_person_id", type_="foreignkey"
            )
        if _column_exists(bind, "operation_case", "holder_person_id"):
            batch_op.drop_column("holder_person_id")
        if _column_exists(bind, "operation_case", "beneficiary_person_id"):
            batch_op.drop_column("beneficiary_person_id")


def upgrade():
    bind = op.get_bind()
    _add_operation_case_columns(bind)
    _backfill_holder_from_declarant(bind)
    _backfill_inhumacion_documentation(bind)


def downgrade():
    bind = op.get_bind()
    _downgrade_inhumacion_documentation(bind)
    _drop_operation_case_columns(bind)
