"""allow nulls for metric values

Create Date: 2019-07-10 22:40:18.787993

"""

import logging
from collections import defaultdict

import sqlalchemy as sa
from alembic import op

revision = "181f10493468"
down_revision = "90e64c465722"
branch_labels = None
depends_on = None
_logger = logging.getLogger(__name__)


def upgrade():
    _logger.info(f"Migration {revision} start")

    conn = op.get_bind()

    _logger.info("Migration 181f10493468 start")
    # ========= Step 1: Get current PK columns of `metrics` =========
    pk_query = sa.text("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE
            TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = 'metrics'
            AND CONSTRAINT_NAME = 'PRIMARY'
        ORDER BY ORDINAL_POSITION
    """)
    old_pk_columns = [row[0] for row in conn.execute(pk_query).fetchall()]
    if not old_pk_columns:
        raise RuntimeError("No primary key found on table 'metrics'")
    _logger.info(f"Old PK columns: {old_pk_columns}")

    # ========= Step 2: Get outgoing FKs from `metrics` (metrics → other tables) =========
    fk_query = sa.text("""
        SELECT
            kcu.CONSTRAINT_NAME,
            kcu.COLUMN_NAME,
            kcu.REFERENCED_TABLE_NAME,
            kcu.REFERENCED_COLUMN_NAME,
            kcu.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        WHERE
            kcu.TABLE_SCHEMA = DATABASE()
            AND kcu.TABLE_NAME = 'metrics'
            AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
    """)
    fk_rows = conn.execute(fk_query).fetchall()

    fk_groups = defaultdict(lambda: {"local_cols": [], "remote_table": None, "remote_cols": []})

    for row in fk_rows:
        name, local_col, remote_table, remote_col, _ = row
        fk_groups[name]["local_cols"].append(local_col)
        fk_groups[name]["remote_table"] = remote_table
        fk_groups[name]["remote_cols"].append(remote_col)

    # Keep only FKs where local columns ⊆ old PK
    saved_fks = []
    old_pk_set = set(old_pk_columns)
    for name, info in fk_groups.items():
        local_cols = info["local_cols"]
        if set(local_cols) <= old_pk_set:
            saved_fks.append((name, info["remote_table"], local_cols, info["remote_cols"]))
            _logger.info(
                f"→ FK '{name}' on metrics({', '.join(local_cols)}) → {info['remote_table']}"
            )

    # ========= Step 3: Drop the outgoing FKs =========
    for fk_name, _, _, _ in saved_fks:
        op.drop_constraint(constraint_name=fk_name, table_name="metrics", type_="foreignkey")

    # ========= Step 4: Alter table in batch =========
    with op.batch_alter_table("metrics") as batch_op:
        # 4a. Alter 'value' to Float (non-null)
        batch_op.alter_column(
            "value",
            type_=sa.types.Float(precision=53),
            nullable=False,  # remains NOT NULL
        )

        # 4b. Add 'is_nan'
        batch_op.add_column(
            sa.Column(
                "is_nan", sa.Boolean(create_constraint=False), nullable=False, server_default="0"
            )
        )

        # 4c. Drop old PK
        batch_op.drop_constraint(constraint_name="PRIMARY", type_="primary")

        # 4d. Create new PK: old PK + ['is_nan']
        new_pk_columns = old_pk_columns + ["is_nan"]
        batch_op.create_primary_key(constraint_name="metric_pk", columns=new_pk_columns)

    _logger.info(f"✓ New PK: {new_pk_columns}")

    # ========= Step 5: Recreate FKs =========
    for fk_name, remote_table, local_cols, remote_cols in saved_fks:
        op.create_foreign_key(
            constraint_name=fk_name,
            source_table="metrics",
            referent_table=remote_table,
            local_cols=local_cols,
            remote_cols=remote_cols,
            ondelete="CASCADE",
        )

    _logger.info(f"✓ Migration {revision} completed.")
