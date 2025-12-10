"""add metric step

Create Date: 2019-04-22 15:29:24.921354

"""

import logging
from collections import defaultdict

import sqlalchemy as sa
from alembic import op

_logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = "451aebb31d03"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    _logger.info(f"Migration {revision} start")

    conn = op.get_bind()

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

    # Aggregate multi-column FKs
    fk_groups = defaultdict(lambda: {"local_cols": [], "remote_table": None, "remote_cols": []})

    for row in fk_rows:
        name, local_col, remote_table, remote_col, _ = row
        fk_groups[name]["local_cols"].append(local_col)
        fk_groups[name]["remote_table"] = remote_table
        fk_groups[name]["remote_cols"].append(remote_col)

    # Filter: keep only FKs where local columns ⊆ old PK
    saved_fks = []
    old_pk_set = set(old_pk_columns)
    for name, info in fk_groups.items():
        local_cols = info["local_cols"]
        if set(local_cols) <= old_pk_set:
            saved_fks.append((name, info["remote_table"], local_cols, info["remote_cols"]))
            _logger.info(
                f"→ FK '{name}' on metrics({', '.join(local_cols)}) "
                f"→ {info['remote_table']}({', '.join(info['remote_cols'])})"
            )

    # ========= Step 3: Drop the outgoing FKs =========
    for fk_name, _, _, _ in saved_fks:
        _logger.info(f"  Dropping FK '{fk_name}'...")
        op.drop_constraint(constraint_name=fk_name, table_name="metrics", type_="foreignkey")

    # ========= Step 4: Alter PK — add `step`, recreate PK =========
    _logger.info("Adding column 'step'...")
    op.add_column(
        "metrics",
        sa.Column("step", sa.BigInteger(), nullable=False, server_default="0"),
    )

    _logger.info("Dropping old primary key and creating new one...")
    with op.batch_alter_table("metrics") as batch_op:
        batch_op.drop_constraint(
            constraint_name="PRIMARY", type_="primary"
        )  # OceanBase uses "PRIMARY"
        new_pk_columns = old_pk_columns + ["step"]
        batch_op.create_primary_key(
            constraint_name="metric_pk",  # named this time
            columns=new_pk_columns,
        )
    _logger.info(f"✓ New PK created on: {new_pk_columns}")

    # ========= Step 5: Recreate the FKs =========
    for fk_name, remote_table, local_cols, remote_cols in saved_fks:
        _logger.info(f"  Recreating FK '{fk_name}'...")
        op.create_foreign_key(
            constraint_name=fk_name,
            source_table="metrics",
            referent_table=remote_table,
            local_cols=local_cols,
            remote_cols=remote_cols,
            ondelete="CASCADE",  # adjust if original used RESTRICT/NO ACTION
        )

    _logger.info(f"✓ Migration {revision} completed.")


def downgrade():
    # This migration cannot safely be downgraded; once metric data with the same
    # (key, timestamp, run_uuid, value) are inserted (differing only in their `step`), we cannot
    # revert to a schema where (key, timestamp, run_uuid, value) is the metric primary key.
    pass
