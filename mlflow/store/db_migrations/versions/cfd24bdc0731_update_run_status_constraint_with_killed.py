"""Update run status constraint with killed

Create Date: 2019-10-11 15:55:10.853449

"""

import logging

import alembic
import sqlalchemy as sa
from alembic import op
from packaging.version import Version
from sqlalchemy import CheckConstraint

from mlflow.entities import RunStatus, ViewType
from mlflow.entities.lifecycle_stage import LifecycleStage
from mlflow.store.tracking.dbmodels.models import SourceTypes, SqlRun

# revision identifiers, used by Alembic.
revision = "cfd24bdc0731"
down_revision = "2b4d017a5e9b"
branch_labels = None
depends_on = None
_logger = logging.getLogger(__name__)

old_run_statuses = [
    RunStatus.to_string(RunStatus.SCHEDULED),
    RunStatus.to_string(RunStatus.FAILED),
    RunStatus.to_string(RunStatus.FINISHED),
    RunStatus.to_string(RunStatus.RUNNING),
]

new_run_statuses = [*old_run_statuses, RunStatus.to_string(RunStatus.KILLED)]

# Certain SQL backends (e.g., SQLite) do not preserve CHECK constraints during migrations.
# For these backends, CHECK constraints must be specified as table arguments. Here, we define
# the collection of CHECK constraints that should be preserved when performing the migration.
# The "status" constraint is excluded from this set because it is explicitly modified
# within the migration's `upgrade()` routine.
check_constraint_table_args = [
    CheckConstraint(SqlRun.source_type.in_(SourceTypes), name="source_type"),
    CheckConstraint(
        SqlRun.lifecycle_stage.in_(LifecycleStage.view_type_to_stages(ViewType.ALL)),
        name="runs_lifecycle_stage",
    ),
]


def upgrade():
    _logger.info(f"Migration {revision} start")

    # --- Detect current CHECK constraint name on 'status' ---
    conn = op.get_bind()
    res = conn.execute(
        sa.text("""
        SELECT cc.CONSTRAINT_NAME
        FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc
        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
          ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
         AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        WHERE
          tc.TABLE_SCHEMA = DATABASE()
          AND tc.TABLE_NAME = 'runs'
          AND cc.CHECK_CLAUSE LIKE '%status%'
    """)
    ).fetchone()

    if res:
        check_name = res[0]
        _logger.info(f"Dropping existing CHECK constraint: {check_name}")
        # Use raw op.execute (outside batch) for reliability
        op.execute(sa.text(f"ALTER TABLE runs DROP CHECK {check_name}"))
    else:
        _logger.info("No CHECK constraint found on 'status' — skipping drop.")

    # In alembic >= 1.7.0, `table_args` is unnecessary since CHECK constraints are preserved
    # during migrations.
    table_args = (
        [] if Version(alembic.__version__) >= Version("1.7.0") else check_constraint_table_args
    )

    with op.batch_alter_table("runs", table_args=table_args) as batch_op:
        # Transform the "status" column to an `Enum` and define a new check constraint. Specify
        # `native_enum=False` to create a check constraint rather than a
        # database-backend-dependent enum (see https://docs.sqlalchemy.org/en/13/core/
        # type_basics.html#sqlalchemy.types.Enum.params.native_enum)

        # Step 2: Alter column — now safe
        batch_op.alter_column(
            "status",
            type_=sa.VARCHAR(9),  # longest: 'SCHEDULED'=9, 'KILLED'=6
            nullable=True,  # or False if your schema requires it
            existing_type=sa.VARCHAR(9),
            existing_nullable=True,
        )

    # --- Re-add new CHECK (outside batch or inside with execute) ---
    vals = ", ".join(f"'{v}'" for v in new_run_statuses)
    op.execute(sa.text(f"ALTER TABLE runs ADD CONSTRAINT status CHECK (status IN ({vals}))"))

    _logger.info(f"✓ Migration {revision} completed.")


def downgrade():
    # Omit downgrade logic for now - we don't currently provide users a command/API for
    # reverting a database migration, instead recommending that they take a database backup
    # before running the migration.
    pass
