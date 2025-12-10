"""add cascading deletion to datasets from experiments

Create Date: 2024-11-11 15:27:53.189685

"""

import logging

import sqlalchemy as sa
from alembic import op

from mlflow.exceptions import MlflowException
from mlflow.store.tracking.dbmodels.models import SqlDataset, SqlExperiment

# revision identifiers, used by Alembic.
revision = "0584bdc529eb"
down_revision = "f5a4f2784254"
branch_labels = None
depends_on = None
_logger = logging.getLogger(__name__)


def get_datasets_experiment_fk_name():
    conn = op.get_bind()
    query = sa.text("""
        SELECT CONSTRAINT_NAME
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
        WHERE
            CONSTRAINT_SCHEMA = DATABASE()
            AND TABLE_NAME = :child_table
            AND REFERENCED_TABLE_NAME = :parent_table
        LIMIT 1
    """)
    result = conn.execute(
        query,
        {
            "child_table": SqlDataset.__tablename__,
            "parent_table": SqlExperiment.__tablename__,
        },
    ).fetchone()

    if not result:
        raise RuntimeError(
            f"No foreign key found from '{SqlDataset.__tablename__}' "
            f"to '{SqlExperiment.__tablename__}'."
        )
        raise MlflowException(
            "Unable to find the foreign key constraint name from datasets to experiments. "
            "All foreign key constraints in datasets table: \n"
        )

    return result[0]  # e.g., 'datasets_OBFK_1734567890'


def upgrade():
    _logger.info(f"Migration {revision} start")

    dialect_name = op.get_context().dialect.name

    # standardize the constraint to sqlite naming convention
    new_fk_constraint_name = (
        f"fk_{SqlDataset.__tablename__}_experiment_id_{SqlExperiment.__tablename__}"
    )

    if dialect_name == "sqlite":
        # Only way to drop unnamed fk constraint in sqllite
        # See https://alembic.sqlalchemy.org/en/latest/batch.html#dropping-unnamed-or-named-foreign-key-constraints
        with op.batch_alter_table(
            SqlDataset.__tablename__,
            schema=None,
            naming_convention={
                "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            },
        ) as batch_op:
            # in SQLite, constraint.name is None, so we have to hardcode it
            batch_op.drop_constraint(new_fk_constraint_name, type_="foreignkey")
            # Need to explicitly name the fk constraint with batch alter table
            batch_op.create_foreign_key(
                new_fk_constraint_name,
                SqlExperiment.__tablename__,
                ["experiment_id"],
                ["experiment_id"],
                ondelete="CASCADE",
            )
    else:
        old_fk_constraint_name = get_datasets_experiment_fk_name()
        op.drop_constraint(old_fk_constraint_name, SqlDataset.__tablename__, type_="foreignkey")
        op.create_foreign_key(
            new_fk_constraint_name,
            SqlDataset.__tablename__,
            SqlExperiment.__tablename__,
            ["experiment_id"],
            ["experiment_id"],
            ondelete="CASCADE",
        )

    _logger.info(f"✓ Migration {revision} completed.")


def downgrade():
    pass
