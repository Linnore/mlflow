"""add metric step

Create Date: 2019-04-22 15:29:24.921354

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect
from sqlalchemy.engine.interfaces import ReflectedForeignKeyConstraint

# revision identifiers, used by Alembic.
revision = "451aebb31d03"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = inspect(conn)

    pk = inspector.get_pk_constraint("metrics")
    pk_columns_set = set(pk["constrained_columns"])

    fks = inspector.get_foreign_keys("metrics")

    op.add_column(
        "metrics",
        sa.Column("step", sa.BigInteger(), nullable=False, server_default="0"),
    )
    # Use batch mode so that we can run "ALTER TABLE" statements against SQLite
    # databases (see more info at https://alembic.sqlalchemy.org/en/latest/batch.html#
    # running-batch-migrations-for-sqlite-and-other-databases)
    with op.batch_alter_table("metrics", recreate="auto") as batch_op:
        cache_fks: list[ReflectedForeignKeyConstraint] = []
        for fk in fks:
            flag = False
            for col in fk["constrained_columns"]:
                if col in pk_columns_set:
                    flag = True
                    break

            if flag:
                cache_fks.append(fk)
                batch_op.drop_constraint(constraint_name=fk["name"], type_="foreignkey")

        batch_op.drop_constraint(constraint_name="metric_pk", type_="primary")
        batch_op.create_primary_key(
            constraint_name="metric_pk",
            columns=["key", "timestamp", "step", "run_uuid", "value"],
        )

        for fk in cache_fks:
            batch_op.create_foreign_key(
                constraint_name=fk["name"],
                referent_table=fk["referred_table"],
                referent_schema=fk["referred_schema"],
                local_cols=fk["constrained_columns"],
                remote_cols=fk["referred_columns"],
                **fk["options"],
            )


def downgrade():
    # This migration cannot safely be downgraded; once metric data with the same
    # (key, timestamp, run_uuid, value) are inserted (differing only in their `step`), we cannot
    # revert to a schema where (key, timestamp, run_uuid, value) is the metric primary key.
    pass
