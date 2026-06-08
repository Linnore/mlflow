"""allow nulls for metric values

Create Date: 2019-07-10 22:40:18.787993

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect
from sqlalchemy.engine.interfaces import ReflectedForeignKeyConstraint

# revision identifiers, used by Alembic.
revision = "181f10493468"
down_revision = "90e64c465722"
branch_labels = None
depends_on = None


def _is_oceanbase(conn):
    dialect_name = conn.dialect.name
    driver = getattr(conn.dialect, "driver", "")

    engine = getattr(conn, "engine", None)
    url_str = str(engine.url) if engine and hasattr(engine, "url") else ""
    url_str = url_str.lower()

    return dialect_name == "mysql" and ("obmysql" in driver or "oceanbase" in url_str)


def upgrade():
    conn = op.get_bind()

    if _is_oceanbase(conn):
        inspector = inspect(conn)

        pk = inspector.get_pk_constraint("metrics")
        pk_columns_set = set(pk["constrained_columns"])

        fks = inspector.get_foreign_keys("metrics")

        with op.batch_alter_table("metrics") as batch_op:
            batch_op.alter_column("value", type_=sa.types.Float(precision=53), nullable=False)
            batch_op.add_column(
                sa.Column(
                    "is_nan",
                    sa.Boolean(create_constraint=False),
                    nullable=False,
                    server_default="0",
                )
            )

            cache_fks: list[ReflectedForeignKeyConstraint] = []
            for fk in fks:
                if any(col in pk_columns_set for col in fk["constrained_columns"]):
                    cache_fks.append(fk)
                    batch_op.drop_constraint(constraint_name=fk["name"], type_="foreignkey")

            batch_op.drop_constraint(constraint_name="metric_pk", type_="primary")
            batch_op.create_primary_key(
                constraint_name="metric_pk",
                columns=["key", "timestamp", "step", "run_uuid", "value", "is_nan"],
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
    else:
        with op.batch_alter_table("metrics") as batch_op:
            batch_op.alter_column("value", type_=sa.types.Float(precision=53), nullable=False)
            batch_op.add_column(
                sa.Column(
                    "is_nan",
                    sa.Boolean(create_constraint=False),
                    nullable=False,
                    server_default="0",
                )
            )
            batch_op.drop_constraint(constraint_name="metric_pk", type_="primary")
            batch_op.create_primary_key(
                constraint_name="metric_pk",
                columns=["key", "timestamp", "step", "run_uuid", "value", "is_nan"],
            )


def downgrade():
    pass
