import os

import MySQLdb.connections
import sqlalchemy


def create_engine() -> sqlalchemy.Engine:
    return sqlalchemy.create_engine(
        f"mysql://{os.environ['DB_USER']}:{os.environ['DB_PASS']}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}"
    )


def create_connection() -> MySQLdb.connections.Connection:
    return MySQLdb.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        database=os.environ["DB_NAME"],
    )


__all__ = ["create_connection", "create_engine"]
