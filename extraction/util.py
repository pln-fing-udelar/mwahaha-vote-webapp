import os

import MySQLdb.connections


def create_connection() -> MySQLdb.connections.Connection:
    return MySQLdb.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        database=os.environ["DB_NAME"],
    )


__all__ = ["create_connection"]
