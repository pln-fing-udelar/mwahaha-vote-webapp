import os
from collections.abc import Iterator, Sequence

import MySQLdb.connections


def chunks[T](seq: Sequence[T], n: int) -> Iterator[Sequence[T]]:
    """Yield successive n-sized chunks."""
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def create_connection() -> MySQLdb.connections.Connection:
    return MySQLdb.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        database=os.environ["DB_NAME"],
    )


__all__ = ["chunks", "create_connection"]
