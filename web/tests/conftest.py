import datetime
import os
import random
import re
import sqlite3
from collections.abc import AsyncGenerator
from typing import Any

from cryptography.fernet import Fernet

# Ensure required environment variables are set before importing application modules.
os.environ.setdefault("BATTLE_TOKEN_SECRET", Fernet.generate_key().decode())
os.environ.setdefault("TURNSTILE_SECRET_KEY", "test-turnstile-secret")
os.environ.setdefault("DB_USER", "root")
os.environ.setdefault("DB_PASS", "password")
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_NAME", "mwahaha")

import pytest
import sqlalchemy
import sqlalchemy.event
import sqlalchemy.ext.asyncio

sqlite3.register_adapter(datetime.datetime, lambda val: val.isoformat(" "))
sqlite3.register_converter("TIMESTAMP", lambda val: datetime.datetime.fromisoformat(val.decode()))
sqlite3.register_converter("BOOL", lambda val: bool(int(val)))

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE prompts (
        prompt_id VARCHAR(10) NOT NULL PRIMARY KEY,
        word1 VARCHAR(50),
        word2 VARCHAR(50),
        headline VARCHAR(2048),
        url VARCHAR(2048),
        prompt VARCHAR(256),
        task VARCHAR(5) NOT NULL,
        phase_id INT NOT NULL
    )
    """,
    """
    CREATE TABLE systems (
        system_id VARCHAR(100) NOT NULL PRIMARY KEY
    )
    """,
    """
    CREATE TABLE outputs (
        prompt_id VARCHAR(10) NOT NULL,
        system_id VARCHAR(100) NOT NULL,
        text VARCHAR(2048) NOT NULL,
        PRIMARY KEY (prompt_id, system_id),
        FOREIGN KEY (prompt_id) REFERENCES prompts (prompt_id),
        FOREIGN KEY (system_id) REFERENCES systems (system_id)
    )
    """,
    """
    CREATE TABLE votes (
        prompt_id VARCHAR(10) NOT NULL,
        system_id_a VARCHAR(100) NOT NULL,
        system_id_b VARCHAR(100) NOT NULL,
        session_id CHAR(100) NOT NULL,
        vote CHAR(1) NOT NULL,
        date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_offensive_a BOOL DEFAULT 0,
        is_offensive_b BOOL DEFAULT 0,
        PRIMARY KEY (prompt_id, system_id_a, system_id_b, session_id),
        FOREIGN KEY (prompt_id, system_id_a) REFERENCES outputs (prompt_id, system_id),
        FOREIGN KEY (prompt_id, system_id_b) REFERENCES outputs (prompt_id, system_id)
    )
    """,
    """
    CREATE TABLE prolific (
        session_id CHAR(100) NOT NULL PRIMARY KEY,
        consent_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        finish_date TIMESTAMP NULL,
        comments VARCHAR(300) DEFAULT NULL
    )
    """,
]


def configure_sqlite_mysql_compat(engine: sqlalchemy.ext.asyncio.AsyncEngine) -> None:
    """Registers SQLite functions and query rewrites so MySQL queries in `database.py` run on in-memory SQLite."""

    @sqlalchemy.event.listens_for(engine.sync_engine, "connect")
    def _on_connect(dbapi_conn: Any, _: Any) -> None:
        dbapi_conn.create_function("RAND", 0, random.random)
        dbapi_conn.create_function(
            "NOW", 0, lambda: datetime.datetime.now(datetime.UTC).replace(tzinfo=None).isoformat(" ")
        )

    @sqlalchemy.event.listens_for(engine.sync_engine, "before_cursor_execute", retval=True)
    def _before_cursor_execute(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> tuple[str, Any]:
        statement = re.sub(r"ON DUPLICATE KEY UPDATE \w+\s*=\s*\w+", "ON CONFLICT DO NOTHING", statement)
        if not executemany and isinstance(parameters, (list, tuple)) and any(isinstance(p, tuple) for p in parameters):
            new_params: list[Any] = []
            parts = statement.split("?")
            new_stmt = [parts[0]]
            for part, param in zip(parts[1:], parameters, strict=True):
                if isinstance(param, tuple):
                    placeholders = ", ".join("?" for _ in param)
                    new_stmt.append(f"({placeholders}){part}")
                    new_params.extend(param)
                else:
                    new_stmt.append(f"?{part}")
                    new_params.append(param)
            statement = "".join(new_stmt)
            parameters = tuple(new_params)
        return statement, parameters


@pytest.fixture
async def db_engine() -> AsyncGenerator[sqlalchemy.ext.asyncio.AsyncEngine]:
    engine = sqlalchemy.ext.asyncio.create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"detect_types": sqlite3.PARSE_DECLTYPES},
        isolation_level="AUTOCOMMIT",
    )
    configure_sqlite_mysql_compat(engine)
    async with engine.begin() as conn:
        for ddl in SCHEMA_STATEMENTS:
            await conn.execute(sqlalchemy.sql.text(ddl))
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def seeded_db_engine(
    db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
) -> sqlalchemy.ext.asyncio.AsyncEngine:
    async with db_engine.begin() as conn:
        await conn.execute(
            sqlalchemy.sql.text("""
                INSERT INTO prompts (prompt_id, word1, word2, headline, url, prompt, task, phase_id) VALUES
                ('en_1', 'cat', 'dog', NULL, NULL, NULL, 't3', 1),
                ('es_1', NULL, NULL, 'Un titular gracioso', NULL, NULL, 't3', 1),
                ('zh_1', NULL, NULL, NULL, 'https://example.com/img.gif', 'Caption this', 't3', 1)
            """)
        )
        await conn.execute(
            sqlalchemy.sql.text("""
                INSERT INTO systems (system_id) VALUES ('sys_a'), ('sys_b'), ('sys_c')
            """)
        )
        await conn.execute(
            sqlalchemy.sql.text("""
                INSERT INTO outputs (prompt_id, system_id, text) VALUES
                ('en_1', 'sys_a', 'A cat and a dog walk into a bar.'),
                ('en_1', 'sys_b', 'Why did the dog chase the cat?'),
                ('en_1', 'sys_c', 'A cat and a dog walk into a bar.'),
                ('es_1', 'sys_a', 'Chiste en español A.'),
                ('es_1', 'sys_b', 'Chiste en español B.'),
                ('zh_1', 'sys_a', '中文笑话 A'),
                ('zh_1', 'sys_b', '中文笑话 B')
            """)
        )
    return db_engine
