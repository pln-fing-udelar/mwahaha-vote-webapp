"""Provides mechanisms to handle the database."""

import datetime
import os
import random
from collections.abc import AsyncIterator, Iterable, MutableMapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal, cast, get_args

import pandas as pd
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.sql

Task = Literal["a-en", "a-es", "a-zh", "b1", "b2"]
TASK_CHOICES = frozenset(get_args(Task))


@dataclass(frozen=True)
class System:
    id: str

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: Any) -> bool:
        return self.id == other.id if isinstance(other, type(self)) else NotImplemented


def prompt_id_to_task(prompt_id: str) -> Task:
    if prompt_id.startswith(("en_", "es_", "zh_")):
        return cast(Task, f"a-{prompt_id[:2]}")
    elif prompt_id.startswith("img_2_"):
        return "b2"
    elif prompt_id.startswith("img_"):
        return "b1"
    else:
        raise ValueError(f"Cannot determine the task for prompt ID '{prompt_id}'")


@dataclass(frozen=True)
class Prompt:
    id: str
    word1: str | None = None
    word2: str | None = None
    headline: str | None = None
    url: str | None = None
    prompt: str | None = None

    def __post_init__(self) -> None:
        if self.word1:
            if not self.word2 or self.headline or self.prompt or self.url:
                raise ValueError(
                    "If `word1` is set, `word2` must be set and `headline`, `prompt` and `url` must be none."
                )
        elif self.headline:
            if self.word1 or self.word2 or self.prompt or self.url:
                raise ValueError("If `headline` is set, `word1`, `word2`, `prompt`, and `url` must be none.")
        elif self.url:
            if self.word1 or self.word2 or self.headline:
                raise ValueError("If `url` is set, `word1`, `word2`, and `headline` must be none.")
        else:
            raise ValueError("One of `word1`+`word2`, `headline`, or `url` must be set.")

    @property
    def task(self) -> Task:
        return prompt_id_to_task(self.id)

    @property
    def language(self) -> Literal["en", "es", "zh"]:
        if self.id.startswith("es_"):
            return "es"
        elif self.id.startswith("zh_"):
            return "zh"
        else:
            return "en"

    @property
    def verbalized(self) -> str | None:
        if self.word1 and self.word2:
            match self.language:
                case "en":
                    return f"The outputs must contain the words <b>{self.word1}</b> and <b>{self.word2}</b>."
                case "es":
                    return f"La salidas deben contener las palabras <b>{self.word1}</b> y <b>{self.word2}</b>."
                case "zh":
                    return f"输出需要包含词语“<b>{self.word1}</b>”和“<b>{self.word2}</b>”。"
                case _:
                    raise ValueError(f"Unknown language: {self.language}")
        elif self.headline:
            match self.language:
                case "en":
                    return f"<b>News headline:</b> {self.headline}"
                case "es":
                    return f"<b>Titular:</b> {self.headline}"
                case "zh":
                    return f"<b>新闻标题:</b> {self.headline}"
                case _:
                    raise ValueError(f"Unknown language: {self.language}")
        elif self.url:
            return self.prompt
        else:
            raise ValueError("The prompt is not properly defined.")

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: Any) -> bool:
        return self.id == other.id if isinstance(other, type(self)) else NotImplemented


@dataclass(frozen=True)
class Output:
    prompt: Prompt
    system: System
    text: str

    def __hash__(self) -> int:
        return hash((self.prompt, self.system))

    def __eq__(self, other: Any) -> bool:
        return (
            (self.prompt == other.prompt and self.system == other.system)
            if isinstance(other, type(self))
            else NotImplemented
        )


@dataclass(frozen=True)
class Battle:
    output_a: Output
    output_b: Output

    def __post_init__(self) -> None:
        if self.output_a.prompt != self.output_b.prompt:
            raise ValueError("Both outputs must belong to the same prompt")

    @property
    def prompt(self) -> Prompt:
        return self.output_a.prompt


VoteString = Literal["a", "b", "n", "t"]  # Left (A), Right (B), Skip, and Tie.
VOTE_CHOICES = frozenset(get_args(VoteString))


@dataclass(frozen=True)
class Vote:
    battle: Battle
    session_id: str
    vote: VoteString
    date: datetime.datetime
    is_offensive_a: bool
    is_offensive_b: bool


# noinspection SqlAggregates
STATEMENT_RANDOM_LEAST_VOTED_UNSEEN_BATTLES = sqlalchemy.sql.text("""
WITH
  unskipped_votes AS (
    SELECT prompt_id, system_id_a AS system_id
    FROM votes NATURAL JOIN prompts
    WHERE vote != 'n' AND task = :task AND phase_id = :phase_id
    UNION ALL
    SELECT prompt_id, system_id_b AS system_id
    FROM votes NATURAL JOIN prompts
    WHERE vote != 'n' AND task = :task AND phase_id = :phase_id
  ),
  votes_from_session AS (
    SELECT prompt_id, system_id_a AS system_id
    FROM votes NATURAL JOIN prompts
    WHERE session_id = :session_id AND task = :task AND phase_id = :phase_id
    UNION
    SELECT prompt_id, system_id_b AS system_id
    FROM votes NATURAL JOIN prompts
    WHERE session_id = :session_id AND task = :task AND phase_id = :phase_id
  ), system_ids_with_outputs AS (
    SELECT system_id
    FROM outputs NATURAL JOIN prompts
    WHERE
      task = :task
      AND phase_id = :phase_id
    GROUP BY system_id
  ), system_votes AS (
    SELECT
      system_id_a,
      system_id_b
    FROM
      votes
      JOIN system_ids_with_outputs ON (
        votes.system_id_a = system_ids_with_outputs.system_id
          OR votes.system_id_b = system_ids_with_outputs.system_id
      )
      NATURAL JOIN prompts
    WHERE
      task = :task
      AND phase_id = :phase_id
      AND vote != 'n'
  ), votes_and_prompts_per_system AS (
    SELECT system_id_a AS system_id FROM system_votes UNION ALL
      SELECT system_id_b AS system_id FROM system_votes
  ), system_unskipped_votes AS (
    SELECT system_id, COUNT(*) AS count
    FROM votes_and_prompts_per_system
    GROUP BY system_id
  ), random_least_voted_unseen_outputs_from_least_voted_systems_a AS (
    SELECT
      outputs.prompt_id,
      outputs.system_id,
      text,
      system_unskipped_votes.count AS system_count,
      COUNT(unskipped_votes.prompt_id) prompt_count
    FROM
      outputs
      NATURAL JOIN system_unskipped_votes
      LEFT JOIN votes_from_session
      ON (
        votes_from_session.prompt_id = outputs.prompt_id
        AND votes_from_session.system_id = outputs.system_id
      )
      LEFT JOIN unskipped_votes
      ON (
        unskipped_votes.prompt_id = outputs.prompt_id
        AND unskipped_votes.system_id = outputs.system_id
      )
  WHERE
    votes_from_session.prompt_id IS NULL
    AND FIND_IN_SET(CONCAT(outputs.prompt_id, '-', outputs.system_id), :ignored_output_ids) = 0
  GROUP BY
    prompt_id,
    system_id
  )
SELECT
  prompts.prompt_id,
  word1,
  word2,
  headline,
  url,
  prompt,
  random_least_voted_unseen_outputs_from_least_voted_systems_a.system_id AS system_id_a,
  random_least_voted_unseen_outputs_from_least_voted_systems_a.text AS text_a,
  outputs_b.system_id AS system_id_b,
  outputs_b.text AS text_b
FROM
  prompts
  NATURAL JOIN random_least_voted_unseen_outputs_from_least_voted_systems_a
  JOIN outputs AS outputs_b
    ON (
      outputs_b.prompt_id = random_least_voted_unseen_outputs_from_least_voted_systems_a.prompt_id
      AND outputs_b.system_id != random_least_voted_unseen_outputs_from_least_voted_systems_a.system_id
    )
  LEFT JOIN votes_from_session AS votes_from_session_b
    ON (
      votes_from_session_b.prompt_id = outputs_b.prompt_id
      AND votes_from_session_b.system_id = outputs_b.system_id
    )
WHERE
  task = :task
  AND phase_id = :phase_id
  AND votes_from_session_b.prompt_id IS NULL
  AND FIND_IN_SET(CONCAT(outputs_b.prompt_id, '-', outputs_b.system_id), :ignored_output_ids) = 0
HAVING text_a != text_b
ORDER BY
  system_count,
  prompt_count,
  RAND()
LIMIT :limit
""")

STATEMENT_RANDOM_BATTLES = sqlalchemy.sql.text("""
SELECT
  prompts.prompt_id,
  word1,
  word2,
  headline,
  url,
  prompt,
  outputs_a.system_id AS system_id_a,
  outputs_a.text AS text_a,
  outputs_b.system_id AS system_id_b,
  outputs_b.text AS text_b
FROM
  prompts
  NATURAL JOIN outputs AS outputs_a
  JOIN outputs AS outputs_b
    ON (
      outputs_b.prompt_id = outputs_a.prompt_id
      AND outputs_b.system_id != outputs_a.system_id
    )
WHERE
  task = :task
  AND phase_id = :phase_id
ORDER BY
  RAND()
LIMIT :limit
""")

STATEMENT_ADD_VOTE = sqlalchemy.sql.text("""
INSERT INTO votes (prompt_id, system_id_a, system_id_b, session_id, vote, date, is_offensive_a, is_offensive_b)
VALUES (:prompt_id, :system_id_a, :system_id_b, :session_id, :vote, NOW(), :is_offensive_a, :is_offensive_b)
ON DUPLICATE KEY UPDATE prompt_id = prompt_id
""")
STATEMENT_SESSION_VOTE_COUNT = sqlalchemy.sql.text(
    "SELECT COUNT(*) FROM votes v WHERE session_id = :session_id AND (NOT :without_skips OR vote != 'n')"
)
STATEMENT_VOTE_COUNT = sqlalchemy.sql.text("SELECT COUNT(*) FROM votes WHERE NOT :without_skips OR vote != 'n'")
STATEMENT_PROLIFIC_CONSENT = sqlalchemy.sql.text(
    "INSERT INTO prolific (session_id) VALUES (:session_id) ON DUPLICATE KEY UPDATE session_id = session_id"
)
STATEMENT_PROLIFIC_FINISH = sqlalchemy.sql.text(
    "UPDATE prolific SET finish_date = :finish_date, comments = :comments WHERE session_id = :session_id"
)
STATEMENT_SESSION_COUNT = sqlalchemy.sql.text(
    "SELECT COUNT(DISTINCT v.session_id) FROM votes v WHERE NOT :without_skips OR vote != 'n'"
)
STATEMENT_HISTOGRAM = sqlalchemy.sql.text("""
WITH
  prompt_counts AS (
    SELECT
      COUNT(votes.prompt_id) c
    FROM
      prompts
      LEFT JOIN votes ON prompts.prompt_id = votes.prompt_id
    GROUP BY prompts.prompt_id
  )
SELECT c, COUNT(*) as freq FROM prompt_counts GROUP BY c ORDER BY c
""")
STATEMENT_VOTE_COUNT_PER_CATEGORY = sqlalchemy.sql.text("SELECT vote, COUNT(*) FROM votes GROUP BY vote ORDER BY vote")


@asynccontextmanager
async def create_engine() -> AsyncIterator[sqlalchemy.ext.asyncio.AsyncEngine]:
    engine = sqlalchemy.ext.asyncio.create_async_engine(
        f"mysql+asyncmy://{os.environ['DB_USER']}:{os.environ['DB_PASS']}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}",
        pool_size=10,
        pool_recycle=3600,
    )
    try:
        yield engine
    finally:
        await engine.dispose()


def _battle_row_to_object(
    row: tuple[str, str | None, str | None, str | None, str | None, str | None, str, str, str, str],
    randomly_swap_systems: bool = True,
) -> Battle:
    prompt_id, word1, word2, headline, url, prompt_text, system_id_a, text_a, system_id_b, text_b = row

    prompt = Prompt(id=prompt_id, word1=word1, word2=word2, headline=headline, url=url, prompt=prompt_text)

    output_a = Output(prompt=prompt, system=System(id=system_id_a), text=text_a)
    output_b = Output(prompt=prompt, system=System(id=system_id_b), text=text_b)

    if randomly_swap_systems:
        if random.random() < 0.5:
            output_a, output_b = output_b, output_a

    return Battle(output_a=output_a, output_b=output_b)


async def random_least_voted_unseen_battles(
    engine: sqlalchemy.ext.asyncio.AsyncEngine,
    phase_id: int,
    session_id: str,
    task: Task,
    batch_size: int,
    ignored_output_ids: Iterable[tuple[str, str]] = (),
) -> AsyncIterator[Battle]:
    """Returns an iterator with a random subsample of the top `batch_size` least-voted unseen outputs (by the session),
    each paired in a battle with a random other unseen output for the same prompt.
    """
    async with engine.connect() as connection:
        for row in await connection.execute(
            STATEMENT_RANDOM_LEAST_VOTED_UNSEEN_BATTLES,
            {
                "phase_id": phase_id,
                "session_id": session_id,
                "task": task,
                "limit": batch_size,
                "ignored_output_ids": ",".join(
                    prompt_id + "-" + system_id for prompt_id, system_id in ignored_output_ids
                ),
            },
        ):
            yield _battle_row_to_object(row)  # type: ignore[invalid-argument-type]


async def random_battles(
    engine: sqlalchemy.ext.asyncio.AsyncEngine, phase_id: int, task: Task, batch_size: int
) -> AsyncIterator[Battle]:
    """Returns an iterator with `batch_size` random battles."""
    async with engine.connect() as connection:
        for row in await connection.execute(
            STATEMENT_RANDOM_BATTLES, {"task": task, "phase_id": phase_id, "limit": batch_size}
        ):
            yield _battle_row_to_object(row)  # type: ignore[invalid-argument-type]


async def battles_with_same_text(
    engine: sqlalchemy.ext.asyncio.AsyncEngine, phase_id: int, task: Task
) -> AsyncIterator[Battle]:
    """Returns an iterator with the battles with the same text."""
    async with engine.connect() as connection:
        for row in await connection.execute(
            sqlalchemy.sql.text("""
                    SELECT
                      prompts.prompt_id,
                      word1,
                      word2,
                      headline,
                      url,
                      prompt,
                      'placeholder',
                      outputs_a.system_id AS system_id_a,
                      outputs_a.text AS text_a,
                      outputs_b.system_id AS system_id_b,
                      outputs_b.text AS text_b
                    FROM
                      prompts
                      NATURAL JOIN outputs AS outputs_a
                      JOIN outputs AS outputs_b
                        ON (
                          outputs_b.prompt_id = outputs_a.prompt_id
                          AND outputs_b.system_id > outputs_a.system_id  -- We avoid repeated battles.
                        )
                    WHERE
                      task = :task
                      AND phase_id = :phase_id
                      AND outputs_a.text = outputs_b.text
                """),
            {"task": task, "phase_id": phase_id},
        ):
            yield _battle_row_to_object(row)  # type: ignore[invalid-argument-type]


async def get_votes_for_battles_with_the_same_text(
    engine: sqlalchemy.ext.asyncio.AsyncEngine, phase_id: int, task: Task
) -> AsyncIterator[Vote]:
    """Return tie votes for any possible battle with the same text."""
    async for battle in battles_with_same_text(engine, phase_id, task):
        yield Vote(
            battle,
            session_id="<placeholder>",
            vote="t",
            date=datetime.datetime(2025, 1, 1),
            is_offensive_a=False,
            is_offensive_b=False,
        )


async def add_vote(
    engine: sqlalchemy.ext.asyncio.AsyncEngine,
    session_id: str,
    prompt_id: str,
    system_id_a: str,
    system_id_b: str,
    vote: VoteString,
    is_offensive_a: bool,
    is_offensive_b: bool,
) -> None:
    """Adds a vote for a battle ID by a determined session."""
    async with engine.begin() as connection:
        await connection.execute(
            STATEMENT_ADD_VOTE,
            {
                "prompt_id": prompt_id,
                "system_id_a": system_id_a,
                "system_id_b": system_id_b,
                "session_id": session_id,
                "vote": vote,
                "is_offensive_a": is_offensive_a,
                "is_offensive_b": is_offensive_b,
            },
        )


async def get_votes_for_scoring(
    engine: sqlalchemy.ext.asyncio.AsyncEngine, phase_id: int, task: Task, excluded_session_ids: Iterable[str] = ()
) -> AsyncIterator[Vote]:
    """Returns the votes for a given phase ID and task to score the systems."""
    excluded_session_ids = tuple(excluded_session_ids)

    if not excluded_session_ids:  # When empty, the SQL syntax breaks, so we have to put something.
        excluded_session_ids = ("__PLACEHOLDER__",)

    async with engine.connect() as connection:
        for (
            prompt_id,
            system_id_a,
            system_id_b,
            session_id,
            vote,
            date,
            is_offensive_a,
            is_offensive_b,
        ) in await connection.execute(
            sqlalchemy.sql.text("""
                WITH votes_and_prompts AS (
                  SELECT
                    prompt_id,
                    system_id_a,
                    system_id_b,
                    session_id,
                    vote,
                    date,
                    is_offensive_a,
                    is_offensive_b
                  FROM
                    votes v
                    NATURAL JOIN prompts
                  WHERE
                    task = :task
                    AND phase_id = :phase_id
                    AND v.vote != 'n'
                    AND session_id NOT IN :excluded_session_ids
                ), systems_a AS (
                  SELECT system_id_a FROM votes_and_prompts GROUP BY system_id_a
                ), systems_b AS (
                  SELECT system_id_b FROM votes_and_prompts GROUP BY system_id_b
                )
                SELECT
                  prompt_id,
                  v.system_id_a,
                  v.system_id_b,
                  session_id,
                  vote,
                  date,
                  is_offensive_a,
                  is_offensive_b
                FROM
                  votes_and_prompts v
                  -- We only want the votes from those systems that appear at least once on each side of the votes.
                  -- Otherwise, it causes issues in the scoring calculation.
                  -- And it'd also mean the system has too few votes.
                  JOIN systems_b ON (v.system_id_a = systems_b.system_id_b)
                  JOIN systems_a ON (v.system_id_b = systems_a.system_id_a)
            """),
            {"task": task, "phase_id": phase_id, "excluded_session_ids": excluded_session_ids},
        ):
            prompt = Prompt(id=prompt_id, headline="<placeholder>")
            yield Vote(
                battle=Battle(
                    output_a=Output(prompt=prompt, system=System(id=system_id_a), text=None),  # type: ignore[invalid-argument-type]
                    output_b=Output(prompt=prompt, system=System(id=system_id_b), text=None),  # type: ignore[invalid-argument-type]
                ),
                session_id=session_id,
                vote=vote,
                date=date,
                is_offensive_a=is_offensive_a,
                is_offensive_b=is_offensive_b,
            )


async def get_systems(engine: sqlalchemy.ext.asyncio.AsyncEngine, phase_id: int, task: Task) -> AsyncIterator[str]:
    """Returns all the systems for a given phase ID and task."""
    async with engine.connect() as connection:
        for (system_id,) in await connection.execute(
            sqlalchemy.sql.text(
                "SELECT system_id FROM outputs NATURAL JOIN prompts"
                " WHERE task = :task AND phase_id = :phase_id GROUP BY system_id"
            ),
            {"task": task, "phase_id": phase_id},
        ):
            yield system_id


async def _get_votes_per_system(
    engine: sqlalchemy.ext.asyncio.AsyncEngine, phase_id: int, task: Task, excluded_session_ids: Iterable[str] = ()
) -> AsyncIterator[tuple[str, int]]:
    """Returns the non-skip votes per system for a given phase ID and task. If a system has no votes, it may not be part
    of the output.
    """
    excluded_session_ids = tuple(excluded_session_ids)

    if not excluded_session_ids:  # When empty, the SQL syntax breaks, so we have to put something.
        excluded_session_ids = ("__PLACEHOLDER__",)

    async with engine.connect() as connection:
        for row in await connection.execute(
            sqlalchemy.sql.text("""
                WITH system_ids_with_outputs AS (
                  SELECT system_id
                  FROM outputs NATURAL JOIN prompts
                  WHERE
                    task = :task
                    AND phase_id = :phase_id
                  GROUP BY system_id
                ), system_votes AS (
                  SELECT
                    system_id_a,
                    system_id_b
                  FROM
                    votes
                    JOIN system_ids_with_outputs ON (
                      votes.system_id_a = system_ids_with_outputs.system_id
                        OR votes.system_id_b = system_ids_with_outputs.system_id
                    )
                    NATURAL JOIN prompts
                  WHERE
                    task = :task
                    AND phase_id = :phase_id
                    AND vote != 'n'
                    AND session_id NOT IN :excluded_session_ids
                ), votes_and_prompts_per_system AS (
                  SELECT system_id_a AS system_id FROM system_votes UNION ALL
                    SELECT system_id_b AS system_id FROM system_votes
                )
                SELECT system_id, COUNT(*) AS count
                FROM votes_and_prompts_per_system
                GROUP BY system_id
                ORDER BY count DESC
            """),
            {"task": task, "phase_id": phase_id, "excluded_session_ids": excluded_session_ids},
        ):
            yield row


async def get_votes_per_system(
    engine: sqlalchemy.ext.asyncio.AsyncEngine, phase_id: int, task: Task, excluded_session_ids: Iterable[str] = ()
) -> dict[str, int]:
    """Returns the non-skip votes per system for a given phase ID and task."""
    system_id_to_vote_count: dict[str, int] = {}
    async for system_id, vote_count in _get_votes_per_system(engine, phase_id, task, excluded_session_ids):
        system_id_to_vote_count[system_id] = vote_count

    # Some systems may not be part of the output as there are no votes for them:
    async for system_id in get_systems(engine, phase_id, task):
        system_id_to_vote_count.setdefault(system_id, 0)

    return system_id_to_vote_count


async def get_votes_per_session(engine: sqlalchemy.ext.asyncio.AsyncEngine, phase_id: int) -> dict[str, int]:
    """Returns the non-skip votes per session for a given phase ID."""
    async with engine.connect() as connection:
        return dict(
            iter(
                await connection.execute(
                    sqlalchemy.sql.text("""
                        SELECT session_id, COUNT(*) AS count
                        FROM votes NATURAL JOIN prompts
                        WHERE phase_id = :phase_id AND vote != 'n'
                        GROUP BY session_id
                        ORDER BY count DESC
                    """),
                    {"phase_id": phase_id},
                )
            )
        )  # type: ignore[no-matching-overload]


async def session_vote_count_without_skips(engine: sqlalchemy.ext.asyncio.AsyncEngine, session_id: str) -> int:
    """Returns the vote count for a given session ID for any phase, including skips."""
    async with engine.connect() as connection:
        return (
            await connection.execute(STATEMENT_SESSION_VOTE_COUNT, {"session_id": session_id, "without_skips": True})
        ).one()[0]


async def vote_count_without_skips(engine: sqlalchemy.ext.asyncio.AsyncEngine) -> int:
    """Returns the vote count for any phase, not including skips."""
    async with engine.connect() as connection:
        return (await connection.execute(STATEMENT_VOTE_COUNT, {"without_skips": True})).one()[0]


async def get_votes(engine: sqlalchemy.ext.asyncio.AsyncEngine, phase_id: int) -> pd.DataFrame:
    """Returns the non-skip votes with all the associated information."""
    async with engine.connect() as connection:
        return pd.DataFrame(
            iter(
                await connection.execute(
                    sqlalchemy.sql.text("""
                        SELECT
                          *
                        FROM
                          votes
                          NATURAL JOIN prompts
                          JOIN outputs o_a ON (votes.prompt_id = o_a.prompt_id AND votes.system_id_a = o_a.system_id)
                          JOIN outputs o_b ON (votes.prompt_id = o_b.prompt_id AND votes.system_id_b = o_b.system_id)
                        WHERE
                          phase_id = :phase_id
                        ORDER BY
                          session_id,
                          date
                    """),
                    {"phase_id": phase_id},
                )
            )
        )


async def prolific_consent(engine: sqlalchemy.ext.asyncio.AsyncEngine, session_id: str) -> None:
    """Sets the current time as the consent date for the prolific session ID."""
    async with engine.connect() as connection:
        await connection.execute(STATEMENT_PROLIFIC_CONSENT, {"session_id": session_id})


async def prolific_finish(engine: sqlalchemy.ext.asyncio.AsyncEngine, session_id: str, comments: str) -> None:
    """Sets the current time as the finish date and the given comments for the prolific session ID."""
    async with engine.connect() as connection:
        await connection.execute(
            STATEMENT_PROLIFIC_FINISH,
            {"session_id": session_id, "finish_date": datetime.datetime.now(), "comments": comments},
        )


async def stats(engine: sqlalchemy.ext.asyncio.AsyncEngine) -> MutableMapping[str, Any]:
    """Returns the vote count, vote count without skips, vote count histogram, and votes per category.

    The results consider all phases.
    """
    async with engine.connect() as connection:
        result: dict[str, Any] = {
            "votes": (await connection.execute(STATEMENT_VOTE_COUNT, {"without_skips": False})).one()[0],
            "sessions": (await connection.execute(STATEMENT_SESSION_COUNT, {"without_skips": False})).one()[0],
            "histogram": dict(iter(await connection.execute(STATEMENT_HISTOGRAM))),  # type: ignore[no-matching-overload]
            "votes-per-category": dict(iter(await connection.execute(STATEMENT_VOTE_COUNT_PER_CATEGORY))),  # type: ignore[no-matching-overload]
            "votes-without-skips": (await connection.execute(STATEMENT_VOTE_COUNT, {"without_skips": True})).one()[0],
            "sessions-without-skips": (
                await connection.execute(STATEMENT_SESSION_COUNT, {"without_skips": True})
            ).one()[0],
        }

    for category in VOTE_CHOICES:
        result["votes-per-category"].setdefault(category, 0)

    return result
