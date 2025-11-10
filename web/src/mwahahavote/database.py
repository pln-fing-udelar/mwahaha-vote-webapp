"""Provides mechanisms to handle the database."""

import os
from collections.abc import Iterable, Iterator, MutableMapping
from dataclasses import dataclass
from typing import Any, Literal, get_args

import sqlalchemy
import sqlalchemy.sql
from sqlalchemy import CursorResult

Task = Literal["a-en", "a-es", "a-zh", "b1", "b2"]
TASK_CHOICES = frozenset(get_args(Task))


def task_to_prompt_id_sql_like_expression(task: Task) -> str:
    match task:
        case "a-en":
            return r"en\_____"
        case "a-es":
            return r"es\_____"
        case "a-zh":
            return r"zh\_____"
        case "b1":
            return r"img\_____"
        case "b2":
            return r"img\_2\_____"
        case _:
            raise ValueError(f"Unknown task: {task}")


@dataclass(frozen=True)
class System:
    id: str

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: Any) -> bool:
        return self.id == other.id if isinstance(other, type(self)) else NotImplemented


def prompt_id_to_task(prompt_id: str) -> Task:
    if prompt_id.startswith(("en_", "es_", "zh_")):
        return f"a-{prompt_id[:2]}"  # type: ignore
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


VOTE_CHOICES = frozenset(("a", "b", "n"))

STATEMENT_RANDOM_LEAST_VOTED_UNSEEN_BATTLES = sqlalchemy.sql.text("""
WITH
  unskipped_votes AS (
    SELECT
      prompt_id,
      system_id_a,
      system_id_b,
      session_id
    FROM
      votes
    WHERE
      vote != 'n'
  ),
  votes_from_session AS (
    SELECT
      prompt_id,
      system_id_a,
      system_id_b,
      session_id
    FROM
      votes
    WHERE
      session_id = :session_id
  ),
  random_least_voted_unseen_outputs_a AS (
    SELECT
      outputs.prompt_id,
      outputs.system_id,
      text
    FROM
      outputs
      LEFT JOIN votes_from_session
      ON (
        votes_from_session.prompt_id = outputs.prompt_id
        AND (
          votes_from_session.system_id_a = outputs.system_id
          OR votes_from_session.system_id_b = outputs.system_id
        )
      )
      LEFT JOIN unskipped_votes
      ON (
        unskipped_votes.prompt_id = outputs.prompt_id
        AND (
          unskipped_votes.system_id_a = outputs.system_id
          OR unskipped_votes.system_id_b = outputs.system_id
        )
      )
  WHERE
    votes_from_session.prompt_id IS NULL
    AND FIND_IN_SET(CONCAT(outputs.prompt_id, outputs.system_id), :ignored_output_ids) = 0
  GROUP BY
    prompt_id,
    system_id
  ORDER BY
    COUNT(unskipped_votes.prompt_id),
    RAND()
  )
SELECT
  prompts.prompt_id,
  word1,
  word2,
  headline,
  url,
  prompt,
  random_least_voted_unseen_outputs_a.system_id AS system_id_a,
  random_least_voted_unseen_outputs_a.text AS text_a,
  outputs_b.system_id AS system_id_b,
  outputs_b.text AS text_b
FROM
  prompts
  NATURAL JOIN random_least_voted_unseen_outputs_a
  JOIN outputs AS outputs_b
    ON (
      outputs_b.prompt_id = random_least_voted_unseen_outputs_a.prompt_id
      AND outputs_b.system_id != random_least_voted_unseen_outputs_a.system_id
    )
  LEFT JOIN votes_from_session AS votes_from_session_b
    ON (
      votes_from_session_b.prompt_id = outputs_b.prompt_id
      AND (
        votes_from_session_b.system_id_a = outputs_b.system_id
        OR votes_from_session_b.system_id_b = outputs_b.system_id
      )
    )
WHERE
  task = :task
  AND votes_from_session_b.prompt_id IS NULL
  AND FIND_IN_SET(CONCAT(outputs_b.prompt_id, outputs_b.system_id), :ignored_output_ids) = 0
ORDER BY
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


def create_engine() -> sqlalchemy.Engine:
    return sqlalchemy.create_engine(
        f"mysql://{os.environ['DB_USER']}:{os.environ['DB_PASS']}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}",
        pool_size=10,
        pool_recycle=3600,
    )


engine = create_engine()


def _battle_rows_to_objects(
    result: CursorResult[tuple[int, str | None, str | None, str | None, str | None, str | None, str, str, str, str]],
) -> Iterator[Battle]:
    for (
        prompt_id,
        word1,
        word2,
        headline,
        url,
        prompt,
        system_id_a,
        text_a,
        system_id_b,
        text_b,
    ) in result:
        prompt = Prompt(id=prompt_id, word1=word1, word2=word2, headline=headline, url=url, prompt=prompt)
        output_a = Output(prompt=prompt, system=System(id=system_id_a), text=text_a)
        output_b = Output(prompt=prompt, system=System(id=system_id_b), text=text_b)
        yield Battle(output_a=output_a, output_b=output_b)


def random_least_voted_unseen_battles(
    session_id: str, task: Task, batch_size: int, ignored_output_ids: Iterable[tuple[str, str]] = ()
) -> Iterator[Battle]:
    """Returns an iterator with a random subsample of the top `batch_size` least-voted unseen outputs (by the session),
    each paired in a battle with a random other unseen output for the same prompt.
    """

    with engine.connect() as connection:
        yield from _battle_rows_to_objects(
            connection.execute(
                STATEMENT_RANDOM_LEAST_VOTED_UNSEEN_BATTLES,
                {
                    "session_id": session_id,
                    "task": task,
                    "limit": batch_size,
                    "ignored_output_ids": ",".join(
                        prompt_id + "-" + system_id for prompt_id, system_id in ignored_output_ids
                    ),
                },
            )
        )


def random_battles(task: Task, batch_size: int) -> Iterator[Battle]:
    """Returns an iterator with `batch_size` random battles."""
    with engine.connect() as connection:
        yield from _battle_rows_to_objects(
            connection.execute(STATEMENT_RANDOM_BATTLES, {"task": task, "limit": batch_size})
        )


def add_vote(
    session_id: str,
    prompt_id: str,
    system_id_a: str,
    system_id_b: str,
    vote: str,
    is_offensive_a: bool,
    is_offensive_b: bool,
) -> None:
    """Adds a vote for a battle ID by a determined session."""
    if vote not in VOTE_CHOICES:
        raise ValueError(f"Invalid vote: {vote}")

    with engine.begin() as connection:
        connection.execute(
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


def session_vote_count_with_skips(session_id: str) -> int:
    """Returns the vote count for a given session ID, including skips."""
    with engine.connect() as connection:
        return connection.execute(  # type: ignore
            STATEMENT_SESSION_VOTE_COUNT, {"session_id": session_id, "without_skips": False}
        ).fetchone()[0]


def vote_count_without_skips() -> int:
    """Returns the vote count, not including skips."""
    with engine.connect() as connection:
        return connection.execute(STATEMENT_VOTE_COUNT, {"without_skips": True}).fetchone()[0]  # type: ignore


def stats() -> MutableMapping[str, Any]:
    """Returns the vote count, vote count without skips, vote count histogram and votes per category."""
    with engine.connect() as connection:
        result: dict[str, Any] = {
            "votes": connection.execute(STATEMENT_VOTE_COUNT, {"without_skips": False}).fetchone()[0],  # type: ignore
            "sessions": connection.execute(STATEMENT_SESSION_COUNT, {"without_skips": False}).fetchone()[0],  # type: ignore
            "histogram": dict(connection.execute(STATEMENT_HISTOGRAM)),  # type: ignore
            "votes-per-category": dict(connection.execute(STATEMENT_VOTE_COUNT_PER_CATEGORY)),  # type: ignore
            "votes-without-skips": connection.execute(STATEMENT_VOTE_COUNT, {"without_skips": True}).fetchone()[0],  # type: ignore
            "sessions-without-skips": connection.execute(STATEMENT_SESSION_COUNT, {"without_skips": True}).fetchone()[  # type: ignore
                0
            ],
        }

    for category in VOTE_CHOICES:
        result["votes-per-category"].setdefault(category, 0)  # type: ignore

    return result
