"""Provides mechanisms to handle the database."""

import datetime
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


VoteString = Literal["a", "b", "n", "t"]
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
  ), system_ids_with_outputs AS (
    SELECT system_id
    FROM outputs NATURAL JOIN prompts
    WHERE
      task = :task
      AND phase_id = :phase_id
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
  )
SELECT
  prompts.prompt_id,
  word1,
  word2,
  headline,
  url,
  prompt,
  @swap := RAND() > 0.5 AS swap,
  IF(@swap, outputs_b.system_id, random_least_voted_unseen_outputs_from_least_voted_systems_a.system_id) AS system_id_a,
  IF(@swap, outputs_b.text, random_least_voted_unseen_outputs_from_least_voted_systems_a.text) AS text_a,
  IF(@swap, random_least_voted_unseen_outputs_from_least_voted_systems_a.system_id, outputs_b.system_id) AS system_id_b,
  IF(@swap, random_least_voted_unseen_outputs_from_least_voted_systems_a.text, outputs_b.text) AS text_b
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
      AND (
        votes_from_session_b.system_id_a = outputs_b.system_id
        OR votes_from_session_b.system_id_b = outputs_b.system_id
      )
    )
WHERE
  task = :task
  AND phase_id = :phase_id
  AND votes_from_session_b.prompt_id IS NULL
  AND FIND_IN_SET(CONCAT(outputs_b.prompt_id, outputs_b.system_id), :ignored_output_ids) = 0
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
        _swap,
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
    phase_id: int, session_id: str, task: Task, batch_size: int, ignored_output_ids: Iterable[tuple[str, str]] = ()
) -> Iterator[Battle]:
    """Returns an iterator with a random subsample of the top `batch_size` least-voted unseen outputs (by the session),
    each paired in a battle with a random other unseen output for the same prompt.
    """

    with engine.connect() as connection:
        yield from _battle_rows_to_objects(
            connection.execute(
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
            )
        )


def random_battles(phase_id: int, task: Task, batch_size: int) -> Iterator[Battle]:
    """Returns an iterator with `batch_size` random battles."""
    with engine.connect() as connection:
        yield from _battle_rows_to_objects(
            connection.execute(STATEMENT_RANDOM_BATTLES, {"task": task, "phase_id": phase_id, "limit": batch_size})
        )


def battles_with_same_text(phase_id: int, task: Task) -> Iterator[Battle]:
    """Returns an iterator with the battles with the same text."""
    with engine.connect() as connection:
        yield from _battle_rows_to_objects(
            connection.execute(
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
            )
        )


def get_votes_for_battles_with_the_same_text(phase_id: int, task: Task) -> Iterator[Vote]:
    """Return tie votes for any possible battle with the same text."""
    for battle in battles_with_same_text(phase_id, task):
        yield Vote(
            battle,
            session_id="<placeholder>",
            vote="t",
            date=datetime.datetime(2025, 1, 1),
            is_offensive_a=False,
            is_offensive_b=False,
        )


def add_vote(
    session_id: str,
    prompt_id: str,
    system_id_a: str,
    system_id_b: str,
    vote: VoteString,
    is_offensive_a: bool,
    is_offensive_b: bool,
) -> None:
    """Adds a vote for a battle ID by a determined session."""
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


def get_votes_for_scoring(phase_id: int, task: Task) -> Iterator[Vote]:
    """Returns the votes for a given phase ID and task to score the systems."""
    with engine.connect() as connection:
        for (
            prompt_id,
            system_id_a,
            system_id_b,
            session_id,
            vote,
            date,
            is_offensive_a,
            is_offensive_b,
        ) in connection.execute(
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
            {"task": task, "phase_id": phase_id},
        ):
            prompt = Prompt(id=prompt_id, headline="<placeholder>")
            yield Vote(
                battle=Battle(
                    output_a=Output(prompt=prompt, system=System(id=system_id_a), text=None),  # type: ignore
                    output_b=Output(prompt=prompt, system=System(id=system_id_b), text=None),  # type: ignore
                ),
                session_id=session_id,
                vote=vote,
                date=date,
                is_offensive_a=is_offensive_a,
                is_offensive_b=is_offensive_b,
            )


def get_systems(phase_id: int, task: Task) -> Iterator[str]:
    """Returns all the systems for a given phase ID and task."""
    with engine.connect() as connection:
        for (system_id,) in connection.execute(
            sqlalchemy.sql.text(
                "SELECT system_id FROM outputs NATURAL JOIN prompts"
                " WHERE task = :task AND phase_id = :phase_id GROUP BY system_id"
            ),
            {"task": task, "phase_id": phase_id},
        ):
            yield system_id


def _get_votes_per_system(phase_id: int, task: Task) -> Iterator[tuple[str, int]]:
    """Returns the non-skip votes per system for a given phase ID and task. If a system has no votes, it may not be part
    of the output.
    """
    with engine.connect() as connection:
        yield from connection.execute(
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
                ), votes_and_prompts_per_system AS (
                  SELECT system_id_a AS system_id FROM system_votes UNION ALL
                    SELECT system_id_b AS system_id FROM system_votes
                )
                SELECT system_id, COUNT(*) AS count
                FROM votes_and_prompts_per_system
                GROUP BY system_id
                ORDER BY count DESC
            """),
            {"task": task, "phase_id": phase_id},
        )


def get_votes_per_system(phase_id: int, task: Task) -> dict[str, int]:
    """Returns the non-skip votes per system for a given phase ID and task."""
    system_id_to_vote_count = dict(_get_votes_per_system(phase_id, task))

    # Some systems may not be part of the output as there are no votes for them:
    for system_id in get_systems(phase_id, task):
        system_id_to_vote_count.setdefault(system_id, 0)

    return system_id_to_vote_count


def session_vote_count_without_skips(session_id: str) -> int:
    """Returns the vote count for a given session ID for any phase, including skips."""
    with engine.connect() as connection:
        return connection.execute(  # type: ignore
            STATEMENT_SESSION_VOTE_COUNT, {"session_id": session_id, "without_skips": True}
        ).fetchone()[0]


def vote_count_without_skips() -> int:
    """Returns the vote count for any phase, not including skips."""
    with engine.connect() as connection:
        return connection.execute(STATEMENT_VOTE_COUNT, {"without_skips": True}).fetchone()[0]  # type: ignore


def prolific_consent(session_id: str) -> None:
    """Sets the current time as the consent date for the prolific session ID."""
    with engine.begin() as connection:
        connection.execute(STATEMENT_PROLIFIC_CONSENT, {"session_id": session_id})


def prolific_finish(session_id: str, comments: str) -> None:
    """Sets the current time as the finish date and the given comments for the prolific session ID."""
    with engine.begin() as connection:
        connection.execute(
            STATEMENT_PROLIFIC_FINISH,
            {"session_id": session_id, "finish_date": datetime.datetime.now(), "comments": comments},
        )


def stats() -> MutableMapping[str, Any]:
    """Returns the vote count, vote count without skips, vote count histogram, and votes per category.

    The results consider all phases.
    """
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
        result["votes-per-category"].setdefault(category, 0)

    return result
