"""Provides mechanisms to handle the database."""

import os
from collections.abc import Iterable, Iterator, MutableMapping
from dataclasses import dataclass
from typing import Any, Literal, get_args

import sqlalchemy
import sqlalchemy.sql

type Task = Literal["a-es", "a-en", "a-zh", "b1", "b2"]
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


@dataclass(frozen=True)
class Prompt:
    id: int
    word1: str | None = None
    word2: str | None = None
    headline: str | None = None
    url: str | None = None
    prompt: str | None = None

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


VOTE_CHOICES = frozenset(("1", "2", "3", "4", "5", "x", "n"))

STATEMENT_RANDOM_LEAST_VOTED_UNSEEN_OUTPUTS = sqlalchemy.sql.text("""
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
      vote != "n"
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
  )
SELECT
  prompts.prompt_id prompt_id,
  word1,
  word2,
  headline,
  url,
  prompt,
  outputs_a.system_id system_id_a,
  outputs_a.text text_a,
  outputs_b.system_id system_id_b,
  outputs_b.text text_b
FROM
  prompts
  NATURAL JOIN outputs outputs_a
  LEFT JOIN votes_from_session votes_from_session_a
    ON (
      votes_from_session_a.prompt_id = outputs_a.prompt_id
      AND (
        votes_from_session_a.system_id_a = outputs_a.system_id
        OR votes_from_session_a.system_id_b = outputs_a.system_id
      )
    )
  LEFT JOIN unskipped_votes
    ON (
      unskipped_votes.prompt_id = outputs_a.prompt_id
      AND (
        unskipped_votes.system_id_a = outputs_a.system_id
        OR unskipped_votes.system_id_b = outputs_a.system_id
      )
    )
  JOIN outputs as outputs_b
    ON (
      outputs_b.prompt_id = outputs_a.prompt_id
      AND outputs_b.system_id != outputs_a.system_id
    )
  LEFT JOIN votes_from_session votes_from_session_b
    ON (
      votes_from_session_b.prompt_id = outputs_b.prompt_id
      AND (
        votes_from_session_b.system_id_a = outputs_b.system_id
        OR votes_from_session_b.system_id_b = outputs_b.system_id
      )
    )
WHERE
  votes_from_session_a.prompt_id IS NULL
  AND votes_from_session_b.prompt_id IS NULL
  AND FIND_IN_SET(CONCAT(outputs_a.prompt_id, outputs_a.system_id), :ignored_output_ids) = 0
  AND FIND_IN_SET(CONCAT(outputs_b.prompt_id, outputs_b.system_id), :ignored_output_ids) = 0
GROUP BY
  prompt_id,
  system_id_a
ORDER BY
  COUNT(unskipped_votes.prompt_id),
  RAND()
 LIMIT :limit
""")
STATEMENT_RANDOM_TWEETS = sqlalchemy.sql.text("SELECT t.tweet_id, text FROM tweets t ORDER BY RAND() LIMIT :limit")
STATEMENT_ADD_VOTE = sqlalchemy.sql.text(
    "INSERT INTO votes (tweet_id, session_id, vote, is_offensive)"
    " VALUES (:tweet_id, :session_id, :vote, :is_offensive)"
    " ON DUPLICATE KEY UPDATE tweet_id = tweet_id"
)
STATEMENT_SESSION_VOTE_COUNT = sqlalchemy.sql.text(
    "SELECT COUNT(*) FROM votes v WHERE session_id = :session_id   AND (NOT :without_skips OR vote != 'n')"
)
STATEMENT_VOTE_COUNT = sqlalchemy.sql.text(
    "SELECT COUNT(*)"
    " FROM votes v"
    "   LEFT JOIN (SELECT session_id"
    "               FROM votes"
    "               WHERE tweet_id = 1092855393188020224 AND vote = 'x') s1"
    "     ON v.session_id = s1.session_id"
    "   LEFT JOIN (SELECT session_id"
    "               FROM votes"
    "               WHERE tweet_id = 1088158691633713152 AND vote = 'x') s2"
    "     ON v.session_id = s2.session_id"
    "   LEFT JOIN (SELECT session_id"
    "               FROM votes"
    "               WHERE tweet_id = 1086371400431095813"
    "                 AND vote != 'x'"
    "                 AND vote != 'n') s3"
    "     ON v.session_id = s3.session_id"
    " WHERE (NOT :without_skips OR vote != 'n')"
    "   AND (NOT :pass_test"
    "     OR (s1.session_id IS NOT NULL"
    "       AND s2.session_id IS NOT NULL"
    "       AND s3.session_id IS NOT NULL))"
)
STATEMENT_SESSION_COUNT = sqlalchemy.sql.text(
    "SELECT COUNT(DISTINCT v.session_id)"
    " FROM votes v"
    "   LEFT JOIN (SELECT session_id"
    "               FROM votes"
    "               WHERE tweet_id = 1092855393188020224 AND vote = 'x') s1"
    "     ON v.session_id = s1.session_id"
    "   LEFT JOIN (SELECT session_id"
    "               FROM votes"
    "               WHERE tweet_id = 1088158691633713152 AND vote = 'x') s2"
    "     ON v.session_id = s2.session_id"
    "   LEFT JOIN (SELECT session_id"
    "               FROM votes"
    "               WHERE tweet_id = 1086371400431095813"
    "                 AND vote != 'x'"
    "                 AND vote != 'n') s3"
    "     ON v.session_id = s3.session_id"
    " WHERE (NOT :without_skips OR vote != 'n')"
    "   AND (NOT :pass_test"
    "     OR (s1.session_id IS NOT NULL"
    "       AND s2.session_id IS NOT NULL"
    "       AND s3.session_id IS NOT NULL))"
)
STATEMENT_TEST_TWEETS_VOTE_COUNT = sqlalchemy.sql.text(
    "SELECT COUNT(v.tweet_id) AS c"
    " FROM tweets t"
    "   LEFT JOIN votes v ON t.tweet_id = v.tweet_id"
    " WHERE weight > 1"
    " GROUP BY t.tweet_id"
    " ORDER BY c DESC"
)
STATEMENT_HISTOGRAM = sqlalchemy.sql.text(
    "SELECT c, COUNT(*) as freq"
    " FROM (SELECT COUNT(v.tweet_id) c"
    "        FROM tweets t"
    "          LEFT JOIN (SELECT tweet_id FROM votes) v"  # WHERE vote != 'n'
    "            ON t.tweet_id = v.tweet_id"
    "        WHERE weight <= 1 AND t.tweet_id <> 1088158691633713152"
    "        GROUP BY t.tweet_id) a"
    " GROUP BY c"
    " ORDER BY c"
)
STATEMENT_VOTE_COUNT_PER_CATEGORY = sqlalchemy.sql.text("SELECT vote, COUNT(*) FROM votes GROUP BY vote ORDER BY vote")


def create_engine() -> sqlalchemy.Engine:
    return sqlalchemy.create_engine(
        f"mysql://{os.environ['DB_USER']}:{os.environ['DB_PASS']}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}"
    )


engine = create_engine()


def random_least_voted_unseen_outputs(
    session_id: str, task: Task, batch_size: int, ignored_outputs: Iterable[Output] = ()
) -> Iterator[Battle]:
    """Returns an iterator with a random subsample the top `batch_size` least-voted unseen outputs (by the session),
    each paired in a battle with a random other output for the same prompt.
    """

    # TODO: use the task.

    with engine.connect() as connection:
        result = connection.execute(
            STATEMENT_RANDOM_LEAST_VOTED_UNSEEN_OUTPUTS,
            {
                "session_id": session_id,
                "limit": batch_size,
                "ignored_output_ids": ",".join(str(output.prompt.id) + output.system.id for output in ignored_outputs),
            },
        )
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
        ) in result.fetchall():
            prompt = Prompt(id=prompt_id, word1=word1, word2=word2, headline=headline, url=url, prompt=prompt)
            output_a = Output(prompt=prompt, system=System(id=system_id_a), text=text_a)
            output_b = Output(prompt=prompt, system=System(id=system_id_b), text=text_b)
            yield Battle(output_a=output_a, output_b=output_b)


def random_tweets(batch_size: int) -> Iterator[Battle]:
    """Returns a random list tweets with size batch_size.

    Each tweet is represented as a dictionary with the fields "id" and "text".

    :param batch_size: Size of the list to return
    :return: Random list of tweets with size batch_size
    """
    with engine.connect() as connection:
        result = connection.execute(STATEMENT_RANDOM_TWEETS, {"limit": batch_size})
        for id_, text in result.fetchall():
            yield {"id": id_, "text": text}  # type: ignore


def add_vote(session_id: str, tweet_id: Battle, vote: str, is_offensive: bool) -> None:
    """Adds a vote for a tweet by a determined session.

    If the vote is not one of `VOTE_CHOICES`, it will do nothing. If the session had already voted, the new vote will be
    ignored.

    :param session_id: Session ID
    :param tweet_id: Tweet ID
    :param vote: Vote of the tweet: "1" to "5" for the stars, "x" for non-humorous and "n" for skipped
    :param is_offensive: If the tweet is considered offensive
    """
    if vote in VOTE_CHOICES:
        with engine.connect() as connection:
            connection.execute(
                STATEMENT_ADD_VOTE,
                {"tweet_id": tweet_id, "session_id": session_id, "vote": vote, "is_offensive": is_offensive},
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
        return connection.execute(STATEMENT_VOTE_COUNT, {"without_skips": True, "pass_test": False}).fetchone()[0]  # type: ignore


def stats() -> MutableMapping[str, Any]:
    """Returns the vote count, vote count without skips, vote count histogram and votes per category."""
    with engine.connect() as connection:
        result: dict[str, Any] = {
            "votes": connection.execute(STATEMENT_VOTE_COUNT, {"without_skips": False, "pass_test": False}).fetchone()[  # type: ignore
                0
            ],
            "sessions": connection.execute(  # type: ignore
                STATEMENT_SESSION_COUNT, {"without_skips": False, "pass_test": False}
            ).fetchone()[0],
            "test-tweets-vote-count": [t[0] for t in connection.execute(STATEMENT_TEST_TWEETS_VOTE_COUNT).fetchall()],
            "histogram": dict(connection.execute(STATEMENT_HISTOGRAM).fetchall()),  # type: ignore
            "votes-per-category": dict(connection.execute(STATEMENT_VOTE_COUNT_PER_CATEGORY).fetchall()),  # type: ignore
            "votes-without-skips": connection.execute(  # type: ignore
                STATEMENT_VOTE_COUNT, {"without_skips": True, "pass_test": False}
            ).fetchone()[0],
            "sessions-without-skips": connection.execute(  # type: ignore
                STATEMENT_SESSION_COUNT, {"without_skips": True, "pass_test": False}
            ).fetchone()[0],
            "votes-pass-test": connection.execute(  # type: ignore
                STATEMENT_VOTE_COUNT, {"without_skips": True, "pass_test": True}
            ).fetchone()[0],
            "sessions-pass-test": connection.execute(  # type: ignore
                STATEMENT_SESSION_COUNT, {"without_skips": True, "pass_test": True}
            ).fetchone()[0],
        }

    for category in VOTE_CHOICES:
        result["votes-per-category"].setdefault(category, 0)  # type: ignore

    return result
