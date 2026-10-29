from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
import sqlalchemy
import sqlalchemy.ext.asyncio

from mwahahavote import database
from mwahahavote.database import (
    TASK_CHOICES,
    VOTE_CHOICES,
    Battle,
    Output,
    Prompt,
    System,
    prompt_id_to_task,
)


def test_task_and_vote_constants() -> None:
    assert "t3" in TASK_CHOICES
    assert VOTE_CHOICES == frozenset({"a", "b", "n", "t"})
    assert prompt_id_to_task("en_1") == "t3"
    assert prompt_id_to_task("anything") == "t3"


def test_system_equality_and_hash() -> None:
    s1 = System(id="sys_a")
    s2 = System(id="sys_a")
    s3 = System(id="sys_b")

    assert s1 == s2
    assert hash(s1) == hash(s2)
    assert s1 != s3
    assert s1 != "sys_a"
    assert s1.__eq__("sys_a") is NotImplemented


def test_prompt_validation() -> None:
    # Valid combinations
    p_words = Prompt(id="en_1", word1="cat", word2="dog")
    assert p_words.word1 == "cat" and p_words.word2 == "dog"

    p_headline = Prompt(id="es_1", headline="Breaking news")
    assert p_headline.headline == "Breaking news"

    p_url = Prompt(id="zh_1", url="https://example.com/a.gif", prompt="Fill in")
    assert p_url.url == "https://example.com/a.gif" and p_url.prompt == "Fill in"

    p_url_no_prompt = Prompt(id="b1_1", url="https://example.com/a.gif")
    assert p_url_no_prompt.prompt is None

    # Invalid combinations
    with pytest.raises(ValueError, match="If `word1` is set"):
        Prompt(id="en_1", word1="cat")

    with pytest.raises(ValueError, match="If `word1` is set"):
        Prompt(id="en_1", word1="cat", word2="dog", headline="News")

    with pytest.raises(ValueError, match="If `word1` is set"):
        Prompt(id="en_1", word1="cat", word2="dog", url="https://example.com")

    with pytest.raises(ValueError, match="If `word1` is set"):
        Prompt(id="en_1", word1="cat", word2="dog", prompt="extra")

    with pytest.raises(ValueError, match="If `headline` is set"):
        Prompt(id="en_1", headline="News", word2="dog")

    with pytest.raises(ValueError, match="If `headline` is set"):
        Prompt(id="en_1", headline="News", url="https://example.com")

    with pytest.raises(ValueError, match="If `headline` is set"):
        Prompt(id="en_1", headline="News", prompt="extra")

    with pytest.raises(ValueError, match="If `url` is set"):
        Prompt(id="en_1", url="https://example.com", word2="dog")

    with pytest.raises(ValueError, match="One of `word1`\\+`word2`, `headline`, or `url` must be set"):
        Prompt(id="en_1")

    with pytest.raises(ValueError, match="One of `word1`\\+`word2`, `headline`, or `url` must be set"):
        Prompt(id="en_1", word2="dog")


def test_prompt_properties_and_verbalized() -> None:
    p_en_words = Prompt(id="en_1", word1="apple", word2="banana")
    assert p_en_words.task == "t3"
    assert p_en_words.language == "en"
    assert p_en_words.verbalized == "The outputs must contain the words <b>apple</b> and <b>banana</b>."

    p_es_words = Prompt(id="es_1", word1="gato", word2="perro")
    assert p_es_words.language == "es"
    assert p_es_words.verbalized == "La salidas deben contener las palabras <b>gato</b> y <b>perro</b>."

    p_zh_words = Prompt(id="zh_1", word1="猫", word2="狗")
    assert p_zh_words.language == "zh"
    assert p_zh_words.verbalized == "输出需要包含词语“<b>猫</b>”和“<b>狗</b>”。"

    p_en_hl = Prompt(id="en_2", headline="Man bites dog")
    assert p_en_hl.verbalized == "<b>News headline:</b> Man bites dog"

    p_es_hl = Prompt(id="es_2", headline="Hombre muerde a perro")
    assert p_es_hl.verbalized == "<b>Titular:</b> Hombre muerde a perro"

    p_zh_hl = Prompt(id="zh_2", headline="人咬狗")
    assert p_zh_hl.verbalized == "<b>新闻标题:</b> 人咬狗"

    p_url = Prompt(id="img_1", url="https://example.com/1.gif", prompt="Caption _____")
    assert p_url.verbalized == "Caption _____"

    p_url_none = Prompt(id="img_2", url="https://example.com/2.gif")
    assert p_url_none.verbalized is None

    # Defensive branches when state is mutated
    p_corrupted = Prompt(id="en_3", headline="Valid")
    object.__setattr__(p_corrupted, "headline", None)
    with pytest.raises(ValueError, match="The prompt is not properly defined"):
        _ = p_corrupted.verbalized


def test_prompt_equality_and_hash() -> None:
    p1 = Prompt(id="en_1", headline="Headline A")
    p2 = Prompt(id="en_1", headline="Headline B")
    p3 = Prompt(id="en_2", headline="Headline A")

    assert p1 == p2
    assert hash(p1) == hash(p2)
    assert p1 != p3
    assert p1 != "en_1"
    assert p1.__eq__("en_1") is NotImplemented


def test_output_and_battle() -> None:
    p1 = Prompt(id="en_1", headline="H1")
    p2 = Prompt(id="en_2", headline="H2")
    s1 = System(id="sys1")
    s2 = System(id="sys2")

    o1 = Output(prompt=p1, system=s1, text="joke 1")
    o1_dup = Output(prompt=p1, system=s1, text="different text")
    o2 = Output(prompt=p1, system=s2, text="joke 2")
    o3 = Output(prompt=p2, system=s2, text="joke 3")

    assert o1 == o1_dup
    assert hash(o1) == hash(o1_dup)
    assert o1 != o2
    assert o1 != "not-an-output"
    assert o1.__eq__("not-an-output") is NotImplemented

    battle = Battle(output_a=o1, output_b=o2)
    assert battle.prompt == p1

    with pytest.raises(ValueError, match="Both outputs must belong to the same prompt"):
        Battle(output_a=o1, output_b=o3)


async def test_create_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_USER", "testuser")
    monkeypatch.setenv("DB_PASS", "testpass")
    monkeypatch.setenv("DB_HOST", "db.local")
    monkeypatch.setenv("DB_NAME", "testdb")

    mock_engine = MagicMock()
    mock_engine.dispose = AsyncMock()

    with patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=mock_engine) as mock_create:
        async with database.create_engine() as eng:
            assert eng is mock_engine
            mock_create.assert_called_once_with(
                "mysql+asyncmy://testuser:testpass@db.local/testdb",
                pool_size=10,
                pool_recycle=3600,
            )
        mock_engine.dispose.assert_awaited_once()


def test_create_battle_with_prompt_and_row_to_object() -> None:
    p = Prompt(id="en_1", headline="H1")

    with patch("random.random", return_value=0.1):
        b_swapped = database._create_battle_with_prompt(p, "sys_a", "text_a", "sys_b", "text_b", True)
        assert b_swapped.output_a.system.id == "sys_b"
        assert b_swapped.output_b.system.id == "sys_a"

    with patch("random.random", return_value=0.9):
        b_unswapped = database._create_battle_with_prompt(p, "sys_a", "text_a", "sys_b", "text_b", True)
        assert b_unswapped.output_a.system.id == "sys_a"
        assert b_unswapped.output_b.system.id == "sys_b"

    row = ("en_1", None, None, "H1", None, None, "sys_a", "text_a", "sys_b", "text_b")
    b_from_row = database._battle_row_to_object(row, randomly_swap_systems=False)
    assert b_from_row.prompt.id == "en_1"
    assert b_from_row.output_a.system.id == "sys_a"
    assert b_from_row.output_b.system.id == "sys_b"


async def test_random_least_voted_unseen_battles(
    seeded_db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
) -> None:
    # Add a vote so sys_a and sys_b on en_1 have votes, making sys_c or other prompts preferred
    await database.add_vote(seeded_db_engine, "session_1", "en_1", "sys_a", "sys_b", "a", False, False)

    battles = [
        b
        async for b in database.random_least_voted_unseen_battles(
            seeded_db_engine,
            phase_id=1,
            session_id="session_1",
            task="t3",
            batch_size=3,
            ignored_output_ids=[("es_1", "sys_a")],
        )
    ]
    assert len(battles) == 3
    for battle in battles:
        assert battle.output_a.system.id != battle.output_b.system.id
        assert battle.output_a.text != battle.output_b.text


async def test_random_least_voted_unseen_battles_skips_unpartnerable_outputs(
    db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
) -> None:
    # Prompt p1 has only identical texts across systems (no valid partner with text != text_a),
    # while p2 has distinct texts. The algorithm should skip p1 candidates and yield from p2.
    async with db_engine.begin() as conn:
        await conn.execute(
            sqlalchemy.sql.text("""
                INSERT INTO prompts (prompt_id, word1, word2, headline, url, prompt, task, phase_id) VALUES
                ('en_1', NULL, NULL, 'Prompt 1', NULL, NULL, 't3', 1),
                ('en_2', NULL, NULL, 'Prompt 2', NULL, NULL, 't3', 1)
            """)
        )
        await conn.execute(sqlalchemy.sql.text("INSERT INTO systems (system_id) VALUES ('s1'), ('s2')"))
        await conn.execute(
            sqlalchemy.sql.text("""
                INSERT INTO outputs (prompt_id, system_id, text) VALUES
                ('en_1', 's1', 'identical'),
                ('en_1', 's2', 'identical'),
                ('en_2', 's1', 'unique 1'),
                ('en_2', 's2', 'unique 2')
            """)
        )

    battles = [b async for b in database.random_least_voted_unseen_battles(db_engine, 1, "sess", "t3", batch_size=1)]
    assert len(battles) == 1
    assert battles[0].prompt.id == "en_2"


async def test_random_least_voted_unseen_battles_empty_db(
    db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
) -> None:
    # When no valid partner exists for any candidate (or the DB is empty), it should terminate cleanly.
    battles = [b async for b in database.random_least_voted_unseen_battles(db_engine, 1, "sess", "t3", batch_size=4)]
    assert battles == []


async def test_random_battles_and_same_text_battles(
    seeded_db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
) -> None:
    random_b = [b async for b in database.random_battles(seeded_db_engine, phase_id=1, task="t3", batch_size=2)]
    assert len(random_b) == 2

    # In seeded_db_engine, en_1 has identical text for sys_a and sys_c
    same_b = [b async for b in database.battles_with_same_text(seeded_db_engine, phase_id=1, task="t3")]
    assert len(same_b) == 1
    assert same_b[0].prompt.id == "en_1"
    assert same_b[0].output_a.system.id == "sys_a"
    assert same_b[0].output_b.system.id == "sys_c"

    tie_votes = [
        v async for v in database.get_votes_for_battles_with_the_same_text(seeded_db_engine, phase_id=1, task="t3")
    ]
    assert len(tie_votes) == 1
    assert tie_votes[0].vote == "t"
    assert tie_votes[0].session_id == "<placeholder>"


async def test_votes_queries_and_stats(
    seeded_db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
) -> None:
    # Add votes across sessions and systems so sys_a and sys_b appear on both sides A and B
    await database.add_vote(seeded_db_engine, "s1", "en_1", "sys_a", "sys_b", "a", True, False)
    # Duplicate key should be ignored gracefully
    await database.add_vote(seeded_db_engine, "s1", "en_1", "sys_a", "sys_b", "b", False, False)
    await database.add_vote(seeded_db_engine, "s1", "es_1", "sys_b", "sys_a", "b", False, True)
    await database.add_vote(seeded_db_engine, "s2", "zh_1", "sys_a", "sys_b", "n", False, False)
    # sys_c only appears on side B, so get_votes_for_scoring should filter it out
    await database.add_vote(seeded_db_engine, "s3", "en_1", "sys_a", "sys_c", "t", False, False)

    assert await database.session_vote_count_without_skips(seeded_db_engine, "s1") == 2
    assert await database.session_vote_count_without_skips(seeded_db_engine, "s2") == 0
    assert await database.vote_count_without_skips(seeded_db_engine) == 3

    session_ids = {s async for s in database.get_session_ids(seeded_db_engine, phase_id=1, task="t3")}
    assert session_ids == {"s1", "s2", "s3"}

    systems = {s async for s in database.get_systems(seeded_db_engine, phase_id=1, task="t3")}
    assert systems == {"sys_a", "sys_b", "sys_c"}

    # Add a 4th system with an output on en_1 but 0 votes to test 0-vote system inclusion
    async with seeded_db_engine.begin() as conn:
        await conn.execute(sqlalchemy.sql.text("INSERT INTO systems (system_id) VALUES ('sys_d')"))
        await conn.execute(
            sqlalchemy.sql.text("INSERT INTO outputs (prompt_id, system_id, text) VALUES ('en_1', 'sys_d', 'Zero')")
        )

    votes_per_sys = await database.get_votes_per_system(seeded_db_engine, phase_id=1, task="t3")
    assert votes_per_sys["sys_a"] == 3
    assert votes_per_sys["sys_b"] == 2
    assert votes_per_sys["sys_c"] == 1
    assert votes_per_sys["sys_d"] == 0

    votes_per_sys_excl = await database.get_votes_per_system(
        seeded_db_engine, phase_id=1, task="t3", excluded_session_ids=["s3"]
    )
    assert votes_per_sys_excl["sys_c"] == 0

    votes_per_session = await database.get_votes_per_session(seeded_db_engine, phase_id=1)
    assert votes_per_session == {"s1": 2, "s3": 1}

    scoring_votes = [v async for v in database.get_votes_for_scoring(seeded_db_engine, phase_id=1, task="t3")]
    assert len(scoring_votes) == 2
    assert {v.vote for v in scoring_votes} == {"a", "b"}

    scoring_votes_excl = [
        v
        async for v in database.get_votes_for_scoring(
            seeded_db_engine, phase_id=1, task="t3", excluded_session_ids=["s1"]
        )
    ]
    assert len(scoring_votes_excl) == 0

    votes_csv = await database.get_votes_csv(seeded_db_engine, phase_id=1)
    async with seeded_db_engine.connect() as conn:
        expected_csv = pd.DataFrame(
            iter(
                await conn.execute(
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
                    {"phase_id": 1},
                )
            )
        ).to_csv(index=False)
    assert votes_csv == expected_csv
    assert len(votes_csv.strip().splitlines()) == 5  # 1 header + 4 rows

    empty_csv = await database.get_votes_csv(seeded_db_engine, phase_id=999)
    assert empty_csv == pd.DataFrame([]).to_csv(index=False)

    stats = await database.stats(seeded_db_engine)
    assert stats["votes"] == 4
    assert stats["sessions"] == 3
    assert stats["votes-without-skips"] == 3
    assert stats["sessions-without-skips"] == 2
    assert set(stats["votes-per-category"].keys()) == {"a", "b", "n", "t"}
    assert stats["votes-per-category"]["a"] == 1
    assert stats["votes-per-category"]["b"] == 1
    assert stats["votes-per-category"]["n"] == 1
    assert stats["votes-per-category"]["t"] == 1


async def test_prolific_consent_and_finish(
    db_engine: sqlalchemy.ext.asyncio.AsyncEngine,
) -> None:
    await database.prolific_consent(db_engine, "prolific-1")
    await database.prolific_finish(db_engine, "prolific-1", "Great study!")
    async with db_engine.connect() as conn:
        row = (
            await conn.execute(
                sqlalchemy.sql.text("SELECT session_id, comments FROM prolific WHERE session_id = 'prolific-1'")
            )
        ).one()
        assert row[0] == "prolific-1"
        assert row[1] == "Great study!"
