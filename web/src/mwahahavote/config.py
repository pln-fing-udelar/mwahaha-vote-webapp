import os
from dataclasses import dataclass, field
from typing import Literal

type Language = Literal["en", "es", "zh"]

type HahaTask = Literal["t3"]
type MwahahaTask = Literal["a-en", "a-es", "a-zh", "b1", "b2"]
type Task = HahaTask | MwahahaTask


@dataclass(frozen=True)
class TaskConfig:
    id: Task
    label: str
    language: Language = "en"
    is_multimodal: bool = False
    prompt_id_prefix: tuple[str, ...] = ()
    codabench_task_id: int | None = None


@dataclass(frozen=True)
class CompetitionConfig:
    id: str
    title: str
    phase_id: int
    tasks: tuple[TaskConfig, ...]
    default_task: Task
    codabench_competition_id: int | None = None
    only_latest_submission_per_user: bool = True
    prolific_completion_codes: dict[Task, str] = field(default_factory=dict)
    ignored_users: frozenset[str] = frozenset()
    excluded_session_ids: frozenset[str] = frozenset()

    @property
    def allow_codabench(self) -> bool:
        return self.codabench_competition_id is not None

    @property
    def task_ids(self) -> tuple[Task, ...]:
        return tuple(t.id for t in self.tasks)

    @property
    def task_by_id(self) -> dict[Task, TaskConfig]:
        return {t.id: t for t in self.tasks}

    @property
    def codabench_task_map(self) -> dict[int, Task]:
        return {t.codabench_task_id: t.id for t in self.tasks if t.codabench_task_id is not None}

    @property
    def task_choices(self) -> frozenset[Task]:
        return frozenset(self.task_ids)


# MWAHAHA CodaBench IDs
CODABENCH_COMPETITION_ID = 9719
EVALUATION_TRIAL_PHASE_ID = 15784
EVALUATION_PHASE_ID = 15785

MWAHAHA_EXCLUDED_SESSION_IDS = frozenset(
    {
        "8F53VDMDMBLZ1W8HTNR4K4GVKJSHD1XLM9WX0B1YKFWN45NR3U34XO3JFB0DBTL7XSUDDUYW2G2AYANTVG6HYTSJCP2S06S2USG1",
        "3SDVXMOG9IYX4MK8FCEM76TUYL6HFOTKY23Q04WHIYNQ1KLQWHXQ3RG2XAN8SZYAORXBV9HLLC3I7QDR95U5UGQUPGOH0Q9IEH5D",
        "J9KI4RXWOXHNK0SAW059NWJKTUUFNFZ77209AKM9V0EOXWN8RM1S1QPFT09IIBRM8EBOQ160GRKCWW46WI86MNB9W3J4OU7ZC71G",
        "DMJ2P6KG0QV0GK06VH8C11JVOJLV74JHFEJYTE9M1NK80ZLM958ERVOYL8WTHNOCL5W3ZTN5OFZZBHCFHHK2OHR43NQNPD0X4YWZ",
        "LLYBSKNFT8UBK4UNJSLI3FAHZ73HH6D065RCSBERTYIPMJCG4QU5VO5Y5EHVVSQVA1FOIBHY5S7VHNZERDVFWVBA7WQ190T6AASQ",
        "3NBGJ6ZWZHET45IXK751318KO8GEZVPHQTL2UHTN58H9V1S9R44Q9XZN97XRLP2IB7MDWX6CRHY8KUN4PNB0QWN1FVNVD6553TQX",
        "XK4N66WNIBB47NROSDBLL3LK6PJJ83EGX86HL3TT4WLDYRLF64IPO4C8SRDTK4RTJHIMDT7QKHCIDD4GXSR4UW8ZBWHRAVIBXCFN",
        "9e178843cf32f691",
        "f6dda751c9aca2ef",
        "71f4642d66ff397f",
        # "37037172e24bfe0e",
    }
)


COMPETITIONS: dict[str, CompetitionConfig] = {
    "mwahaha": CompetitionConfig(
        id="mwahaha",
        title="MWAHAHA Competition",
        phase_id=EVALUATION_PHASE_ID,
        default_task="a-en",
        codabench_competition_id=CODABENCH_COMPETITION_ID,
        ignored_users=frozenset({"dangnt"}),
        excluded_session_ids=MWAHAHA_EXCLUDED_SESSION_IDS,
        prolific_completion_codes={
            "a-en": "CC4WY7K5",
            "a-es": "C5SU1Q6U",
            "a-zh": "C11V5NNR",
            "b1": "CCI5PILX",
            "b2": "C13AY54S",
        },
        tasks=(
            TaskConfig(
                "a-en",
                "Task A: Joke in English",
                language="en",
                prompt_id_prefix=("en_",),
                codabench_task_id=21358,
            ),
            TaskConfig(
                "a-es",
                "Task A: Joke in Spanish",
                language="es",
                prompt_id_prefix=("es_",),
                codabench_task_id=21359,
            ),
            TaskConfig(
                "a-zh",
                "Task A: Joke in Chinese",
                language="zh",
                prompt_id_prefix=("zh_",),
                codabench_task_id=21360,
            ),
            TaskConfig(
                "b1",
                "Task B1: Caption the GIF",
                language="en",
                is_multimodal=True,
                prompt_id_prefix=("img_",),
                codabench_task_id=21361,
            ),
            TaskConfig(
                "b2",
                "Task B2: Fill in the GIF Caption",
                language="en",
                is_multimodal=True,
                prompt_id_prefix=("img_2_",),
                codabench_task_id=22992,
            ),
        ),
    ),
    "mwahaha-dev": CompetitionConfig(
        id="mwahaha-dev",
        title="MWAHAHA Competition (Dev Phase)",
        phase_id=EVALUATION_TRIAL_PHASE_ID,
        default_task="a-en",
        codabench_competition_id=CODABENCH_COMPETITION_ID,
        only_latest_submission_per_user=False,
        ignored_users=frozenset({"dangnt"}),
        excluded_session_ids=MWAHAHA_EXCLUDED_SESSION_IDS,
        prolific_completion_codes={
            "a-en": "CC4WY7K5",
            "a-es": "C5SU1Q6U",
            "a-zh": "C11V5NNR",
            "b1": "CCI5PILX",
            "b2": "C13AY54S",
        },
        tasks=(
            TaskConfig(
                "a-en",
                "Task A: Joke in English",
                language="en",
                prompt_id_prefix=("en_",),
                codabench_task_id=21358,
            ),
            TaskConfig(
                "a-es",
                "Task A: Joke in Spanish",
                language="es",
                prompt_id_prefix=("es_",),
                codabench_task_id=21359,
            ),
            TaskConfig(
                "a-zh",
                "Task A: Joke in Chinese",
                language="zh",
                prompt_id_prefix=("zh_",),
                codabench_task_id=21360,
            ),
            TaskConfig(
                "b1",
                "Task B1: Caption the GIF",
                language="en",
                is_multimodal=True,
                prompt_id_prefix=("img_",),
                codabench_task_id=21361,
            ),
            TaskConfig(
                "b2",
                "Task B2: Fill in the GIF Caption",
                language="en",
                is_multimodal=True,
                prompt_id_prefix=("img_2_",),
                codabench_task_id=22992,
            ),
        ),
    ),
    "haha": CompetitionConfig(
        id="haha",
        title="HAHA 2026 task 3",
        phase_id=1,
        default_task="t3",
        tasks=(TaskConfig("t3", "Task 3", language="es"),),
    ),
}


COMPETITION_CHOICES: tuple[str, ...] = tuple(COMPETITIONS.keys())


def get_active_competition() -> CompetitionConfig:
    comp_name = os.getenv("COMPETITION", "mwahaha").lower()
    return COMPETITIONS.get(comp_name, COMPETITIONS["mwahaha"])


def get_competition(comp_name: str | None = None) -> CompetitionConfig:
    if comp_name is None:
        return get_active_competition()
    comp_name = comp_name.lower()
    if comp_name not in COMPETITIONS:
        raise ValueError(f"Unknown competition '{comp_name}'. Available competitions: {', '.join(COMPETITIONS.keys())}")
    return COMPETITIONS[comp_name]


ACTIVE_COMPETITION = get_active_competition()
TASK_CHOICES: frozenset[Task] = ACTIVE_COMPETITION.task_choices


def is_only_latest_submission_per_user(competition: CompetitionConfig | None = None) -> bool:
    env_val = os.getenv("ONLY_LATEST_SUBMISSION_PER_USER")
    if env_val is not None:
        return env_val.strip().lower() in ("1", "true", "yes", "on")
    if competition is not None:
        return competition.only_latest_submission_per_user
    return ACTIVE_COMPETITION.only_latest_submission_per_user
