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


COMPETITIONS: dict[str, CompetitionConfig] = {
    "mwahaha": CompetitionConfig(
        id="mwahaha",
        title="MWAHAHA Competition",
        phase_id=EVALUATION_PHASE_ID,
        default_task="a-en",
        codabench_competition_id=CODABENCH_COMPETITION_ID,
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
        title="HAHA 2026",
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
