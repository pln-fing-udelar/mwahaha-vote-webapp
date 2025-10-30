#!/usr/bin/env python
import logging

import fsspec
from tqdm.auto import tqdm

from ingestion.codabench import Submission, get_submission_url, list_submissions
from ingestion.submission import ingest_submission, list_ingested_system_ids


def main() -> None:
    print(
        "Obtaining the metadata for all submissions that passed the submission test for at least one subtask and were"
        " not deleted… ",
        end="",
    )
    already_ingested_system_ids = frozenset(list_ingested_system_ids())
    print("✅")

    # We sort them so they are ingested in order,
    # so that the latest submission per user per task is the one that remains.
    submissions = sorted(list_submissions())

    print()
    print("Stats so far for the submissions that passed the test:")
    print()
    print(f"Total number of submissions: {len(submissions)}")
    print(f"Total number of submission-subtask pairs: {sum(len(submission.tasks) for submission in submissions)}")
    print(
        f"Unique number of users that submitted at least once:"
        f" {len(frozenset(submission.user for submission in submissions))}"
    )
    print()

    affected_rows = 0
    successful: set[Submission] = set()
    skipped: set[Submission] = set()

    for submission in tqdm(submissions, desc="Ingesting submissions", unit="submission"):
        if submission.system_id in already_ingested_system_ids:
            skipped.add(submission)
        else:
            # Note we don't check the list of tasks the person submitted for but the actual files.
            url = get_submission_url(submission.id)
            with fsspec.open(url) as file:
                # noinspection PyBroadException
                try:
                    affected_rows += ingest_submission(submission, file)
                    successful.add(submission)
                except Exception:
                    logging.exception(f"Failed to ingest the submission {submission}'. See below.")

    if skipped:
        print()
        print(f"{len(skipped)}/{len(submissions)} submissions were skipped because there were already ingested before.")

    attempted = set(submissions) - skipped

    if successful:
        print()
        print(f"The following {len(successful)}/{len(attempted)} new submissions ingested successfully:")
        for submission in sorted(successful):
            print(f"- {submission}")

        print()
        print(f"{affected_rows} affected rows.")
        print()

    if failed := attempted - successful:
        print()
        print(f"The following {len(failed)}/{len(attempted)} new submissions failed to be ingested:")
        for submission in sorted(failed):
            print(f"- {submission}")


if __name__ == "__main__":
    main()
