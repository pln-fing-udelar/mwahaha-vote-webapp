#!/usr/bin/env python
import logging
import tempfile

import requests
from tqdm.auto import tqdm

from ingestion.codabench import Submission, get_submission_url, list_submissions
from ingestion.submission import ingest_submission


def main() -> None:
    # We sort them so they are ingested in order,
    # so that the latest submission per user per task is the one that remains.
    submissions = sorted(list_submissions())

    affected_rows = 0
    successful: set[Submission] = set()

    for submission in tqdm(submissions, desc="Ingesting submissions", unit="submission"):
        # Note we don't check the list of tasks the person submitted for but the actual files.

        url = get_submission_url(submission.id)

        # Note `ZipFile` will need a seekable file, so we need to download it fully first.
        with requests.get(url, stream=True) as response, tempfile.TemporaryFile() as file:
            response.raise_for_status()

            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

            # noinspection PyBroadException
            try:
                affected_rows += ingest_submission(submission, file)
                successful.add(submission)
            except Exception:
                logging.exception(f"Failed to ingest the submission {submission}'. See below.")

    print(f"{affected_rows} affected rows.")
    print(f"{len(successful)}/{len(submissions)} submissions ingested successfully:")
    for submission in sorted(successful):
        print(f"- {submission}")

    if failed := set(submissions) - successful:
        print("Failed submissions:")
        for submission in sorted(failed):
            print(f"- {submission}")


if __name__ == "__main__":
    main()
