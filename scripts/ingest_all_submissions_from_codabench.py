#!/usr/bin/env python
import logging
import tempfile

import requests
from tqdm.auto import tqdm

from ingestion.codabench import Submission, get_submission_url, list_submissions
from ingestion.submission import ingest_submission


def main() -> None:
    submissions = list(list_submissions())
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

            try:
                affected_rows += ingest_submission(file, submission.user)
                successful.add(submission)
            except Exception:
                logging.exception(
                    f"Failed to ingest the submission ID {submission.id} from the user '{submission.user}'. See below."
                )

    print(f"{affected_rows} affected rows.")
    print(f"Ingested {len(successful)}/{len(submissions)} submissions.")
    print(f"Successful submissions: {sorted(successful)}")
    print(f"Failed submissions: {sorted(set(submissions) - successful)}")


if __name__ == "__main__":
    main()
