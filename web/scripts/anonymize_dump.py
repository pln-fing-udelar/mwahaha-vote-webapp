#!/usr/bin/env -S uv run --script --extra scripts --env-file ../.env
import re
from contextlib import contextmanager
lazy import argparse
lazy import csv
lazy import gzip
lazy import os
lazy from collections.abc import Generator, Iterable
lazy from pathlib import Path
lazy from typing import TextIO

PROLIFIC_ID_PATTERN = re.compile(r"prolific-id-([a-zA-Z0-9]+)(?:-([a-zA-Z0-9]+))?")
PROLIFIC_TABLE_PREFIX = "INSERT INTO `prolific` VALUES "


@contextmanager
def open_text_reader(path: Path) -> Generator[TextIO]:
    """Opens a text file for reading, transparently handling .gz compression."""
    f = gzip.open(path, "rt", encoding="utf-8") if path.suffix == ".gz" else open(path, encoding="utf-8")
    try:
        yield f
    finally:
        f.close()


@contextmanager
def open_text_writer(path: Path, newline: str | None = None) -> Generator[TextIO]:
    """Opens a text file for writing, transparently handling .gz compression."""
    f = (
        gzip.open(path, "wt", encoding="utf-8", newline=newline)
        if path.suffix == ".gz"
        else open(path, "w", encoding="utf-8", newline=newline)
    )
    try:
        yield f
    finally:
        f.close()


def _parse_tuple_elements(values_str: str, start: int) -> tuple[list[str], int]:
    elements: list[str] = []
    cur: list[str] = []
    in_quote = False
    escape = False
    i = start
    n = len(values_str)
    while i < n:
        char = values_str[i]
        if in_quote:
            cur.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == "'":
                in_quote = False
        elif char == "'":
            in_quote = True
            cur.append(char)
        elif char == ",":
            elements.append("".join(cur).strip())
            cur = []
        elif char == ")":
            elements.append("".join(cur).strip())
            return elements, i + 1
        else:
            cur.append(char)
        i += 1
    return elements, i


def parse_prolific_tuples(values_str: str) -> list[list[str]]:
    """Parses SQL tuples from the VALUES clause of the prolific table."""
    tuples: list[list[str]] = []
    i = 0
    n = len(values_str)
    while i < n:
        if values_str[i] == "(":
            elements, i = _parse_tuple_elements(values_str, i + 1)
            tuples.append(elements)
        else:
            i += 1
    return tuples


def sanitize_prolific_line(line: str) -> str:
    """Replaces comments in the INSERT INTO `prolific` VALUES statement with NULL."""
    if not line.startswith(PROLIFIC_TABLE_PREFIX):
        return line
    values_str = line[len(PROLIFIC_TABLE_PREFIX) :].rstrip(";\n")
    tuples = parse_prolific_tuples(values_str)
    sanitized_tuples: list[str] = []
    for t in tuples:
        if len(t) >= 4:
            t[3] = "NULL"
        sanitized_tuples.append(f"({','.join(t)})")
    return PROLIFIC_TABLE_PREFIX + ",".join(sanitized_tuples) + ";\n"


def build_anonymization_maps(
    prolific_ids: Iterable[str],
) -> tuple[dict[str, str], dict[str, str]]:
    """Builds deterministic anonymization mappings for Prolific worker and session IDs."""
    workers: set[str] = set()
    sessions: set[str] = set()
    for pid in prolific_ids:
        for m in PROLIFIC_ID_PATTERN.finditer(pid):
            workers.add(m.group(1))
            if m.group(2):
                sessions.add(m.group(2))

    sorted_workers = sorted(workers)
    sorted_sessions = sorted(sessions)

    worker_map = {w: f"worker_{i + 1:04d}" for i, w in enumerate(sorted_workers)}
    session_map = {s: f"session_{i + 1:04d}" for i, s in enumerate(sorted_sessions)}
    return worker_map, session_map


def anonymize_prolific_id(
    raw_id: str,
    worker_map: dict[str, str],
    session_map: dict[str, str],
) -> str:
    """Anonymizes a single Prolific ID string while preserving its structure."""

    def repl(m: re.Match[str]) -> str:
        w = worker_map.get(m.group(1), m.group(1))
        if m.group(2):
            s = session_map.get(m.group(2), m.group(2))
            return f"prolific-id-{w}-{s}"
        return f"prolific-id-{w}"

    return PROLIFIC_ID_PATTERN.sub(repl, raw_id)


def anonymize_dump(
    dump_input: Path,
    dump_output: Path,
    worker_map: dict[str, str],
    session_map: dict[str, str],
) -> tuple[int, int]:
    """Anonymizes a MySQL dump file, stripping GTID purged statements and scrubbing comments."""

    def repl(m: re.Match[str]) -> str:
        w = worker_map.get(m.group(1), m.group(1))
        if m.group(2):
            s = session_map.get(m.group(2), m.group(2))
            return f"prolific-id-{w}-{s}"
        return f"prolific-id-{w}"

    lines_processed = 0
    gtid_stripped = 0

    with open_text_reader(dump_input) as f_in, open_text_writer(dump_output) as f_out:
        for line in f_in:
            lines_processed += 1
            if line.startswith("SET @@GLOBAL.GTID_PURGED="):
                gtid_stripped += 1
                continue
            if line.startswith(PROLIFIC_TABLE_PREFIX):
                line = sanitize_prolific_line(line)
            line = PROLIFIC_ID_PATTERN.sub(repl, line)
            f_out.write(line)

    return lines_processed, gtid_stripped


def anonymize_csv(
    csv_input: Path,
    csv_output: Path,
    worker_map: dict[str, str],
    session_map: dict[str, str],
) -> tuple[int, int]:
    """Anonymizes Prolific IDs in the session_id column of a CSV file."""
    rows_processed = 0
    rows_anonymized = 0

    with open_text_reader(csv_input) as f_in, open_text_writer(csv_output, newline="") as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        header = next(reader)
        writer.writerow(header)
        sess_idx = header.index("session_id") if "session_id" in header else -1

        for row in reader:
            rows_processed += 1
            if sess_idx != -1 and sess_idx < len(row):
                sid = row[sess_idx]
                if sid.startswith("prolific-id-"):
                    row[sess_idx] = anonymize_prolific_id(sid, worker_map, session_map)
                    rows_anonymized += 1
            writer.writerow(row)

    return rows_processed, rows_anonymized


def collect_prolific_ids(dump_path: Path | None, csv_paths: list[Path]) -> set[str]:
    """Scans SQL dump and CSV files to extract all raw Prolific IDs."""
    all_prolific_ids: set[str] = set()

    if dump_path and dump_path.exists():
        with open_text_reader(dump_path) as f:
            for line in f:
                for m in PROLIFIC_ID_PATTERN.finditer(line):
                    all_prolific_ids.add(m.group(0))

    for csv_file in csv_paths:
        if csv_file.exists():
            with open_text_reader(csv_file) as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header and "session_id" in header:
                    sess_idx = header.index("session_id")
                    for row in reader:
                        if sess_idx < len(row) and row[sess_idx].startswith("prolific-id-"):
                            all_prolific_ids.add(row[sess_idx])

    return all_prolific_ids


def main() -> None:
    parser = argparse.ArgumentParser(description="Anonymize MySQL database dump and CSV annotation files for release.")
    parser.add_argument(
        "--dump-input",
        "-i",
        type=Path,
        default=Path("/home/sacastro/Downloads/dump-mwahaha-20260222.sql"),
        help="Path to input MySQL dump file (.sql or .sql.gz).",
    )
    parser.add_argument(
        "--dump-output",
        "-o",
        type=Path,
        default=Path("/home/sacastro/Downloads/dump-mwahaha-sanitized.sql.gz"),
        help="Path to output sanitized MySQL dump file (.sql or .sql.gz).",
    )
    parser.add_argument(
        "--csv-inputs",
        type=Path,
        nargs="*",
        default=[],
        help="Paths to CSV files to anonymize with the same worker/session mapping.",
    )
    parser.add_argument(
        "--csv-outputs",
        type=Path,
        nargs="*",
        default=[],
        help="Paths to output CSV files (must match count of --csv-inputs if provided).",
    )

    args = parser.parse_args()

    all_prolific_ids = collect_prolific_ids(args.dump_input, args.csv_inputs)
    worker_map, session_map = build_anonymization_maps(all_prolific_ids)
    print(f"Mapped {len(worker_map)} unique workers and {len(session_map)} unique sessions.")

    if args.dump_input.exists():
        print(f"Anonymizing dump: {args.dump_input} -> {args.dump_output}")
        lines, gtid_stripped = anonymize_dump(args.dump_input, args.dump_output, worker_map, session_map)
        out_size_mb = os.path.getsize(args.dump_output) / (1024 * 1024)
        print(f"Dump done: {lines} lines, {gtid_stripped} GTID lines stripped, output size: {out_size_mb:.2f} MB")

    csv_outputs = args.csv_outputs or [f.with_name(f"{f.stem}-sanitized{f.suffix}") for f in args.csv_inputs]

    for csv_in, csv_out in zip(args.csv_inputs, csv_outputs, strict=True):
        print(f"Anonymizing CSV: {csv_in} -> {csv_out}")
        rows, anon_rows = anonymize_csv(csv_in, csv_out, worker_map, session_map)
        csv_size_mb = os.path.getsize(csv_out) / (1024 * 1024)
        print(f"CSV done: {rows} rows ({anon_rows} anonymized), output size: {csv_size_mb:.2f} MB")


if __name__ == "__main__":
    main()
