FROM python:3.14
COPY --from=ghcr.io/astral-sh/uv /uv /uvx /bin/
WORKDIR /app
RUN touch README.md
COPY pyproject.toml uv.lock ./
COPY src/mwahahavote src/mwahahavote
RUN uv sync --no-default-groups --no-managed-python --inexact --locked
EXPOSE 80
CMD uv run uvicorn mwahahavote.__main__:app --host :: --port 80 --workers "$(($(nproc)*2 + 1))"
