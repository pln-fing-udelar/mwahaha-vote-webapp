FROM python:3.14

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"
ENV UV_COMPILE_BYTECODE=1

# Because of the caching:
ENV UV_LINK_MODE=copy

RUN touch README.md
COPY pyproject.toml uv.lock ./
COPY src/mwahahavote src/mwahahavote

RUN \
    --mount=from=ghcr.io/astral-sh/uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-default-groups --locked --inexact

EXPOSE 80

CMD uvicorn mwahahavote.__main__:app --host :: --port 80 --workers "$(($(nproc)*2 + 1))"
