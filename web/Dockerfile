FROM python:3.15-slim

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"
ENV UV_COMPILE_BYTECODE=1

# Because of the caching:
ENV UV_LINK_MODE=copy

RUN \
    --mount=from=ghcr.io/astral-sh/uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --no-default-groups --locked --no-install-project

COPY pyproject.toml uv.lock ./
COPY src/mwahahavote src/mwahahavote

RUN \
    --mount=from=ghcr.io/astral-sh/uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-default-groups --locked

EXPOSE 80

# We tried with `--workers "$(($(nproc)*2 + 1))"` but it uses too much memory for DigialOcean CPU-RAM ratios
# of the machines we use (about <.5GB per worker).
# And fewer seems fine.
CMD ["sh", "-c", "exec uvicorn mwahahavote.__main__:app --host :: --port 80 --workers \"$(($(nproc) + 1))\""]
