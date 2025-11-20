FROM python:3.13
COPY --from=ghcr.io/astral-sh/uv /uv /uvx /bin/
WORKDIR /usr/src/app
RUN touch README.md
COPY pyproject.toml uv.lock ./
COPY src/mwahahavote src/mwahahavote
RUN uv sync --no-default-groups --no-managed-python --inexact --locked
EXPOSE 5000
CMD ["uv", "run", "flask", "run", "-h", "::", "--debug"]
