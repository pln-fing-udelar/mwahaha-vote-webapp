FROM python:3.13
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /usr/src/app
COPY pyproject.toml uv.lock README.md ./
COPY mwahahavote mwahahavote
RUN uv sync --locked
EXPOSE 5000
CMD ["uv", "run", "flask", "run", "-h", "::", "--debug"]
