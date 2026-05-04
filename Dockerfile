FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_NO_PROGRESS=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PYTHONPATH=/app/src:/app/scripts \
    PATH=/opt/venv/bin:/usr/local/bin:/usr/bin:/bin

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.10 /uv /uvx /usr/local/bin/

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (cache layer) — server extra only, no GUI/viz.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --extra server --no-dev

# Copy source and install the project itself.
COPY src/ ./src/
COPY scripts/ ./scripts/
RUN uv sync --frozen --extra server --no-dev

RUN useradd --create-home --uid 1000 birdology \
 && mkdir -p /app/output \
 && chown -R birdology:birdology /app /opt/venv
USER birdology

EXPOSE 5000

CMD ["python", "scripts/web_chat.py", "--host", "0.0.0.0", "--port", "5000", "--input", "/app/output/birdology.ttl"]
