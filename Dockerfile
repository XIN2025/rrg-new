FROM python:3.12-slim-bookworm

WORKDIR /app

RUN mkdir -p /app/data

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    gcc \
    g++ \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml .
COPY requirements.txt .

RUN useradd -m -u 1000 appuser

RUN uv sync

ENV PATH="/app/.venv/bin:$PATH"

COPY . .

RUN chown -R appuser:appuser /app

USER appuser
# Expose port
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
