# syntax=docker/dockerfile:1.7
FROM python:3.10-slim

WORKDIR /app

# OS deps
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
      iputils-ping dnsutils iproute2 && rm -rf /var/lib/apt/lists/*

# Python deps (note path from repo root)
COPY cloud/requirements.txt /app/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /app/requirements.txt

# App code (from repo root context)
COPY cloud /app/cloud
COPY shared /app/shared

ENV PYTHONPATH=/app:/app/cloud:/app/shared
EXPOSE 8081

# runtime dirs and ownership for uid 1000
RUN mkdir -p /app/models /app/status && chown -R 1000:1000 /app
USER 1000:1000

CMD ["uvicorn","cloud.communication.app_factory:app","--host","0.0.0.0","--port","8081"]
