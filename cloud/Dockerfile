# syntax=docker/dockerfile:1.7
FROM python:3.10-slim

ARG PIP_INDEX_URL=http://offline:devpi-password@192.168.201.21:3141/offline/dev/+simple/
ARG PIP_TRUSTED_HOST=192.168.201.21
ENV PIP_INDEX_URL=${PIP_INDEX_URL}

WORKDIR /app

# OS deps
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
      iputils-ping dnsutils iproute2 && rm -rf /var/lib/apt/lists/*

# Python deps (note path from repo root)
COPY cloud/requirements.txt /app/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
<<<<<<< HEAD
    pip install --no-cache-dir --trusted-host ${PIP_TRUSTED_HOST} -r /app/requirements.txt

ARG CACHE_BUST=1
=======
    pip install -r /app/requirements.txt
>>>>>>> d713743c2c6a65a787e35b4fec23833e426ee6af

# App code (from repo root context)
COPY cloud /app/cloud
COPY shared /app/shared

ENV PYTHONPATH=/app:/app/cloud:/app/shared
EXPOSE 8081

# runtime dirs and ownership for uid 1000
RUN mkdir -p /app/models /app/status && chown -R 1000:1000 /app
USER 1000:1000

CMD ["uvicorn","cloud.communication.app_factory:app","--host","0.0.0.0","--port","8081"]
