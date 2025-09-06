# syntax=docker/dockerfile:1.7
FROM python:3.10-slim
WORKDIR /app

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
      iputils-ping dnsutils iproute2 && rm -rf /var/lib/apt/lists/*

COPY fog/requirements.txt /app/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /app/requirements.txt

COPY fog /app/fog
COPY shared /app/shared

ENV PYTHONPATH=/app:/app/fog:/app/shared
EXPOSE 8081

RUN mkdir -p /app/models /app/models/outbox && chown -R 1000:1000 /app
USER 1000:1000

CMD ["uvicorn","fog.communication.app_factory:app","--host","0.0.0.0","--port","8081"]
