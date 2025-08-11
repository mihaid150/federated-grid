FROM python:3.10-slim

WORKDIR /app

RUN mkdir -p "/app/models"
RUN mkdir -p "/app/data"
RUN mkdir =p "/app/data/filtered_data"

COPY requirements.txt /app/
COPY *federated_topology* /app/

RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    iputils-ping \
    iproute2 \
    dnsutils \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY edge /app/edge
COPY shared /app/shared

COPY /data/input_data.csv /app/data/

ENV PYTHONPATH=/app:/app/edge:/app/shared

EXPOSE 8081

CMD ["uvicorn", "edge.communication..app_factory:app", "--host", "0.0.0.0", "--port", "8081"]
