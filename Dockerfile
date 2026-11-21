# One image for producer, scorer, db_writer, window_aggregator and the topic-creation job.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ common/
COPY infra/kafka/ infra/kafka/
COPY producer/ producer/
COPY scorer/ scorer/
COPY db_writer/ db_writer/
COPY window_aggregator/ window_aggregator/
COPY ops/ ops/

# data/ and models/ are mounted as volumes by docker-compose (not baked into the image)
RUN mkdir -p /app/state
