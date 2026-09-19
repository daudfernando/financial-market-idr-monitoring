FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace/streaming
COPY requirements.txt /tmp/streaming-requirements.txt
RUN pip install --no-cache-dir -r /tmp/streaming-requirements.txt
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/workspace/streaming
ENTRYPOINT ["python"]
