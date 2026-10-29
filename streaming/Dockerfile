FROM python:3.11-slim-bookworm
WORKDIR /workspace
COPY requirements.txt /tmp/streaming-requirements.txt
RUN pip install --no-cache-dir -r /tmp/streaming-requirements.txt
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/workspace
ENTRYPOINT ["python"]
