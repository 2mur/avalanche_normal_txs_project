FROM python:3.11-slim

# Prevent Python buffering
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

# Default command (can be overridden)
CMD ["python", "src/ingest.py"]