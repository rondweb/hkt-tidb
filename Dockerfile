FROM python:3.11-slim

# Install system deps for common DB drivers
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install pip dependencies
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . /app

ENV PYTHONUNBUFFERED=1

# Entrypoint
CMD ["python", "entrypoint.py"]
