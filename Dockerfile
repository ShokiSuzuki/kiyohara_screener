FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install minimal build deps needed by some wheels (lxml, pandas)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       libxml2-dev \
       libxslt1-dev \
       zlib1g-dev \
       libffi-dev \
       libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

ENV PORT=5000
EXPOSE 5000

# Run with Gunicorn in production
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "kiyohara_server:app"]
