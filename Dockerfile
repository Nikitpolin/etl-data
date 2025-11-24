FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    locales \
    && rm -rf /var/lib/apt/lists/* \
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8
ENV PYTHONIOENCODING=utf-8
ENV PYTHONUTF8=1
ENV PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY . .

RUN echo "=== Checking files ===" && \
    ls -la && \
    ls -la data-pipeline/src/ && \
    ls -la tests/

CMD ["python", "data-pipeline/src/main.py"]