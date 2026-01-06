FROM python:3.9-slim

WORKDIR /app

# Копирование файла зависимостей
COPY requirements.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование ВСЕХ исходных файлов
COPY data-pipeline/ ./data-pipeline/
COPY sql/ ./sql/
COPY tests/ ./tests/

# Создаем точку входа которая будет ждать команд
CMD ["tail", "-f", "/dev/null"]