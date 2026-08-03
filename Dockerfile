FROM python:3.11-slim

# ffmpeg requis pour le traitement des messages vocaux Telegram (conversion audio)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Requirements fusionnés (bot + asr-worker + s3-publisher)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie de tout le repo applicatif (bot/, asr-worker/, s3-publisher/)
# Kafka et ELK tournent sur des VM dédiées, on ne les copie/build pas ici.
COPY bot/ ./bot/
COPY asr-worker/ ./asr-worker/
COPY s3-publisher/ ./s3-publisher/

# La commande réelle est surchargée par service dans docker-compose.yml
CMD ["python3", "-m", "bot.main"]
