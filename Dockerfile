FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md requirements.txt ./
COPY app ./app
COPY scripts ./scripts
COPY samples ./samples
COPY examples ./examples
COPY index_organizer.py organizer.py quick_organizer.py main.py ./

RUN pip install --upgrade pip && pip install .

ENTRYPOINT ["index-organizer"]
CMD ["status", "--config", "/config/folder-organizer-v2.yml"]
