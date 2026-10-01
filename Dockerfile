FROM python:3.14-slim-bookworm AS python-builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY requirements.txt .
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt


FROM python:3.14-slim-bookworm AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
    && ln -snf /usr/share/zoneinfo/America/Sao_Paulo /etc/localtime \
    && echo America/Sao_Paulo > /etc/timezone \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app \
    && mkdir -p /app /data \
    && chown -R app:app /app /data \
    && rm -rf /var/lib/apt/lists/*

COPY --from=python-builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}" \
    TZ=America/Sao_Paulo \
    DATA_DIR=/data \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY --chown=app:app app ./app

USER 10001:10001
EXPOSE 8080 444
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
