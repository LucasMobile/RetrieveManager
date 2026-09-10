FROM python:3.12-slim-bookworm AS python-builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY requirements.txt .
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt


FROM python:3.12-slim-bookworm AS dcmtk-builder

ARG DCMTK_VERSION=3.7.0
ARG DCMTK_URL=https://dicom.offis.de/download/dcmtk/release/bin/dcmtk-3.7.0-linux-x86_64.tar.bz2

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bzip2 \
        ca-certificates \
        libpng16-16 \
        libssl3 \
        libtiff6 \
        libwrap0 \
        libxml2 \
        tar \
        wget \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*
COPY vendor/ /tmp/vendor/
RUN if [ -f "/tmp/vendor/dcmtk-${DCMTK_VERSION}-linux-x86_64.tar.bz2" ]; then \
        cp "/tmp/vendor/dcmtk-${DCMTK_VERSION}-linux-x86_64.tar.bz2" /tmp/dcmtk.tar.bz2; \
    else \
        wget -q -O /tmp/dcmtk.tar.bz2 "${DCMTK_URL}"; \
    fi \
    && mkdir -p /tmp/dcmtk-extract /opt/dcmtk/bin /opt/dcmtk/lib /opt/dcmtk/share \
    && tar -xjf /tmp/dcmtk.tar.bz2 -C /tmp/dcmtk-extract \
    && BINDIR="$(dirname "$(find /tmp/dcmtk-extract -type f -name findscu | head -n1)")" \
    && PREFIX="$(dirname "$BINDIR")" \
    && cp -a "$BINDIR"/. /opt/dcmtk/bin/ \
    && if [ -d "$PREFIX/lib" ]; then cp -a "$PREFIX/lib"/. /opt/dcmtk/lib/; fi \
    && DICT="$(find /tmp/dcmtk-extract -type f -name dicom.dic | head -n1)" \
    && test -n "$DICT" \
    && cp -a "$(dirname "$DICT")"/. /opt/dcmtk/share/ \
    && /opt/dcmtk/bin/findscu --version \
    && /opt/dcmtk/bin/echoscu --version \
    && /opt/dcmtk/bin/movescu --version \
    && /opt/dcmtk/bin/storescp --version \
    && /opt/dcmtk/bin/dcmcjpeg --version


FROM python:3.12-slim-bookworm AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libpng16-16 \
        libssl3 \
        libtiff6 \
        libwrap0 \
        libxml2 \
        tzdata \
        zlib1g \
    && ln -snf /usr/share/zoneinfo/America/Sao_Paulo /etc/localtime \
    && echo America/Sao_Paulo > /etc/timezone \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app \
    && mkdir -p /app /data \
    && chown -R app:app /app /data \
    && rm -rf /var/lib/apt/lists/*

COPY --from=python-builder /opt/venv /opt/venv
COPY --from=dcmtk-builder /opt/dcmtk /opt/dcmtk

ENV PATH="/opt/venv/bin:/opt/dcmtk/bin:${PATH}" \
    LD_LIBRARY_PATH="/opt/dcmtk/lib" \
    DCMDICTPATH="/opt/dcmtk/share/dicom.dic:/opt/dcmtk/share/private.dic" \
    FINDSCU=/opt/dcmtk/bin/findscu \
    ECHOSCU=/opt/dcmtk/bin/echoscu \
    MOVESCU=/opt/dcmtk/bin/movescu \
    STORESCP=/opt/dcmtk/bin/storescp \
    DCMCJPEG=/opt/dcmtk/bin/dcmcjpeg \
    TZ=America/Sao_Paulo \
    DATA_DIR=/data \
    DATABASE_URL=sqlite:////data/retrieve.db \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY --chown=app:app app ./app

USER 10001:10001
EXPOSE 8080 444
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
