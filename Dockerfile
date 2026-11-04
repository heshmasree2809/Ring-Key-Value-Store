FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

# Standard library only: nothing to pip install.
COPY ringkv ./ringkv

RUN useradd --system --no-create-home ringkv \
    && mkdir /data && chown ringkv /data
USER ringkv
VOLUME /data

EXPOSE 8000
HEALTHCHECK --interval=5s --timeout=2s --retries=5 \
  CMD python -c "import urllib.request as u; u.urlopen('http://127.0.0.1:8000/health', timeout=1)"

ENTRYPOINT ["python", "-m", "ringkv.node"]
CMD ["--data-dir", "/data"]
