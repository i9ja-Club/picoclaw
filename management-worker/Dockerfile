FROM python:3.12-slim

WORKDIR /app

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin management

COPY management-worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY management-worker/management_worker.py .
COPY management-worker/minerva_ingestor.py .
RUN chown -R management:management /app

USER management

CMD ["python", "-u", "management_worker.py"]
