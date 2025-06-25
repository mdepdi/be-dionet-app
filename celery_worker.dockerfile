FROM python:3.12

WORKDIR /app

COPY requirements.txt /app/
RUN pip install -r requirements.txt
# Copy the rest of the application code
COPY modules /app/modules/
COPY ["database.py","module.py","authentication.py","populate_database.py","celery_worker.py","/app/"]

# Create dionet_data directory structure
RUN mkdir -p /app/dionet_data/step

# Define the command with QUEUE ISOLATION - only listen to dionet_queue
CMD ["celery", "-A", "celery_worker.celery", "worker", "--pool=solo", "--concurrency=1", "--loglevel=info", "--without-heartbeat", "--without-gossip", "--without-mingle", "--purge", "--queues=dionet_queue"]