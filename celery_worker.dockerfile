FROM python:3.12

WORKDIR /app

COPY requirements.txt /app/
RUN pip install -r requirements.txt
# Copy the rest of the application code
COPY modules /app/modules/
COPY ["database.py","module.py","authentication.py","populate_database.py","celery_worker.py","/app/"]

# Create dionet_data directory structure
RUN mkdir -p /app/dionet_data/step

# Define the default command
CMD ["celery", "-A", "celery_worker.celery", "worker", "--concurrency=4","--loglevel=info"]