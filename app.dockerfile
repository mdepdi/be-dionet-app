FROM python:3.12

WORKDIR /app

COPY requirements.txt /app/
RUN pip install -r requirements.txt

COPY modules /app/modules/
COPY ["main.py","database.py","module.py","authentication.py","populate_database.py","celery_worker.py","/app/"]

# Create dionet_data directory structure
RUN mkdir -p /app/dionet_data/step

CMD ["uvicorn", "main:app", "--host", "0.0.0.0"]