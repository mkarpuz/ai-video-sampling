FROM python:3.13-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY database.py pipeline.py settings.json ./

ENTRYPOINT ["python", "pipeline.py"]
CMD ["--help"]
