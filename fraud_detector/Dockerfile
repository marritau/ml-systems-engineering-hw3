FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY src ./src
COPY models ./models
COPY artifacts ./artifacts

CMD ["python", "./app/app.py"]

