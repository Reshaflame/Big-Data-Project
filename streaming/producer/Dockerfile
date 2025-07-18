FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY produce_csv.py .
CMD ["python", "produce_csv.py"]
