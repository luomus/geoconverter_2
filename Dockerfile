FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/main.py .
COPY src/process_data_dask.py .
COPY src/lookup_table.csv .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]