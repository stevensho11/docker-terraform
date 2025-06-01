FROM python:3.12

RUN pip install pandas sqlalchemy psycopg2 requests pyarrow

WORKDIR /app
COPY ingest_csv_or_parquet.py ingest_csv_or_parquet.py

ENTRYPOINT [ "python", "ingest_csv_or_parquet.py" ]