FROM python:3.12

RUN pip install pandas sqlalchemy psycopg2 requests pyarrow

WORKDIR /app
COPY ingest_parquet.py ingest_parquet.py

ENTRYPOINT [ "python", "ingest_parquet.py" ]