#!/usr/bin/env python
# coding: utf-8

from time import time
import sys
import tempfile
from sqlalchemy import create_engine
import requests
import pandas as pd
import argparse
import os


def download_file(url: str, local_path: str) -> None:
    with requests.get(url, stream=True) as r:
        try:
            r.raise_for_status()
        except requests.HTTPError as err:
            print(f"Error downloading {url}: {err}", file=sys.stderr)
            sys.exit(1)

        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8_192):
                if chunk:
                    f.write(chunk)


def main(params):
    user       = params.user
    passwd     = params.passwd
    host       = params.host
    port       = int(params.port)
    db         = params.db
    table_name = params.table_name
    url        = params.url

    # Create a temporary directory; it will auto‐clean when we exit the with‐block.
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = os.path.join(tmpdir, "dl_parquet.parquet")
        csv_path     = os.path.join(tmpdir, "output_csv.csv")

        print(f"Downloading Parquet from {url} → {parquet_path}")
        download_file(url, parquet_path)

        # Build the SQLAlchemy engine once
        engine = create_engine(f"postgresql://{user}:{passwd}@{host}:{port}/{db}")

        # Read the entire Parquet into a DataFrame, then immediately dump it to CSV
        df = pd.read_parquet(parquet_path)
        print(f"File has {len(df)} rows")
        df.to_csv(csv_path, index=False)

        # Now open that CSV as a chunked iterator:
        reader = pd.read_csv(csv_path, iterator=True, chunksize=100_000)

        # 1) Grab the very first chunk to create/replace the table schema:
        try:
            first_chunk = next(reader)
        except StopIteration:
            # If CSV was empty, nothing to do.
            print("No rows found in the CSV. Exiting.")
            return
        
        # Convert datetime columns on the first chunk
        if "tpep_pickup_datetime" in first_chunk.columns:
            first_chunk["tpep_pickup_datetime"] = pd.to_datetime(
                first_chunk["tpep_pickup_datetime"]
            )
        if "tpep_dropoff_datetime" in first_chunk.columns:
            first_chunk["tpep_dropoff_datetime"] = pd.to_datetime(
                first_chunk["tpep_dropoff_datetime"]
            )

        # Create (or replace) the table schema using zero rows of first_chunk
        first_chunk.head(0).to_sql(
            name=table_name, con=engine, if_exists="replace", index=False
        )

        # Now insert that first chunk into Postgres:
        t0 = time()
        first_chunk.to_sql(
            name=table_name, con=engine, if_exists="append", index=False
        )
        t1 = time()
        print(f"Inserted first chunk of {len(first_chunk):,} rows in {t1 - t0:.3f} s")

        # 2) Loop over the remaining chunks automatically:
        for chunk in reader:
            t_start = time()
            
            if "tpep_pickup_datetime" in chunk.columns:
                chunk["tpep_pickup_datetime"] = pd.to_datetime(
                chunk["tpep_pickup_datetime"]
            )
            if "tpep_dropoff_datetime" in chunk.columns:
                chunk["tpep_dropoff_datetime"] = pd.to_datetime(
                chunk["tpep_dropoff_datetime"]
            )

            chunk.to_sql(name=table_name, con=engine, if_exists="append", index=False)
            t_end = time()
            print(f"Inserted chunk of {len(chunk):,} rows in {(t_end - t_start):.3f} s")

        print("All done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest CSV data (from Parquet) into Postgres, chunked."
    )

    parser.add_argument("--user",       help="username for Postgres", required=True)
    parser.add_argument("--passwd",     help="password for Postgres", required=True)
    parser.add_argument("--host",       help="host for Postgres", required=True)
    parser.add_argument("--port",       help="port for Postgres", required=True)
    parser.add_argument("--db",         help="database name for Postgres", required=True)
    parser.add_argument("--table_name", help="table name for Postgres", required=True)
    parser.add_argument("--url",        help="URL for Parquet file",  required=True)

    args = parser.parse_args()
    main(args)
