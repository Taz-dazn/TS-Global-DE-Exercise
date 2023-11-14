import os
import sys
import tempfile
from argparse import ArgumentParser
from datetime import datetime
from typing import Tuple

import boto3
import yaml
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour
from yaml.loader import SafeLoader


def process_file(batch_date: str) -> None:
    """
    Download the file with the given batch_date, transform and upload it to S3

    :param batch_date: batch date of the file to process
    """
    set_env_variables()
    year, month, day = format_batch_date(batch_date)

    s3_client = boto3.client("s3")
    # source_s3_bucket = "global-interview-bucket"
    source_s3_bucket = "dazn-dev-as-run-logs-sync"
    source_s3_key = f"{year}/{month}/{day}/{batch_date}.csv"

    with tempfile.TemporaryDirectory() as tmp_directory:
        tmp_downloaded_file_name = f"{batch_date}.csv"

        try:
            s3_client.download_file(source_s3_bucket, source_s3_key, f"{tmp_directory}/{tmp_downloaded_file_name}.csv")
        except ClientError:
            raise FileNotFoundError(f"File Not in S3!!!, Bucket: {source_s3_bucket}, Key: {source_s3_key}")

        tmp_output_file_path = transform_file(tmp_downloaded_file_name, tmp_directory)

        s3_client.upload_file(tmp_output_file_path, source_s3_bucket, f"results/daily_agg_{year}{month}{day}_TS.csv")
        print('SUCCESS - Processed file uploaded to S3')


def set_env_variables() -> None:
    """
    Get the AWS Access Keys from config.yaml and set as environment variables
    """
    # Open the file and load the file
    with open('config.yaml') as f:
        credentials = yaml.load(f, Loader=SafeLoader)

    # access values from dictionary and set as env vars
    os.environ["AWS_ACCESS_KEY_ID"] = credentials['AWS_ACCESS_KEY_ID']
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials['AWS_SECRET_ACCESS_KEY']


def format_batch_date(batch_date_str: str) -> Tuple[str, str, str]:
    """
    Formats and returns the batch_date as Year, Month and Day

    :param batch_date_str: batch_date as a string
    :return: Year, Month, Day as a Tuple
    """
    try:
        datetime.strptime(batch_date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect Date Format!!!, It must be YYYY-MM-DD!")

    batch_date_split = batch_date_str.split('-')

    return batch_date_split[0], batch_date_split[1], batch_date_split[2]


def transform_file(tmp_file_name: str, tmp_directory: str) -> str:
    """
    De-Duplicate and Aggregate the source file using PySpark

    :param tmp_file_name: temporary file name of the downloaded file
    :param tmp_directory: temporary directory for storing the file
    :return: temporary path of the processed file
    """
    tmp_output_file_path = f"{tmp_directory}/output_{tmp_file_name}.csv"

    spark = SparkSession \
        .builder \
        .appName("GlobalDataExercise") \
        .getOrCreate()

    # De-Duplication
    df = spark.read.format("csv").options(header='true', inferschema='true', delimiter=',').load(f"{tmp_directory}/{tmp_file_name}.csv")
    df = df.dropDuplicates(['IMPRESSION_ID', 'IMPRESSION_DATETIME'])

    # Aggregation
    df = df.groupBy("CAMPAIGN_ID", hour("IMPRESSION_DATETIME").alias("IMPRESSION_HOUR")).count()
    df.toPandas().to_csv(tmp_output_file_path, index=False)

    return tmp_output_file_path


if __name__ == "__main__":
    parser = ArgumentParser(description="downloading files")
    parser.add_argument(
        "--batch_date",
        nargs="?",
        help="Date for which to fetch data [format: YYYY-MM-DD]",
    )
    args = parser.parse_args()
    sys.exit(
        process_file(
            batch_date=args.batch_date,
        ),
    )
