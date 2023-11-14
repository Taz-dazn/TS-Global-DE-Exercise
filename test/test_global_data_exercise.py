import os
from mock import patch
import pytest
import boto3
from moto import mock_s3
from pyspark.sql import SparkSession

from global_data_exercise import process_file, format_batch_date, set_env_variables, transform_file

BATCH_DATE = '2023-02-28'
BATCH_YEAR = '2023'
BATCH_MONTH = '02'
BATCH_DAY = '28'
AWS_ACCESS_KEY_ID = 'MAJOR_KEY'
AWS_SECRET_ACCESS_KEY = 'Shh'
AWS_CREDENTIALS = {
    'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
    'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY
}
S3_BUCKET = 'dazn-dev-as-run-logs-sync'
OUTPUT_KEY = f"results/daily_agg_{BATCH_YEAR}{BATCH_MONTH}{BATCH_DAY}_TS.csv"


@mock_s3
@patch("global_data_exercise.yaml.load")
def test_end_to_end_success(mock_yaml_load):
    """Full end to end test to show that the loading, transformation and upload works"""
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=S3_BUCKET)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(S3_BUCKET, f"{BATCH_YEAR}/{BATCH_MONTH}/{BATCH_DAY}/{BATCH_DATE}.csv").put(Body=open('test/resources/test_data.txt', 'rb'))

    mock_yaml_load.return_value = AWS_CREDENTIALS
    process_file(BATCH_DATE)

    s3_bucket = s3_resource.Bucket(S3_BUCKET)
    s3_keys = [objSum.key for objSum in s3_bucket.objects.filter(Prefix='results/')]
    assert OUTPUT_KEY in s3_keys

    with open('test/resources/result_data.txt', 'r') as file:
        output = ''.join(file.readlines())

    obj = s3_resource.Object(S3_BUCKET, OUTPUT_KEY)
    assert obj.get()['Body'].read().decode('utf-8') == output


@patch('global_data_exercise.transform_file')
@patch('global_data_exercise.boto3.client')
@patch("global_data_exercise.set_env_variables")
def test_process_file(mock_set_env_variables, mock_s3_client, mock_transform_file):
    """To confirm the correct S3 calls are made within this method"""
    process_file(BATCH_DATE)
    assert mock_set_env_variables.called
    assert mock_s3_client.return_value.download_file.call_count == 1
    assert mock_transform_file.called
    assert mock_s3_client.return_value.upload_file.call_count == 1


@mock_s3
@patch("global_data_exercise.set_env_variables")
def test_process_file_error(mock_set_env_variables):
    """ If the file is not in S3, this should raise an Error"""
    batch_date = '2023-11-01'
    source_s3_key = f"2023/11/01/{batch_date}.csv"
    error_message = f"File Not in S3!!!, Bucket: {S3_BUCKET}, Key: {source_s3_key}"
    with pytest.raises(FileNotFoundError, match=error_message):
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=S3_BUCKET)
        process_file(batch_date)


@patch("builtins.open")
@patch("global_data_exercise.yaml.load")
def test_set_env_variables(mock_yaml_load, mock_open):
    """ To ensure the credentials are set from the yaml file"""
    mock_yaml_load.return_value = AWS_CREDENTIALS
    set_env_variables()
    mock_open.assert_called_with('config.yaml')
    assert os.getenv('AWS_ACCESS_KEY_ID') == AWS_ACCESS_KEY_ID
    assert os.getenv('AWS_SECRET_ACCESS_KEY') == AWS_SECRET_ACCESS_KEY


def test_format_batch_date():
    """Confirming the splitting of the input batch_date"""
    year,month,day = format_batch_date(BATCH_DATE)

    assert year == BATCH_YEAR
    assert month == BATCH_MONTH
    assert day == BATCH_DAY


def test_format_batch_date_error():
    """If the date is an incorrect format, this should raise an Error"""
    with pytest.raises(ValueError, match="Incorrect Date Format!!!, It must be YYYY-MM-DD!"):
        format_batch_date('01-01-2023')


@pytest.fixture(scope="session")
def spark_session(request):
    """Allow the creation of a spark session for Testing"""
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("this-is-an-app") \
        .getOrCreate()

    request.addfinalizer(lambda: spark_session.sparkContext.stop())

    return spark_session


@patch("pyspark.sql.SparkSession.read")
@patch("pyspark.sql.DataFrame")
@pytest.mark.usefixtures("spark_session")
def test_transform_file(mock_df, mock_spark_read):
    """Confirm the transformation is working with the right call and export file path"""
    mock_spark_read.return_value = mock_df
    tmp_directory = 'tmp_dir/'
    tmp_file_name = 'test_file_name'
    output_file_path = f"{tmp_directory}/output_{tmp_file_name}.csv"
    return_value = transform_file(tmp_file_name, tmp_directory)

    assert return_value == output_file_path
