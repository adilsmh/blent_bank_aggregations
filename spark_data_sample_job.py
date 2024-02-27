# sample_data_spark_job

# Libraries
from google.cloud import storage
import findspark
import logging
import pandas as pd
from io import StringIO
from datetime import datetime

# from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Initiating a local Spark session named SparkSampleJob
# findspark.init()
# spark = SparkSession.builder.master("local[*]").appName("SparkSampleJob").getOrCreate()


def spark_sample_pipeline(
    main_data_path: str,
    fraud_data_path: str,
    destination_path: str,
    start_date: str,
    end_date: str,
):
    """
    Data processing pipeline which queries the data from gs bucket to join laundering and transactions csv files
    to return a final csv containing aggregated analytics client data.

    :param main_data_path: The URI where raw input data is located, such as 'gs://data/raw/laundering-small.csv'.
    :param fraud_data_path: The URI where additional raw input data is located, such as 'gs://data/raw/transactions-small.csv'.
    :param destination_path: The URI where output is written, such as 'gs://final-data.csv'.
    :param start_date: Starting date treshold where to start aggregate data.
    :param end_date: Ending date treshold where to end aggregate data.
    """

    # Convert string dates into appropriate ISO date format
    start_date = datetime.strptime(start_date, "%d/%m/%Y").isoformat()
    end_date = datetime.strptime(end_date, "%d/%m/%Y").isoformat()

    # Asserting ending threshold date is not past the start date
    assert end_date > start_date, "Ending date should be greater than starting date"

    # -------------------------------------------------------------------------------------------------------------------------------------------- #

    # s3_client = boto3.client(
    #     "s3",
    #     aws_access_key_id="AKIA5FTZFS2SXYQJQXNH",
    #     aws_secret_access_key="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",
    # )

    # bucket = "myawsblentbucket"
    # prefix = "data/raw"

    # csv_data_l = []
    # txt_data_l = []

    # for object_summary in s3_client.list_objects(Bucket=bucket, Prefix=prefix)[
    #     "Contents"
    # ]:
    #     key = object_summary["Key"]
    #     LOGGER.info(f"key: {key}")
    #     if key.endswith(".txt"):
    #         content = (
    #             s3_client.get_object(Bucket=bucket, Key=key)["Body"]
    #             .read()
    #             .decode("utf-8")
    #         )
    #         LOGGER.info(f"content: {content}")
    #         txt_data_l.append(content)
    #     elif key.endswith(".csv"):
    #         content = (
    #             s3_client.get_object(Bucket=bucket, Key=key)["Body"]
    #             .read()
    #             .decode("utf-8")
    #         )
    #         LOGGER.info(f"content: {content}")
    #         csv_data_l.append(content)

    # lines = txt_data_l[0].splitlines()

    client = storage.Client()
    bucket = client.get_bucket("bank-bucket1")

    fraud_transactions_f = bucket.get_blob(f"gs://bank-bucket1/{main_data_path}")
    print(fraud_transactions_f.download_as_string()[:10])

    fraud_transactions_l = []
    for line in lines:
        if line.strip():
            stripped_line = line.strip()
            if stripped_line.startswith("BEGIN") or stripped_line.startswith("END"):
                continue  # skip the lines that start with "BEGIN" or "END"
            fraud_transactions_l.append(line.split(","))

    # Define Spark dataframe schema
    dataSchema = StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField("from_bank", StringType(), True),
            StructField("account2", StringType(), True),
            StructField("to_bank", StringType(), True),
            StructField("account4", StringType(), True),
            StructField("amount_received", StringType(), True),
            StructField("receiving_currency", StringType(), True),
            StructField("amount_paid", StringType(), True),
            StructField("payment_currency", StringType(), True),
            StructField("payment_format", StringType(), True),
            StructField("attempts_count", StringType(), True),
        ]
    )

    # Converting rows list into a spark rdd, then convering it in a spark dataframe defining the appropriate schema
    fraud_transactions_rdd = spark.sparkContext.parallelize(fraud_transactions_l)
    fraud_transactions_df = spark.createDataFrame(fraud_transactions_rdd, dataSchema)

    # Defining columns appropriate data type
    fraud_transactions_df = (
        fraud_transactions_df.withColumn("timestamp", F.col("timestamp").cast("string"))
        .withColumn("from_bank", F.col("from_bank").cast("string"))
        .withColumn("account2", F.col("account2").cast("string"))
        .withColumn("to_bank", F.col("to_bank").cast("string"))
        .withColumn("account4", F.col("account4").cast("string"))
        .withColumn("amount_received", F.col("amount_received").cast("float"))
        .withColumn("receiving_currency", F.col("receiving_currency").cast("string"))
        .withColumn("amount_paid", F.col("amount_paid").cast("float"))
        .withColumn("payment_currency", F.col("payment_currency").cast("string"))
        .withColumn("payment_format", F.col("payment_format").cast("string"))
        .withColumn("attempts_count", F.col("attempts_count").cast("int"))
    )
    fraud_transactions_df.show(10)

    # df = spark.read.csv("s3://myawsblentbucket/data/raw/transactions-small.csv")
    # df.show(10)

    # -------------------------------------------------------------------------------------------------------------------------------------------- #

    # Export final aggregated data into gs bucket dedicated folder
    # if not destination_path:
    #     s3.meta.client.upload_file('aggregated_bank_transactions_data.csv', bucket,'data/final'+'aggregated_bank_transactions_data.csv')
    # else:
    #     df.to_csv(destination_path/aggregated_bank_transactions_data.csv)


spark_sample_pipeline(
    "",
    "data/raw/laundering-small.txt",
    "",
    start_date="20/05/2023",
    end_date="20/10/2023",
)
