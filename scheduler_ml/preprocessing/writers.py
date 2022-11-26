import pickle

import numpy as np
from minio import Minio
from pyspark.sql import SparkSession
from scheduler_ml.meta import schemas
from scheduler_ml.preprocessing import readers


class Parquet2MinioWriter:
    def __init__(
            self,
            bucket_name: str, 
            spark_host: str=None,
            spark_port: int=None,
            host: str=None,
            access_key: str=None,
            secret_key: str=None,
            spark=None):
            
        if spark is None:
            self.spark = (
                SparkSession
                .builder
                .appName('test')
                .master(f"spark://{spark_host}:{spark_port}")
                .config("spark.jars.packages", 'org.apache.hadoop:hadoop-aws:3.2.2')
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.fast.upload", 'true')
                .config("spark.sql.files.ignoreMissingFiles", "true")
                .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
                .config("spark.network.timeout", "10000s")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
                .config("spark.hadoop.fs.s3.multiobjectdelete.enable", "true")
                .config("spark.hadoop.fs.s3a.endpoint", host)
                .config("spark.hadoop.fs.s3a.access.key", access_key)
                .config("spark.hadoop.fs.s3a.secret.key", secret_key)
                .config("spark.hadoop.fs.s3a.path.style.access", 'true')
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .getOrCreate()
            )
        else:
            self.spark = spark
        self.bucket_name = bucket_name

    def write(self, df, filename):
        filename = f"{self.bucket_name}/{filename}"
        df.write.mode("overwrite").parquet(f"s3a://{filename}")


class Parquet2PostgresWriter:
    def __init__(
            self, 
            db_host: str,
            db_port: int,
            db_user: str,
            db_password: str,
            db_name: str,
            spark_host: str = None,
            spark_port: int = None,
            spark = None):

        self.spark_host = spark_host
        self.spark_port = spark_port
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name
        if spark is None:
            self.spark = (
                SparkSession
                .builder
                .appName(type(self).__name__)
                .master(f"spark://{self.spark_host}:{self.spark_port}")
                .config("spark.jars.packages", 'org.postgresql:postgresql:42.2.10')
                .config("spark.sql.files.ignoreMissingFiles", "true")
                .config("spark.network.timeout", "10000s")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .getOrCreate()
            )
        else:
            self.spark = spark

    def write(self, df, table_name):
        # reader = readers.Postgres2ParquetReader(self.db_host, self.db_port, self.db_user, self.db_password, self.db_name)
        # shop_ids = reader.read("base_shop").select("id").withColumnRenamed("id", "shop_id").distinct()
        print(df.columns)
        print(df.show(3))
        (
            df
            .write
            .format("jdbc")
            .option(f"url", f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}")
            .option("dbtable", table_name)
            .option("user", self.db_user)
            .option("password", self.db_password)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
