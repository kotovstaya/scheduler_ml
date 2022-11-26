import ftplib
import json
import pickle
import tempfile
import typing as tp

import numpy as np
import pandas as pd
from minio import Minio
from pyspark import SparkFiles
from pyspark.sql import SparkSession


class BaseReader:
    def __init__(self, base_path: str):
        self.base_path = base_path

    def read(self, filename):
        raise NotImplementedError


class Postgres2ParquetReader(BaseReader):
    def __init__(
        self, 
        db_host: str,
        db_port: int,
        db_user: str,
        db_password: str,
        db_name: str,
        host: str = None,
        access_key: str = None,
        secret_key: str = None,
        spark_host: tp.Optional[str] = None,
        spark_port: tp.Optional[int] = None,
        spark=None
    ):
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
                .config("spark.jars.packages", 'org.postgresql:postgresql:42.2.10,org.apache.hadoop:hadoop-aws:3.2.2')
                .config("spark.sql.files.ignoreMissingFiles", "true")
                .config("spark.network.timeout", "10000s")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.fast.upload", 'true')
                .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
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

    def read(self, table_name: str):
        return (
            self.spark
            .read
            .format("jdbc")
            .option(f"url", f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}")
            .option("dbtable", table_name)
            .option("user", self.db_user)
            .option("password", self.db_password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )


class MinioBaseReader:
    def __init__(
            self,
            host: str,
            access_key: str,
            secret_key: str,
            bucket_name: str,
    ):

        self.bucket_name = bucket_name

        self.client = Minio(
            host,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
    
    def read(self, filename: str):
        filename = f"{self.bucket_name}/{filename}"
        bucket, path = filename.split("/")
        if type(path) != str:
            path = "/".join(path)
        obj = self.client.get_object(bucket, path)
        return obj


class Minio2PandasReader(MinioBaseReader):
    def __init__(
            self,
            host: str,
            access_key: str,
            secret_key: str,
            bucket_name: str,
            csv_params: tp.Dict[str, tp.Any],
            **kwargs):

        super().__init__(host, access_key, secret_key, bucket_name)

        self.csv_params = csv_params
    
    def read(self, filename):
        obj = super().read(filename)
        return pd.read_csv(obj, **self.csv_params)


class Minio2ArrayReader(MinioBaseReader):
    def __init__(
            self,
            host: str,
            access_key: str,
            secret_key: str,
            bucket_name: str,
            **kwargs):
        super().__init__(host, access_key, secret_key, bucket_name)
    
    def read(self, filename):
        elems =  pickle.load(super().read(filename))
        new_elems = []
        for el in elems:
            el['dttm'] = el['dttm'].to_pydatetime()
            el["shop_id"] = int(el["shop_id"])
            new_elems.append(el)
        return new_elems[:10]


class Minio2DataFrameReader(BaseReader):
    def __init__(
            self,
            spark_host: str=None,
            spark_port: int=None,
            host: str=None,
            access_key: str=None,
            secret_key: str=None,
            bucket_name: str=None, 
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

    def _read_parquet(self, path):
        return self.spark.read.parquet(f"s3a://{path}")

    def _read_csv(self, path):
        return self.spark.read.csv(f"s3a://{path}", sep=";") # sep=";", 

    def read(self, filename, source_type="parquet"):
        filename = f"{self.bucket_name}/{filename}"
        if source_type == 'csv':
            return self._read_csv(filename)
        elif source_type == 'parquet':
            return self._read_parquet(filename)
