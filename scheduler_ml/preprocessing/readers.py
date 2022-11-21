import ftplib
import tempfile
import typing as tp

import pandas as pd
from minio import Minio
from pyspark.sql import SparkSession


class BaseReader:
    def __init__(self, base_path: str):
        self.base_path = base_path

    def read(self, filename):
        raise NotImplementedError


class Minio2ParquetReader(BaseReader):
    def __init__(
            self,
            host: str,
            access_key: str,
            secret_key: str,
            bucket_name: str):
        self.spark = (
            SparkSession
            .builder
            .appName("test")
            .master("spark://spark-master:7077")
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.2')
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

    def read(self, path):
        return self.spark.read.parquet(f"s3a://{path}")


class Oracle2ParquetReader(BaseReader):
    def __init__(self):
        self.spark = (
            SparkSession
            .builder
            .appName("test")
            .master("spark://spark-master:7077")
            .config("spark.jars.packages", 'org.postgresql:postgresql:42.2.10')
            .config("spark.sql.files.ignoreMissingFiles", "true")
            .config("spark.network.timeout", "10000s")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate()
        )

    def read(self, table_name):
        return (
            self.spark
            .read
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/qos")
            .option("dbtable", table_name)
            .option("user", "qos")
            .option("password", "qos")
            .option("driver", "org.postgresql.Driver")
            .load()
        )


class FTP2PandasCSVReader(BaseReader):
    def __init__(
            self,
            host: str,
            username: str,
            password: str,
            csv_params: tp.Dict[str, tp.Any],
            **kwargs):
        self.host = host
        self.username = username
        self.password = password
        self.csv_params = csv_params
        super().__init__(**kwargs)
        self.ftp = self._init_ftp()

    def _init_ftp(self):  # TODO: нужно ли закрывать коннекшн?
        ftp = ftplib.FTP(host=self.host, user=self.username, passwd=self.password)
        ftp.cwd(self.base_path)
        return ftp

    def read(self, filename: str):
        tmp_f = tempfile.NamedTemporaryFile(mode='wb+')
        try:
            self.ftp.retrbinary(f'RETR {filename}', tmp_f.write)
        except (*ftplib.all_errors,):
            self.ftp = self._init_ftp()
            self.ftp.retrbinary(f'RETR {filename}', tmp_f.write)
        tmp_f.seek(0)
        with tmp_f as f:
            dfs = pd.read_csv(f, **self.csv_params)
            for df in dfs:
                yield df


class Minio2PandasCSVReader(BaseReader):
    def __init__(
            self,
            host: str,
            access_key: str,
            secret_key: str,
            bucket_name: str,
            csv_params: tp.Dict[str, tp.Any],
            **kwargs):
        self.client = Minio(
            host,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        self.csv_params = csv_params
        self.bucket_name = bucket_name
    
    def read(self, filename):
        filename = f"{self.bucket_name}/{filename}"
        bucket, path = filename.split("/")
        if type(path) != str:
            path = "/".join(path)
        obj = self.client.get_object(bucket, path)
        print(obj)
        for df in pd.read_csv(obj, **self.csv_params):
            yield df
