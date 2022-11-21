import numpy as np
from minio import Minio
from pyspark.sql import SparkSession


class BaseWriter:
    def __init__(self, base_path: str):
        self.base_path = base_path

    def load(self, obj, filename):
        raise NotImplementedError


class Array2MinioWriter(BaseWriter):
    def __init__(
            self,
            host: str,
            access_key: str,
            secret_key: str,
            bucket_name: str,
            **kwargs):
        self.client = Minio(
            host,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        self.bucket_name = bucket_name
    
    def write(self, objects, filename):
        file_path = f"./{filename}"
        np.save(file_path, objects)
        self.client.fput_object(
            bucket_name=self.bucket_name,
            object_name=filename,
            file_path=filename)


class Parquet2OracleWriter(BaseWriter):
    def __init__(self):
        spark = None

    def write(self, table_name):
        pass


# select * from forecast_operationtype fo limit 5;

