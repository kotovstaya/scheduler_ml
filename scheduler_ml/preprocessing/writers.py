import pickle

import numpy as np
from minio import Minio
from pyspark.sql import SparkSession
from scheduler_ml.meta import schemas
from scheduler_ml.preprocessing import readers


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

        with open(file_path, 'wb') as fp:
            pickle.dump(objects, fp)

        self.client.fput_object(
            bucket_name=self.bucket_name,
            object_name=filename,
            file_path=filename)


class Parquet2PostgresWriter(BaseWriter):
    def __init__(
        self, 
        spark_host: str,
        spark_port: int,
        db_host: str,
        db_port: int,
        db_user: str,
        db_password: str,
        db_name: str
    ):
        self.spark_host = spark_host
        self.spark_port = spark_port
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name

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

    def write(self, df, table_name):
        reader = readers.Postgres2ParquetReader(self.db_host, self.db_port, self.db_user, self.db_password, self.db_name)
        shop_ids = reader.read("base_shop").select("id").withColumnRenamed("id", "shop_id").distinct()

        (
            df
            .join(shop_ids, on=["shop_id"], how="inner")
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


class Array2PostgresWriter(Parquet2PostgresWriter):
    def __init__(
        self,
        spark_host: str,
        spark_port: int,
        db_host: str,
        db_port: int,
        db_user: str,
        db_password: str,
        db_name: str,
    ):
        super().__init__(spark_host, spark_port, db_host, db_port, db_user, db_password, db_name)
    
    def write(self, array, table_name: str):
        schema_cls = schemas.SCHEMAS[table_name]
        schema = schema_cls.get_schema()
        df = self.spark.createDataFrame(array, schema)
        df = schema_cls.set_default(df)
        print(df.show())
        super().write(df, table_name)
        



# select * from forecast_operationtype fo limit 5;

