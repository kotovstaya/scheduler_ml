import datetime as dt

import pyspark.sql.types as T
from pyspark.sql import functions as F


class Receipt:
    def __init__(self):
        self.columns = [
            "code", "dttm", "dt", 
            "dttm_added", "info", "data_type", "version"]

    @classmethod
    def get_schema(cls):
        schema = T.StructType([
            T.StructField("shop_id",       T.IntegerType(),   True),
            T.StructField("code",          T.StringType(),    True),
            T.StructField("dttm",          T.TimestampType(), True),
            T.StructField("dt",            T.DateType(),      True),
            T.StructField("dttm_added",    T.TimestampType(), True),
            T.StructField("info",          T.StringType(),    True),
            T.StructField("data_type",     T.StringType(),    True),
            T.StructField("version",       T.IntegerType(),   True),
            T.StructField("dttm_modified", T.TimestampType(), True)
        ])
        return schema

    @classmethod
    def set_default(cls, df):
        df = (
            df
            .fillna(0, subset=["version"])
            .withColumn("dttm_added", F.lit(dt.datetime.now().date()))
            .withColumn("dttm_modified", F.lit(dt.datetime.now().date()))
        )
        return df

    @classmethod
    def get_description(cls):
        return {
            "shop_id": "",
            "code": "",
            "dttm": "Дата и время события",
            "dt": "",
            "dttm_added": "",
            "info": "",
            "data_type": "Тип данных",
            "dttm_modified": "",
            "version": "Версия объекта"
        }

SCHEMAS = {
    "forecast_receipt": Receipt
}