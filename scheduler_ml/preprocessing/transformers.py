import copy
import datetime as dt
import logging
import typing as tp

import numpy as np
import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


class HistDataTransformerSpark:
    def __init__(self,
                 system_code,
                 data_type,
                 separated_file_for_each_shop,
                 shop_num_column_name,
                 dt_or_dttm_column_name,
                 dt_or_dttm_format,
                 fix_date: bool = False,
                 columns: tp.Optional[tp.List[str]] = None,
                 receipt_code_columns: tp.Optional[tp.List[str]] = None,
                 **kwargs):
        self.system_code = system_code
        self.data_type = data_type
        self.fix_date = fix_date
        self.separated_file_for_each_shop = separated_file_for_each_shop
        self.shop_num_column_name = shop_num_column_name
        self.dt_or_dttm_column_name = dt_or_dttm_column_name
        self.dt_or_dttm_format = dt_or_dttm_format
        self.columns = columns
        self.receipt_code_columns = receipt_code_columns

    def transform(
            self, df, dtt: dt.date, filename: str, 
            cached_data: tp.Dict[str, int]) -> tp.Set[str]:

        self.cached_data = cached_data

        unused_cols = ["index", "shop_id", "updated_dttm", "dttm_error"]

        def _get_dttm(dtt, dt_or_dttm_format, fix_date):
            def wrapper(x):
                try:
                    dttm = dt.datetime.strptime(x, dt_or_dttm_format,)
                    if fix_date:
                        # https://mindandmachine.myjetbrains.com/youtrack/issue/RND-572
                        # use a date from a filename pattern
                        if abs((dtt - dttm.date()).days) > 7:
                            raise TypeError(f"Range between {dtt} and {dttm} is greater"
                                            f" than 1 week")
                        else:
                            dttm = dt.datetime(
                                dtt.year, dtt.month, dtt.day, min(dttm.hour, 20),
                                dttm.minute, dttm.second)
                    return dttm, None, None
                except Exception as e:
                    return None, e.__class__.__name__, str(e)
            return wrapper

        dt_or_dttm_format = copy.deepcopy(self.dt_or_dttm_format)
        fix_date = copy.deepcopy(self.fix_date)

        if self.receipt_code_columns:
            receipt_code_getter = F.concat_ws("", *self.receipt_code_columns)
        else:
            receipt_code_getter = F.hash(*df.columns)

        udf_get_dttm = F.udf(
            lambda x: _get_dttm(dtt, dt_or_dttm_format, fix_date)(x)[0], 
            returnType=T.TimestampType())
        udf_get_dttm_error_class = F.udf(
            lambda x: _get_dttm(dtt, dt_or_dttm_format, fix_date)(x)[1], 
            returnType=T.StringType())
        udf_get_dttm_error_value = F.udf(
            lambda x: _get_dttm(dtt, dt_or_dttm_format, fix_date)(x)[2], 
            returnType=T.StringType())
        udf_shop_num_error = F.udf(
            lambda x: f"can't map shop_id for shop_num='{x}'", 
            returnType=T.StringType())
        udf_dttm_error = F.udf(
            lambda dttm_error_class, dttm_error_value, index: f"{dttm_error_class}: "
                        f"{dttm_error_value}: {filename}: "
                        f"row: {index}", returnType=T.StringType())

        df = (
            df
            .withColumn("receipt_code", receipt_code_getter)
            .withColumn("index", F.row_number().over(Window.orderBy(df.columns[0])))
            .withColumn("code", F.col(self.shop_num_column_name))
            .join(
                self.cached_data.withColumnRenamed("object_id", "shop_id"),  # get shop_id
                on=["code"], how="left"
            )
            .withColumn("updated_dttm", udf_get_dttm(self.dt_or_dttm_column_name))
            .withColumn(
                "dttm_error_class", 
                udf_get_dttm_error_class(self.dt_or_dttm_column_name))
            .withColumn(
                "dttm_error_value", 
                udf_get_dttm_error_value(self.dt_or_dttm_column_name))
            .drop("code")
        )

        shops_id = df.filter(~F.col("shop_id").isNull()).select("shop_id")
        valid_df = (
            df
            .filter((~F.col("shop_id").isNull()) & (~F.col("updated_dttm").isNull()))
            .withColumn(
                "info", 
                F.to_json(F.struct(*list(set(df.columns) - set(unused_cols)))))
            .withColumnRenamed("receipt_code", "code")
            .withColumn("dttm", F.col("updated_dttm"))
            .withColumn("dt", F.lit(dtt))
            .withColumn("data_type", F.lit(self.data_type))
            .withColumn("dttm_added", F.lit(dt.datetime.now()))
            .withColumn("dttm_modified", F.lit(dt.datetime.now()))
            .withColumn("version", F.lit(0))
            .select("code", "dttm", "dttm_added", "dttm_modified", "version",  "shop_id", "dt", "data_type", "info")
        )

        errors_df = (
            df
            .filter(F.col("shop_id").isNull()).select(self.shop_num_column_name)
            .withColumn("Error", udf_shop_num_error(self.shop_num_column_name))
            .select("Error")
            .unionByName(
                df
                .filter(F.col("updated_dttm").isNull())
                .withColumn(
                    "Error", 
                    udf_dttm_error("dttm_error_class", "dttm_error_value", "index"))
                .select("Error")
            )
        )

        return shops_id, valid_df, errors_df

