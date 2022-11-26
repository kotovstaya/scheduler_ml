import logging
import typing as tp

import numpy as np
import pandas as pd
from dateutil import parser
from pyspark.sql import functions as F
from scheduler_ml.preprocessing import readers, transformers, writers


class HistDataExtractorSpark:
    def __init__(
            self, 
            db_reader_params: tp.Dict[str, tp.Any], 
            raw_reader_params: tp.Dict[str, tp.Any], 
            writer_params: tp.Dict[str, tp.Any],
            transformer_params: tp.Dict[str, tp.Any], 
            columns: tp.List[str],
            dt_from: str, 
            dt_to: str, 
            filename_fmt: str,):

        self.dt_from = dt_from
        self.dt_to = dt_to
        self.filename_fmt = filename_fmt
        self.separated_file_for_each_shop = False
        self.columns = columns
        self.transformer_params = transformer_params

        self.raw_reader = readers.Minio2DataFrameReader(**raw_reader_params)
        self.db_reader = readers.Postgres2ParquetReader(**db_reader_params)
        self.writer =  writers.Parquet2MinioWriter(**writer_params)
        self.transformer = transformers.HistDataTransformerSpark(**self.transformer_params)

        self.cached_data_df = self._init_cached_data()

    def _init_cached_data(self) -> None:
        self.cached_data = {}

        external_system = (
            self.db_reader.read("integration_externalsystem")
            .filter(F.col("code") == self.transformer_params['system_code'])
            .drop('code')
            .withColumnRenamed("id", "external_system_id")
        )
        django_content_type = self.db_reader.read("django_content_type").withColumnRenamed("id", "object_type_id")
        generic_external_code = self.db_reader.read("integration_genericexternalcode")

        cached_data = (
            generic_external_code
            .join(external_system, on=["external_system_id"], how="inner")
            .join(django_content_type, on=["object_type_id"], how="inner")
            .select("code", "object_id")
        )

        cached_data_df = cached_data.toPandas()
        self.cached_data["generic_shop_ids"] = {
            code: object_id 
            for code, object_id in zip(
                cached_data_df["code"].values, cached_data_df["object_id"].values)}
        return cached_data

    def get_filename(self, dt, shop_code=None):
        kwargs = dict(
            data_type=self.transformer_params["data_type"],
            year=dt.year,
            month=dt.month,
            day=dt.day,
        )
        if self.separated_file_for_each_shop:
            kwargs['shop_code'] = shop_code
        return self.filename_fmt.format(**kwargs)

    def get_dates_range(self):
        dt_from = parser.parse(self.dt_from)
        dt_to = parser.parse(self.dt_to)
        return list(pd.date_range(dt_from, dt_to).date)

    def get_dt_and_filename_pairs(self):
        dt_and_filename_pairs = []
        for dt in self.get_dates_range():
            if self.separated_file_for_each_shop:
                for shop_code in self.cached_data.get('generic_shop_ids').keys():
                    dt_and_filename_pairs.append(
                        (dt, self.get_filename(dt, shop_code=shop_code)))
            else:
                dt_and_filename_pairs.append((dt, self.get_filename(dt)))
        return dt_and_filename_pairs

    def _columns_rename(self, df, columns):
        for ix, col in enumerate(columns):
            df = df.withColumnRenamed(f"_c{ix}", col)
        for col in df.columns:
            if col.startswith("_c"):
                df = df.drop(col)
        return df

    def extract(self):
        dt_and_filename_pairs = self.get_dt_and_filename_pairs()
        for dtt, filename in dt_and_filename_pairs:
            logging.error(f"{filename}, {dtt}")
            df = self.raw_reader.read(filename, source_type="csv")
            df = self._columns_rename(df, self.columns)

            shops_id, valid_df, errors_df = self.transformer.transform(
                df, dtt, filename, self.cached_data_df)
            sc = self.transformer_params['system_code']
            datp = self.transformer_params['data_type']

            logging.error(f"dtt ({dtt}). DataFrame size: {df.count()}")
            logging.error(f"dtt ({dtt}). Errors size: {errors_df.count()}")
            logging.error(f"dtt ({dtt}). Elements size: {valid_df.count()}")

            dtt_str = str(dtt).replace("-", "")
            self.writer.write(valid_df, f"{sc}_{datp}_{dtt_str}_transformed.parquet")
            self.writer.write(errors_df, f"{sc}_{datp}_{dtt_str}_errors.parquet")
            self.writer.write(shops_id, f"{sc}_{datp}_{dtt_str}_shop_ids.parquet")
