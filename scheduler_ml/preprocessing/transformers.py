import datetime as dt
import logging
import typing as tp

import numpy as np
import pandas as pd


class BaseHistDataTransformer:
    def __init__(self):
        pass

    def transform(self, **kwargs):
        raise NotImplemented


class HistDataTransformer(BaseHistDataTransformer):
    def __init__(self,
                 system_code,
                 data_type,
                 separated_file_for_each_shop,
                 shop_num_column_name,
                 dt_or_dttm_column_name,
                 dt_or_dttm_format,
                 columns: tp.Optional[tp.List[str]] = None,
                 receipt_code_columns: tp.Optional[tp.List[str]] = None,
                 **kwargs):
        self.system_code = system_code
        self.data_type = data_type
        self.separated_file_for_each_shop = separated_file_for_each_shop
        self.shop_num_column_name = shop_num_column_name
        self.dt_or_dttm_column_name = dt_or_dttm_column_name
        self.dt_or_dttm_format = dt_or_dttm_format
        self.columns = columns
        self.receipt_code_columns = receipt_code_columns
        self._init_cached_data()
        super().__init__()

    def _init_cached_data(self):
        self.cached_data = {}

    def get_shop_id_by_shop_num(self, code):
        shop_id = np.random.randint(0, 100)
        if np.random.random() > 0.5:
            shop_id = None
        # shop_id = self.cached_data['generic_shop_ids'].get(code)
        return shop_id

    def _get_dttm(self, row, dtt):
        """
        we must check a type and correct form of our date.
        If all is good and data_type is equal to 'delivery' then we
        1. check is this date clear:
            the difference between dates is less than 7 days
            (because it's very weird to get the date like that in file
            with the current date)
        2. change current date to another: get a date from filename
        """
        try:
            dttm = dt.datetime.strptime(
                row[self.dt_or_dttm_column_name],
                self.dt_or_dttm_format,
            )
            if self.data_type == "delivery":
                # https://mindandmachine.myjetbrains.com/youtrack/issue/RND-572
                # use a date from a filename pattern
                if abs((dtt - dttm.date()).days) > 7:
                    raise TypeError("Range between dt and dttm is greater"
                                    f" than 1 week")
                else:
                    dttm = dt.datetime(
                        dtt.year,
                        dtt.month,
                        dtt.day,
                        dttm.hour,
                        dttm.minute,
                        dttm.second
                    )
            return dttm, None
        except TypeError as e:
            return None, e

    def _get_receipt_code(self, obj):
        if self.receipt_code_columns:
            receipt_code = "".join(obj[self.receipt_code_columns].values)
        else:
            receipt_code = hash(tuple(obj))
        return receipt_code

    def _create_object(
            self,
            row: pd.Series,
            dtt: dt.date,
            all_columns: tp.List[str],
            unused_columns: tp.List[str]) -> tp.Dict[str, tp.Any]:
        return {
            "code": row['receipt_code'],
            "dttm": row["updated_dttm"],
            "shop_id": row["shop_id"],
            "dt": dtt,
            "data_type": self.data_type,
            "info": row[list(set(all_columns) - set(unused_columns))].to_json(),
        }

    def _chunk_transform(
            self,
            df: pd.DataFrame,
            shops_id: set,
            objects: list,
            load_errors: set,
            dtt: dt.date,
            metainfo: tp.Dict[str, tp.Any]):
        # Hash generation is different in each python
        # launch! Can only compare in receipts imported
        # from the same task

        df["receipt_code"] = df.apply(self._get_receipt_code, axis=1)
        df["index"] = df.index.values
        df['shop_id'] = (df[self.shop_num_column_name]
                            .apply(self.get_shop_id_by_shop_num))
        df["updated_dttm"] = (
            df.apply(lambda obj: self._get_dttm(obj, dtt)[0], axis=1))
        df["dttm_error"] = (
            df.apply(lambda obj: self._get_dttm(obj, dtt)[1], axis=1))

        # update containers with main info

        shops_id |= set(df.loc[~df['shop_id'].isna(), "shop_id"])

        df_for_objects = df[(~df["shop_id"].isna()) & (~df["updated_dttm"].isna())]
        if df_for_objects.shape[0]:
            objects += (df_for_objects.apply(
                lambda row: self._create_object(row, dtt, df.columns, metainfo['unused_cols']),
                axis=1).values.tolist())

        # update sets with errors
        df_shop_load_error = df.loc[df["shop_id"].isna(), self.shop_num_column_name]
        if df_shop_load_error.shape[0]:
            load_errors |= set(
                df_shop_load_error.apply(
                    lambda x: f"can't map shop_id for shop_num='{x}'"))

        df_dttm_error = df.loc[df["updated_dttm"].isna(), ["dttm_error", "index"]]
        if df_dttm_error.shape[0]:
            load_errors |= set(
                df_dttm_error
                .apply(
                    lambda row: f"{row['dttm_error'].__class__.__name__}: "
                                f"{str(row['dttm_error'])}: {metainfo['filename']}: "
                                f"row: {row['index']}",
                    axis=1))

        artefacts = (shops_id, objects, load_errors)

        return artefacts

    def transform(
            self,
            reader_generator: tp.Iterator,
            dtt: dt.date,
            filename: str) -> tp.Set[str]:

        load_errors = set()
        shops_id = set()
        objects = []

        unused_cols = ["index", "shop_id", "updated_dttm", "dttm_error"]
        metainfo = {"unused_cols": unused_cols, "filename": filename}

        try:
            for ix, df in enumerate(reader_generator):
                logging.error(df.head())
                shops_id, objects, load_errors = self._chunk_transform(
                    df, shops_id, objects, load_errors, dtt, metainfo)

        except (FileNotFoundError, PermissionError) as e:
            load_errors.add(f'{e.__class__.__name__}: {str(e)}: {filename}')
        return objects, load_errors
