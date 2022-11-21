import logging
import os

import click
from scheduler_ml.preprocessing import extractors, readers


@click.group()
def messages():
  pass

@click.command()
def postgres_read():
    o2pr = readers.Oracle2ParquetReader()
    print(o2pr.read('base_shop').show(3))


@click.command()
@click.option('--folder', type=str)
@click.option('--filename', type=str, )
@click.option('--bucket', type=str, )
def ftp_2_s3(folder: str, filename: str, bucket: str):
    os.system(f"cd {folder} && mc cp {filename} myminio/{bucket}/{filename}")


@click.command()
@click.option('--host', type=str, )
@click.option('--access-key', type=str, )
@click.option('--secret-key', type=str, )
@click.option('--system-code', type=str, )
@click.option('--secret-key', type=str, )
@click.option('--data-type', type=str, )
@click.option('--bucket-name', type=str, )
def delivery_extractor(host, access_key, secret_key, system_code, data_type, bucket_name):
    logging.error("DELIVERY EXTRACTOR")

    hde = extractors.HistDataExtractor(
        reader_params={
            "host": host,
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket_name": bucket_name,
            "base_path": "/Upload",
            "csv_params": {
                "sep": ";", 
                "dtype": str, 
                "chunksize": 10000, 
                "index_col": False,
            }
        },
        writer_params={
            "host": host,
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket_name": bucket_name,
        },
        transformer_params={
            "system_code": system_code,
            "separated_file_for_each_shop": False,
            "data_type": data_type,
            "columns": [
                'Какой-то guid',
                'Номер магазина id',
                'Дата и время',
                'Тип поставки',
                'Id SKU',
                'Количество товара',
            ],
            "shop_num_column_name": 'Номер магазина id',
            "dt_or_dttm_column_name": 'Дата и время',
            "receipt_code_columns":[
                'Какой-то guid',
                'Id SKU',
            ],
            "dt_or_dttm_format": '%d.%m.%Y %H:%M:%S',
        },
        dt_from='2022-11-14',
        dt_to='2022-11-14',
        filename_fmt='{data_type}_{year:04d}{month:02d}{day:02d}.csv',
        filename_to_save="delivery_20221114_transformed.npy"
    )

    hde.extract()


messages.add_command(ftp_2_s3)
messages.add_command(delivery_extractor)
messages.add_command(postgres_read)


if __name__ == '__main__':
    messages()