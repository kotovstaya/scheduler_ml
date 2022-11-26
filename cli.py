import logging
import os

import click
from scheduler_ml import utils
from scheduler_ml.preprocessing import extractors, importers, readers, writers


@click.group()
def messages():
  pass


@click.command()
@click.option('--host', type=str, )
@click.option('--access-key', type=str, )
@click.option('--secret-key', type=str, )
@click.option('--bucket-name', type=str, )
@click.option('--filename', type=str, )
def minio_array_read(
        host: str, 
        access_key: str, 
        secret_key: str, 
        bucket_name: str,
        filename: str):

    args = {
        "host": host,
        "access_key": access_key,
        "secret_key": secret_key,
        "bucket_name": bucket_name,
    }
    m2ar = readers.Minio2ArrayReader(**args)
    array = m2ar.read(filename)

    args = {
        "spark_host": "spark-master",
        "spark_port": 7077,
        "db_host": "postgres",
        "db_port": 5432,
        "db_user": "qos",
        "db_password": "qos",
        "db_name": "qos"
    }
    a2pw = writers.Array2PostgresWriter(**args)
    a2pw.write(array, "forecast_receipt")


@click.command()
def postgres_read():
    args = {
        "spark_host": "spark-master",
        "spark_port": 7077,
        "db_host": "postgres",
        "db_port": 5432,
        "db_user": "qos",
        "db_password": "qos",
        "db_name": "qos"
    }

    o2pr = readers.Postgres2ParquetReader(**args)
    print(o2pr.read('base_shop').show(3))


@click.command()
def ftp_read():
    args = {
        "spark_host": "spark-master",
        "spark_port": 7077,
    }

    f2pr = readers.FTP2ParquetReader(**args)
    print(f2pr.read('delivery_20221114.csv').show(3))


@click.command()
@click.option('--filename', type=str)
@click.option('--config-name', type=str)
def ftp_2_s3(filename: str, config_name: str):
    info = utils.get_config('raw', config_name)['info']
    os.system(f"cd {info['folder']} && mc cp {filename} {info['minio_name']}/{info['bucket_name']}/{filename}")


@click.command()
@click.option('--date', type=str)
@click.option('--config-name', type=str, )
def minio_2_postgres(date, config_name):

    config = utils.get_config('preprocessing', config_name)
    info = config['info']
    params = config['parameters']

    spark = utils.get_spark(
        spark_host=info['spark_host'], 
        spark_port=info['spark_port'],
        s3_host=info['s3_host'], 
        s3_access_key=info['s3_access_key'], 
        s3_secret_key=info['s3_secret_key'])

    bi = importers.BaseImporter(
        readers.Minio2DataFrameReader(spark=spark, bucket_name='data-science'),
        writers.Parquet2PostgresWriter(
            spark=spark,
            db_host=info['db_host'],
            db_port=info['db_port'],
            db_user=info['db_user'],
            db_password=info['db_password'],
            db_name=info['db_name'],
        )
    )
    bi.run(input_file=f"{config_name}_{date}_transformed.parquet", output_file="forecast_receipt")


@click.command()
@click.option('--config-name', type=str, )
def extractor(config_name):

    config = utils.get_config('preprocessing', config_name)
    info = config['info']
    params = config['parameters']

    logging.error(config)

    spark = utils.get_spark(
        spark_host=info['spark_host'], 
        spark_port=info['spark_port'],
        s3_host=info['s3_host'], 
        s3_access_key=info['s3_access_key'], 
        s3_secret_key=info['s3_secret_key'])

    hde = extractors.HistDataExtractorSpark(
        db_reader_params={
            "spark": spark,
            "db_host": info['db_host'],
            "db_port": info['db_port'],
            "db_user": info['db_user'],
            "db_password": info['db_password'],
            "db_name": info['db_name'],
        },
        raw_reader_params={"spark": spark, "bucket_name": info['bucket_name']},
        writer_params={"spark": spark, "bucket_name": info['bucket_name']},
        columns=params['columns'],
        transformer_params={
            "fix_date": params['fix_date'],
            "system_code": params["system_code"],
            "separated_file_for_each_shop": params["separated_file_for_each_shop"],
            "data_type": params["data_type"],
            "shop_num_column_name": params["shop_num_column_name"],
            "dt_or_dttm_column_name": params["dt_or_dttm_column_name"],
            "receipt_code_columns": params["receipt_code_columns"],
            "dt_or_dttm_format": params["dt_or_dttm_format"],
        },
        dt_from=params["dt_from"],
        dt_to=params["dt_to"],
        filename_fmt=params["filename_fmt"],
    )

    hde.extract()

messages.add_command(ftp_2_s3)
messages.add_command(extractor)
messages.add_command(postgres_read)
messages.add_command(ftp_read)
messages.add_command(minio_2_postgres)

messages.add_command(minio_array_read)

if __name__ == '__main__':
    messages()

# python cli.py delivery-extractor --config-name=pobeda_delivery
