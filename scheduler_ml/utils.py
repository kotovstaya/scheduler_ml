import yaml
from pyspark.sql import SparkSession


def get_spark(
        spark_host: str, 
        spark_port: int, 
        s3_host: str, 
        s3_access_key: str, 
        s3_secret_key: str, 
        app_name="test_app"):

    spark = (
        SparkSession
        .builder
        .appName(app_name)
        .master(f"spark://{spark_host}:{spark_port}")
        .config("spark.jars.packages", 'org.postgresql:postgresql:42.2.10,org.apache.hadoop:hadoop-aws:3.2.2')
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.network.timeout", "10000s")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", 'true')
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .config("spark.hadoop.fs.s3.multiobjectdelete.enable", "true")
        .config("spark.hadoop.fs.s3a.endpoint", s3_host)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", 'true')
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        .getOrCreate()
    )

    return spark


def get_config(stage, config_name):
    root = "/srv/scheduler_ml_configs/configs"
    with open(f"{root}/{stage}/{config_name}.yaml", 'r') as f:
        config = yaml.safe_load(f)
        return config