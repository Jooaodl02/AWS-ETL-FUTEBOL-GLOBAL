from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, to_date

# Parâmetros explícitos (substitua por variáveis de ambiente em produção)
JOB_NAME = "futebol_goalsscorers_job"
SOURCE_S3_PATH = "s3://bucket-futebol/raw/goalsscorers/goalsscorers.csv"
TARGET_S3_PATH = "s3://bucket-futebol/processed/goalsscorers/"
INPUT_DATE_FORMAT = "yyyy-MM-dd"
OUTPUT_DATE_FORMAT = "yyyyMMdd"
WRITE_MODE = "overwrite"