from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, to_date

# Parâmetros explícitos (substitua por variáveis de ambiente em produção)
JOB_NAME = "futebol_former_names_job"
SOURCE_S3_PATH = "s3://bucket-futebol/raw/former_names/former_names.csv"
TARGET_S3_PATH = "s3://bucket-futebol/processed/former_names/"
INPUT_DATE_FORMAT = "yyyy-MM-dd"
OUTPUT_DATE_FORMAT = "yyyyMMdd"
WRITE_MODE = "overwrite"


def read_source(spark, source_path: str) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",")
        .csv(source_path)
    )


def transform_former_names(df: DataFrame, input_format: str, output_format: str) -> DataFrame:
    return (
        df
        .withColumn("star_date", to_date(col("star_date"), input_format))
        .withColumn("end_date", to_date(col("end_date"), input_format))
        .withColumn("star_date", date_format(col("star_date"), output_format))
        .withColumn("end_date", date_format(col("end_date"), output_format))
    )


def write_target(df: DataFrame, target_path: str, mode: str = "overwrite") -> None:
    df.write.mode(mode).parquet(target_path)


def run_job(
    source_path: str = SOURCE_S3_PATH,
    target_path: str = TARGET_S3_PATH,
    input_format: str = INPUT_DATE_FORMAT,
    output_format: str = OUTPUT_DATE_FORMAT,
    write_mode: str = WRITE_MODE,
    job_name: str = JOB_NAME,
) -> None:
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(job_name, {"JOB_NAME": job_name})

    raw_df = read_source(spark, source_path)
    transformed_df = transform_former_names(
        raw_df, input_format, output_format)
    write_target(transformed_df, target_path, write_mode)

    job.commit()


if __name__ == "__main__":
    run_job()
