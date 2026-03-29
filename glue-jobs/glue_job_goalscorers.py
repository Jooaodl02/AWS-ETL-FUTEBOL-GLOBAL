from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, to_date

# Parâmetros explícitos (substitua por variáveis de ambiente em produção)
JOB_NAME = "futebol_goalscorers_job"
SOURCE_S3_PATH = "s3://bucket-futebol/raw/goalscorers/goalscorers.csv"
TARGET_S3_PATH = "s3://bucket-futebol/processed/goalscorers/"
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
        .withColumn("date", to_date(col("date"), input_format))
        .withColumn("date", date_format(col("date"), output_format))
        .withColumnRenamed("date", "anomesdia_partida")
        .withColumnRenamed("home_team", "time_mandante")
        .withColumnRenamed("team", "time_gol")
        .withColumnRenamed("scorer", "nome_marcador_gol")
        .withColumnRenamed("minute", "minuto_gol")
        .withColumnRenamed("own_goal", "flag_gol_contra")
        .withColumnRenamed("penalty", "flag_gol_penalti")
    )
 
def write_target(df: DataFrame, target_path: str, mode: str = "overwrite") -> None:
    df.write.mode(mode).parquet(target_path)


def main() -> None:
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(JOB_NAME, {"JOB_NAME": JOB_NAME})

    raw_df = read_source(spark, SOURCE_S3_PATH)

    transformed_df = transform_former_names(
        raw_df, INPUT_DATE_FORMAT, OUTPUT_DATE_FORMAT)
    
    write_target(transformed_df, TARGET_S3_PATH, WRITE_MODE)

    job.commit()


if __name__ == "__main__":
    main()
