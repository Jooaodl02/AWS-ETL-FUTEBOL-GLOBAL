from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, date_format, to_date

# Parâmetros explícitos
JOB_NAME = "futebol_former_names_job"
SOURCE_S3_PATH = "s3://bucket-futebol/raw/former_names/former_names.csv"
TARGET_S3_PATH = "s3://bucket-futebol/processed/former_names/"
# altere se o CSV tiver outro formato, ex: "dd/MM/yyyy"
INPUT_DATE_FORMAT = "yyyy-MM-dd"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, {"JOB_NAME": JOB_NAME})

# Lê CSV da camada raw
raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv(SOURCE_S3_PATH)
)

# Transformações de data necessárias
transformed_df = (
    raw_df
    .withColumn("star_date", to_date(col("star_date"), "yyyy-MM-dd"))
    .withColumn("end_date", to_date(col("end_date"), "yyyy-MM-dd"))
    .withColumn("star_date", date_format(col("star_date"), "yyyyMMdd"))
    .withColumn("end_date", date_format(col("end_date"), "yyyyMMdd"))
)

# Salva como parquet na pasta de destino
(
    transformed_df
    .write
    .mode("overwrite")
    .parquet(TARGET_S3_PATH)
)

job.commit()
