"""
Script de AWS Glue (PySpark) compatible con DataDrain::GlueRunner.

Para crear el Glue Job programmatically (en vez de la consola):

    # 1. Subir script a S3
    # aws s3 cp glue_pyspark_example.py s3://my-bucket/scripts/

    # 2. Crear job idempotentemente
    DataDrain::GlueRunner.ensure_job(
      "my-export-job",
      role_arn: "arn:aws:iam::123:role/GlueServiceRole",
      script_location: "s3://my-bucket/scripts/glue_pyspark_example.py",
      glue_version: "4.0",
      worker_type: "G.1X",
      number_of_workers: 10,
      timeout: 1440
    )

    # 3. Ejecutar
    DataDrain::GlueRunner.run_and_wait("my-export-job", { "--start_date" => "2025-01-01", ... })

Argumentos requeridos del job: JOB_NAME, start_date, end_date, s3_bucket, s3_folder,
db_url, db_user, db_password, db_table, partition_by.

Personalizar la sección de columnas derivadas según las partition_keys de cada tabla.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "start_date",
        "end_date",
        "s3_bucket",
        "s3_folder",
        "db_url",
        "db_user",
        "db_password",
        "db_table",
        "partition_by",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

options = {
    "url": args["db_url"],
    "dbtable": args["db_table"],
    "user": args["db_user"],
    "password": args["db_password"],
    "sampleQuery": (
        f"SELECT * FROM {args['db_table']} "
        f"WHERE created_at >= '{args['start_date']}' "
        f"AND created_at < '{args['end_date']}'"
    ),
}

df = spark.read.format("jdbc").options(**options).load()

# Agregar columnas derivadas necesarias para las particiones.
# isp_id ya existe en la tabla fuente — solo agregar las que se calculan.
# Personalizar esta seccion segun las partition_keys de cada tabla.
df_final = df.withColumn("year", year(col("created_at"))).withColumn(
    "month", month(col("created_at"))
)

output_path = f"s3://{args['s3_bucket']}/{args['s3_folder']}/"
partitions = args["partition_by"].split(",")

(
    df_final.write.mode("overwrite")
    .partitionBy(*partitions)
    .format("parquet")
    .option("compression", "zstd")
    .save(output_path)
)

job.commit()
