# DataDrain 🚰

DataDrain es un micro-framework de nivel empresarial diseñado para extraer, archivar y purgar datos históricos desde bases de datos PostgreSQL transaccionales, así como para **ingerir archivos crudos (CSV, JSON, Parquet)**, hacia un Data Lake analítico.

Utiliza **DuckDB** en memoria para lograr velocidades de procesamiento y compresión extremas. Garantiza la retención segura de datos mediante chequeos de integridad estrictos antes de purgar las bases de datos de origen, y automatiza la conversión y subida de archivos pesados a la nube.

## Características Principales

* **ETL de Alto Rendimiento:** Transfiere millones de registros desde Postgres a Parquet utilizando DuckDB sin cargar los objetos en la memoria RAM de Ruby.
* **File Ingestion:** Convierte archivos crudos masivos (ej. logs de Netflow en CSV) a Parquet (ZSTD) y los sube directamente a S3 en milisegundos.
* **Hive Partitioning:** Organiza automáticamente los archivos en carpetas optimizadas para consultas (`year=X/month=Y/tenant_id=Z`).
* **Storage Adapters:** Soporte nativo y transparente para almacenamiento en Disco Local y AWS S3.
* **Integridad Garantizada:** Verifica matemáticamente que los datos exportados coincidan exactamente con el origen antes de ejecutar sentencias `DELETE`.
* **ORM Analítico Integrado:** Incluye una clase base (`DataDrain::Record`) compatible con `ActiveModel` para consultar y destruir particiones históricas de forma idiomática.

## Instalación

Agrega esta línea al `Gemfile` de tu aplicación o microservicio:

```ruby
gem 'data_drain', git: '[https://github.com/tu-organizacion/data_drain.git](https://github.com/tu-organizacion/data_drain.git)', branch: 'main'
```

Y ejecuta:
```bash
$ bundle install
```

## Configuración

Crea un inicializador en tu aplicación (ej. `config/initializers/data_drain.rb`) para configurar las credenciales y el comportamiento del motor:

```ruby
DataDrain.configure do |config|
  # Almacenamiento (:local o :s3)
  config.storage_mode = ENV.fetch('STORAGE_MODE', 'local').to_sym

  # AWS S3 (Requerido solo si storage_mode es :s3)
  # config.aws_region = ENV['AWS_REGION']
  # config.aws_access_key_id = ENV['AWS_ACCESS_KEY_ID']
  # config.aws_secret_access_key = ENV['AWS_SECRET_ACCESS_KEY']

  # Base de Datos PostgreSQL de Origen (Requerido solo para DataDrain::Engine)
  config.db_host = ENV.fetch('DB_HOST', '127.0.0.1')
  config.db_port = ENV.fetch('DB_PORT', '5432')
  config.db_user = ENV.fetch('DB_USER', 'postgres')
  config.db_pass = ENV.fetch('DB_PASS', '')
  config.db_name = ENV.fetch('DB_NAME', 'core_production')

  # Rendimiento y Tuning de Postgres
  config.batch_size     = 5000 # Registros a borrar por transacción
  config.throttle_delay = 0.5  # Segundos de pausa entre borrados
  
  # Timeout de inactividad de transacciones en PostgreSQL (en milisegundos).
  # Útil establecerlo en 0 para evitar que la conexión se cierre prematuramente 
  # durante el borrado de grandes volúmenes de datos.
  config.idle_in_transaction_session_timeout = 0
  
  config.logger         = Rails.logger

  # Tuning de DuckDB
  # Límite máximo de RAM para las consultas en memoria de DuckDB (ej. '2GB', '512MB').
  # Evita que el proceso OOM (Out Of Memory) si el contenedor o servidor tiene memoria limitada.
  config.limit_ram      = '2GB'
  
  # Directorio temporal de DuckDB para desbordar memoria (spill to disk) durante
  # transformaciones pesadas o creación de archivos Parquet masivos.
  # Es muy recomendable que este directorio resida en un disco SSD/NVMe rápido.
  config.tmp_directory  = '/tmp/duckdb_work'
end
```

## Uso

El framework provee tres herramientas principales: **Ingestor de Archivos**, **Drenaje de Base de Datos**, y el **ORM Analítico**.

### 1. Ingestión de Archivos Crudos (FileIngestor)

Ideal para servicios que generan grandes volúmenes de datos (ej. métricas de Netflow). Toma un archivo local, lo transforma, lo comprime a Parquet y lo sube particionado a S3.

```ruby
# Un archivo generado temporalmente por tu servicio
archivo_temporal = "/tmp/netflow_metrics_1600.csv"

ingestor = DataDrain::FileIngestor.new(
  bucket: 'my-bucket-store',
  source_path: archivo_temporal,
  folder_name: 'netflow',
  # Particionamos dinámicamente según columnas extraídas al vuelo
  partition_keys: %w[year month isp_id],
  # Transformación SQL ejecutada por DuckDB durante la lectura
  select_sql: "*, EXTRACT(YEAR FROM timestamp) AS year, EXTRACT(MONTH FROM timestamp) AS month",
  delete_after_upload: true # Limpia el archivo temporal al terminar
)

ingestor.call
```

### 2. Extracción y Purga de BD (Engine)

Ideal para crear Ventanas Rodantes de retención (ej. mantener solo 6 meses de datos vivos en Postgres y archivar el resto). 

**Modo Purga con Exportación Externa (AWS Glue):** 
Si tu arquitectura ya utiliza **AWS Glue** o **AWS EMR** para mover datos pesados, puedes configurar DataDrain para que actúe únicamente como **Garante de Integridad**. En este modo, el motor omitirá el paso de exportación, pero verificará matemáticamente que los datos existan en el Data Lake antes de proceder a eliminarlos de PostgreSQL.

```ruby
# lib/tasks/archive_with_glue.rake
task purge_only: :environment do
  engine = DataDrain::Engine.new(
    bucket:         'my-bucket-store',
    start_date:     6.months.ago.beginning_of_month,
    end_date:       6.months.ago.end_of_month,
    table_name:     'versions',
    partition_keys: %w[year month],
    skip_export:    true # ⚡️ No exporta nada, solo valida S3 y purga Postgres
  )

  engine.call
end
```

### 3. Orquestación con AWS Glue (Big Data)

Para tablas de gran volumen (**ej. > 500GB o 1TB**), se recomienda delegar el movimiento de datos a **AWS Glue** (basado en Apache Spark) para evitar saturar el servidor de Ruby. `DataDrain` actúa como el orquestador que dispara el Job, espera a que termine y luego realiza la validación y purga.

```ruby
# 1. Disparar el Job de Glue y esperar su finalización exitosa
config = DataDrain.configuration
bucket = "my-bucket"
table  = "versions"

DataDrain::GlueRunner.run_and_wait(
  "my-glue-export-job",
  {
    "--start_date"    => start_date.to_fs(:db),
    "--end_date"      => end_date.to_fs(:db),
    "--s3_bucket"     => bucket,
    "--s3_folder"     => table,
    "--db_url"        => "jdbc:postgresql://#{config.db_host}:#{config.db_port}/#{config.db_name}",
    "--db_user"       => config.db_user,
    "--db_password"   => config.db_pass,
    "--db_table"      => table,
    "--partition_by"  => "year,month,isp_id" # <--- Columnas dinámicas
  }
)

# 2. Una vez que Glue exportó el TB, DataDrain valida integridad y purga Postgres
DataDrain::Engine.new(
  bucket:         bucket,
  folder_name:    table,
  start_date:     start_date,
  end_date:       end_date,
  table_name:     table,
  partition_keys: %w[year month isp_id],
  skip_export:    true # <--- Modo Validación + Purga
).call
```

#### Script de AWS Glue (PySpark) compatible con DataDrain

Crea un Job en la consola de AWS Glue (Spark 4.0+) y utiliza este script como base. Está diseñado para extraer datos de PostgreSQL de forma dinámica:

```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month

# Parámetros recibidos desde DataDrain::GlueRunner
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'start_date', 'end_date', 's3_bucket', 's3_folder',
    'db_url', 'db_user', 'db_password', 'db_table', 'partition_by'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Leer de PostgreSQL (vía JDBC dinámico)
options = {
    "url": args['db_url'],
    "dbtable": args['db_table'],
    "user": args['db_user'],
    "password": args['db_password'],
    "sampleQuery": f"SELECT * FROM {args['db_table']} WHERE created_at >= '{args['start_date']}' AND created_at < '{args['end_date']}'"
}

df = spark.read.format("jdbc").options(**options).load()

# 2. Agregar columnas de partición temporales (Hive Partitioning)
df_final = df.withColumn("year", year(col("created_at"))) \
             .withColumn("month", month(col("created_at")))

# 3. Escribir a S3 en Parquet con compresión ZSTD
# Construimos el path dinámicamente: s3://bucket/folder/
output_path = f"s3://{args['s3_bucket']}/{args['s3_folder']}/"
partitions = args['partition_by'].split(",")

df_final.write.mode("overwrite") \
        .partitionBy(*partitions) \
        .format("parquet") \
        .option("compression", "zstd") \
        .save(output_path)

job.commit()
```

### 4. Consultar el Data Lake (Record)

Para consultar los datos archivados sin salir de Ruby, crea un modelo que herede de `DataDrain::Record`.

```ruby
# app/models/archived_version.rb
class ArchivedVersion < DataDrain::Record
  self.bucket = 'my-bucket-storage'
  self.folder_name = 'versions'
  self.partition_keys = [:year, :month, :isp_id]

  attribute :id, :string
  attribute :item_type, :string
  attribute :item_id, :string
  attribute :event, :string
  attribute :whodunnit, :string
  attribute :created_at, :datetime

  # Utiliza el tipo :json provisto por la gema para hidratar Hashes
  attribute :object, :json
  attribute :object_changes, :json
end
```

Consultas altamente optimizadas mediante Hive Partitioning:

```ruby
# Búsqueda puntual hiper-rápida aislando las particiones
version = ArchivedVersion.find("un-uuid", year: 2026, month: 3, isp_id: 42)
puts version.object_changes # => {"status" => ["active", "suspended"]}

# Colecciones
history = ArchivedVersion.where(limit: 10, year: 2026, month: 3, isp_id: 42)
```

### 4. Destrucción de Datos (Retención y Cumplimiento)

El framework permite eliminar físicamente carpetas completas en S3 o Local utilizando comodines.

```ruby
# Elimina todo el historial de un cliente en específico a través de todos los años
ArchivedVersion.destroy_all(isp_id: 42)

# Elimina todos los datos de marzo de 2024 globalmente
ArchivedVersion.destroy_all(year: 2024, month: 3)
```

## Arquitectura

DataDrain implementa el patrón **Storage Adapter**, lo que permite aislar completamente la lógica del sistema de archivos de los motores de procesamiento. 
* DuckDB mantiene una conexión persistente (`Thread-Safe`) para maximizar el rendimiento de las consultas web.
* El ORM Analítico incluye sanitización de parámetros para prevenir Inyección SQL al consultar archivos Parquet.

## Licencia

La gema está disponible como código abierto bajo los términos de la Licencia MIT.
