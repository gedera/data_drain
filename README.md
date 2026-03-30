# DataDrain

DataDrain es un micro-framework de nivel empresarial diseñado para extraer, archivar y purgar datos históricos desde bases de datos PostgreSQL transaccionales, así como para **ingerir archivos crudos (CSV, JSON, Parquet)**, hacia un Data Lake analítico.

Utiliza **DuckDB** en memoria para lograr velocidades de procesamiento y compresión extremas. Garantiza la retención segura de datos mediante chequeos de integridad estrictos antes de purgar las bases de datos de origen, y automatiza la conversión y subida de archivos pesados a la nube.

## Características Principales

* **ETL de Alto Rendimiento:** Transfiere millones de registros desde Postgres a Parquet utilizando DuckDB sin cargar los objetos en la memoria RAM de Ruby.
* **File Ingestion:** Convierte archivos crudos masivos (ej. logs de Netflow en CSV) a Parquet (ZSTD) y los sube directamente a S3 en milisegundos.
* **Hive Partitioning:** Organiza automáticamente los archivos en carpetas optimizadas para consultas (`year=X/month=Y/tenant_id=Z`).
* **Storage Adapters:** Soporte nativo y transparente para almacenamiento en Disco Local y AWS S3.
* **Integridad Garantizada:** Verifica matemáticamente que los datos exportados coincidan exactamente con el origen antes de ejecutar sentencias `DELETE`.
* **ORM Analítico Integrado:** Incluye una clase base (`DataDrain::Record`) compatible con `ActiveModel` para consultar y destruir particiones históricas de forma idiomática.
* **Observabilidad Estructurada:** Todos los eventos emiten logs en formato `key=value` compatibles con Datadog, CloudWatch y `exis_ray`. Los fallos de logging nunca interrumpen el flujo principal.

## Instalación

Agrega esta línea al `Gemfile` de tu aplicación o microservicio:

```ruby
gem 'data_drain', git: 'https://github.com/gedera/data_drain.git', branch: 'main'
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
  # El valor 0 DESACTIVA el timeout (sin límite de tiempo).
  # Mandatorio para purgas de gran volumen donde cada lote puede tardar segundos.
  config.idle_in_transaction_session_timeout = 0

  config.logger = Rails.logger

  # Tuning de DuckDB
  # Límite máximo de RAM para las consultas en memoria de DuckDB (ej. '2GB', '512MB').
  # Evita que el proceso muera por OOM en contenedores con memoria limitada.
  config.limit_ram = '2GB'

  # Directorio temporal de DuckDB para desbordar memoria (spill to disk) durante
  # transformaciones pesadas o creación de archivos Parquet masivos.
  # Se recomienda que este directorio resida en un disco SSD/NVMe rápido.
  config.tmp_directory = '/tmp/duckdb_work'
end
```

## Uso

El framework provee cuatro herramientas principales: **Ingestor de Archivos**, **Drenaje de Base de Datos**, **ORM Analítico** y **Orquestación con AWS Glue**.

### 1. Ingestión de Archivos Crudos (FileIngestor)

Ideal para servicios que generan grandes volúmenes de datos (ej. métricas de Netflow). Toma un archivo local, lo transforma, lo comprime a Parquet y lo sube particionado a S3.

```ruby
ingestor = DataDrain::FileIngestor.new(
  bucket:              'my-bucket-store',
  source_path:         '/tmp/netflow_metrics_1600.csv',
  folder_name:         'netflow',
  partition_keys:      %w[isp_id year month],
  select_sql:          "*, EXTRACT(YEAR FROM timestamp) AS year, EXTRACT(MONTH FROM timestamp) AS month",
  delete_after_upload: true
)

ingestor.call
```

### 2. Extracción y Purga de BD (Engine)

Ideal para crear ventanas rodantes de retención (ej. mantener solo 6 meses de datos vivos en Postgres y archivar el resto).

**Flujo completo (Export + Verify + Purge):**

```ruby
engine = DataDrain::Engine.new(
  bucket:         'my-bucket-store',
  start_date:     6.months.ago.beginning_of_month,
  end_date:       6.months.ago.end_of_month,
  table_name:     'versions',
  partition_keys: %w[year month]
)

engine.call
```

**Modo Purga con Exportación Externa (skip_export):**

Si tu arquitectura ya utiliza **AWS Glue** o **AWS EMR** para mover datos pesados, puedes configurar DataDrain para que actúe únicamente como garante de integridad. En este modo omite la exportación pero verifica matemáticamente que los datos existan en el Data Lake antes de eliminarlos de PostgreSQL.

```ruby
engine = DataDrain::Engine.new(
  bucket:         'my-bucket-store',
  start_date:     6.months.ago.beginning_of_month,
  end_date:       6.months.ago.end_of_month,
  table_name:     'versions',
  partition_keys: %w[year month],
  skip_export:    true
)

engine.call
```

### 3. Orquestación con AWS Glue (Big Data)

Para tablas de gran volumen (**ej. > 500GB o 1TB**), se recomienda delegar el movimiento de datos a **AWS Glue** (basado en Apache Spark) para evitar saturar el servidor de Ruby. `DataDrain` actúa como el orquestador que dispara el Job, espera a que termine y luego realiza la validación y purga.

```ruby
config = DataDrain.configuration
bucket = "my-bucket"
table  = "versions"

# 1. Disparar el Job de Glue y esperar su finalización exitosa
DataDrain::GlueRunner.run_and_wait(
  "my-glue-export-job",
  {
    "--start_date"   => start_date.to_fs(:db),
    "--end_date"     => end_date.to_fs(:db),
    "--s3_bucket"    => bucket,
    "--s3_folder"    => table,
    "--db_url"       => "jdbc:postgresql://#{config.db_host}:#{config.db_port}/#{config.db_name}",
    "--db_user"      => config.db_user,
    "--db_password"  => config.db_pass,
    "--db_table"     => table,
    "--partition_by" => "isp_id,year,month"
  }
)

# 2. Una vez que Glue exportó el TB, DataDrain valida integridad y purga Postgres
DataDrain::Engine.new(
  bucket:         bucket,
  folder_name:    table,
  start_date:     start_date,
  end_date:       end_date,
  table_name:     table,
  partition_keys: %w[isp_id year month],
  skip_export:    true
).call
```

#### Script de AWS Glue (PySpark) compatible con DataDrain

Crea un Job en la consola de AWS Glue (Spark 4.0+) y utiliza este script como base:

```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'start_date', 'end_date', 's3_bucket', 's3_folder',
    'db_url', 'db_user', 'db_password', 'db_table', 'partition_by'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

options = {
    "url": args['db_url'],
    "dbtable": args['db_table'],
    "user": args['db_user'],
    "password": args['db_password'],
    "sampleQuery": f"SELECT * FROM {args['db_table']} WHERE created_at >= '{args['start_date']}' AND created_at < '{args['end_date']}'"
}

df = spark.read.format("jdbc").options(**options).load()

# Agregar columnas derivadas necesarias para las particiones.
# isp_id ya existe en la tabla fuente — solo agregar las que se calculan.
# Personalizar esta sección según las partition_keys de cada tabla.
df_final = df.withColumn("year", year(col("created_at"))) \
             .withColumn("month", month(col("created_at")))

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
  self.bucket       = 'my-bucket-storage'
  self.folder_name  = 'versions'
  self.partition_keys = [:isp_id, :year, :month]

  attribute :id,             :string
  attribute :item_type,      :string
  attribute :item_id,        :string
  attribute :event,          :string
  attribute :whodunnit,      :string
  attribute :created_at,     :datetime
  attribute :object,         :json
  attribute :object_changes, :json
end
```

Consultas optimizadas mediante Hive Partitioning:

```ruby
# Búsqueda puntual aislando la partición exacta
version = ArchivedVersion.find("un-uuid", isp_id: 42, year: 2026, month: 3)
puts version.object_changes # => {"status" => ["active", "suspended"]}

# Colecciones
history = ArchivedVersion.where(limit: 10, isp_id: 42, year: 2026, month: 3)
```

### 5. Destrucción de Datos (Retención y Cumplimiento)

El framework permite eliminar físicamente carpetas completas en S3 o Local utilizando comodines.

```ruby
# Elimina todo el historial de un cliente a través de todos los años
ArchivedVersion.destroy_all(isp_id: 42)

# Elimina todos los datos de marzo de 2024 globalmente
ArchivedVersion.destroy_all(year: 2024, month: 3)
```

## Arquitectura

DataDrain implementa el patrón **Storage Adapter**, lo que permite aislar completamente la lógica del sistema de archivos de los motores de procesamiento.

* **Conexión DuckDB thread-local:** `DataDrain::Record` mantiene una conexión DuckDB por thread (`Thread.current[:data_drain_duckdb]`). Cada thread inicializa su propia conexión una sola vez, incluyendo la carga de extensiones como `httpfs`. Tener esto en cuenta en entornos Puma o Sidekiq.
* **Storage Adapter cacheado:** `DataDrain::Storage.adapter` cachea la instancia del adaptador. Si `storage_mode` cambia en runtime, llamar `DataDrain::Storage.reset_adapter!` para invalidar el cache.
* **ORM Analítico con sanitización:** `DataDrain::Record` incluye sanitización de parámetros para prevenir inyección SQL al consultar archivos Parquet.

## Observabilidad

Todos los eventos emiten logs estructurados en formato `key=value` procesables por herramientas como Datadog, CloudWatch Logs Insights o `exis_ray`:

```
component=data_drain event=engine.complete table=versions duration_s=12.4 export_duration_s=8.1 purge_duration_s=3.9 count=150000
component=data_drain event=engine.integrity_error table=versions duration_s=5.2 count=150000
component=data_drain event=engine.purge_heartbeat table=versions batches_processed_count=100 rows_deleted_count=500000
component=data_drain event=file_ingestor.complete source_path=/tmp/data.csv duration_s=2.1 count=85000
component=data_drain event=glue_runner.failed job=my-export-job run_id=jr_abc123 status=FAILED duration_s=301.0
```

Los fallos internos del sistema de logging nunca interrumpen el flujo principal de datos.

## Licencia

La gema está disponible como código abierto bajo los términos de la Licencia MIT.
