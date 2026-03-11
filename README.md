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
  config.base_path    = Rails.root.join('storage', 'cold_storage').to_s
  
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

  # Rendimiento y Tuning
  config.batch_size     = 5000 # Registros a borrar por transacción
  config.throttle_delay = 0.5  # Segundos de pausa entre borrados
  config.logger         = Rails.logger
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

```ruby
# lib/tasks/archive.rake
task versions: :environment do
  target_date = 6.months.ago.beginning_of_month

  select_sql = <<~SQL
    id, item_type, item_id, event, whodunnit,
    object::VARCHAR AS object,
    object_changes::VARCHAR AS object_changes,
    created_at,
    EXTRACT(YEAR FROM created_at)::INT AS year,
    EXTRACT(MONTH FROM created_at)::INT AS month,
    isp_id
  SQL

  engine = DataDrain::Engine.new(
    start_date:     target_date.beginning_of_month,
    end_date:       target_date.end_of_month,
    table_name:     'versions',
    select_sql:     select_sql,
    partition_keys: %w[year month isp_id],
    where_clause:   "event = 'update'"
  )

  # Cuenta, exporta a Parquet, verifica integridad y purga Postgres.
  engine.call 
end
```

### 3. Consultar el Data Lake (Record)

Para consultar los datos archivados sin salir de Ruby, crea un modelo que herede de `DataDrain::Record`.

```ruby
# app/models/archived_version.rb
class ArchivedVersion < DataDrain::Record
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
