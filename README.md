# DataDrain 🚰

DataDrain es un micro-framework de nivel empresarial diseñado para extraer, archivar y purgar (drenar) datos históricos desde bases de datos PostgreSQL transaccionales hacia un Data Lake analítico basado en Apache Parquet.

Utiliza **DuckDB** en memoria para lograr velocidades de procesamiento y compresión extremas, y garantiza la retención segura de datos mediante un chequeo de integridad estricto antes de purgar la base de datos de origen.

## Características Principales

* **ETL de Alto Rendimiento:** Transfiere millones de registros directamente desde Postgres a Parquet utilizando DuckDB sin cargar los objetos en la memoria RAM de Ruby.
* **Hive Partitioning:** Organiza automáticamente los archivos en carpetas optimizadas (`year=X/month=Y/tenant_id=Z`).
* **Storage Adapters:** Soporte nativo para almacenamiento en Disco Local y en la nube mediante Amazon S3.
* **Integridad Garantizada:** Verifica matemáticamente que los datos exportados coincidan exactamente con el origen antes de ejecutar sentencias `DELETE`.
* **ORM Analítico Integrado:** Incluye una clase base (`DataDrain::Record`) compatible con `ActiveModel` para consultar y destruir particiones históricas de forma idiomática.
* **Casteo Inteligente:** Incluye un tipo `:json` personalizado para hidratar atributos JSON/JSONB nativamente.

## Instalación

Agrega esta línea al `Gemfile` de tu aplicación:

```ruby
# Para desarrollo local / monorepo
gem 'data_drain', path: '../data_drain'

# O desde un repositorio Git privado
# gem 'data_drain', git: '[https://github.com/tu-organizacion/data_drain.git](https://github.com/tu-organizacion/data_drain.git)'
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
  
  # AWS S3 (Solo requerido si storage_mode es :s3)
  # config.aws_region = ENV['AWS_REGION']
  # config.aws_access_key_id = ENV['AWS_ACCESS_KEY_ID']
  # config.aws_secret_access_key = ENV['AWS_SECRET_ACCESS_KEY']

  # Base de Datos PostgreSQL de Origen
  config.db_host = ENV.fetch('DB_HOST', '127.0.0.1') # Usar IP para forzar TCP/IP
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

El framework se divide en dos grandes responsabilidades: **Extracción (Engine)** y **Consulta (Record)**.

### 1. Extracción y Purga (El Motor)

Para archivar datos, instancia el `DataDrain::Engine` pasando las opciones de rango de tiempo y las claves de partición, y ejecuta el método `#call`.

Es ideal ejecutar esto desde una tarea Rake o un Job programado (ej. Sidekiq-Cron) para crear una **Ventana de Retención Rodante (Rolling Window)**.

```ruby
# lib/tasks/archive.rake
task versions: :environment do
  # Queremos archivar un mes específico de hace 6 meses
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
    where_clause:   "event = 'update'" # Opcional: Filtro extra
  )

  # El motor cuenta, exporta a Parquet, verifica integridad y purga Postgres.
  engine.call 
end
```

### 2. Consultar el Data Lake (El ORM Analítico)

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

  # Utiliza el tipo :json provisto por la gema para hidratar Hashes automáticamente
  attribute :object, :json
  attribute :object_changes, :json
end
```

Ahora puedes consultar tus archivos Parquet (ya sea en Local o en S3) usando una interfaz familiar similar a ActiveRecord:

```ruby
# Buscar por ID (dentro de un año y mes específico para máxima velocidad)
version = ArchivedVersion.find("un-uuid", year: 2026, month: 3)
puts version.object_changes # => {"status" => ["active", "suspended"]}

# Buscar los últimos registros de un inquilino (tenant) en un mes
history = ArchivedVersion.where(limit: 10, year: 2026, month: 3, isp_id: "uuid-del-isp")
```

### 3. Destrucción de Datos (Derecho al Olvido / Retención)

El framework permite eliminar físicamente carpetas completas de particiones utilizando comodines (omitiendo claves).

```ruby
# Elimina todo el historial de un cliente en específico a través de todos los años
ArchivedVersion.destroy_all(isp_id: "uuid-del-cliente")

# Elimina todos los datos de marzo de 2024, sin importar el isp_id
ArchivedVersion.destroy_all(year: 2024, month: 3)
```

## Arquitectura

DataDrain implementa el patrón **Storage Adapter**, lo que permite aislar completamente la lógica del sistema de archivos o la nube del motor de procesamiento. 
* DuckDB mantiene una conexión Thread-Safe persistente para evitar recargar extensiones y maximizar el rendimiento de las consultas web.
* Las consultas `find` implementan mitigación de Inyección SQL.
* Las purgas en la base de datos de origen utilizan el driver nativo `pg` para evitar los cuellos de botella de instanciación de memoria de ActiveRecord.

## Licencia

La gema está disponible como código abierto bajo los términos de la Licencia MIT.
