# API Detallada

Firmas completas, parámetros, retornos y comportamientos de cada clase pública de DataDrain.

## `DataDrain` (módulo)

### `DataDrain.configure { |config| ... }`
Bloque de configuración global. `config` es una instancia singleton de `Configuration`.

### `DataDrain.configuration`
Retorna la `Configuration` singleton (lazy init).

### `DataDrain.reset_configuration!`
Resetea config y resetea `Storage.adapter`. Útil en tests.

---

## `DataDrain::Configuration`

Atributos (`attr_accessor`):

| Atributo | Tipo | Default | Descripción |
|----------|------|---------|-------------|
| `storage_mode` | Symbol | `:local` | `:local` o `:s3` |
| `aws_region` | String | `nil` | Requerido si `:s3` |
| `aws_access_key_id` | String | `nil` | Requerido si `:s3` |
| `aws_secret_access_key` | String | `nil` | Requerido si `:s3` |
| `db_host` | String | `"127.0.0.1"` | Host PostgreSQL |
| `db_port` | Integer | `5432` | Puerto PostgreSQL |
| `db_user` | String | `nil` | Usuario PostgreSQL |
| `db_pass` | String | `nil` | Password PostgreSQL |
| `db_name` | String | `nil` | Base de datos |
| `batch_size` | Integer | `5000` | Registros por DELETE en purga |
| `throttle_delay` | Float | `0.5` | Segundos de pausa entre lotes |
| `idle_in_transaction_session_timeout` | Integer | `0` | Milisegundos. `0` = DESACTIVADO. `nil` = no setear |
| `limit_ram` | String | `nil` | Límite memoria DuckDB (ej. `"2GB"`) |
| `tmp_directory` | String | `nil` | Spill-to-disk DuckDB |
| `logger` | Logger | `Logger.new($stdout)` | Logger |

### `#duckdb_connection_string`
Retorna URI: `postgresql://user:pass@host:port/db?options=-c%20idle_in_transaction_session_timeout%3D<val>`

### `#validate!`
Valida invariantes generales. Llamada automáticamente por `FileIngestor#initialize` y `GlueRunner.run_and_wait`.

Raises `DataDrain::ConfigurationError` si:
- `storage_mode` no es `:local` ni `:s3`
- `storage_mode == :s3` y `aws_region` es nil o vacío

### `#validate_for_engine!`
Valida invariantes de Engine. Además de `#validate!`, verifica `db_host`, `db_user`, `db_name` no nil ni vacíos.

Llamada automáticamente por `Engine#initialize`.

**No valida `db_pass`** — puede ser nil con auth peer/trust (sockets locales) o IAM (RDS).
**No valida `db_port`** — tiene default `5432`, nunca nil tras `Configuration#initialize`.

---

## `DataDrain::Engine`

### `#initialize(options)`

| Opción | Tipo | Requerido | Descripción |
|--------|------|-----------|-------------|
| `:start_date` | Time/DateTime/Date | sí | Convertido a `beginning_of_day` |
| `:end_date` | Time/DateTime/Date | sí | Convertido a `next_day.beginning_of_day` (semi-abierto `<`) |
| `:table_name` | String | sí | Tabla origen en `public` |
| `:partition_keys` | Array<String, Symbol> | sí | Orden = jerarquía Hive |
| `:bucket` | String | no | Ruta local o nombre bucket S3 |
| `:folder_name` | String | no | Default = `table_name` |
| `:select_sql` | String | no | Default = `"*"` |
| `:primary_key` | String | no | Default = `"id"`. Usado en DELETE |
| `:where_clause` | String | no | SQL extra (concat con `AND`) |
| `:skip_export` | Boolean | no | Default `false`. `true` omite export |

Internamente: crea `DuckDB::Database.open(":memory:")`, captura `@config`, `@logger`, `@adapter`.

### `#call → Boolean`
Ejecuta flujo completo: setup → count → [export] → verify → [purge].

- Retorna `true` si flujo completó (incluyendo caso `pg_count == 0`).
- Retorna `false` si verify falla o si error leyendo Parquet.
- Cuando retorna `false`, **NO ejecuta purga** (garantía de seguridad).

Eventos emitidos: ver [eventos-telemetria.md](eventos-telemetria.md).

### Métodos privados notables

- `#base_where_sql` — `created_at >= START AND created_at < END_BOUNDARY [AND where_clause]`. Semi-abierto.
- `#setup_duckdb` — INSTALL/LOAD postgres, set max_memory/temp_directory, ATTACH pg_source READ_ONLY, delega `setup_duckdb` al adapter.
- `#get_postgres_count → Integer` — Vía `postgres_query('pg_source', ...)`.
- `#export_to_parquet` — `COPY ... TO ... PARTITION_BY (...) COMPRESSION 'ZSTD' OVERWRITE_OR_IGNORE 1`.
- `#verify_integrity → Boolean` — `COUNT(*) read_parquet(...) == @pg_count`. Captura `DuckDB::Error` → `false`.
- `#purge_from_postgres` — Loop `DELETE WHERE pk IN (SELECT pk ... LIMIT batch_size)` hasta que `cmd_tuples == 0`. Heartbeat cada 100 lotes. `sleep(throttle_delay)` si `> 0`. Cierra conexión PG en `ensure`.

---

## `DataDrain::FileIngestor`

### `#initialize(options)`

| Opción | Tipo | Requerido | Descripción |
|--------|------|-----------|-------------|
| `:source_path` | String | sí | Ruta absoluta al archivo |
| `:folder_name` | String | sí | Carpeta destino en Data Lake |
| `:bucket` | String | no | Ruta local o bucket S3 |
| `:partition_keys` | Array | no | Default `[]` (sin particionamiento) |
| `:select_sql` | String | no | Default `"*"`. Útil para extraer columnas derivadas (`EXTRACT(YEAR FROM ts) AS year`) |
| `:delete_after_upload` | Boolean | no | Default `true` |

### `#call → Boolean`

Flujo:
1. Valida que el archivo exista. Si no, log `file_ingestor.file_not_found`, retorna `false`.
2. Aplica `limit_ram`, `tmp_directory` y delega `setup_duckdb` al adapter.
3. Determina reader según extensión (`.csv`, `.json`, `.parquet`). Otras extensiones → `raise DataDrain::Error`.
4. Conteo de seguridad. Si `0`, limpia y retorna `true`.
5. `COPY ... TO ... [PARTITION_BY (...)] COMPRESSION 'ZSTD' OVERWRITE_OR_IGNORE 1`.
6. `cleanup_local_file` si `delete_after_upload`.
7. Retorna `true`.

`rescue DuckDB::Error` → log `file_ingestor.duckdb_error`, retorna `false`. `ensure` cierra conexión DuckDB.

### Formatos soportados

- `.csv` → `read_csv_auto`
- `.json` → `read_json_auto`
- `.parquet` → `read_parquet`
- Otros → `raise DataDrain::Error, "Formato de archivo no soportado para ingestión: ..."`

---

## `DataDrain::Record`

Clase abstracta. Subclasificar para cada tabla archivada.

```ruby
class ArchivedX < DataDrain::Record
  self.bucket          = "..."
  self.folder_name     = "..."
  self.partition_keys  = [:isp_id, :year, :month]  # ORDEN CRÍTICO

  attribute :id,         :string
  attribute :created_at, :datetime
  attribute :payload,    :json   # usa DataDrain::Types::JsonType
end
```

Hereda de `ActiveModel::Model` + `ActiveModel::Attributes`.

### `class_attribute`s
- `bucket` — String
- `folder_name` — String
- `partition_keys` — Array<Symbol>

### `.connection → DuckDB::Connection`
Conexión persistente por thread (cacheada en `Thread.current[:data_drain_duckdb] = { db:, conn: }`). El hash ancla la `Database` para evitar GC. La conexión nunca se cierra explícitamente.

### `.where(limit: 50, **partitions) → Array<self>`
Construye path Hive en orden de `partition_keys` (no del orden de kwargs). SQL: `SELECT ... FROM read_parquet(path) ORDER BY created_at DESC LIMIT n`. Si Parquet no existe, retorna `[]` y loguea `record.parquet_not_found` en WARN.

### `.find(id, **partitions) → self | nil`
SQL: `WHERE id = '<safe_id>' LIMIT 1`. Sanitiza `id` con `gsub("'", "''")` (escape de comillas simples). Retorna primera fila o `nil`.

### `.destroy_all(**partitions) → Integer`
Delega a `Storage.adapter#destroy_partitions`. Loguea `record.destroy_all`. Si no se especifica una key de `partition_keys`, se usa wildcard (`*` en Local, regex `[^/]+` en S3). Retorna cantidad de particiones/objetos borrados.

### `#inspect → String`
Formato: `#<Class attr1: val1, attr2: val2, ...>`.

### Métodos privados
- `.build_query_path(partitions)` — Itera `partition_keys` (no kwargs) y arma `key=val/key=val/...`. Acepta keys como Symbol o String en `partitions`.
- `.execute_and_instantiate(sql, columns)` — Ejecuta query, captura `DuckDB::Error` → `[]` con WARN, mapea filas a instancias.

---

## `DataDrain::GlueRunner`

### `.run_and_wait(job_name, arguments = {}, polling_interval: 30, max_wait_seconds: nil) → true`

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `job_name` | String | Nombre del Job en consola AWS |
| `arguments` | Hash | Args con prefijo `--` (ej. `"--start_date" => "..."`) |
| `polling_interval` | Integer | Segundos entre chequeos. Default `30` |
| `max_wait_seconds` | Integer, nil | Timeout máximo. nil = sin límite. Default `nil` |

Flujo:
1. `Aws::Glue::Client.new(region: config.aws_region)`
2. `start_job_run` → captura `run_id`
3. Loop: `get_job_run`, evalúa `job_run_state`:
   - Si `max_wait_seconds` excede → log `glue_runner.timeout`, `raise DataDrain::Error`
   - `SUCCEEDED` → log `glue_runner.complete`, retorna `true`
   - `FAILED|STOPPED|TIMEOUT` → log `glue_runner.failed` (incluye `error_message` truncado a 200 chars), `raise RuntimeError`
   - Otro → log `glue_runner.polling`, `sleep polling_interval`

---

## `DataDrain::Storage`

### `.adapter → Storage::Base`
Memoizada. Devuelve `Local.new` o `S3.new` según `config.storage_mode`. `raise InvalidAdapterError` si modo desconocido.

### `.reset_adapter!`
Limpia memoización. **Obligatorio** si se cambia `storage_mode` en runtime.

### `Storage::Base` (interfaz)
Métodos abstractos:
- `#setup_duckdb(connection)` — `raise NotImplementedError`
- `#prepare_export_path(bucket, folder_name)` — No-op por defecto
- `#build_path(bucket, folder_name, partition_path) → String` — `raise NotImplementedError`
- `#destroy_partitions(bucket, folder_name, partition_keys, partitions) → Integer` — `raise NotImplementedError`

### `Storage::Local`
- `#setup_duckdb` — No-op (DuckDB nativo)
- `#prepare_export_path` — `FileUtils.mkdir_p`
- `#build_path` — `"<bucket>/<folder>/<partition_path>/**/*.parquet"`
- `#destroy_partitions` — Construye glob con `key=*` para nulos, `Dir.glob`, `FileUtils.rm_rf` cada match

### `Storage::S3`
- `#setup_duckdb` — `INSTALL httpfs; LOAD httpfs;` + `SET s3_region/s3_access_key_id/s3_secret_access_key`
- `#prepare_export_path` — No-op (S3 no requiere mkdir)
- `#build_path` — `"s3://<bucket>/<folder>/<partition_path>/**/*.parquet"`
- `#destroy_partitions` — `Aws::S3::Client.list_objects_v2` con prefix optimizado (primera key si no es nula), filtra con regex (`key=[^/]+` para nulos), `delete_objects` en lotes de 1000

---

## `DataDrain::Observability` (mixín)

Diseñado para `include` (instance methods, requiere `@logger`) o `extend` (class methods, requiere `@logger` de clase).

### `#safe_log(level, event, metadata = {})` (privado)
- Si `@logger` es nil, no-op.
- Construye `fields = { component: observability_name, event: event }.merge(metadata)`.
- Filtra valores cuyas keys matcheen `SENSITIVE_KEY_PATTERN = /password|passwd|pass|secret|token|api_key|apikey|auth|credential|private_key/i` → `[FILTERED]`. Aplica a claves exactas (`password`) y variantes (`db_password`, `aws_secret_access_key`, `bearer_token`, etc.).
- Emite `@logger.send(level) { "k1=v1 k2=v2 ..." }`.
- `rescue StandardError` silencioso (resilience).

### `#exception_metadata(error)` (privado)
Retorna `{ error_class: error.class.name, error_message: error.message.gsub('"', "'")[0, 200] }`.

### `#observability_name` (privado)
Extrae el primer namespace del nombre de clase y lo convierte a snake_case. Ej. `DataDrain::Engine` → `data_drain`.

**Importante:** cuando se usa `extend`, marcar los métodos como `private_class_method :safe_log, :exception_metadata, :observability_name`.

---

## `DataDrain::Types::JsonType`

`ActiveModel::Type::Value` registrado como `:json`. `#cast`:
- Si valor es Hash, Array o nil → retorna tal cual.
- Si String → `JSON.parse(value)`.
- Si parse falla → retorna valor original (no levanta).

---

## `DataDrain::Error` jerarquía

```
StandardError
└── DataDrain::Error
    ├── DataDrain::ConfigurationError
    ├── DataDrain::IntegrityError
    ├── DataDrain::StorageError
    └── DataDrain::Storage::InvalidAdapterError
```

`DuckDB::Error` y `Aws::S3::Errors::*` NO se envuelven en `StorageError` actualmente — se capturan puntualmente o se propagan.
