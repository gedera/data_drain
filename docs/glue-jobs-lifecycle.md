# Glue Jobs Lifecycle

Gestión completa de AWS Glue Jobs desde la gema.

## Métodos

### `job_exists?(job_name)` → Boolean

Verifica si un job existe en Glue.

```ruby
DataDrain::GlueRunner.job_exists?("my-job")
# => true
```

- Lanza `DataDrain::ConfigurationError` si `job_name` es inválido.
- Lanza `Aws::Glue::Errors::EntityNotFoundException` si el job no existe.
- Lanza otros errores de AWS sin atrapar.

### `get_job(job_name)` → Aws::Glue::Types::Job

Obtiene la configuración completa de un job.

```ruby
job = DataDrain::GlueRunner.get_job("my-job")
job.name               # => "my-job"
job.role               # => "arn:aws:iam::123:role/GlueRole"
job.command            # => { name: "glueetl", python_version: "3", script_location: "s3://..." }
job.default_arguments  # => { "--extra-files" => "s3://..." }
```

- Lanza `DataDrain::ConfigurationError` si `job_name` es inválido.
- Lanza `Aws::Glue::Errors::EntityNotFoundException` si el job no existe.

### `create_job(job_name, role_arn:, script_location:, ...)` → Aws::Glue::Types::Job

Crea un nuevo job en Glue y retorna el job creado.

**Parámetros requeridos:**
- `job_name` (String): nombre del job
- `role_arn` (String): ARN del IAM role de Glue
- `script_location` (String): path S3 del script Python

**Parámetros opcionales:**
- `command_name` (String): nombre del comando (`"glueetl"`, `"pythonshell"`). Default: `"glueetl"`.
- `default_arguments` (Hash): argumentos default del job
- `description` (String): descripción del job
- `timeout` (Integer): timeout en minutos. Default: `2880` (48h)
- `max_retries` (Integer): reintentos. Default: `0`
- `allocated_capacity` (Integer): DPU legacy. Preferir `worker_type` + `number_of_workers`
- `worker_type` (String): `"Standard"`, `"G.1X"`, `"G.2X"`, `"G.4X"`, `"G.8X"`
- `number_of_workers` (Integer): número de workers (requiere `worker_type`)
- `glue_version` (String): versión de Glue (ej. `"4.0"`)

```ruby
job = DataDrain::GlueRunner.create_job(
  "my-job",
  role_arn: "arn:aws:iam::123:role/GlueServiceRole",
  script_location: "s3://my-bucket/scripts/export.py",
  default_arguments: { "--extra-files" => "s3://my-bucket/scripts/udf.py" },
  timeout: 1440,
  max_retries: 2,
  worker_type: "G.1X",
  number_of_workers: 10
)
```

- Lanza `DataDrain::ConfigurationError` si `job_name` es inválido.
- Lanza errores de AWS sin atrapar (nombre duplicado, rol inválido, etc.)

### `update_job(job_name, ...)` → Aws::Glue::Types::Job

Actualiza un job existente y retorna el job actualizado.

Mismos parámetros que `create_job`, todos opcionales. Solo los parámetros provistos se actualizan.

```ruby
job = DataDrain::GlueRunner.update_job(
  "my-job",
  script_location: "s3://my-bucket/scripts/export-v2.py",
  timeout: 720
)
```

- Lanza `DataDrain::ConfigurationError` si `job_name` es inválido.
- Lanza `Aws::Glue::Errors::EntityNotFoundException` si el job no existe.

### `delete_job(job_name)` → Boolean

Elimina un job de Glue. Es idempotente.

```ruby
DataDrain::GlueRunner.delete_job("my-job")
# => true (job existía y fue eliminado)

DataDrain::GlueRunner.delete_job("nonexistent")
# => false (job no existía)
```

- Lanza `DataDrain::ConfigurationError` si `job_name` es inválido.
- Lanza otros errores de AWS sin atrapar.

### `ensure_job(job_name, role_arn:, script_location:, ...)` → Aws::Glue::Types::Job

Crea o actualiza un job de forma idempotente con diffing de configuración.

- Si el job no existe → `create_job`
- Si el job existe con config diferente → `update_job`
- Si el job existe con config idéntica → no-op, retorna el job actual (`:unchanged`)

```ruby
job = DataDrain::GlueRunner.ensure_job(
  "my-job",
  role_arn: "arn:aws:iam::123:role/GlueServiceRole",
  script_location: "s3://my-bucket/scripts/export.py",
  timeout: 1440
)
```

- Lanza `DataDrain::ConfigurationError` si `job_name` es inválido.
- Lanza errores de AWS sin atrapar.

### `run_and_wait(job_name, arguments = {}, ...)` → Boolean

Ejecuta un job existente y espera a que complete.

```ruby
DataDrain::GlueRunner.run_and_wait(
  "my-job",
  { "--start_date" => "2025-01-01", "--end_date" => "2025-02-01" },
  polling_interval: 60,
  max_wait_seconds: 7200
)
# => true (SUCCEEDED)
```

- Lanza `RuntimeError` si el job falla (`FAILED`, `STOPPED`, `TIMEOUT`).
- Lanza `DataDrain::Error` si `max_wait_seconds` excede.

## Patrón completo: ensure_job + run_and_wait + PySpark

Workflow end-to-end para archivar y purgar tablas PostgreSQL usando AWS Glue:

```ruby
# 1. Asegurar que el Glue Job existe con la config deseada (idempotente)
DataDrain::GlueRunner.ensure_job(
  "my-export-job",
  role_arn: "arn:aws:iam::123:role/GlueServiceRole",
  script_location: "s3://my-bucket/scripts/glue_pyspark_export.py",
  glue_version: "4.0",
  worker_type: "G.1X",
  number_of_workers: 10,
  timeout: 1440
)

# 2. Ejecutar el export (delegado a Glue Spark distribuido)
DataDrain::GlueRunner.run_and_wait(
  "my-export-job",
  {
    "--start_date"   => start_date.to_fs(:db),
    "--end_date"     => end_date.to_fs(:db),
    "--s3_bucket"    => bucket,
    "--s3_folder"    => table,
    "--db_url"       => "jdbc:postgresql://#{host}:#{port}/#{db}",
    "--db_user"      => db_user,
    "--db_password"  => db_password,
    "--db_table"     => table,
    "--partition_by" => partition_keys.join(",")
  },
  polling_interval: 60,
  max_wait_seconds: 7200
)

# 3. Verificar integridad y purgar Postgres (DataDrain solo lee Parquet)
DataDrain::Engine.new(
  bucket: bucket,
  folder_name: table,
  start_date: start_date,
  end_date: end_date,
  table_name: table,
  partition_keys: partition_keys,
  skip_export: true  # export ya lo hizo Glue
).call
```

### Prerequisites

1. **Subir el script a S3:**
   ```bash
   aws s3 cp glue_pyspark_export.py s3://my-bucket/scripts/
   ```

2. **IAM Role** con permisos para: Glue, S3 (lectura del script + escritura del bucket destino), RDS/Postgres (vía JDBC)

3. **Script PySpark** en `s3://my-bucket/scripts/glue_pyspark_export.py` (ver [ejemplo](../glue_pyspark_example.py))

## Convenciones de nombres

AWS Glue permite: letras (`a-zA-Z`), números (`0-9`), guiones (`-`), guiones bajos (`_`). No permite espacios ni caracteres especiales.

```ruby
# Válido
DataDrain::GlueRunner.job_exists?("my-export-job-v2")
DataDrain::GlueRunner.job_exists?("my_export_job")

# Inválido — lanza ConfigurationError
DataDrain::GlueRunner.job_exists?("-starts-with-dash")
# DataDrain::ConfigurationError: job_name '-starts-with-dash' no es un nombre válido para Glue Job
```

## Eventos de telemetría

| Evento | Nivel | Descripción |
|--------|-------|-------------|
| `glue_runner.start` | INFO | Antes de `start_job_run` |
| `glue_runner.job_create` | INFO | Job creado exitosamente |
| `glue_runner.job_update` | INFO | Job actualizado (incluye `changed_fields`) |
| `glue_runner.job_delete` | INFO | Job eliminado exitosamente |
| `glue_runner.job_delete_skipped` | INFO | `delete_job` sobre job inexistente |
| `glue_runner.job_exists` | INFO | Job encontrado en `ensure_job` (y difiere) |
| `glue_runner.job_created` | INFO | Job creado en `ensure_job` |
| `glue_runner.job_unchanged` | INFO | Job existe con config idéntica en `ensure_job` |
| `glue_runner.job_create_error` | ERROR | Error en `create_job` |
| `glue_runner.job_update_error` | ERROR | Error en `update_job` |
| `glue_runner.job_delete_error` | ERROR | Error en `delete_job` |
| `glue_runner.polling` | INFO | Chequeo de estado durante `run_and_wait` |
| `glue_runner.complete` | INFO | Job terminó `SUCCEEDED` |
| `glue_runner.failed` | ERROR | Job falló con `FAILED\|STOPPED\|TIMEOUT` |
| `glue_runner.timeout` | ERROR | `max_wait_seconds` excedido |
