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

### `delete_job(job_name)` → nil

Elimina un job de Glue.

```ruby
DataDrain::GlueRunner.delete_job("my-job")
# => nil
```

- Lanza `DataDrain::ConfigurationError` si `job_name` es inválido.
- Lanza `Aws::Glue::Errors::EntityNotFoundException` si el job no existe.

### `ensure_job(job_name, role_arn:, script_location:, ...)` → Aws::Glue::Types::Job

Crea o actualiza un job de forma idempotente.

- Si el job existe → `update_job`
- Si el job no existe → `create_job`

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

## Convenciones de nombres

AWS Glue permite: letras (`a-zA-Z`), números (`0-9`), guiones (`-`). No permite guiones bajos ni espacios.

```ruby
# Válido
DataDrain::GlueRunner.job_exists?("my-export-job-v2")

# Inválido — lanza ConfigurationError
DataDrain::GlueRunner.job_exists?("my_export_job")
# DataDrain::ConfigurationError: job_name 'my_export_job' no es un nombre válido para Glue Job
```

## Eventos de telemetría

| Evento | Nivel | Descripción |
|--------|-------|-------------|
| `glue_runner.start` | INFO | Antes de `start_job_run` |
| `glue_runner.job_exists` | INFO | Job encontrado en `ensure_job` |
| `glue_runner.job_created` | INFO | Job creado en `ensure_job` |
| `glue_runner.polling` | INFO | Chequeo de estado durante `run_and_wait` |
| `glue_runner.complete` | INFO | Job terminó `SUCCEEDED` |
| `glue_runner.failed` | ERROR | Job falló con `FAILED\|STOPPED\|TIMEOUT` |
| `glue_runner.timeout` | ERROR | `max_wait_seconds` excedido |
