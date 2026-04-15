# Eventos y Telemetría

Catálogo completo de eventos KV emitidos por DataDrain. Formato Wispro-Observability-Spec v1.

## Convenciones

- Formato: `component=data_drain event=<clase>.<suceso> [campos]`
- `component=` y `event=` siempre primeros
- Tiempos: sufijo `_s`, valor Float redondeado a 2 decimales
- Contadores: sufijo `_count`, valor Integer
- Sin unidades en valores (NO `"0.5s"`, SÍ `0.5`)
- snake_case en todas las keys
- `source=` lo inyecta `exis_ray` automáticamente — DataDrain NO lo emite
- Secretos (`password|token|secret|api_key|auth`) → `[FILTERED]`
- Fallos del logger nunca rompen flujo principal

## Engine

### `engine.start`
**Nivel:** INFO. Emite al inicio de `#call`.
**Campos:** `table`, `start_date`, `end_date`.

### `engine.skip_empty`
**Nivel:** INFO. Emite cuando `pg_count == 0`.
**Campos:** `table`, `duration_s`, `db_query_duration_s`.

### `engine.skip_export`
**Nivel:** INFO. Emite cuando `skip_export: true`.
**Campos:** `table`.

### `engine.export_start`
**Nivel:** INFO. Emite antes del `COPY ... TO`.
**Campos:** `table`, `count`.

### `engine.integrity_check`
**Nivel:** INFO. Emite tras el conteo de Parquet (siempre que se pueda leer).
**Campos:** `table`, `pg_count`, `parquet_count`.

### `engine.parquet_read_error`
**Nivel:** ERROR. Emite si `read_parquet` levanta `DuckDB::Error` durante verify.
**Campos:** `table`, `error_class`, `error_message` (truncado a 200 chars).
**Consecuencia:** `verify_integrity` retorna `false`, purga abortada.

### `engine.purge_start`
**Nivel:** INFO. Emite antes del primer DELETE.
**Campos:** `table`, `batch_size`.

### `engine.purge_heartbeat`
**Nivel:** INFO. Emite cada 100 lotes durante purga.
**Campos:** `table`, `batches_processed_count`, `rows_deleted_count`.

### `engine.complete`
**Nivel:** INFO. Emite al final exitoso del flujo.
**Campos:** `table`, `duration_s`, `db_query_duration_s`, `export_duration_s`, `integrity_duration_s`, `purge_duration_s`, `count`.

### `engine.integrity_error`
**Nivel:** ERROR. Emite si `pg_count != parquet_count`.
**Campos:** `table`, `duration_s`, `count`.
**Consecuencia:** Retorna `false`, purga abortada.

---

## FileIngestor

### `file_ingestor.start`
**Nivel:** INFO. Emite al inicio de `#call`.
**Campos:** `source_path`.

### `file_ingestor.file_not_found`
**Nivel:** ERROR. Emite si `File.exist?(source_path) == false`.
**Campos:** `source_path`.

### `file_ingestor.count`
**Nivel:** INFO. Emite tras el conteo del archivo origen.
**Campos:** `source_path`, `count`, `source_query_duration_s`.

### `file_ingestor.skip_empty`
**Nivel:** INFO. Emite si conteo es `0`.
**Campos:** `source_path`, `duration_s`.

### `file_ingestor.export_start`
**Nivel:** INFO. Emite antes del `COPY ... TO`.
**Campos:** `dest_path`.

### `file_ingestor.complete`
**Nivel:** INFO. Emite al final exitoso.
**Campos:** `source_path`, `duration_s`, `source_query_duration_s`, `export_duration_s`, `count`.

### `file_ingestor.duckdb_error`
**Nivel:** ERROR. Emite si `DuckDB::Error` durante el proceso.
**Campos:** `source_path`, `error_class`, `error_message`, `duration_s`.

### `file_ingestor.cleanup`
**Nivel:** INFO. Emite tras borrar el archivo local (si `delete_after_upload`).
**Campos:** `source_path`.

---

## Record

### `record.destroy_all`
**Nivel:** INFO. Emite al inicio de `.destroy_all`.
**Campos:** `folder`, `partitions` (inspect del hash).

### `record.parquet_not_found`
**Nivel:** WARN. Emite si `read_parquet` levanta `DuckDB::Error` en queries `where`/`find`.
**Campos:** `error_class`, `error_message`.
**Consecuencia:** Retorna `[]` (no levanta).

---

## GlueRunner

### `glue_runner.start`
**Nivel:** INFO. Emite antes de `start_job_run`.
**Campos:** `job`.

### `glue_runner.script_uploaded`
**Nivel:** INFO. Emite tras subir un script a S3 (v0.5.0+).
**Campos:** `local_path`, `s3_path`, `bytes`.

### `glue_runner.script_upload_error`
**Nivel:** ERROR. Emite si el upload a S3 falla (v0.5.0+).
**Campos:** `local_path`, `bucket`, `error_class`, `error_message`.
**Consecuencia:** propaga el `Aws::S3::Errors::ServiceError`.

### `glue_runner.job_exists`
**Nivel:** INFO. Emite en `ensure_job` cuando el job ya existe y se actualiza.
**Campos:** `job`.

### `glue_runner.job_created`
**Nivel:** INFO. Emite en `ensure_job` cuando el job se crea.
**Campos:** `job`.

### `glue_runner.polling`
**Nivel:** INFO. Emite cada chequeo de estado mientras Job no terminó.
**Campos:** `job`, `run_id`, `status`, `next_check_in_s`.

### `glue_runner.complete`
**Nivel:** INFO. Emite cuando estado es `SUCCEEDED`.
**Campos:** `job`, `run_id`, `duration_s`.

### `glue_runner.failed`
**Nivel:** ERROR. Emite cuando estado es `FAILED|STOPPED|TIMEOUT`.
**Campos:** `job`, `run_id`, `status`, `duration_s`, `error_message` (si Glue lo provee, truncado a 200 chars).
**Consecuencia:** `raise RuntimeError`.

### `glue_runner.timeout`
**Nivel:** ERROR. Emite cuando `max_wait_seconds` excede antes de `SUCCEEDED`.
**Campos:** `job`, `run_id`, `max_wait_seconds`.
**Consecuencia:** `raise DataDrain::Error`.

---

## Ejemplos reales

```
component=data_drain event=engine.start table=versions start_date=2025-10-01 end_date=2025-11-01
component=data_drain event=engine.export_start table=versions count=1500000
component=data_drain event=engine.integrity_check table=versions pg_count=1500000 parquet_count=1500000
component=data_drain event=engine.purge_heartbeat table=versions batches_processed_count=100 rows_deleted_count=500000
component=data_drain event=engine.complete table=versions duration_s=185.4 db_query_duration_s=2.1 export_duration_s=42.7 integrity_duration_s=18.3 purge_duration_s=122.3 count=1500000

component=data_drain event=file_ingestor.complete source_path=/tmp/netflow.csv duration_s=12.4 source_query_duration_s=0.8 export_duration_s=11.2 count=4500000

component=data_drain event=glue_runner.polling job=my-export-job run_id=jr_abc123 status=RUNNING next_check_in_s=30
component=data_drain event=glue_runner.failed job=my-export-job run_id=jr_abc123 status=FAILED duration_s=301.0 error_message='Out of memory in executor 4'
```

## Cómo agregar un nuevo evento

1. En la clase, asegurar `include Observability` (instance) o `extend Observability` + `private_class_method :safe_log, :exception_metadata, :observability_name` (class).
2. Asegurar `@logger = config.logger` (instance: en `initialize`; class: antes del primer `safe_log`).
3. Llamar `safe_log(:level, "clase.suceso", { campo1: val1, campo2: val2 })`.
4. Validar: keys snake_case, tiempos `_s` Float, contadores `_count` Integer, sin unidades en valores, sin `source=`.
5. Para errores: incluir `exception_metadata(e)` mergeado al hash de campos.
