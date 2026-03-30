# DataDrain - Contexto de Desarrollo

## Arquitectura y Patrones Core

- **Engine (`DataDrain::Engine`):** Orquesta el flujo ETL: Conteo → Export → Verify → Purge. El paso de export es omitible con `skip_export: true` (para delegar a AWS Glue).
- **Storage Adapters (`DataDrain::Storage`):** Patrón Strategy. La instancia se cachea en `DataDrain::Storage.adapter`. Si `storage_mode` cambia en runtime, llamar `DataDrain::Storage.reset_adapter!` antes de la próxima operación.
- **Analytical ORM (`DataDrain::Record`):** Interfaz tipo ActiveRecord de solo lectura sobre Parquet vía DuckDB. Usa una conexión DuckDB por thread (`Thread.current[:data_drain_duckdb_conn]`) que se inicializa una vez y se reutiliza — nunca se cierra explícitamente. Tener en cuenta en Puma/Sidekiq.
- **Glue Orchestrator (`DataDrain::GlueRunner`):** Para tablas 1TB+. Patrón: `GlueRunner.run_and_wait(...)` seguido de `Engine.new(..., skip_export: true).call` para verificar + purgar.

## Convenciones Críticas

### Seguridad en Purga
`purge_from_postgres` nunca debe ejecutarse si `verify_integrity` devuelve `false`. La verificación matemática de conteos (Postgres vs Parquet) es el único gate de seguridad antes de borrar datos.

### Precisión de Fechas
Las consultas SQL de rango siempre deben usar **límites semi-abiertos**:
```sql
created_at >= 'START' AND created_at < 'END_BOUNDARY'
```
Donde `END_BOUNDARY` es el inicio del periodo siguiente (ej. `next_day.beginning_of_day`). Nunca usar `<= end_of_day` — los microsegundos en el límite pueden quedar fuera.

### Partition Keys — Orden y Contrato

El array `partition_keys` es **completamente dinámico** — cada tabla/modelo define el suyo. No existe un orden estándar en la librería.

**Regla crítica:** el orden de `partition_keys` al **escribir** (Engine/FileIngestor) debe ser idéntico al declarado en el modelo **Record** que lee esos archivos. Un mismatch genera paths que no coinciden y DuckDB retorna vacío sin error.

```ruby
# Escritura
Engine.new(partition_keys: %w[isp_id year month], ...)

# Lectura — debe coincidir
class ArchivedVersion < DataDrain::Record
  self.partition_keys = [:isp_id, :year, :month]
end
```

**Criterio de diseño del orden:** el primer key debe ser la dimensión de mayor cardinalidad o la que más se usa como filtro (ej. `isp_id` si las consultas son siempre por ISP). Esto determina la jerarquía de carpetas Hive y el rendimiento del prefix scan en S3.

### Idempotencia
Las exportaciones usan `OVERWRITE_OR_IGNORE 1` de DuckDB. Los procesos son seguros de reintentar.

### `idle_in_transaction_session_timeout`
El valor `0` **desactiva** el timeout (sin límite). Para purgas de gran volumen esto es mandatorio. Internamente, se debe validar con `!nil?` ya que `0.present?` es falso.

## Logging (Wispro-Observability-Spec v1)

La telemetría debe ser estructurada (KV) para ser procesada por `exis_ray`.

- **Formato:** `component=data_drain event=<clase>.<suceso> [campos]`
- **Unidades:** Prohibido incluir unidades en los valores (ej: NO usar "0.5s").
- **Tiempos:** Usar el sufijo `_s` en la key y valor `Float`. Ej: `duration_s=0.57`.
- **Contadores:** Usar la palabra `count` en la key y valor `Integer`. Ej: `pg_count=100`.
- **Naming:** Todas las llaves deben ser `snake_case`.
- **Automatización:** El campo `source` lo inyecta automáticamente `exis_ray` — no incluirlo manualmente.
- **DEBUG:** Siempre en forma de bloque: `logger.debug { "k=#{v}" }`.
- **Duraciones:** Usar siempre `Process.clock_gettime(Process::CLOCK_MONOTONIC)`.
- **Sensibilidad:** Filtrar datos sensibles (`password`, `token`, `secret`) → `[FILTERED]`.

## Código Ruby

- Todo código nuevo o modificado debe pasar `bundle exec rubocop` sin ofensas
- Documentación pública con YARD (`@param`, `@return`, `@raise`, `@example`)
- No modificar ni agregar YARD/comentarios a código existente no tocado

## Comandos

```bash
bundle exec rspec       # tests
bundle exec rubocop     # linting
bin/console             # REPL de desarrollo
```

## Rendimiento

- `limit_ram` y `tmp_directory` en la configuración evitan OOM en contenedores
- DuckDB usa spill-to-disk automáticamente cuando `tmp_directory` está seteado
