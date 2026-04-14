# DataDrain — Plan de Mejora v0.2.0 → v0.3.1

**Versión actual:** 0.2.0
**Última actualización:** 2026-04-13
**Owner:** Gabriel
**Estado global:** No iniciado

Documento de seguimiento para coordinar la evolución de la gema con otros agentes (Claude, Gemini) y revisores humanos. Cada item es autocontenido: contexto, cambios, archivos afectados, criterios de aceptación, riesgos.

---

## Índice

- [Resumen ejecutivo](#resumen-ejecutivo)
- [Convenciones del documento](#convenciones-del-documento)
- [Releases planificados](#releases-planificados)
- [Items detallados](#items-detallados)
  - [P0 — Seguridad y correctitud (v0.2.0)](#p0--seguridad-y-correctitud-v020)
  - [P1 — Performance y robustez (v0.2.1 / v0.3.0)](#p1--performance-y-robustez-v021--v030)
  - [P2 — Calidad y DX (v0.3.1)](#p2--calidad-y-dx-v031)
- [Riesgos transversales](#riesgos-transversales)
- [Checklist de release](#checklist-de-release)

---

## Resumen ejecutivo

DataDrain v0.1.19 es una gema bien arquitecturada (Storage Adapter, Observability, thread-local DuckDB) con observabilidad estructurada de clase empresarial. Sin embargo presenta:

- **Riesgos de seguridad moderados:** SQL injection en `table_name`/`select_sql`, credenciales S3 interpoladas en queries DuckDB.
- **Cobertura de tests baja:** solo 4 specs, sin cobertura de Record/Storage/GlueRunner.
- **Memory leak potencial:** conexión DuckDB thread-local sin cleanup.
- **Documentación de tuning ausente:** sin guía para purgas masivas, índices, particionamiento.

Este plan agrupa 17 items en 4 releases incrementales (v0.2.0 → v0.3.1) priorizados por impacto.

---

## Convenciones del documento

### Estados

- `[ ]` no iniciado
- `[~]` en progreso
- `[x]` completado
- `[!]` bloqueado o requiere decisión

### Prioridades

- **P0** — bloqueante para producción enterprise. Hardening esencial.
- **P1** — mejora robustez/performance significativa. No bloqueante.
- **P2** — calidad de vida del desarrollador, no afecta runtime.

### Etiquetas de tipo

- `feat` — funcionalidad nueva
- `fix` — corrección de bug
- `refactor` — reorganización sin cambio de comportamiento
- `docs` — documentación
- `test` — agregar/mejorar tests
- `security` — corrección o hardening de seguridad
- `perf` — performance
- `chore` — infra (CI, gemspec, etc.)

### Compatibilidad

Cada item indica si es **breaking** o **backward-compatible**. Breaking changes exigen bump de minor (v0.2.0 → v0.3.0) según la política semver pre-1.0.

---

## Releases planificados

### v0.2.0 — Hardening de seguridad y testing
**Foco:** cerrar gaps P0. Producción-ready para datos sensibles.
**Items:** 1, 2, 3, 4
**Breaking:** parcial (item 1 y 2 cambian comportamiento si caller dependía del modo viejo).

### v0.2.1 — Robustez operacional
**Foco:** validaciones, timeouts, alertas, docs de tuning.
**Items:** 5, 7, 8, 9, 11a
**Breaking:** no.

### v0.3.0 — Refactor y observabilidad avanzada
**Foco:** simplificar Engine, sandboxing DuckDB, alertas runtime.
**Items:** 6, 10, 11b
**Breaking:** no (refactor interno).

### v0.3.1 — Calidad de código y DX
**Foco:** YARD, CI, deduplicación, DuckDB Friendly SQL.
**Items:** 12, 13, 14, 15, 16
**Breaking:** no.

---

## Items detallados

### P0 — Seguridad y correctitud (v0.2.0)

---

#### Item 1 — Migrar credenciales S3 a `credential_chain` de DuckDB

**Estado:** `[x]`
**Prioridad:** P0
**Tipo:** `security` `feat`
**Compatibilidad:** backward-compatible (con fallback al modo explícito)
**Estimación:** S (2-4h)

##### Contexto

`Storage::S3#setup_duckdb` interpola credenciales AWS directamente en queries DuckDB:

```ruby
connection.query("SET s3_access_key_id='#{@config.aws_access_key_id}';")
connection.query("SET s3_secret_access_key='#{@config.aws_secret_access_key}';")
```

Las credenciales quedan en el proceso DuckDB y, si el query log de DuckDB se activa, en logs. La skill DuckDB oficial `read-file` muestra el patrón moderno: `CREATE SECRET (TYPE S3, PROVIDER credential_chain)` que usa el AWS credential chain (IAM roles, env vars, `~/.aws/credentials`).

##### Cambios

1. En `lib/data_drain/storage/s3.rb#setup_duckdb`:
   - Si `aws_access_key_id` está seteado en config → modo explícito (compatibilidad):
     ```sql
     CREATE OR REPLACE SECRET s3_secret (
       TYPE S3,
       KEY_ID '...',
       SECRET '...',
       REGION '...'
     );
     ```
   - Si NO está seteado → `credential_chain`:
     ```sql
     CREATE OR REPLACE SECRET s3_secret (
       TYPE S3,
       PROVIDER credential_chain,
       REGION '...'
     );
     ```
2. Reemplazar todos los `SET s3_*` por el `CREATE SECRET`.
3. Documentar en CLAUDE.md / SKILL.md el nuevo comportamiento.

##### Archivos afectados

- `lib/data_drain/storage/s3.rb` (cambio principal)
- `lib/data_drain/configuration.rb` (sin cambios pero documentar opcionalidad de aws_*)
- `CLAUDE.md` (sección "Seguridad")
- `skill/SKILL.md` y `skill/references/api-detallada.md`
- `README.md` (sección Configuración: aclarar que `aws_access_key_id`/`aws_secret_access_key` son opcionales si hay IAM role / env vars)
- `CHANGELOG.md`
- `spec/data_drain/storage/s3_spec.rb` (nuevo, ver item 4)

##### Criterios de aceptación

- [ ] `Storage::S3#setup_duckdb` usa `CREATE SECRET` en lugar de `SET s3_*`.
- [ ] Si `aws_access_key_id` y `aws_secret_access_key` están seteados, usa modo explícito (KEY_ID/SECRET).
- [ ] Si están vacíos, usa `credential_chain`.
- [ ] Test integración con env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) confirma que `credential_chain` resuelve correctamente.
- [ ] Test que con credenciales explícitas también funciona.
- [ ] CHANGELOG documenta el cambio y la backward-compat.

##### Riesgos

- **Versión de DuckDB:** `CREATE SECRET` requiere DuckDB ≥ 0.10. La gema requiere `~> 1.4`, OK.
- **Permisos IAM rol insuficientes:** caller puede tener config con KEY/SECRET por costumbre y al borrarlos esperando IAM rol, descubrir que el rol no tiene `s3:GetObject`. Documentar en CHANGELOG.
- **Compatibilidad regional:** `REGION` debe seguir siendo obligatorio en config.

##### Notas para el revisor

- Verificar que el `CREATE OR REPLACE SECRET` no rompe si se llama múltiples veces en la misma conexión DuckDB.
- Confirmar que el `secret_name` (`s3_secret`) no entra en conflicto con secrets pre-existentes en una sesión compartida.

---

#### Item 2 — Validación regex de `table_name`, `primary_key` (anti-SQL injection)

**Estado:** `[x]`
**Prioridad:** P0
**Tipo:** `security` `fix`
**Compatibilidad:** backward-compatible (rechaza inputs que antes pasaban silenciosamente)
**Estimación:** S (1-2h)

##### Contexto

`Engine#initialize` acepta `table_name`, `primary_key`, `select_sql`, `where_clause` y los interpola en SQL sin validación. Aunque la gema asume "caller trusted", una validación cheap del identificador SQL en `table_name` y `primary_key` cierra el vector más obvio.

`select_sql` y `where_clause` se documentan explícitamente como SQL crudo trusted (no se validan).

##### Cambios

1. En `Engine#initialize` agregar validación:
   ```ruby
   IDENTIFIER_REGEX = /\A[a-zA-Z_][a-zA-Z0-9_]*\z/.freeze

   def initialize(options)
     # ...
     @table_name  = options.fetch(:table_name)
     @primary_key = options.fetch(:primary_key, "id")

     unless IDENTIFIER_REGEX.match?(@table_name)
       raise DataDrain::ConfigurationError,
             "table_name '#{@table_name}' no es un identificador SQL válido"
     end
     unless IDENTIFIER_REGEX.match?(@primary_key)
       raise DataDrain::ConfigurationError,
             "primary_key '#{@primary_key}' no es un identificador SQL válido"
     end
     # ...
   end
   ```
2. Agregar mismo guard en `FileIngestor#initialize` para `folder_name`.
3. Documentar en `skill/references/antipatrones.md` (item 13 ya existe — actualizar).

##### Archivos afectados

- `lib/data_drain/engine.rb`
- `lib/data_drain/file_ingestor.rb`
- `skill/references/antipatrones.md`
- `CLAUDE.md` (sección "Seguridad")
- `CHANGELOG.md`
- `spec/data_drain/engine_spec.rb` (agregar tests de validación)

##### Criterios de aceptación

- [ ] `Engine.new(table_name: "; DROP TABLE foo; --")` levanta `ConfigurationError`.
- [ ] `Engine.new(primary_key: "id; DROP")` levanta `ConfigurationError`.
- [ ] Identificadores válidos (`"versions"`, `"my_table_2"`) pasan.
- [ ] Tests cubren happy + sad paths.
- [ ] Documentado en CHANGELOG y antipatrones.

##### Riesgos

- **Tablas con schemas (`schema.table`):** la regex actual no acepta `.`. Si algún caller usa `"public.versions"`, romperá. Decisión: forzar `table_name` solo (sin schema) y mantener el `public.` hardcodeado en el SQL como ya está. Documentar.
- **Tablas con mayúsculas comilladas:** PostgreSQL acepta `"WeirdTable"` con comillas. La gema NO lo soporta hoy. Mantener restricción.

##### Notas para el revisor

- Verificar que ningún caller del repo Wispro pasa `table_name` con `.` o caracteres especiales.

---

#### Item 3 — Cleanup de conexión DuckDB thread-local

**Estado:** `[x]`
**Prioridad:** P0
**Tipo:** `fix` `feat`
**Compatibilidad:** backward-compatible (agrega API, no quita)
**Estimación:** M (4-6h)

##### Contexto

`Record.connection` cachea `Thread.current[:data_drain_duckdb] = { db:, conn: }` indefinidamente. En Puma/Sidekiq donde los threads son reutilizados, la conexión DuckDB persiste mientras vive el thread (potencialmente días). No hay API para cerrarla.

Riesgos:
- Memoria DuckDB (caches internos) crece sin liberarse.
- Configuración stale: si cambia `storage_mode` o credenciales, la conexión cacheada queda inválida.

##### Cambios

1. Agregar `Record.disconnect!` (método de clase):
   ```ruby
   def self.disconnect!
     return unless Thread.current[:data_drain_duckdb]

     entry = Thread.current.delete(:data_drain_duckdb)
     entry[:conn]&.close
     entry[:db]&.close
   rescue StandardError
     # silencio en cleanup
   end
   ```
2. Documentar en CLAUDE.md uso recomendado:
   - **Sidekiq:** middleware server que llama `Record.disconnect!` después de cada job.
   - **Puma:** llamar en `on_worker_shutdown` / hooks de lifecycle.
3. Considerar `at_exit { Record.disconnect! }` como safety net (opcional, debatir).
4. Evaluar agregar `Record.reconnect!` (cierra + lazy reabre en next call).

##### Archivos afectados

- `lib/data_drain/record.rb`
- `CLAUDE.md` (sección "Conexiones thread-local")
- `skill/references/api-detallada.md` (sección Record)
- `skill/references/antipatrones.md` (actualizar item 12)
- `CHANGELOG.md`
- `spec/data_drain/record_spec.rb` (nuevo)

##### Criterios de aceptación

- [ ] `Record.disconnect!` existe y limpia `Thread.current`.
- [ ] Llamarlo dos veces seguidas no rompe (idempotente).
- [ ] Después de `disconnect!`, la próxima query reabre conexión nueva.
- [ ] Test simula múltiples threads con conexiones independientes.
- [ ] Test confirma que un thread no afecta a otro al desconectar.
- [ ] Documentación incluye snippet Sidekiq middleware.

##### Riesgos

- **Race condition:** si un thread está en medio de una query y otro thread llama `disconnect!`... pero `disconnect!` solo afecta `Thread.current`, así que es seguro.
- **Snippet Sidekiq:** verificar que el middleware corre incluso en jobs que fallan (probablemente sí, por `ensure`).

##### Notas para el revisor

- Confirmar que `DuckDB::Connection#close` es seguro de llamar incluso si la conexión nunca ejecutó queries.

---

#### Item 4 — Cobertura de tests P0 (Record, Storage, GlueRunner, Observability)

**Estado:** `[x]`
**Prioridad:** P0
**Tipo:** `test`
**Compatibilidad:** N/A
**Estimación:** L (1-2 días)

##### Contexto

Cobertura actual: 4 specs (Engine: 2, FileIngestor: 1, version: 1). Sin tests de Record, Storage::*, GlueRunner, Observability, Configuration. Bloquea confianza para refactors futuros (item 10).

##### Cambios

Crear specs para cada componente. Estructura sugerida:

```
spec/
  data_drain/
    engine_spec.rb              [existe, agregar tests de validación item 2]
    file_ingestor_spec.rb       [existe, agregar tests de validación item 2]
    record_spec.rb              [NUEVO]
    glue_runner_spec.rb         [NUEVO]
    observability_spec.rb       [NUEVO]
    configuration_spec.rb       [NUEVO]
    storage_spec.rb             [NUEVO — factory]
    storage/
      local_spec.rb             [NUEVO]
      s3_spec.rb                [NUEVO]
    types/
      json_type_spec.rb         [NUEVO]
```

##### Tests por componente

**`record_spec.rb`:**
- `.where` con partition_keys completas y parciales
- `.find` con id que existe / no existe
- `.find` sanitiza id con comilla simple (`"foo' OR 1=1"`)
- `.destroy_all` delega correctamente al adapter
- `.connection` es thread-local (test con 2 threads)
- `.disconnect!` (item 3)
- `.where` retorna `[]` si Parquet no existe (no levanta)
- `build_query_path` respeta orden de `partition_keys` no de kwargs

**`storage/local_spec.rb`:**
- `prepare_export_path` crea directorio
- `build_path` arma path correcto con/sin partition
- `destroy_partitions` con todas las keys → borra directorio específico
- `destroy_partitions` con keys parciales → wildcard glob
- `destroy_partitions` retorna count correcto

**`storage/s3_spec.rb`:**
- `setup_duckdb` ejecuta `CREATE SECRET` (item 1)
- `build_path` retorna `s3://...`
- `destroy_partitions` mockeado con `Aws::S3::Client.stub_responses`:
  - prefix correcto
  - regex matching
  - batches de 1000
  - retorna count

**`glue_runner_spec.rb`:**
- `run_and_wait` con stub `SUCCEEDED` retorna `true`
- `run_and_wait` con `FAILED` levanta `RuntimeError`
- `run_and_wait` con `STOPPED` y `TIMEOUT` ídem
- Polling: stub que devuelve `RUNNING` 2 veces y luego `SUCCEEDED`
- `error_message` truncado a 200 chars
- Logs emitidos con campos correctos

**`observability_spec.rb`:**
- `safe_log` no-op si `@logger` es nil
- `safe_log` formato KV (`component=X event=Y k=v`)
- `safe_log` filtra secretos (regex después de item 9)
- `safe_log` `rescue StandardError` no propaga
- `exception_metadata` trunca message a 200, escapa `"`
- `observability_name` extrae primer namespace en snake_case
- Funciona con `include` (instance) y `extend` (class)

**`configuration_spec.rb`:**
- Defaults correctos
- `duckdb_connection_string` formato URI correcto
- `idle_in_transaction_session_timeout = 0` se incluye (no se ignora)
- `Configuration#validate!` (item 8) levanta cuando falta config

**`storage_spec.rb`:**
- `Storage.adapter` retorna Local cuando `:local`
- `Storage.adapter` retorna S3 cuando `:s3`
- `Storage.adapter` levanta `InvalidAdapterError` con modo desconocido
- `Storage.adapter` cachea (misma instancia entre llamadas)
- `Storage.reset_adapter!` invalida cache

**`types/json_type_spec.rb`:**
- `cast` con String JSON válido → Hash
- `cast` con String JSON inválido → retorna String original (no levanta)
- `cast` con Hash → retorna Hash
- `cast` con nil → nil

##### Archivos afectados

- 9 archivos de spec nuevos
- `spec/spec_helper.rb` (posibles helpers compartidos)

##### Criterios de aceptación

- [ ] Cobertura medida con SimpleCov ≥ 80% líneas.
- [ ] `bundle exec rspec` corre en < 30s.
- [ ] No hay tests que dependan de S3 real (todo mockeado).
- [ ] Tests de Engine e Integration pueden requerir Postgres real (documentar en README).
- [ ] CI corre todo el suite sin flakes (10 corridas seguidas pasan).

##### Riesgos

- **Mocking AWS:** `aws-sdk-s3 ~> 1.114` soporta `Client.stub_responses`. Verificar versión instalada.
- **Mocking DuckDB:** no hay mock library nativa. Usar archivos Parquet de fixture en `spec/fixtures/`.
- **Tests con Postgres real:** decidir si CI levanta Postgres (Docker) o si Engine specs se marcan `:integration` y se corren aparte.

##### Notas para el revisor

- Definir convención de fixtures (dónde, formato).
- Decidir umbral de SimpleCov mínimo (sugerido 80%).

---

### P1 — Performance y robustez (v0.2.1 / v0.3.0)

---

#### Item 5 — VACUUM ANALYZE opcional post-purga

**Estado:** `[ ]`
**Prioridad:** P1
**Tipo:** `feat` `perf`
**Compatibilidad:** backward-compatible (default `false`, opt-in)
**Estimación:** S (2-3h)
**Release:** v0.2.1

##### Contexto

Purgar millones de rows deja dead tuples en Postgres. Sin `VACUUM`, el espacio no se libera y el siguiente seq scan recorre páginas vacías. En tablas no particionadas esto degrada performance progresivamente.

##### Cambios

1. Agregar a `Configuration`:
   ```ruby
   attr_accessor :vacuum_after_purge  # default: false
   ```
2. En `Engine#purge_from_postgres`, al final (después del loop, dentro del mismo `begin`/`ensure`):
   ```ruby
   if @config.vacuum_after_purge && total_deleted.positive?
     vacuum_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
     dead_before = fetch_dead_tuple_count(conn)
     conn.exec("VACUUM ANALYZE #{@table_name};")
     dead_after = fetch_dead_tuple_count(conn)
     vacuum_duration = Process.clock_gettime(...) - vacuum_start

     safe_log(:info, "engine.vacuum_complete", {
       table: @table_name,
       duration_s: vacuum_duration.round(2),
       dead_tuples_before: dead_before,
       dead_tuples_after: dead_after
     })
   end
   ```
3. Helper `fetch_dead_tuple_count(conn)` que consulta `pg_stat_user_tables`.

##### Archivos afectados

- `lib/data_drain/configuration.rb`
- `lib/data_drain/engine.rb`
- `skill/references/eventos-telemetria.md` (agregar `engine.vacuum_complete`)
- `CLAUDE.md`
- `README.md` (mencionar opción)
- `CHANGELOG.md`
- `spec/data_drain/engine_spec.rb`

##### Criterios de aceptación

- [ ] `vacuum_after_purge = true` ejecuta VACUUM ANALYZE post-purga.
- [ ] No corre si `total_deleted == 0`.
- [ ] No corre si la verificación de integridad falló (purga abortada).
- [ ] Emite `engine.vacuum_complete` con métricas.
- [ ] VACUUM no bloquea por errores (rescue + log warning).

##### Riesgos

- **VACUUM no se puede correr dentro de transacción.** El método `conn.exec` directo está bien (autocommit). Verificar que no estamos en bloque BEGIN.
- **`VACUUM ANALYZE` es costoso.** En tablas grandes puede tardar horas. Documentar.
- **`VACUUM FULL` ≠ `VACUUM`.** No usar FULL — bloquea la tabla.
- **Permisos:** usuario Postgres debe tener `VACUUM` privilege (owner de tabla o `MAINTAIN` en PG16+).

##### Notas para el revisor

- ¿Permitir `vacuum_after_purge` por tabla, no solo global? Por ahora global, simplifica.

---

#### Item 7 — `max_wait_seconds` en `GlueRunner.run_and_wait`

**Estado:** `[ ]`
**Prioridad:** P1
**Tipo:** `feat`
**Compatibilidad:** backward-compatible
**Estimación:** S (1-2h)
**Release:** v0.2.1

##### Contexto

`GlueRunner.run_and_wait` tiene un loop de polling sin timeout. Si Glue queda colgado en `RUNNING`, bloquea indefinidamente.

##### Cambios

1. Agregar parámetro `max_wait_seconds:` (default `nil` = sin límite):
   ```ruby
   def self.run_and_wait(job_name, arguments = {}, polling_interval: 30, max_wait_seconds: nil)
     # ...
     loop do
       if max_wait_seconds && (Process.clock_gettime(...) - start_time) > max_wait_seconds
         safe_log(:error, "glue_runner.timeout", { job: job_name, run_id:, max_wait_seconds: })
         raise DataDrain::Error, "Glue Job #{job_name} excedió max_wait_seconds=#{max_wait_seconds}"
       end
       # ...
     end
   end
   ```

##### Archivos afectados

- `lib/data_drain/glue_runner.rb`
- `skill/references/eventos-telemetria.md` (agregar `glue_runner.timeout`)
- `skill/references/antipatrones.md` (actualizar item 14 — ya menciona la falta)
- `README.md`
- `CHANGELOG.md`
- `spec/data_drain/glue_runner_spec.rb`

##### Criterios de aceptación

- [ ] Sin `max_wait_seconds` (default), comportamiento idéntico al actual.
- [ ] Con `max_wait_seconds: 60`, si polling tarda > 60s en SUCCEEDED, levanta `DataDrain::Error`.
- [ ] Emite log `glue_runner.timeout`.
- [ ] No interfiere con detección de `FAILED|STOPPED|TIMEOUT` de Glue (esos son estados, no timeout local).

##### Riesgos

- Ninguno significativo.

---

#### Item 8 — `Configuration#validate!`

**Estado:** `[ ]`
**Prioridad:** P1
**Tipo:** `feat`
**Compatibilidad:** backward-compatible (validación opcional explícita)
**Estimación:** S (2-3h)
**Release:** v0.2.1

##### Contexto

`Configuration` no valida defaults ni invariantes. Errores comunes (storage_mode inválido, credenciales faltantes con `:s3`, db_* faltantes en Engine) se manifiestan tarde con errores oscuros (`NoMethodError`, `Aws::Errors`, `PG::ConnectionBad`).

##### Cambios

1. Agregar `Configuration#validate!`:
   ```ruby
   def validate!
     validate_storage_mode!
     validate_aws_config! if storage_mode.to_sym == :s3
     # NO validar db_* acá — depende de si se usa Engine o no
   end

   def validate_for_engine!
     validate!
     %i[db_host db_port db_user db_name].each do |attr|
       val = send(attr)
       raise ConfigurationError, "config.#{attr} es obligatorio" if val.nil? || val.to_s.empty?
     end
   end

   private

   def validate_storage_mode!
     return if [:local, :s3].include?(storage_mode.to_sym)

     raise ConfigurationError, "storage_mode debe ser :local o :s3, recibido #{storage_mode.inspect}"
   end

   def validate_aws_config!
     raise ConfigurationError, "aws_region es obligatorio con storage_mode = :s3" if aws_region.nil?
     # NO validar key_id / secret — el credential_chain (item 1) puede usar IAM rol
   end
   ```
2. Llamar `validate_for_engine!` al inicio de `Engine#initialize`.
3. Llamar `validate!` al inicio de `FileIngestor#initialize` (no requiere db_*).
4. Llamar `validate!` al inicio de `GlueRunner.run_and_wait` (requiere `aws_region`).

##### Archivos afectados

- `lib/data_drain/configuration.rb`
- `lib/data_drain/engine.rb`
- `lib/data_drain/file_ingestor.rb`
- `lib/data_drain/glue_runner.rb`
- `CHANGELOG.md`
- `spec/data_drain/configuration_spec.rb`

##### Criterios de aceptación

- [ ] `Engine.new` con `storage_mode = :foo` levanta `ConfigurationError` claro.
- [ ] `Engine.new` con `storage_mode = :s3` y `aws_region = nil` levanta.
- [ ] `Engine.new` sin `db_host` levanta.
- [ ] `FileIngestor.new` con `storage_mode = :s3` y `aws_region = nil` levanta.
- [ ] `Configuration#validate!` puede llamarse manualmente desde el initializer del cliente.

##### Riesgos

- **Backward-compat:** si algún caller actual tiene config medio rota (ej. `db_user=""`) y andaba "de casualidad", romperá. Documentar en CHANGELOG con nota explícita.

---

#### Item 9 — Filtro de secretos por regex en Observability

**Estado:** `[ ]`
**Prioridad:** P1
**Tipo:** `security`
**Compatibilidad:** backward-compatible (filtra más, no menos)
**Estimación:** XS (30min)
**Release:** v0.2.1

##### Contexto

`Observability#safe_log` filtra solo claves exactas:
```ruby
%i[password token secret api_key auth].include?(k.to_sym)
```
No filtra `db_password`, `aws_secret_access_key`, `bearer_token`, etc.

##### Cambios

```ruby
SENSITIVE_KEY_PATTERN = /password|passwd|pass|secret|token|api_key|apikey|auth|credential|private_key/i.freeze

# en safe_log:
val = SENSITIVE_KEY_PATTERN.match?(k.to_s) ? "[FILTERED]" : v
```

##### Archivos afectados

- `lib/data_drain/observability.rb`
- `spec/data_drain/observability_spec.rb`
- `CHANGELOG.md`

##### Criterios de aceptación

- [ ] `safe_log(:info, "x", { db_password: "x" })` emite `db_password=[FILTERED]`.
- [ ] `safe_log(:info, "x", { aws_secret_access_key: "x" })` emite filtrado.
- [ ] `safe_log(:info, "x", { bearer_token: "x" })` emite filtrado.
- [ ] `safe_log(:info, "x", { user_id: 42 })` no se filtra (no match).
- [ ] Coordinado con global standards (`/Users/gabriel/.claude/CLAUDE.md` línea "Filter sensitive keys (password|pass|passwd|secret|token|api_key|auth) → [FILTERED]").

##### Riesgos

- **Falsos positivos:** `authorization_id` matchearía con `auth`. ¿Aceptable? Sí, mejor false positive que leak.

---

#### Item 11a — Documentación de Postgres tuning por tamaño de tabla

**Estado:** `[ ]`
**Prioridad:** P1
**Tipo:** `docs`
**Compatibilidad:** N/A
**Estimación:** M (4-6h)
**Release:** v0.2.1

##### Contexto

DataDrain hoy no documenta cómo tunear Postgres para purgas masivas. Items recurrentes:
- ¿Qué índice ayuda al DELETE en lotes?
- ¿Cuándo migrar a particionamiento?
- ¿Cómo diagnosticar purgas lentas?

##### Cambios

Crear `skill/references/postgres-tuning.md` y referenciarlo desde:
- `skill/SKILL.md` (sección "Referencias")
- `CLAUDE.md` (sección nueva "Postgres tuning")

Contenido:

1. **Tabla de decisión por tamaño:**
   | Tamaño | Estrategia |
   |--------|-----------|
   | <10GB | Índice composite `(created_at, pk)` con `CREATE INDEX CONCURRENTLY` |
   | 10-100GB | Mismo + `SET maintenance_work_mem='4GB'` + checklist |
   | 100GB-1TB | Particionamiento declarativo por mes |
   | >1TB | Particionamiento obligatorio + `DROP PARTITION` reemplaza DELETE |

2. **Checklist pre-`CREATE INDEX CONCURRENTLY`:**
   - Tamaño actual: `SELECT pg_size_pretty(pg_total_relation_size('table'));`
   - Espacio libre disco (>2x tabla)
   - `SET maintenance_work_mem = '4GB'`
   - `SET statement_timeout = 0`
   - Ventana baja carga
   - Plan rollback (DROP INDEX CONCURRENTLY si saturas I/O)

3. **Riesgos `CONCURRENTLY`:**
   - 2 pasadas (puede tardar horas en 500GB)
   - I/O sostenido
   - Puede fallar y dejar índice INVALID
   - Espacio disco alto

4. **VACUUM ANALYZE post-purga** (link a item 5).

5. **Diagnóstico de purga lenta:**
   ```sql
   EXPLAIN (ANALYZE, BUFFERS) DELETE FROM versions WHERE id IN (...);
   SELECT * FROM pg_stat_activity WHERE query LIKE '%versions%';
   SELECT * FROM pg_stat_user_tables WHERE relname = 'versions';
   ```

6. **Migración a particionamiento:**
   ```sql
   CREATE TABLE versions (...) PARTITION BY RANGE (created_at);
   CREATE TABLE versions_2026_03 PARTITION OF versions
     FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
   ```
   Con esto cada `Engine#call` mensual puede reducirse a `DROP TABLE versions_2026_03` (instant). DataDrain podría agregar soporte nativo en futuro (item out-of-scope ahora).

##### Archivos afectados

- `skill/references/postgres-tuning.md` (NUEVO)
- `skill/SKILL.md` (sección Referencias)
- `CLAUDE.md` (sección Postgres tuning)
- `CHANGELOG.md`

##### Criterios de aceptación

- [ ] `postgres-tuning.md` cubre las 4 categorías de tamaño.
- [ ] Incluye SQL ejecutables y verificados (no pseudo-código).
- [ ] Incluye checklist pre-índice.
- [ ] Linkea con `engine.purge_heartbeat` y `engine.vacuum_complete`.
- [ ] Linkea con item 11b (warning runtime).

##### Riesgos

- Ninguno (es docs).

---

#### Item 6 — Sandboxing de `Record.connection`

**Estado:** `[ ]`
**Prioridad:** P1
**Tipo:** `security`
**Compatibilidad:** backward-compatible (con risk de breaking si caller hizo workarounds raros)
**Estimación:** M (3-4h)
**Release:** v0.3.0

##### Contexto

`Record.connection` es read-only por diseño (consultas Parquet). DuckDB skill `query` sugiere sandboxing post-setup:
```sql
SET enable_external_access=false  -- pero S3 ya cargado vía httpfs queda activo
SET lock_configuration=true
```
Reduce blast radius si alguien intenta inyectar SQL malicioso vía `where_clause` (improbable hoy pero defensa en profundidad).

##### Cambios

1. En `Record.connection`, después de `setup_duckdb`:
   ```ruby
   conn.query("SET lock_configuration=true;")
   ```
2. Probar que httpfs y secretos cargados previamente siguen funcionando.
3. NO setear `enable_external_access=false` porque rompe S3 — verificar.
4. NO setear `allowed_paths` porque la lista es dinámica (cada query distinta).

##### Archivos afectados

- `lib/data_drain/record.rb`
- `spec/data_drain/record_spec.rb`
- `CHANGELOG.md`

##### Criterios de aceptación

- [ ] `lock_configuration` activado tras setup.
- [ ] Test confirma que `Record.where(...)` sigue funcionando con S3.
- [ ] Test que intenta `SET memory_limit='1KB'` post-setup falla (locked).

##### Riesgos

- **Compatibilidad:** algún caller exótico podría haber dependido de cambiar config en runtime sobre la conexión thread-local. Improbable.

---

#### Item 10 — Refactor `Engine#call` (CC=13 → ~5)

**Estado:** `[ ]`
**Prioridad:** P1
**Tipo:** `refactor`
**Compatibilidad:** backward-compatible
**Estimación:** M (4-6h)
**Release:** v0.3.0

##### Contexto

`Engine#call` tiene complejidad ciclomática 13 (alta). Hace todo: setup, count, export condicional, verify, purge, logging granular. Difícil de testear en aislamiento y de extender.

##### Cambios

1. Extraer en métodos privados:
   ```ruby
   def call
     start_time = monotonic
     log_start

     setup_duckdb
     return skip_empty if step_count.zero?
     step_export unless @skip_export
     return integrity_failed unless step_verify

     step_purge
     log_complete(start_time)
     true
   end

   private

   def step_count
     timed(:db_query) { @pg_count = get_postgres_count }
     @pg_count
   end

   def step_export
     log(:info, "engine.export_start", count: @pg_count)
     timed(:export) { export_to_parquet }
   end

   def step_verify
     timed(:integrity) { verify_integrity }
   end

   def step_purge
     timed(:purge) { purge_from_postgres }
   end

   def timed(name)
     t = monotonic
     yield
     @durations[name] = monotonic - t
   end
   ```
2. `@durations` hash acumula los timings, `log_complete` lo reporta.

##### Archivos afectados

- `lib/data_drain/engine.rb`
- `spec/data_drain/engine_spec.rb` (agregar tests granulares por step)
- `CHANGELOG.md`

##### Criterios de aceptación

- [ ] CC de `#call` ≤ 6 (medido con `rubocop --only Metrics/CyclomaticComplexity`).
- [ ] Eventos emitidos idénticos al actual (mismos campos, mismos valores).
- [ ] Tests existentes siguen pasando sin cambios.
- [ ] Tests nuevos cubren cada `step_*` en aislamiento.

##### Riesgos

- **Cambio de orden de eventos:** verificar que `engine.start` sigue siendo el primer evento, `engine.complete` el último, etc.

---

#### Item 11b — Warning runtime de purga lenta sin avance

**Estado:** `[ ]`
**Prioridad:** P1
**Tipo:** `feat` `perf`
**Compatibilidad:** backward-compatible
**Estimación:** M (3-4h)
**Release:** v0.3.0

##### Contexto

Hoy `engine.purge_heartbeat` se emite cada 100 lotes, sin importar si los lotes son lentos o rápidos. Si un lote tarda 5 minutos (índice faltante, lock contention), no hay alerta hasta el lote 100 (que podría ser horas).

##### Cambios

1. Agregar a `Configuration`:
   ```ruby
   attr_accessor :slow_batch_threshold_s  # default: 30
   attr_accessor :slow_batch_alert_after  # default: 5 (lotes lentos consecutivos antes de degraded)
   ```
2. En `Engine#purge_from_postgres`:
   ```ruby
   loop do
     batch_start = monotonic
     result = conn.exec(sql)
     batch_duration = monotonic - batch_start

     count = result.cmd_tuples
     break if count.zero?

     batches_processed += 1
     total_deleted += count

     if batch_duration > @config.slow_batch_threshold_s
       slow_batch_streak += 1
       safe_log(:warn, "engine.slow_batch", {
         table: @table_name,
         batch_duration_s: batch_duration.round(2),
         batch_size: count,
         streak: slow_batch_streak
       })

       if slow_batch_streak == @config.slow_batch_alert_after
         safe_log(:warn, "engine.purge_degraded", {
           table: @table_name,
           consecutive_slow_batches: slow_batch_streak,
           hint: "considerar índice composite o particionamiento (ver postgres-tuning.md)"
         })
       end
     else
       slow_batch_streak = 0
     end

     # ... heartbeat existente
     sleep(@config.throttle_delay) if @config.throttle_delay.positive?
   end
   ```

##### Archivos afectados

- `lib/data_drain/configuration.rb`
- `lib/data_drain/engine.rb`
- `skill/references/eventos-telemetria.md` (agregar `engine.slow_batch`, `engine.purge_degraded`)
- `CHANGELOG.md`
- `spec/data_drain/engine_spec.rb`

##### Criterios de aceptación

- [ ] Lote > `slow_batch_threshold_s` emite `engine.slow_batch` WARN.
- [ ] N lotes consecutivos lentos emiten `engine.purge_degraded` una sola vez por streak.
- [ ] Streak se resetea si un lote es rápido.
- [ ] Defaults razonables (30s threshold, 5 streak).
- [ ] Configurable.

##### Riesgos

- **Spam de warnings:** límite con `slow_batch_alert_after` evita esto.

---

### P2 — Calidad y DX (v0.3.1)

---

#### Item 12 — YARD coverage 50% → 90%

**Estado:** `[ ]`
**Prioridad:** P2
**Tipo:** `docs`
**Compatibilidad:** N/A
**Estimación:** M (4-6h)

##### Cambios

Documentar con YARD (`@param`, `@return`, `@raise`, `@example`):
- `Configuration` (todos atributos)
- `Observability` (3 métodos)
- `Storage::*` (todos métodos públicos)
- `Record.destroy_all`
- `Record.connection`
- `Record.disconnect!` (item 3)

##### Criterios de aceptación

- [ ] `bundle exec yard stats --list-undoc` reporta 0 métodos públicos sin documentar.
- [ ] Cobertura ≥ 90%.

---

#### Item 13 — Extraer `build_path_base` en Storage::Base

**Estado:** `[ ]`
**Prioridad:** P2
**Tipo:** `refactor`
**Compatibilidad:** backward-compatible
**Estimación:** XS (30min)

##### Cambios

```ruby
# en Base
def build_path_base(bucket, folder_name, partition_path)
  base = File.join(bucket, folder_name)
  base = File.join(base, partition_path) if partition_path && !partition_path.empty?
  base
end

# en Local
def build_path(bucket, folder_name, partition_path)
  "#{build_path_base(bucket, folder_name, partition_path)}/**/*.parquet"
end

# en S3
def build_path(bucket, folder_name, partition_path)
  "s3://#{build_path_base(bucket, folder_name, partition_path)}/**/*.parquet"
end
```

##### Criterios de aceptación

- [ ] Tests existentes pasan sin cambios.
- [ ] Cobertura agrega test directo de `build_path_base`.

---

#### Item 14 — CI con GitHub Actions

**Estado:** `[ ]`
**Prioridad:** P2
**Tipo:** `chore`
**Compatibilidad:** N/A
**Estimación:** M (3-4h)

##### Cambios

Crear `.github/workflows/ci.yml`:
- Matrix Ruby 3.0, 3.2, 3.3
- Service container Postgres 14 (para tests integration)
- Steps: bundle install → rubocop → rspec
- Cache de Bundler
- Run en push y PR a `main`

##### Criterios de aceptación

- [ ] CI verde en main.
- [ ] PRs requieren CI verde para merge (configurar branch protection — manual).
- [ ] Tiempo total < 5min.

---

#### Item 15 — Docs DEBUG en bloque y tuning ejemplos

**Estado:** `[ ]`
**Prioridad:** P2
**Tipo:** `docs`
**Compatibilidad:** N/A
**Estimación:** S (2h)

##### Cambios

En `CLAUDE.md` y `skill/SKILL.md`:
- Recordar `logger.debug { "k=#{v}" }` para extensiones.
- Tabla de tuning recomendado por tamaño de tabla:
  | Tabla | batch_size | throttle_delay |
  |-------|-----------|----------------|
  | <1M filas | 5000 | 0.1 |
  | 1M-100M | 5000 | 0.5 |
  | >100M | 10000 | 1.0 |
- Contexto: tablas con tráfico OLTP concurrente → throttle alto. Tablas frías → throttle 0.

---

#### Item 16 — Adoptar DuckDB Friendly SQL (cosmético)

**Estado:** `[ ]`
**Prioridad:** P2
**Tipo:** `refactor`
**Compatibilidad:** backward-compatible
**Estimación:** S (1-2h)

##### Cambios

En queries internas:
- `COUNT(*)` → `count()`
- `SELECT * FROM table` → `FROM table` (cuando aplique)

Solo donde sea limpio. No forzar.

##### Criterios de aceptación

- [ ] Tests existentes pasan.
- [ ] No cambia comportamiento.

---

## Riesgos transversales

### Coordinación con Gemini

Gemini trabaja en paralelo en este worktree. Antes de mergear cambios:
- Verificar `source=` no se agregó manualmente en logs.
- Verificar orden de campos: `{ component:, event: }.merge(metadata)`.
- Verificar `include` vs `extend` de Observability.
- Verificar `private_class_method` tras `extend`.
- Fechas en CHANGELOG con la fecha real, no copiar la del item original.

### Compatibilidad con consumidores actuales

La gema se usa en producción en Wispro (al menos `versions` y posiblemente otras tablas). Antes de mergear breaking changes:
- Buscar todos los call sites con `rg "DataDrain::" --type ruby` en el monorepo consumidor.
- Validar que los cambios de items 1, 2 no rompen casos en uso.

### Versión de DuckDB

`CREATE SECRET` (item 1) y `lock_configuration` (item 6) requieren DuckDB ≥ 0.10. La gema requiere `~> 1.4`. OK.

### Postgres version

`pg_stat_user_tables.n_dead_tup` (item 5) está disponible desde Postgres 8.x. OK.
`MAINTAIN` privilege (item 5 nota de permisos) es Postgres 16+. Documentar fallback (owner de tabla o superuser).

---

## Checklist de release

Para cada release (v0.2.0, v0.2.1, etc.):

- [ ] Todos los items del release marcados `[x]`.
- [ ] `bundle exec rspec` pasa.
- [ ] `bundle exec rubocop` sin ofensas.
- [ ] CHANGELOG actualizado con fecha actual y todos los items del release.
- [ ] `lib/data_drain/version.rb` bumped.
- [ ] `skill/SKILL.md` y `references/` actualizadas si aplica.
- [ ] README actualizado si aplica.
- [ ] Tag git `v0.X.Y`.
- [ ] Si aplica, invocar skill `gem-release` para empaquetar y publicar.
- [ ] Skill `data_drain` empaquetada en el `.gem` (ver skill-builder doc).

---

## Cómo usar este documento

### Para Claude/Gemini

1. Leer este archivo al inicio de cada sesión que toque DataDrain.
2. Filtrar por estado: items `[ ]` están disponibles; `[~]` los está trabajando alguien (verificar).
3. Antes de empezar un item, marcarlo `[~]` en este archivo (commit aparte).
4. Al terminar, marcar `[x]` y actualizar la sección "Última actualización".
5. Si surge bloqueo, marcar `[!]` y agregar nota explicando.

### Para revisores humanos

- Cada item es autocontenido: contexto + cambios + archivos + criterios + riesgos.
- "Notas para el revisor" señala puntos que requieren tu ojo.
- Las "Estimaciones" son orientativas (XS <1h, S 1-3h, M 3-6h, L 6h+, XL días).

### Para tracking en herramientas externas

Cada item puede mapearse 1:1 a:
- ClickUp task (vía skill `clickup`)
- GitHub issue (vía MCP `github`)
- AI report (vía skill `ai-reports`)

Convención sugerida de título: `[DataDrain v0.X.Y] Item N — Resumen corto`.
