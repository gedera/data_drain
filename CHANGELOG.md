## [Unreleased]

## [0.1.18] - 2026-03-23

- Feature: Módulo `Observability` centraliza el logging estructurado en toda la gema.
- Feature: Heartbeat de progreso para purgas masivas (`engine.purge_heartbeat`).
- Telemetry: Separación de contexto de error (`error_class`, `error_message`) en todos los eventos de falla.
- Resilience: Los fallos en el sistema de logs nunca interrumpen el flujo principal de datos.

## [0.1.17] - 2026-03-17

- Feature: Telemetría granular por fases (Ingeniería de Performance).
- Telemetry: Inclusión de métricas específicas como \`db_query_duration_s\`, \`export_duration_s\`, \`integrity_duration_s\` y \`purge_duration_s\` en el evento \`engine.complete\`.
- Telemetry: Inclusión de \`source_query_duration_s\` y \`export_duration_s\` en \`file_ingestor.complete\`.

## [0.1.16] - 2026-03-17

- Refactor: Cumplimiento con el estándar **Wispro-Observability-Spec (v1)**.
- Telemetry: Renombrado de métricas de tiempo a \`duration_s\` y \`next_check_in_s\` eliminando sufijos de unidad en los valores.
- Observability: Garantía de valores numéricos puros para contadores y tiempos, facilitando el procesamiento por \`exis_ray\`.

## [0.1.15] - 2026-03-17

- Performance: Medición de duraciones con reloj monotónico (`Process.clock_gettime`) en eventos terminales de `Engine`, `FileIngestor` y `GlueRunner`.
- Fix: `idle_in_transaction_session_timeout` ahora se aplica correctamente cuando el valor es `0` (desactiva el timeout). Antes `0.present?` evaluaba a `false` y se ignoraba.
- Fix: Objeto `DuckDB::Database` en `Record` ahora se ancla en el thread-local junto a la conexión, previniendo garbage collection prematura.
- Fix: `Storage.adapter` cachea la instancia en vez de crearla en cada llamada.
- Documentation: Agregado `CLAUDE.md` con guía de arquitectura y estándares del proyecto.

## [0.1.14] - 2026-03-17

- Feature: Implementación de **Logging Estructurado** en toda la gema (\`key=value\`) para mejor observabilidad en producción.
- Optimization: Caching automático de adaptadores de almacenamiento para mejorar el rendimiento de consultas repetidas.
- Testing: Mejora en la robustez de los tests de \`Engine\` desacoplándolos de cambios menores en el setup de DuckDB.

## [0.1.13] - 2026-03-17

- Feature: Parametrización total en la orquestación con Glue. Se añadieron \`s3_bucket\`, \`s3_folder\` y \`partition_by\` como argumentos dinámicos, permitiendo que el mismo Job de Glue sirva para múltiples tablas y destinos.

## [0.1.12] - 2026-03-17

- Feature: Parametrización dinámica de la base de datos en \`GlueRunner\` y el script de PySpark. Ahora se pasan \`db_url\`, \`db_user\`, \`db_password\` y \`db_table\` como argumentos al Job de Glue.

## [0.1.11] - 2026-03-17

- Feature: Se agregó \`DataDrain::GlueRunner\` para orquestar Jobs de AWS Glue.
- Feature: Soporte oficial para procesamiento de Big Data (ej. tablas de 1TB) mediante delegación a AWS Glue.
- Documentation: Se incluyó un script maestro de PySpark en el README compatible con el formato de la gema.

## [0.1.10] - 2026-03-17

- Feature: Se agregó la opción \`skip_export\` a \`DataDrain::Engine\`. Permite utilizar herramientas externas (como AWS Glue) para la exportación de datos, dejando que DataDrain se encargue solo de la validación de integridad y la purga de PostgreSQL.

## [0.1.9] - 2026-03-17

- Fix: Mejora en la precisión del rango de fechas en consultas SQL usando límites semi-abiertos (<) para evitar pérdida de registros por microsegundos.

## [0.1.8] - 2026-03-16

- Fix: Se cambió la cadena de conexión de DuckDB a formato URI para propagar el timeout de sesión en el ATTACH.

## [0.1.7] - 2026-03-16

- Se agrego soporte para idle_in_transaction_session_timeout.

## [0.1.6] - 2026-03-16

- Se agrego el tem_directory para duckdb.

## [0.1.5] - 2026-03-16

- Se agrego el attach para duckdb.

## [0.1.4] - 2026-03-16

- Corrección de error por comilla simple

## [0.1.3] - 2026-03-16

- Corrección de la sintaxis para postres_query

## [0.1.2] - 2026-03-16

- Cambiamos postgres_scan por postgres_query

## [0.1.1] - 2026-03-16

- Se agrega al configure la posibliidad de agregar el limit de ram para duckdb.

## [0.1.0] - 2026-03-11

- Initial release
