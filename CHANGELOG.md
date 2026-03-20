## [Unreleased]

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
