## [Unreleased]

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
