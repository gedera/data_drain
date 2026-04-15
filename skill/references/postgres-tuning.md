# Postgres Tuning para DataDrain

Guía operacional para tablas que DataDrain archiva y purga. Cubre índices,
VACUUM, particionamiento y diagnóstico.

## Tabla de decisión por tamaño

| Tamaño | Estrategia |
|--------|-----------|
| <10GB | Índice composite `(created_at, pk)` con `CREATE INDEX CONCURRENTLY` |
| 10-100GB | Mismo + `SET maintenance_work_mem='4GB'` + checklist |
| 100GB-1TB | Particionamiento declarativo por mes |
| >1TB | Particionamiento obligatorio + `DROP PARTITION` reemplaza DELETE |

## Índice recomendado

Para tablas <100GB, DataDrain se beneficia de un índice composite:

    CREATE INDEX CONCURRENTLY idx_versions_created_at_id
    ON versions (created_at, id);

El DELETE en batches usa `WHERE created_at >= X AND created_at < Y` + `IN (SELECT id LIMIT N)`.
El índice composite lo convierte en index scan por rango + acceso directo al id.

### Checklist pre-`CREATE INDEX CONCURRENTLY`

- [ ] Tamaño actual: `SELECT pg_size_pretty(pg_total_relation_size('versions'));`
- [ ] Espacio libre disco (>2x tabla)
- [ ] `SET maintenance_work_mem = '4GB';` (sesión)
- [ ] `SET statement_timeout = 0;`
- [ ] Ventana de baja carga
- [ ] Plan rollback: `DROP INDEX CONCURRENTLY` si satura I/O

### Riesgos de `CONCURRENTLY`

1. **Dos pasadas** (puede tardar horas en 500GB)
2. **I/O sostenido** (satura IOPS en EBS gp3 sin provisioned)
3. **Puede fallar y dejar índice INVALID** → recuperar con `DROP INDEX CONCURRENTLY idx; CREATE INDEX CONCURRENTLY idx ...`
4. **Espacio en disco alto** durante build (sort externo si `maintenance_work_mem` bajo)

## VACUUM ANALYZE post-purga

En tablas no particionadas, purgar millones de rows deja dead tuples.
Sin VACUUM, el espacio no se libera y los seq scan recorren páginas vacías.

    VACUUM ANALYZE versions;

Item 5 del roadmap agrega `config.vacuum_after_purge` para automatizar esto.
Hasta v0.3.0, correr manualmente después de cada `Engine#call` en tablas
grandes no particionadas.

**NO usar `VACUUM FULL`** — bloquea la tabla entera (ACCESS EXCLUSIVE lock).

## Diagnóstico de purga lenta

    -- Plan del DELETE en lotes
    EXPLAIN (ANALYZE, BUFFERS)
    DELETE FROM versions
    WHERE id IN (
      SELECT id FROM versions
      WHERE created_at >= '2026-01-01' AND created_at < '2026-02-01'
      LIMIT 5000
    );

    -- Sesiones activas sobre la tabla
    SELECT pid, state, wait_event, query_start, query
    FROM pg_stat_activity
    WHERE query LIKE '%versions%'
      AND state != 'idle';

    -- Estadísticas de la tabla
    SELECT relname, n_live_tup, n_dead_tup, last_vacuum, last_autovacuum
    FROM pg_stat_user_tables
    WHERE relname = 'versions';

    -- Top queries lentas (requiere pg_stat_statements)
    SELECT substring(query, 1, 100) AS query, calls, mean_exec_time, rows
    FROM pg_stat_statements
    WHERE query LIKE '%versions%'
    ORDER BY mean_exec_time DESC
    LIMIT 10;

## Particionamiento declarativo (tablas > 100GB)

Migrar a tabla particionada cambia DataDrain de "DELETE masivo throttled" a
"DROP PARTITION instantáneo".

### Setup

    -- 1. Crear tabla particionada (vacía, misma estructura que versions)
    CREATE TABLE versions_new (
      id UUID PRIMARY KEY,
      created_at TIMESTAMP NOT NULL,
      ... -- resto de columnas
    ) PARTITION BY RANGE (created_at);

    -- 2. Crear partición por mes
    CREATE TABLE versions_2026_03 PARTITION OF versions_new
      FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

    -- 3. Migrar datos (lotes, una partición por vez)
    INSERT INTO versions_2026_03
    SELECT * FROM versions
    WHERE created_at >= '2026-03-01' AND created_at < '2026-04-01';

    -- 4. Swap nombres (downtime mínimo)
    BEGIN;
      ALTER TABLE versions RENAME TO versions_old;
      ALTER TABLE versions_new RENAME TO versions;
    COMMIT;

### Beneficio para DataDrain

    -- v0.2.x: DELETE en lotes, VACUUM después, horas en TB
    DataDrain::Engine.new(...).call

    -- Con particiones: DataDrain sigue funcionando pero si el rango
    -- coincide con una partición, el operador puede hacer:
    DROP TABLE versions_2026_03;  -- instantáneo, sin bloat

DataDrain no detecta particiones automáticamente (futuro item). Hoy el
operador decide.

## Tuning de parámetros DataDrain por tamaño

| Filas tabla | `batch_size` | `throttle_delay` | `vacuum_after_purge` | `slow_batch_threshold_s` |
|------------|-------------|-----------------|---------------------|-------------------------|
| <1M | 5000 | 0.1 | false | 30 |
| 1M-100M | 5000 | 0.5 | true | 30 |
| 100M-1B | 10000 | 1.0 | true | 60 |
| >1B | migrar a particionamiento (ver arriba) | | | |

Contexto operacional:
- **OLTP concurrente**: `throttle_delay` alto (≥0.5s) para no saturar la DB.
- **Tablas frías** (sin queries de usuarios): `throttle_delay` 0 OK.
- **`slow_batch_threshold_s`** alto en tablas grandes porque cada batch tarda más legítimamente.

## Referencias

- Skill: `.agents/skills/postgresql-optimization/SKILL.md`
- PG docs: https://www.postgresql.org/docs/current/ddl-partitioning.html
- Item 5 roadmap (VACUUM automático): ../IMPROVEMENT_PLAN.md#item-5
- Item 11b roadmap (warning runtime): ../IMPROVEMENT_PLAN.md#item-11b
