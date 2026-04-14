# frozen_string_literal: true

require "duckdb"
require "pg"

module DataDrain
  # Motor principal de extracción y purga de datos (DataDrain).
  # rubocop:disable Metrics/ClassLength, Metrics/AbcSize, Metrics/MethodLength, Naming/AccessorMethodName
  #
  # Orquesta el flujo ETL desde PostgreSQL hacia un Data Lake analítico
  # delegando la interacción del almacenamiento al adaptador configurado.
  class Engine
    include Observability
    # Inicializa una nueva instancia del motor de extracción.
    #
    # @param options [Hash] Diccionario de configuración para la extracción.
    # @option options [Time, DateTime, Date] :start_date Fecha y hora de inicio.
    # @option options [Time, DateTime, Date] :end_date Fecha y hora de fin.
    # @option options [String] :table_name Nombre de la tabla en PostgreSQL.
    # @option options [String] :folder_name (Opcional) Nombre de la carpeta destino.
    # @option options [String] :select_sql (Opcional) Sentencia SELECT personalizada.
    # @option options [Array<String, Symbol>] :partition_keys Columnas para particionar.
    # @option options [String] :primary_key (Opcional) Clave primaria para borrado. Por defecto 'id'.
    # @option options [String] :where_clause (Opcional) Condición SQL extra.
    # @option options [Boolean] :skip_export (Opcional) Si true, no exporta
    #   a Parquet — solo valida y purga (para uso con GlueRunner).
    def initialize(options)
      @start_date = options.fetch(:start_date).beginning_of_day

      @end_date = options.fetch(:end_date).to_date.next_day.beginning_of_day

      @table_name = options.fetch(:table_name)
      Validations.validate_identifier!(:table_name, @table_name)

      @folder_name = options.fetch(:folder_name, @table_name)
      @select_sql = options.fetch(:select_sql, "*")
      @partition_keys = options.fetch(:partition_keys)
      @primary_key = options.fetch(:primary_key, "id")
      Validations.validate_identifier!(:primary_key, @primary_key)
      @where_clause = options[:where_clause]
      @bucket = options[:bucket]
      @skip_export = options.fetch(:skip_export, false)

      @config = DataDrain.configuration
      @logger = @config.logger
      @adapter = DataDrain::Storage.adapter

      database = DuckDB::Database.open(":memory:")
      @duckdb = database.connect
    end

    # Ejecuta el flujo completo del motor: Setup, Conteo, Exportación (opcional), Verificación y Purga.
    #
    # @return [Boolean] `true` si el proceso finalizó con éxito, `false` si falló la integridad.
    def call
      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      safe_log(:info, "engine.start",
               { table: @table_name, start_date: @start_date.to_date, end_date: @end_date.to_date })

      setup_duckdb

      # 1. Conteo inicial en Postgres
      step_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      @pg_count = get_postgres_count
      db_query_duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - step_start

      if @pg_count.zero?
        duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
        safe_log(:info, "engine.skip_empty",
                 { table: @table_name, duration_s: duration.round(2), db_query_duration_s: db_query_duration.round(2) })
        return true
      end

      # 2. Exportación
      export_duration = 0.0
      if @skip_export
        safe_log(:info, "engine.skip_export", { table: @table_name })
      else
        safe_log(:info, "engine.export_start", { table: @table_name, count: @pg_count })
        step_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        export_to_parquet
        export_duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - step_start
      end

      # 3. Verificación de Integridad
      step_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      integrity_ok = verify_integrity
      integrity_duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - step_start

      if integrity_ok
        # 4. Purga en Postgres
        step_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        purge_from_postgres
        purge_duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - step_start

        duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
        safe_log(:info, "engine.complete", {
                   table: @table_name,
                   duration_s: duration.round(2),
                   db_query_duration_s: db_query_duration.round(2),
                   export_duration_s: export_duration.round(2),
                   integrity_duration_s: integrity_duration.round(2),
                   purge_duration_s: purge_duration.round(2),
                   count: @pg_count
                 })
        true
      else
        duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
        safe_log(:error, "engine.integrity_error",
                 { table: @table_name, duration_s: duration.round(2), count: @pg_count })
        false
      end
    end

    private

    # @api private
    # @return [String]
    def base_where_sql
      sql = "created_at >= '#{@start_date.to_fs(:db)}' AND created_at < '#{@end_date.to_fs(:db)}'"
      sql += " AND #{@where_clause}" if @where_clause && !@where_clause.empty?
      sql
    end

    # @api private
    def setup_duckdb
      @duckdb.query("INSTALL postgres; LOAD postgres;")
      @duckdb.query("SET max_memory='#{@config.limit_ram}';") if @config.limit_ram.present?
      @duckdb.query("SET temp_directory='#{@config.tmp_directory}'") if @config.tmp_directory.present?
      @duckdb.query("ATTACH '#{@config.duckdb_connection_string}' AS pg_source (TYPE POSTGRES, READ_ONLY)")

      # 💡 Magia del Adapter: Él sabe si cargar httpfs y setear credenciales o no hacer nada
      @adapter.setup_duckdb(@duckdb)
    end

    # @api private
    # @return [Integer]
    def get_postgres_count
      pg_sql = "SELECT COUNT(*) AS row_count FROM public.#{@table_name} WHERE #{base_where_sql}"
      pg_sql = pg_sql.gsub("'", "''")
      query = "SELECT row_count FROM postgres_query('pg_source', '#{pg_sql}')"
      @duckdb.query(query).first.first
    end

    # @api private
    def export_to_parquet
      # 💡 Magia del Adapter: Si es local crea las carpetas, si es S3 no hace nada.
      @adapter.prepare_export_path(@bucket, @folder_name)

      # Determinamos el path base de destino según el adaptador
      dest_path = if @config.storage_mode.to_sym == :s3
                    "s3://#{@bucket}/#{@folder_name}/"
                  else
                    File.join(@bucket,
                              @folder_name, "")
                  end

      pg_sql = "SELECT #{@select_sql} FROM public.#{@table_name} WHERE #{base_where_sql}"
      pg_sql = pg_sql.gsub("'", "''")

      query = <<~SQL
        COPY (
          SELECT #{@select_sql}
          FROM postgres_query('pg_source', '#{pg_sql}')
        ) TO '#{dest_path}'
        (
          FORMAT PARQUET,
          PARTITION_BY (#{@partition_keys.join(", ")}),
          COMPRESSION 'ZSTD',
          OVERWRITE_OR_IGNORE 1
        );
      SQL
      @duckdb.query(query)
    end

    # @api private
    # @return [Boolean]
    def verify_integrity
      # 💡 Magia del Adapter: Construye la ruta de búsqueda global ('**/*.parquet')
      archive_path = @adapter.build_path(@bucket, @folder_name, nil)

      begin
        query = <<~SQL
          SELECT COUNT(*)
          FROM read_parquet('#{archive_path}')
          WHERE #{base_where_sql}
        SQL
        parquet_result = @duckdb.query(query).first.first
      rescue DuckDB::Error => e
        safe_log(:error, "engine.parquet_read_error", { table: @table_name }.merge(exception_metadata(e)))
        return false
      end

      safe_log(:info, "engine.integrity_check",
               { table: @table_name, pg_count: @pg_count, parquet_count: parquet_result })
      @pg_count == parquet_result
    end

    # @api private
    def purge_from_postgres
      safe_log(:info, "engine.purge_start", { table: @table_name, batch_size: @config.batch_size })

      conn = PG.connect(
        host: @config.db_host,
        port: @config.db_port,
        user: @config.db_user,
        password: @config.db_pass,
        dbname: @config.db_name
      )

      unless @config.idle_in_transaction_session_timeout.nil?
        conn.exec("SET idle_in_transaction_session_timeout = #{@config.idle_in_transaction_session_timeout};")
      end

      batches_processed = 0
      total_deleted = 0

      loop do
        sql = <<~SQL
          DELETE FROM #{@table_name}
          WHERE #{@primary_key} IN (
            SELECT #{@primary_key} FROM #{@table_name}
            WHERE #{base_where_sql}
            LIMIT #{@config.batch_size}
          )
        SQL

        result = conn.exec(sql)
        count = result.cmd_tuples
        break if count.zero?

        batches_processed += 1
        total_deleted += count

        # Heartbeat cada 100 lotes para monitorear procesos largos de 1TB
        if (batches_processed % 100).zero?
          safe_log(:info, "engine.purge_heartbeat", {
                     table: @table_name,
                     batches_processed_count: batches_processed,
                     rows_deleted_count: total_deleted
                   })
        end

        sleep(@config.throttle_delay) if @config.throttle_delay.positive?
      end
    ensure
      conn&.close
    end
  end
  # rubocop:enable Metrics/ClassLength, Metrics/AbcSize, Metrics/MethodLength, Naming/AccessorMethodName
end
