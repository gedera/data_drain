# frozen_string_literal: true

require "duckdb"
require "pg"

module DataDrain
  # Motor principal de extracción y purga de datos (DataDrain).
  #
  # Orquesta el flujo ETL desde PostgreSQL hacia un Data Lake analítico
  # delegando la interacción del almacenamiento al adaptador configurado.
  class Engine
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
    def initialize(options)
      @start_date     = options.fetch(:start_date).beginning_of_day
      @end_date       = options.fetch(:end_date).end_of_day
      @table_name     = options.fetch(:table_name)
      @folder_name    = options.fetch(:folder_name, @table_name)
      @select_sql     = options.fetch(:select_sql, "*")
      @partition_keys = options.fetch(:partition_keys)
      @primary_key    = options.fetch(:primary_key, "id")
      @where_clause   = options[:where_clause]
      @bucket         = options[:bucket]

      @config  = DataDrain.configuration
      @logger  = @config.logger
      @adapter = DataDrain::Storage.adapter

      database = DuckDB::Database.open(":memory:")
      @duckdb  = database.connect
    end

    # Ejecuta el flujo completo del motor: Setup, Conteo, Exportación, Verificación y Purga.
    #
    # @return [Boolean] `true` si el proceso finalizó con éxito, `false` si falló la integridad.
    def call
      @logger.info "[DataDrain Engine] 🚀 Preparando '#{@table_name}' (#{@start_date.to_date} a #{@end_date.to_date})..."

      setup_duckdb

      @pg_count = get_postgres_count

      if @pg_count.zero?
        @logger.info "[DataDrain Engine] ⏭️ No hay registros que cumplan las condiciones."
        return true
      end

      @logger.info "[DataDrain Engine] 📦 Exportando #{@pg_count} registros a Parquet..."
      export_to_parquet

      if verify_integrity
        purge_from_postgres
        @logger.info "[DataDrain Engine] ✅ Proceso completado exitosamente para '#{@table_name}'."
        true
      else
        @logger.error "[DataDrain Engine] ❌ ERROR de integridad en '#{@table_name}'. Abortando purga."
        false
      end
    end

    private

    # @api private
    # @return [String]
    def base_where_sql
      sql = "created_at >= '#{@start_date.to_fs(:db)}' AND created_at <= '#{@end_date.to_fs(:db)}'"
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
      dest_path = @config.storage_mode.to_sym == :s3 ? "s3://#{@bucket}/#{@folder_name}/" : File.join(@bucket, @folder_name, "")

      pg_sql = "SELECT #{@select_sql} FROM public.#{@table_name} WHERE #{base_where_sql}"
      pg_sql = pg_sql.gsub("'", "''")

      query = <<~SQL
        COPY (
          SELECT #{@select_sql}
          FROM postgres_query('pg_source', '#{pg_sql}')
        ) TO '#{dest_path}'
        (
          FORMAT PARQUET,
          PARTITION_BY (#{@partition_keys.join(', ')}),
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
        @logger.error "[DataDrain Engine] ❌ Error leyendo Parquet: #{e.message}"
        return false
      end

      @logger.info "[DataDrain Engine] 📊 Verificación -> Postgres: #{@pg_count} | Parquet: #{parquet_result}"
      @pg_count == parquet_result
    end

    # @api private
    def purge_from_postgres
      @logger.info "[DataDrain Engine] 🗑️ Purgando en base de datos (Lotes de #{@config.batch_size})..."

      conn = PG.connect(
        host:     @config.db_host,
        port:     @config.db_port,
        user:     @config.db_user,
        password: @config.db_pass,
        dbname:   @config.db_name
      )

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
        break if result.cmd_tuples.zero?

        sleep(@config.throttle_delay) if @config.throttle_delay.positive?
      end
    ensure
      conn&.close
    end
  end
end
