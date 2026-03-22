# frozen_string_literal: true

module DataDrain
  # Clase encargada de ingerir archivos locales (CSV, JSON, Parquet)
  # generados por otros servicios (ej. Netflow) y subirlos al Data Lake
  # aplicando compresión ZSTD y particionamiento Hive.
  class FileIngestor
    # @param options [Hash] Opciones de ingestión.
    # @option options [String] :source_path Ruta absoluta al archivo local.
    # @option options [String] :folder_name Nombre de la carpeta destino en el Data Lake.
    # @option options [Array<String, Symbol>] :partition_keys (Opcional) Columnas para particionar.
    # @option options [String] :select_sql (Opcional) Sentencia SELECT para transformar datos al vuelo.
    # @option options [Boolean] :delete_after_upload (Opcional) Borra el archivo local al terminar. Por defecto true.
    def initialize(options)
      @source_path         = options.fetch(:source_path)
      @folder_name         = options.fetch(:folder_name)
      @partition_keys      = options.fetch(:partition_keys, [])
      @select_sql          = options.fetch(:select_sql, "*")
      @delete_after_upload = options.fetch(:delete_after_upload, true)
      @bucket              = options[:bucket]

      @config  = DataDrain.configuration
      @logger  = @config.logger
      @adapter = DataDrain::Storage.adapter

      database = DuckDB::Database.open(":memory:")
      @duckdb  = database.connect
    end

    # Ejecuta el flujo de ingestión.
    # @return [Boolean] true si el proceso fue exitoso.
    def call
      @logger.info "component=data_drain event=file_ingestor.start source_path=#{@source_path}"

      unless File.exist?(@source_path)
        @logger.error "component=data_drain event=file_ingestor.file_not_found source_path=#{@source_path}"
        return false
      end

      @duckdb.query("SET max_memory='#{@config.limit_ram}';") if @config.limit_ram.present?
      @duckdb.query("SET temp_directory='#{@config.tmp_directory}'") if @config.tmp_directory.present?

      @adapter.setup_duckdb(@duckdb)

      # Determinamos la función lectora de DuckDB según la extensión del archivo
      reader_function = determine_reader

      # 1. Conteo de seguridad
      source_count = @duckdb.query("SELECT COUNT(*) FROM #{reader_function}").first.first
      @logger.info "component=data_drain event=file_ingestor.count source_path=#{@source_path} count=#{source_count}"

      if source_count.zero?
        cleanup_local_file
        return true
      end

      # 2. Exportación / Subida
      @adapter.prepare_export_path(@bucket, @folder_name)
      dest_path = @config.storage_mode.to_sym == :s3 ? "s3://#{@bucket}/#{@folder_name}/" : File.join(@bucket, @folder_name, "")

      partition_clause = @partition_keys.any? ? "PARTITION_BY (#{@partition_keys.join(', ')})," : ""

      query = <<~SQL
        COPY (
          SELECT #{@select_sql}
          FROM #{reader_function}
        ) TO '#{dest_path}'
        (
          FORMAT PARQUET,
          #{partition_clause}
          COMPRESSION 'ZSTD',
          OVERWRITE_OR_IGNORE 1
        );
      SQL

      @logger.info "component=data_drain event=file_ingestor.export_start dest_path=#{dest_path}"
      @duckdb.query(query)

      @logger.info "component=data_drain event=file_ingestor.complete source_path=#{@source_path}"

      cleanup_local_file
      true
    rescue DuckDB::Error => e
      @logger.error "component=data_drain event=file_ingestor.duckdb_error source_path=#{@source_path} error=#{e.message}"
      false
    ensure
      @duckdb&.close
    end

    private

    # @api private
    def determine_reader
      case File.extname(@source_path).downcase
      when '.csv'
        "read_csv_auto('#{@source_path}')"
      when '.json'
        "read_json_auto('#{@source_path}')"
      when '.parquet'
        "read_parquet('#{@source_path}')"
      else
        raise DataDrain::Error, "Formato de archivo no soportado para ingestión: #{@source_path}"
      end
    end

    # @api private
    def cleanup_local_file
      if @delete_after_upload && File.exist?(@source_path)
        File.delete(@source_path)
        @logger.info "component=data_drain event=file_ingestor.cleanup source_path=#{@source_path}"
      end
    end
  end
end
