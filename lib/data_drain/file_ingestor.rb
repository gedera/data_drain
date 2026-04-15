# frozen_string_literal: true

module DataDrain
  # Clase encargada de ingerir archivos locales (CSV, JSON, Parquet)
  # generados por otros servicios (ej. Netflow) y subirlos al Data Lake
  # aplicando compresión ZSTD y particionamiento Hive.
  class FileIngestor
    include Observability
    include Observability::Timing

    # @param options [Hash] Opciones de ingestión.
    # @option options [String] :source_path Ruta absoluta al archivo local.
    # @option options [String] :folder_name Nombre de la carpeta destino en el Data Lake.
    # @option options [Array<String, Symbol>] :partition_keys (Opcional) Columnas para particionar.
    # @option options [String] :select_sql (Opcional) Sentencia SELECT para transformar datos al vuelo.
    # @option options [Boolean] :delete_after_upload (Opcional) Borra el archivo local al terminar. Por defecto true.
    def initialize(options)
      @source_path = options.fetch(:source_path)
      @folder_name = options.fetch(:folder_name)
      Validations.validate_identifier!(:folder_name, @folder_name)
      @partition_keys = options.fetch(:partition_keys, [])
      @select_sql = options.fetch(:select_sql, "*")
      @delete_after_upload = options.fetch(:delete_after_upload, true)
      @bucket = options[:bucket]

      @config = DataDrain.configuration
      @config.validate!
      @logger = @config.logger
      @adapter = DataDrain::Storage.adapter

      database = DuckDB::Database.open(":memory:")
      @duckdb = database.connect
    end

    # Ejecuta el flujo de ingestión.
    # @return [Boolean] true si el proceso fue exitoso.
    def call
      @durations = {}
      start_time = monotonic
      safe_log(:info, "file_ingestor.start", { source_path: @source_path })

      return file_not_found(start_time) unless step_validate_file

      step_setup_duckdb
      @reader_function = determine_reader
      @source_count = step_count_source

      return skip_empty(start_time) if @source_count.zero?

      step_export
      log_complete(start_time)
      cleanup_local_file
      true
    rescue DuckDB::Error => e
      duration = monotonic - start_time
      safe_log(:error, "file_ingestor.duckdb_error",
               { source_path: @source_path }.merge(exception_metadata(e)).merge(duration_s: duration.round(2)))
      false
    ensure
      @duckdb&.close
    end

    private

    # @api private
    def file_not_found(_start_time)
      safe_log(:error, "file_ingestor.file_not_found", { source_path: @source_path })
      false
    end

    # @api private
    def step_validate_file
      File.exist?(@source_path)
    end

    # @api private
    def step_setup_duckdb
      @duckdb.query("SET max_memory='#{@config.limit_ram}';") if @config.limit_ram.present?
      @duckdb.query("SET temp_directory='#{@config.tmp_directory}'") if @config.tmp_directory.present?
      @adapter.setup_duckdb(@duckdb)
    end

    # @api private
    def step_count_source
      source_count = timed(:source_query) { @duckdb.query("SELECT count() FROM #{@reader_function}").first.first }
      safe_log(:info, "file_ingestor.count", {
                 source_path: @source_path,
                 count: source_count,
                 source_query_duration_s: @durations.fetch(:source_query, 0).round(2)
               })
      source_count
    end

    # @api private
    def skip_empty(start_time)
      cleanup_local_file
      duration = monotonic - start_time
      safe_log(:info, "file_ingestor.skip_empty", { source_path: @source_path, duration_s: duration.round(2) })
      true
    end

    # @api private
    def step_export
      @adapter.prepare_export_path(@bucket, @folder_name)
      dest_path = if @config.storage_mode.to_sym == :s3
                    "s3://#{@bucket}/#{@folder_name}/"
                  else
                    File.join(@bucket, @folder_name, "")
                  end

      partition_clause = @partition_keys.any? ? "PARTITION_BY (#{@partition_keys.join(", ")})," : ""

      query = <<~SQL
        COPY (
          SELECT #{@select_sql}
          FROM #{@reader_function}
        ) TO '#{dest_path}'
        (
          FORMAT PARQUET,
          #{partition_clause}
          COMPRESSION 'ZSTD',
          OVERWRITE_OR_IGNORE 1
        );
      SQL

      safe_log(:info, "file_ingestor.export_start", { dest_path: dest_path })
      timed(:export) { @duckdb.query(query) }
    end

    # @api private
    def log_complete(start_time)
      duration = monotonic - start_time
      safe_log(:info, "file_ingestor.complete", {
                 source_path: @source_path,
                 duration_s: duration.round(2),
                 source_query_duration_s: @durations.fetch(:source_query, 0).round(2),
                 export_duration_s: @durations.fetch(:export, 0).round(2),
                 count: @source_count
               })
    end

    # @api private
    def determine_reader
      case File.extname(@source_path).downcase
      when ".csv"
        "read_csv_auto('#{@source_path}')"
      when ".json"
        "read_json_auto('#{@source_path}')"
      when ".parquet"
        "read_parquet('#{@source_path}')"
      else
        raise DataDrain::Error, "Formato de archivo no soportado para ingestión: #{@source_path}"
      end
    end

    # @api private
    def cleanup_local_file
      return unless @delete_after_upload && File.exist?(@source_path)

      File.delete(@source_path)
      safe_log(:info, "file_ingestor.cleanup", { source_path: @source_path })
    end
  end
end
