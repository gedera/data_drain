# frozen_string_literal: true

require "logger"

module DataDrain
  # Contenedor para todas las opciones de configuración del motor DataDrain.
  class Configuration
    attr_accessor :storage_mode, :aws_region,
                  :aws_access_key_id, :aws_secret_access_key,
                  :db_host, :db_port, :db_user, :db_pass, :db_name,
                  :batch_size, :throttle_delay, :logger, :limit_ram, :tmp_directory,
                  :idle_in_transaction_session_timeout,
                  :vacuum_after_purge,
                  :slow_batch_threshold_s,
                  :slow_batch_alert_after

    def initialize
      @storage_mode   = :local
      @db_host        = "127.0.0.1"
      @db_port        = 5432
      @batch_size     = 5000
      @throttle_delay = 0.5
      @limit_ram      = nil # eg 2GB
      @tmp_directory  = nil # eg /tmp/duckdb_work
      @idle_in_transaction_session_timeout = 0
      @vacuum_after_purge = false
      @slow_batch_threshold_s = 30
      @slow_batch_alert_after = 5
      @logger = Logger.new($stdout)
    end

    # @return [String] Cadena de conexión optimizada para DuckDB.
    def duckdb_connection_string
      "postgresql://#{@db_user}:#{@db_pass}@#{@db_host}:#{@db_port}/#{@db_name}?options=-c%20idle_in_transaction_session_timeout%3D#{@idle_in_transaction_session_timeout}"
    end

    # Valida invariantes generales (storage_mode + AWS si aplica).
    # Llamado por FileIngestor#initialize y GlueRunner.run_and_wait.
    #
    # @raise [DataDrain::ConfigurationError]
    def validate!
      validate_storage_mode!
      validate_aws_config! if storage_mode.to_sym == :s3
    end

    # Valida además las credenciales PostgreSQL.
    # Llamado por Engine#initialize.
    #
    # @raise [DataDrain::ConfigurationError]
    def validate_for_engine!
      validate!
      validate_db_config!
    end

    private

    def validate_storage_mode!
      return if %i[local s3].include?(storage_mode.to_sym)

      raise DataDrain::ConfigurationError,
            "storage_mode debe ser :local o :s3, recibido #{storage_mode.inspect}"
    end

    def validate_aws_config!
      return unless aws_region.nil? || aws_region.to_s.empty?

      raise DataDrain::ConfigurationError,
            "aws_region es obligatorio con storage_mode = :s3"
    end

    def validate_db_config!
      %i[db_host db_user db_name].each do |attr|
        val = public_send(attr)
        next unless val.nil? || val.to_s.empty?

        raise DataDrain::ConfigurationError,
              "config.#{attr} es obligatorio para Engine (storage_mode=#{storage_mode})"
      end
    end
  end
end
