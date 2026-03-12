# frozen_string_literal: true

require "logger"

module DataDrain
  # Contenedor para todas las opciones de configuración del motor DataDrain.
  class Configuration
    attr_accessor :storage_mode, :aws_region,
      :aws_access_key_id, :aws_secret_access_key,
      :db_host, :db_port, :db_user, :db_pass, :db_name,
      :batch_size, :throttle_delay, :logger

    def initialize
      @storage_mode   = :local
      @db_host        = "127.0.0.1"
      @db_port        = 5432
      @batch_size     = 5000
      @throttle_delay = 0.5
      @logger         = Logger.new($stdout)
    end

    # @return [String] Cadena de conexión optimizada para DuckDB.
    def duckdb_connection_string
      "host=#{@db_host} port=#{@db_port} dbname=#{@db_name} user=#{@db_user} password=#{@db_pass}"
    end
  end
end
