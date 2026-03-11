# frozen_string_literal: true

require "fileutils"

module DataDrain
  module Storage
    # Implementación del adaptador de almacenamiento para el disco local.
    class Local < Base
      # (DuckDB ya soporta archivos locales de forma nativa, no requiere extensiones extras)
      # @param connection [DuckDB::Connection]
      def setup_duckdb(connection)
        # No-op
      end

      # Crea la jerarquía de carpetas en el disco si no existe.
      # @param folder_name [String]
      def prepare_export_path(folder_name)
        FileUtils.mkdir_p(File.join(@config.base_path, folder_name))
      end

      # @param folder_name [String]
      # @param partition_path [String, nil]
      # @return [String]
      def build_path(folder_name, partition_path)
        base = File.join(@config.base_path, folder_name)
        base = File.join(base, partition_path) if partition_path && !partition_path.empty?
        "#{base}/**/*.parquet"
      end

      # @param folder_name [String]
      # @param partition_keys [Array<Symbol>]
      # @param partitions [Hash]
      # @return [Integer]
      def destroy_partitions(folder_name, partition_keys, partitions)
        path_parts = partition_keys.map do |key|
          val = partitions[key]
          val.nil? || val.to_s.empty? ? "#{key}=*" : "#{key}=#{val}"
        end

        pattern = File.join(@config.base_path, folder_name, path_parts.join("/"))
        folders_to_delete = Dir.glob(pattern)

        return 0 if folders_to_delete.empty?

        folders_to_delete.each { |folder| FileUtils.rm_rf(folder) }
        folders_to_delete.size
      end
    end
  end
end
