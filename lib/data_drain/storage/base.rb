# frozen_string_literal: true

module DataDrain
  module Storage
    # Interfaz abstracta para los adaptadores de almacenamiento de DataDrain.
    # Define los métodos obligatorios que cada proveedor (Local, S3, etc.)
    # debe implementar para interactuar con DuckDB y el sistema de archivos.
    #
    # @abstract
    class Base
      # @return [DataDrain::Configuration] Configuración actual del framework.
      attr_reader :config

      # Inicializa el adaptador con la configuración proveída.
      #
      # @param config [DataDrain::Configuration]
      def initialize(config)
        @config = config
      end

      # Configura las extensiones y credenciales necesarias en la conexión de DuckDB.
      #
      # @param connection [DuckDB::Connection] Conexión activa a DuckDB.
      # @raise [NotImplementedError] Si la subclase no lo implementa.
      def setup_duckdb(connection)
        raise NotImplementedError, "#{self.class} debe implementar #setup_duckdb"
      end

      # Prepara el directorio destino antes de una exportación (ej. crear carpetas).
      #
      # @param bucket [String] nombre del bucket tanto local o de S3.
      # @param folder_name [String] Nombre de la carpeta principal de la tabla.
      def prepare_export_path(bucket, folder_name)
        # Operación nula por defecto. Las subclases pueden sobreescribirlo.
      end

      # Construye la ruta de lectura compatible con la función `read_parquet` de DuckDB.
      #
      # @param bucket [String] nombre del bucket tanto local o de S3.
      # @param folder_name [String] Carpeta de la tabla (ej. 'versions').
      # @param partition_path [String, nil] Ruta parcial de particiones (ej. 'year=2026/month=3').
      # @return [String] Ruta completa con comodines (ej. '.../**/*.parquet').
      def build_path(bucket, folder_name, partition_path)
        raise NotImplementedError, "#{self.class} debe implementar #build_path"
      end

      # Elimina físicamente las particiones que coincidan con los criterios.
      #
      # @param bucket [String] nombre del bucket tanto local o de S3.
      # @param folder_name [String] Carpeta de la tabla.
      # @param partition_keys [Array<Symbol>] Claves de partición esperadas.
      # @param partitions [Hash] Valores de las particiones a eliminar (puede contener nulos).
      # @return [Integer] Cantidad de particiones o archivos eliminados.
      def destroy_partitions(bucket, folder_name, partition_keys, partitions)
        raise NotImplementedError, "#{self.class} debe implementar #destroy_partitions"
      end

      protected

      # @param bucket [String]
      # @param folder_name [String]
      # @param partition_path [String, nil]
      # @return [String] path sin prefix de protocolo ni sufijo glob
      def build_path_base(bucket, folder_name, partition_path)
        base = File.join(bucket, folder_name)
        base = File.join(base, partition_path) if partition_path && !partition_path.empty?
        base
      end
    end
  end
end
