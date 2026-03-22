# frozen_string_literal: true

require_relative "storage/base"
require_relative "storage/local"
require_relative "storage/s3"

module DataDrain
  # Espacio de nombres para las estrategias de almacenamiento físico.
  module Storage
    # Excepción lanzada cuando se intenta usar un modo de almacenamiento no registrado.
    class InvalidAdapterError < DataDrain::Error; end

    # Resuelve e instancia el adaptador de almacenamiento correspondiente
    # basándose en la configuración actual del framework. La instancia se
    # cachea para evitar allocations innecesarias entre queries.
    #
    # @return [DataDrain::Storage::Base] Una instancia de Local o S3.
    # @raise [InvalidAdapterError] Si el storage_mode no es válido.
    def self.adapter
      @adapter ||= begin
        mode = DataDrain.configuration.storage_mode
        case mode.to_sym
        when :local
          Local.new(DataDrain.configuration)
        when :s3
          S3.new(DataDrain.configuration)
        else
          raise InvalidAdapterError, "Storage mode '#{mode}' no está soportado."
        end
      end
    end

    # Descarta el adaptador cacheado. Llamar cuando cambia storage_mode.
    def self.reset_adapter!
      @adapter = nil
    end
  end
end
