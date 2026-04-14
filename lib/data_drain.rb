# frozen_string_literal: true

require "active_model"
require_relative "data_drain/version"
require_relative "data_drain/errors"
require_relative "data_drain/configuration"
require_relative "data_drain/validations"
require_relative "data_drain/storage"
require_relative "data_drain/observability"
require_relative "data_drain/engine"
require_relative "data_drain/record"
require_relative "data_drain/file_ingestor"
require_relative "data_drain/glue_runner"

# Registramos el tipo JSON personalizado de ActiveModel
require_relative "data_drain/types/json_type"
ActiveModel::Type.register(:json, DataDrain::Types::JsonType)

# DSL para extraer, archivar y purgar datos entre PostgreSQL y un Data Lake en Parquet.
module DataDrain
  class << self
    # @return [DataDrain::Configuration]
    def configuration
      @configuration ||= Configuration.new
    end

    # @yieldparam config [DataDrain::Configuration]
    def configure
      yield(configuration)
    end

    # @api private
    def reset_configuration!
      @configuration = Configuration.new
      DataDrain::Storage.reset_adapter!
    end
  end
end
