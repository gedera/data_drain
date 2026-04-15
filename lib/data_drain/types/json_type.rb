# frozen_string_literal: true

require "json"

module DataDrain
  # Tipos personalizados para ActiveModel registrados por DataDrain.
  module Types
    # Tipo personalizado para ActiveModel que maneja la conversión de
    # cadenas JSON de DuckDB hacia Hashes de Ruby.
    class JsonType < ActiveModel::Type::Value
      # @param value [String, Hash, Array, nil]
      # @return [Hash, Array, String, nil]
      def cast(value)
        return value if value.is_a?(Hash) || value.is_a?(Array) || value.nil?

        begin
          JSON.parse(value.to_s)
        rescue JSON::ParserError
          value
        end
      end
    end
  end
end
