# frozen_string_literal: true

module DataDrain
  module Validations
    IDENTIFIER_REGEX = /\A[a-zA-Z_][a-zA-Z0-9_]*\z/

    module_function

    def validate_identifier!(name, value)
      return if IDENTIFIER_REGEX.match?(value.to_s)

      raise DataDrain::ConfigurationError,
            "#{name} '#{value}' no es un identificador SQL válido"
    end
  end
end
