# frozen_string_literal: true

module DataDrain
  # Módulo de validación de configuración para prevenir errores de uso.
  module Validations
    # Regex que valida identificadores SQL (tablas, columnas, etc.).
    # Permite letras, guiones bajos y números (no al inicio).
    IDENTIFIER_REGEX = /\A[a-zA-Z_][a-zA-Z0-9_]*\z/
    GLUE_NAME_REGEX = /\A[a-zA-Z0-9][a-zA-Z0-9-]*\z/

    module_function

    def validate_glue_name!(name, value)
      return if GLUE_NAME_REGEX.match?(value.to_s)

      raise DataDrain::ConfigurationError,
            "#{name} '#{value}' no es un nombre válido para Glue Job (usa solo letras, números y guiones)"
    end

    def validate_identifier!(name, value)
      return if IDENTIFIER_REGEX.match?(value.to_s)

      raise DataDrain::ConfigurationError,
            "#{name} '#{value}' no es un identificador SQL válido"
    end
  end
end
