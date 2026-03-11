# frozen_string_literal: true

module DataDrain
  # Clase base para todos los errores originados en el framework DataDrain.
  class Error < StandardError; end

  # Levantado cuando falta configuración obligatoria.
  class ConfigurationError < Error; end

  # Levantado cuando la verificación matemática entre Postgres y Parquet no coincide.
  class IntegrityError < Error; end

  # Levantado cuando hay problemas interactuando con DuckDB, el disco local o AWS S3.
  class StorageError < Error; end
end
