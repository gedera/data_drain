# frozen_string_literal: true

module DataDrain
  # Módulo interno para garantizar que la telemetría cumpla con los 
  # Global-Observability-Standards: resiliencia, KV-structured y precisión.
  #
  # Este módulo es genérico y puede ser utilizado en otras gemas.
  # @api private
  module Observability
    private

    # Emite un log estructurado de forma segura.
    # Garantiza que el logging nunca interrumpa el proceso principal (Resilience).
    def safe_log(level, event, metadata = {})
      return unless @logger

      # component y event siempre primeros, luego el contexto
      fields = { component: observability_name, event: event }.merge(metadata)

      # Enmascaramiento preventivo de secretos (Security)
      log_line = fields.map do |k, v|
        val = %i[password token secret api_key auth].include?(k.to_sym) ? "[FILTERED]" : v
        "#{k}=#{val}"
      end.join(" ")

      @logger.send(level) { log_line }
    rescue StandardError
      # Silencio absoluto en fallos de log para no detener procesos críticos
    end

    # Formatea excepciones siguiendo el Standard Error Context.
    def exception_metadata(error)
      {
        error_class: error.class.name,
        error_message: error.message.gsub("\"", "'")[0, 200]
      }
    end

    # Nombre del componente para los logs.
    # Funciona tanto en métodos de instancia (self = objeto) como de clase (self = Class).
    def observability_name
      klass = is_a?(Class) ? self : self.class
      klass.name.split("::").first.gsub(/([a-z\d])([A-Z])/, '\1_\2').downcase
    rescue StandardError
      "unknown"
    end
  end
end
