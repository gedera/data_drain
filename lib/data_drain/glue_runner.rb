# frozen_string_literal: true

require "aws-sdk-glue"

module DataDrain
  # Orquestador para AWS Glue. Permite disparar y monitorear Jobs en AWS
  # para delegar el movimiento masivo de datos (ej. tablas de 1TB).
  class GlueRunner
    extend Observability
    private_class_method :safe_log, :exception_metadata, :observability_name

    # Dispara un Job de Glue y espera a que termine exitosamente.
    #
    # @param job_name [String] Nombre del Job en la consola de AWS.
    # @param arguments [Hash] Argumentos de ejecución (deben empezar con --).
    # @param polling_interval [Integer] Segundos de espera entre cada chequeo de estado.
    # @param max_wait_seconds [Integer, nil] Timeout máximo en segundos.
    #   nil = sin límite (comportamiento anterior).
    # @return [Boolean] true si el Job terminó exitosamente (SUCCEEDED).
    # @raise [DataDrain::Error] si max_wait_seconds excede antes de SUCCEEDED.
    # @raise [RuntimeError] si el Job falla o se detiene.
    def self.run_and_wait(job_name, arguments = {}, polling_interval: 30, max_wait_seconds: nil)
      config = DataDrain.configuration
      client = Aws::Glue::Client.new(region: config.aws_region)
      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      @logger = config.logger

      safe_log(:info, "glue_runner.start", { job: job_name })
      resp = client.start_job_run(job_name: job_name, arguments: arguments)
      run_id = resp.job_run_id

      loop do
        if max_wait_seconds &&
           (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time) > max_wait_seconds
          safe_log(:error, "glue_runner.timeout", {
                     job: job_name,
                     run_id: run_id,
                     max_wait_seconds: max_wait_seconds
                   })
          raise DataDrain::Error,
                "Glue Job #{job_name} (Run ID: #{run_id}) excedió max_wait_seconds=#{max_wait_seconds}"
        end

        run_info = client.get_job_run(job_name: job_name, run_id: run_id).job_run
        status = run_info.job_run_state

        case status
        when "SUCCEEDED"
          duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
          safe_log(:info, "glue_runner.complete", { job: job_name, run_id: run_id, duration_s: duration.round(2) })
          return true
        when "FAILED", "STOPPED", "TIMEOUT"
          duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
          error_metadata = { job: job_name, run_id: run_id, status: status, duration_s: duration.round(2) }

          error_metadata[:error_message] = run_info.error_message.gsub("\"", "'")[0, 200] if run_info.error_message

          safe_log(:error, "glue_runner.failed", error_metadata)
          raise "Glue Job #{job_name} (Run ID: #{run_id}) falló con estado #{status}."
        else
          safe_log(:info, "glue_runner.polling",
                   { job: job_name, run_id: run_id, status: status, next_check_in_s: polling_interval })
          sleep polling_interval
        end
      end
    end
  end
end
