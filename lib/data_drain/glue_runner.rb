# frozen_string_literal: true

require "aws-sdk-glue"

module DataDrain
  # Orquestador para AWS Glue. Permite disparar y monitorear Jobs en AWS
  # para delegar el movimiento masivo de datos (ej. tablas de 1TB).
  class GlueRunner
    # Dispara un Job de Glue y espera a que termine exitosamente.
    #
    # @param job_name [String] Nombre del Job en la consola de AWS.
    # @param arguments [Hash] Argumentos de ejecución (deben empezar con --).
    # @param polling_interval [Integer] Segundos de espera entre cada chequeo de estado.
    # @return [Boolean] true si el Job terminó exitosamente (SUCCEEDED).
    # @raise [RuntimeError] Si el Job falla o se detiene.
    def self.run_and_wait(job_name, arguments = {}, polling_interval: 30)
      config = DataDrain.configuration
      client = Aws::Glue::Client.new(region: config.aws_region)
      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      config.logger.info "component=data_drain event=glue_runner.start job=#{job_name}"
      resp = client.start_job_run(job_name: job_name, arguments: arguments)
      run_id = resp.job_run_id

      loop do
        run_info = client.get_job_run(job_name: job_name, run_id: run_id).job_run
        status = run_info.job_run_state

        case status
        when "SUCCEEDED"
          duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
          config.logger.info "component=data_drain event=glue_runner.complete job=#{job_name} run_id=#{run_id} duration=#{duration.round(2)}s"
          return true
        when "FAILED", "STOPPED", "TIMEOUT"
          error_msg = run_info.error_message || "Sin mensaje de error disponible."
          duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
          config.logger.error "component=data_drain event=glue_runner.failed job=#{job_name} run_id=#{run_id} status=#{status} error=#{error_msg} duration=#{duration.round(2)}s"
          raise "Glue Job #{job_name} (Run ID: #{run_id}) falló con estado #{status}."
        else
          config.logger.info "component=data_drain event=glue_runner.polling job=#{job_name} run_id=#{run_id} status=#{status} next_check_in=#{polling_interval}s"
          sleep polling_interval
        end
      end
    end
  end
end
