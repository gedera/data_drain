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
    def self.client
      @client ||= Aws::Glue::Client.new(region: DataDrain.configuration.aws_region)
    end

    class << self
      attr_writer :client
    end

    def self.job_exists?(job_name)
      DataDrain::Validations.validate_glue_name!(:job_name, job_name)
      get_job(job_name)
      true
    rescue Aws::Glue::Errors::EntityNotFoundException
      false
    end

    def self.get_job(job_name)
      DataDrain::Validations.validate_glue_name!(:job_name, job_name)
      client.get_job(job_name: job_name).job
    end

    def self.create_job(job_name, role_arn:, script_location:, command_name: "glueetl",
                        default_arguments: {}, description: nil, worker_type: nil, number_of_workers: nil,
                        timeout: 2880, max_retries: 0, allocated_capacity: nil, glue_version: nil)
      DataDrain::Validations.validate_glue_name!(:job_name, job_name)
      opts = {
        name: job_name,
        role: role_arn,
        command: {
          name: command_name,
          python_version: "3",
          script_location: script_location
        }
      }
      opts[:default_arguments] = default_arguments unless default_arguments.empty?
      opts[:description] = description if description
      opts[:timeout] = timeout if timeout
      opts[:max_retries] = max_retries if max_retries
      opts[:allocated_capacity] = allocated_capacity if allocated_capacity
      opts[:worker_type] = worker_type if worker_type
      opts[:number_of_workers] = number_of_workers if number_of_workers
      opts[:glue_version] = glue_version if glue_version

      client.create_job(**opts)
      get_job(job_name)
    end

    def self.update_job(job_name, role_arn: nil, command_name: nil, script_location: nil,
                        default_arguments: nil, description: nil, worker_type: nil,
                        number_of_workers: nil, timeout: nil, max_retries: nil, allocated_capacity: nil,
                        glue_version: nil)
      DataDrain::Validations.validate_glue_name!(:job_name, job_name)
      job_update = {}
      job_update[:role] = role_arn if role_arn
      if command_name && script_location
        job_update[:command] =
          { name: command_name, python_version: "3", script_location: script_location }
      end
      job_update[:default_arguments] = default_arguments if default_arguments
      job_update[:description] = description if description
      job_update[:timeout] = timeout if timeout
      job_update[:max_retries] = max_retries if max_retries
      job_update[:allocated_capacity] = allocated_capacity if allocated_capacity
      job_update[:worker_type] = worker_type if worker_type
      job_update[:number_of_workers] = number_of_workers if number_of_workers
      job_update[:glue_version] = glue_version if glue_version

      client.update_job(job_name: job_name, job_update: job_update)
      get_job(job_name)
    end

    def self.delete_job(job_name)
      DataDrain::Validations.validate_glue_name!(:job_name, job_name)
      client.delete_job(job_name: job_name)
      nil
    end

    def self.run_and_wait(job_name, arguments = {}, polling_interval: 30, max_wait_seconds: nil)
      config = DataDrain.configuration
      config.validate!
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
