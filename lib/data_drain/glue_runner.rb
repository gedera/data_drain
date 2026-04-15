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

    def self.create_job(job_name, role_arn:, script_location: nil, script_path: nil,
                        script_bucket: nil, script_folder: "scripts", script_filename: nil,
                        command_name: "glueetl", default_arguments: {}, description: nil,
                        worker_type: nil, number_of_workers: nil, timeout: 2880,
                        max_retries: 0, allocated_capacity: nil, glue_version: nil)
      @logger = DataDrain.configuration.logger
      DataDrain::Validations.validate_glue_name!(:job_name, job_name)

      final_script_location = resolve_script_location(
        script_location: script_location,
        script_path: script_path,
        script_bucket: script_bucket,
        script_folder: script_folder,
        script_filename: script_filename
      )

      opts = {
        name: job_name,
        role: role_arn,
        command: {
          name: command_name,
          python_version: "3",
          script_location: final_script_location
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
      safe_log(:info, "glue_runner.job_create", {
                 job: job_name,
                 glue_version: glue_version,
                 worker_type: worker_type,
                 number_of_workers: number_of_workers
               })
      get_job(job_name)
    rescue Aws::Glue::Errors::ServiceError => e
      safe_log(:error, "glue_runner.job_create_error",
               { job: job_name }.merge(exception_metadata(e)))
      raise
    end

    def self.update_job(job_name, role_arn: nil, command_name: nil, script_location: nil,
                        default_arguments: nil, description: nil, worker_type: nil,
                        number_of_workers: nil, timeout: nil, max_retries: nil, allocated_capacity: nil,
                        glue_version: nil)
      @logger = DataDrain.configuration.logger
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
      safe_log(:info, "glue_runner.job_update", {
                 job: job_name,
                 changed_fields: job_update.keys.map(&:to_s)
               })
      get_job(job_name)
    rescue Aws::Glue::Errors::ServiceError => e
      safe_log(:error, "glue_runner.job_update_error",
               { job: job_name }.merge(exception_metadata(e)))
      raise
    end

    def self.delete_job(job_name)
      @logger = DataDrain.configuration.logger
      DataDrain::Validations.validate_glue_name!(:job_name, job_name)
      client.delete_job(job_name: job_name)
      safe_log(:info, "glue_runner.job_delete", { job: job_name })
      true
    rescue Aws::Glue::Errors::EntityNotFoundException
      safe_log(:info, "glue_runner.job_delete_skipped", { job: job_name, reason: "not_found" })
      false
    rescue Aws::Glue::Errors::ServiceError => e
      safe_log(:error, "glue_runner.job_delete_error",
               { job: job_name }.merge(exception_metadata(e)))
      raise
    end

    def self.ensure_job(job_name, role_arn:, script_location: nil, script_path: nil,
                        script_bucket: nil, script_folder: "scripts", script_filename: nil,
                        command_name: "glueetl", default_arguments: {}, description: nil,
                        worker_type: nil, number_of_workers: nil, timeout: 2880,
                        max_retries: 0, allocated_capacity: nil, glue_version: nil)
      @logger = DataDrain.configuration.logger

      final_script_location = resolve_script_location(
        script_location: script_location,
        script_path: script_path,
        script_bucket: script_bucket,
        script_folder: script_folder,
        script_filename: script_filename
      )

      if job_exists?(job_name)
        current = get_job(job_name)
        desired = {
          role: role_arn,
          command_name: command_name,
          script_location: final_script_location,
          default_arguments: default_arguments,
          description: description,
          worker_type: worker_type,
          number_of_workers: number_of_workers,
          timeout: timeout,
          max_retries: max_retries,
          glue_version: glue_version
        }
        changed = changed_fields(desired, current)
        if changed.empty?
          safe_log(:info, "glue_runner.job_unchanged", { job: job_name })
          current
        else
          safe_log(:info, "glue_runner.job_exists", { job: job_name })
          update_job(job_name, role_arn: role_arn, command_name: command_name,
                               script_location: final_script_location, default_arguments: default_arguments,
                               description: description, worker_type: worker_type,
                               number_of_workers: number_of_workers, timeout: timeout,
                               max_retries: max_retries, allocated_capacity: allocated_capacity,
                               glue_version: glue_version)
        end
      else
        safe_log(:info, "glue_runner.job_created", { job: job_name })
        create_job(job_name, role_arn: role_arn, script_location: final_script_location,
                             command_name: command_name, default_arguments: default_arguments,
                             description: description, worker_type: worker_type,
                             number_of_workers: number_of_workers, timeout: timeout,
                             max_retries: max_retries, allocated_capacity: allocated_capacity,
                             glue_version: glue_version)
      end
    end

    def self.changed_fields(desired, current)
      changed = []
      changed << :role if current.role != desired[:role]
      changed << :command if current.command.name != desired[:command_name] ||
                             current.command.script_location != desired[:script_location]
      changed << :default_arguments if current.default_arguments != desired[:default_arguments]
      changed << :description if current.description != desired[:description]
      changed << :worker_type if current.worker_type != desired[:worker_type]
      changed << :number_of_workers if current.number_of_workers != desired[:number_of_workers]
      changed << :timeout if current.timeout != desired[:timeout]
      changed << :max_retries if current.max_retries != desired[:max_retries]
      changed << :glue_version if current.glue_version != desired[:glue_version]
      changed
    end
    private_class_method :changed_fields

    def self.resolve_script_location(script_location:, script_path:, script_bucket:, script_folder:, script_filename:)
      both_set = script_location && script_path
      raise DataDrain::ConfigurationError, "provee script_location o script_path, no ambos" if both_set

      return script_location if script_location
      raise ArgumentError, "script_location o script_path es requerido" unless script_path
      raise DataDrain::ConfigurationError, "script_path requiere script_bucket" unless script_bucket

      upload_script(
        local_path: script_path,
        bucket: script_bucket,
        folder: script_folder,
        filename: script_filename
      )
    end
    private_class_method :resolve_script_location

    def self.upload_script(local_path:, bucket:, folder: "scripts", filename: nil)
      @logger = DataDrain.configuration.logger

      unless File.exist?(local_path)
        raise DataDrain::ConfigurationError,
              "Script local '#{local_path}' no existe"
      end

      actual_filename = filename || File.basename(local_path)
      s3_key = "#{folder.chomp("/")}/#{actual_filename}"
      bytes = File.size(local_path)

      adapter = DataDrain::Storage.adapter
      unless adapter.is_a?(DataDrain::Storage::S3)
        raise DataDrain::ConfigurationError,
              "upload_script requiere storage_mode = :s3, actual: #{DataDrain.configuration.storage_mode}"
      end

      s3_path = adapter.upload_file(local_path, bucket, s3_key, content_type: "text/x-python")

      safe_log(:info, "glue_runner.script_uploaded", {
                 local_path: local_path,
                 s3_path: s3_path,
                 bytes: bytes
               })

      s3_path
    rescue Aws::S3::Errors::ServiceError => e
      safe_log(:error, "glue_runner.script_upload_error",
               { local_path: local_path, bucket: bucket }.merge(exception_metadata(e)))
      raise
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
