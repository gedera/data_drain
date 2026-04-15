# frozen_string_literal: true

RSpec.describe DataDrain::GlueRunner do
  let(:config) do
    DataDrain::Configuration.new.tap do |c|
      c.aws_region = "us-east-1"
      c.logger = Logger.new(StringIO.new)
    end
  end

  let(:glue_client) { Aws::Glue::Client.new(stub_responses: true, region: "us-east-1") }

  before do
    allow(DataDrain).to receive(:configuration).and_return(config)
    allow(Aws::Glue::Client).to receive(:new).and_return(glue_client)
    described_class.client = glue_client
  end

  describe ".run_and_wait" do
    it "retorna true cuando SUCCEEDED inmediato" do
      glue_client.stub_responses(:start_job_run, { job_run_id: "run-123" })
      glue_client.stub_responses(:get_job_run, {
                                   job_run: { job_run_state: "SUCCEEDED", error_message: nil }
                                 })

      result = described_class.run_and_wait("my-job", { "--key" => "val" })
      expect(result).to be true
    end

    it "hace polling hasta SUCCEEDED" do
      glue_client.stub_responses(:start_job_run, { job_run_id: "run-123" })
      glue_client.stub_responses(:get_job_run, [
                                   { job_run: { job_run_state: "RUNNING", error_message: nil } },
                                   { job_run: { job_run_state: "SUCCEEDED", error_message: nil } }
                                 ])

      allow(Kernel).to receive(:sleep)

      result = described_class.run_and_wait("my-job", {}, polling_interval: 5)
      expect(result).to be true
    end

    it "levanta RuntimeError cuando FAILED" do
      glue_client.stub_responses(:start_job_run, { job_run_id: "run-456" })
      glue_client.stub_responses(:get_job_run, {
                                   job_run: { job_run_state: "FAILED", error_message: "Out of memory" }
                                 })

      expect do
        described_class.run_and_wait("failing-job")
      end.to raise_error(RuntimeError, /failing-job/)
    end

    it "levanta RuntimeError cuando STOPPED" do
      glue_client.stub_responses(:start_job_run, { job_run_id: "run-789" })
      glue_client.stub_responses(:get_job_run, {
                                   job_run: { job_run_state: "STOPPED", error_message: nil }
                                 })

      expect do
        described_class.run_and_wait("stopped-job")
      end.to raise_error(RuntimeError, /STOPPED/)
    end

    it "trunca error_message a 200 chars" do
      glue_client.stub_responses(:start_job_run, { job_run_id: "run-999" })
      glue_client.stub_responses(:get_job_run, {
                                   job_run: { job_run_state: "FAILED", error_message: "x" * 300 }
                                 })

      expect do
        described_class.run_and_wait("failing-job")
      end.to raise_error(RuntimeError) { |e| expect(e.message.length).to be <= 220 }
    end

    it "levanta cuando hay error_message" do
      glue_client.stub_responses(:start_job_run, { job_run_id: "run-101" })
      glue_client.stub_responses(:get_job_run, {
                                   job_run: { job_run_state: "FAILED", error_message: "Out of memory" }
                                 })

      expect do
        described_class.run_and_wait("failing-job")
      end.to raise_error(RuntimeError, /FAILED/)
    end

    it "levanta DataDrain::Error cuando max_wait_seconds se excede" do
      glue_client.stub_responses(:start_job_run, { job_run_id: "run-timeout" })
      glue_client.stub_responses(:get_job_run, {
                                   job_run: { job_run_state: "RUNNING", error_message: nil }
                                 })

      times = [0.0, 0.1, 200.0]
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) do
        times.shift || 300.0
      end
      allow(Kernel).to receive(:sleep)

      expect do
        described_class.run_and_wait("slow-job", {}, polling_interval: 1, max_wait_seconds: 60)
      end.to raise_error(DataDrain::Error, /max_wait_seconds=60/)
    end

    it "sin max_wait_seconds mantiene comportamiento anterior (no timeout local)" do
      glue_client.stub_responses(:start_job_run, { job_run_id: "run-ok" })
      glue_client.stub_responses(:get_job_run, {
                                   job_run: { job_run_state: "SUCCEEDED", error_message: nil }
                                 })

      expect { described_class.run_and_wait("ok-job") }.not_to raise_error
    end
  end

  describe ".get_job" do
    it "retorna el Job object cuando existe" do
      glue_client.stub_responses(:get_job, {
                                   job: {
                                     name: "my-job",
                                     role: "GlueServiceRole",
                                     command: { name: "glueetl", python_version: "3",
                                                script_location: "s3://bucket/script.py" }
                                   }
                                 })

      job = described_class.get_job("my-job")
      expect(job.name).to eq "my-job"
      expect(job.role).to eq "GlueServiceRole"
    end

    it "levanta ConfigurationError para nombre inválido" do
      expect { described_class.get_job("invalid name!") }.to raise_error(DataDrain::ConfigurationError)
    end
  end

  describe ".job_exists?" do
    it "retorna true cuando el job existe" do
      glue_client.stub_responses(:get_job, {
                                   job: { name: "my-job" }
                                 })

      expect(described_class.job_exists?("my-job")).to be true
    end

    it "retorna false cuando el job no existe" do
      glue_client.stub_responses(:get_job, Aws::Glue::Errors::EntityNotFoundException.new(nil, "not found"))

      expect(described_class.job_exists?("nonexistent-job")).to be false
    end

    it "propaga errores que no sean EntityNotFoundException" do
      glue_client.stub_responses(:get_job, Aws::Glue::Errors::ValidationException.new(nil, "validation error"))

      expect { described_class.job_exists?("my-job") }.to raise_error(Aws::Glue::Errors::ValidationException)
    end

    it "levanta ConfigurationError para nombre inválido" do
      expect { described_class.job_exists?("invalid name!") }.to raise_error(DataDrain::ConfigurationError)
    end
  end

  describe ".create_job" do
    it "crea el job y retorna el job object" do
      glue_client.stub_responses(:create_job, {})
      glue_client.stub_responses(:get_job, {
                                   job: { name: "new-job", role: "arn:aws:iam::123:role/GlueRole" }
                                 })

      job = described_class.create_job(
        "new-job",
        role_arn: "arn:aws:iam::123:role/GlueRole",
        script_location: "s3://bucket/script.py"
      )
      expect(job.name).to eq "new-job"
    end

    it "solo incluye opts no-nil en la llamada a create_job" do
      glue_client.stub_responses(:create_job, {})
      glue_client.stub_responses(:get_job, { job: { name: "my-job", max_retries: 2 } })

      expect do
        described_class.create_job(
          "my-job",
          role_arn: "arn:aws:iam::123:role/GlueRole",
          script_location: "s3://bucket/script.py",
          max_retries: 2,
          description: "test"
        )
      end.not_to raise_error
    end

    it "levanta ConfigurationError para nombre inválido" do
      expect do
        described_class.create_job("invalid!", role_arn: "arn:aws:iam::123:role/GlueRole",
                                               script_location: "s3://bucket/script.py")
      end.to raise_error(DataDrain::ConfigurationError)
    end
  end

  describe ".update_job" do
    it "actualiza el job y retorna el job actualizado" do
      glue_client.stub_responses(:update_job, {})
      glue_client.stub_responses(:get_job, {
                                   job: { name: "my-job", description: "updated description" }
                                 })

      job = described_class.update_job("my-job", description: "updated description")
      expect(job.description).to eq "updated description"
    end

    it "levanta ConfigurationError para nombre inválido" do
      expect { described_class.update_job("invalid!") }.to raise_error(DataDrain::ConfigurationError)
    end
  end

  describe ".delete_job" do
    it "elimina el job y retorna nil" do
      glue_client.stub_responses(:delete_job, {})

      result = described_class.delete_job("my-job")
      expect(result).to be_nil
    end

    it "levanta ConfigurationError para nombre inválido" do
      expect { described_class.delete_job("invalid!") }.to raise_error(DataDrain::ConfigurationError)
    end
  end

  describe ".ensure_job" do
    it "crea el job cuando no existe" do
      glue_client.stub_responses(:get_job, Aws::Glue::Errors::EntityNotFoundException.new(nil, "not found"))
      glue_client.stub_responses(:create_job, {})
      glue_client.stub_responses(:update_job, {})
      glue_client.stub_responses(:get_job, { job: { name: "my-job", role: "arn:aws:iam::123:role/GlueRole" } })

      job = described_class.ensure_job(
        "my-job",
        role_arn: "arn:aws:iam::123:role/GlueRole",
        script_location: "s3://bucket/script.py"
      )
      expect(job.name).to eq "my-job"
    end

    it "actualiza el job cuando ya existe" do
      glue_client.stub_responses(:get_job, { job: { name: "existing-job" } })
      glue_client.stub_responses(:update_job, {})
      glue_client.stub_responses(:get_job, { job: { name: "existing-job", description: "updated" } })

      job = described_class.ensure_job(
        "existing-job",
        role_arn: "arn:aws:iam::123:role/GlueRole",
        script_location: "s3://bucket/script.py",
        description: "updated"
      )
      expect(job.description).to eq "updated"
    end

    it "levanta ConfigurationError para nombre inválido" do
      expect do
        described_class.ensure_job("invalid!", role_arn: "arn:aws:iam::123:role/GlueRole",
                                               script_location: "s3://bucket/script.py")
      end.to raise_error(DataDrain::ConfigurationError)
    end
  end
end
