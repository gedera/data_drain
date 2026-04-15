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
end
