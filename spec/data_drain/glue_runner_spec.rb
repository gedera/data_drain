# frozen_string_literal: true

RSpec.describe DataDrain::GlueRunner do
  let(:config) do
    DataDrain::Configuration.new.tap do |c|
      c.aws_region = "us-east-1"
      c.logger = Logger.new(StringIO.new)
    end
  end

  before do
    allow(DataDrain).to receive(:configuration).and_return(config)
  end

  describe ".run_and_wait" do
    let(:mock_client) { instance_double("AwsGlueClient") }

    before do
      stub_const("Aws::Glue::Client", Class.new do
        def initialize(**opts); end
      end)
      allow(Aws::Glue::Client).to receive(:new).and_return(mock_client)
    end

    it "retorna true cuando SUCCEEDED inmediato" do
      start_response = double("start_resp", job_run_id: "run-123")
      run_info = double("run_info", job_run_state: "SUCCEEDED", error_message: nil)

      expect(mock_client).to receive(:start_job_run)
        .with(hash_including(job_name: "my-job"))
        .and_return(start_response)
      expect(mock_client).to receive(:get_job_run)
        .with(hash_including(job_name: "my-job", run_id: "run-123"))
        .and_return(double("get_resp", job_run: run_info))

      result = described_class.run_and_wait("my-job", { "--key" => "val" })
      expect(result).to be true
    end

    it "hace polling hasta SUCCEEDED" do
      start_response = double("start_resp", job_run_id: "run-123")
      running_info = double("run_info", job_run_state: "RUNNING", error_message: nil)
      succeeded_info = double("run_info", job_run_state: "SUCCEEDED", error_message: nil)

      expect(mock_client).to receive(:start_job_run)
        .and_return(start_response)
      expect(mock_client).to receive(:get_job_run).and_return(double("get_resp", job_run: running_info))
      expect(mock_client).to receive(:get_job_run).and_return(double("get_resp", job_run: succeeded_info))

      allow(Kernel).to receive(:sleep)

      result = described_class.run_and_wait("my-job", {}, polling_interval: 5)
      expect(result).to be true
    end

    it "levanta RuntimeError cuando FAILED" do
      start_response = double("start_resp", job_run_id: "run-456")
      run_info = double("run_info", job_run_state: "FAILED", error_message: "Out of memory")

      expect(mock_client).to receive(:start_job_run).and_return(start_response)
      expect(mock_client).to receive(:get_job_run)
        .and_return(double("get_resp", job_run: run_info))

      expect do
        described_class.run_and_wait("failing-job")
      end.to raise_error(RuntimeError, /failing-job/)
    end

    it "levanta RuntimeError cuando STOPPED" do
      start_response = double("start_resp", job_run_id: "run-789")
      run_info = double("run_info", job_run_state: "STOPPED", error_message: nil)

      expect(mock_client).to receive(:start_job_run).and_return(start_response)
      expect(mock_client).to receive(:get_job_run)
        .and_return(double("get_resp", job_run: run_info))

      expect do
        described_class.run_and_wait("stopped-job")
      end.to raise_error(RuntimeError, /STOPPED/)
    end

    it "trunca error_message a 200 chars" do
      start_response = double("start_resp", job_run_id: "run-999")
      long_msg = "x" * 300
      run_info = double("run_info", job_run_state: "FAILED", error_message: long_msg)

      expect(mock_client).to receive(:start_job_run).and_return(start_response)
      expect(mock_client).to receive(:get_job_run).and_return(
        double("get_resp", job_run: run_info)
      )

      expect do
        described_class.run_and_wait("failing-job")
      end.to raise_error(RuntimeError) { |e| expect(e.message.length).to be <= 220 }
    end

    it "levanta cuando hay error_message" do
      start_response = double("start_resp", job_run_id: "run-101")
      run_info = double("run_info", job_run_state: "FAILED", error_message: "Out of memory")

      expect(mock_client).to receive(:start_job_run).and_return(start_response)
      expect(mock_client).to receive(:get_job_run)
        .and_return(double("get_resp", job_run: run_info))

      expect do
        described_class.run_and_wait("failing-job")
      end.to raise_error(RuntimeError, /FAILED/)
    end

    it "levanta DataDrain::Error cuando max_wait_seconds se excede" do
      start_response = double("start_resp", job_run_id: "run-timeout")
      running_info = double("run_info", job_run_state: "RUNNING", error_message: nil)

      allow(mock_client).to receive(:start_job_run).and_return(start_response)
      allow(mock_client).to receive(:get_job_run)
        .and_return(double("get_resp", job_run: running_info))

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
      start_response = double("start_resp", job_run_id: "run-ok")
      succeeded_info = double("run_info", job_run_state: "SUCCEEDED", error_message: nil)

      allow(mock_client).to receive(:start_job_run).and_return(start_response)
      allow(mock_client).to receive(:get_job_run)
        .and_return(double("get_resp", job_run: succeeded_info))

      expect { described_class.run_and_wait("ok-job") }.not_to raise_error
    end
  end
end
