# frozen_string_literal: true

RSpec.describe DataDrain::Observability do
  let(:test_logger) { StringIO.new }
  let(:logger) { Logger.new(test_logger) }

  describe "con include (instance methods)" do
    let(:instance) do
      Class.new do
        include DataDrain::Observability
        attr_accessor :logger

        def initialize(logger)
          @logger = logger
        end

        def emit(level, event, meta = {})
          safe_log(level, event, meta)
        end
      end.new(logger)
    end

    describe "#safe_log" do
      it "no emite log si @logger es nil" do
        instance.logger = nil
        expect { instance.emit(:info, "test.event", foo: "bar") }.not_to raise_error
      end

      it "incluye component y event primero" do
        instance.emit(:info, "engine.start", table: "versions")
        line = test_logger.string
        expect(line).to include("component=")
        expect(line).to include("event=engine.start")
        expect(line).to include("table=versions")
      end

      it "filtra password" do
        instance.emit(:info, "test", password: "secret123")
        expect(test_logger.string).to include("password=[FILTERED]")
        expect(test_logger.string).not_to include("secret123")
      end

      it "filtra token" do
        instance.emit(:info, "test", token: "mytoken")
        expect(test_logger.string).to include("token=[FILTERED]")
        expect(test_logger.string).not_to include("mytoken")
      end

      it "filtra secret" do
        instance.emit(:info, "test", secret: "mysecret")
        expect(test_logger.string).to include("secret=[FILTERED]")
      end

      it "filtra api_key" do
        instance.emit(:info, "test", api_key: "key123")
        expect(test_logger.string).to include("api_key=[FILTERED]")
      end

      it "filtra auth" do
        instance.emit(:info, "test", auth: "bearer123")
        expect(test_logger.string).to include("auth=[FILTERED]")
      end

      it "no filtra campos no sensibles" do
        instance.emit(:info, "test", count: 42, table: "versions")
        expect(test_logger.string).to include("count=42")
        expect(test_logger.string).to include("table=versions")
      end

      it "no propaga excepciones del logger" do
        bad_logger = Class.new do
          def info(&_block)
            raise "logger broken"
          end
        end.new
        instance.logger = bad_logger
        expect { instance.emit(:info, "test", foo: "bar") }.not_to raise_error
      end
    end

    describe "#exception_metadata" do
      it "retorna error_class y error_message" do
        error = RuntimeError.new("something went wrong")
        meta = instance.send(:exception_metadata, error)
        expect(meta[:error_class]).to eq("RuntimeError")
        expect(meta[:error_message]).to eq("something went wrong")
      end

      it "trunca mensaje a 200 caracteres" do
        long_msg = "x" * 300
        error = RuntimeError.new(long_msg)
        meta = instance.send(:exception_metadata, error)
        expect(meta[:error_message].length).to eq(200)
      end

      it "escapa comillas dobles" do
        error = RuntimeError.new('msg with "quotes"')
        meta = instance.send(:exception_metadata, error)
        expect(meta[:error_message]).not_to include('"')
        expect(meta[:error_message]).to include("'")
      end
    end

    describe "#observability_name" do
      it "extrae primer namespace en snake_case" do
        engine_instance = DataDrain::Engine.new(
          bucket: "tmp",
          start_date: Time.now,
          end_date: Time.now,
          table_name: "versions",
          partition_keys: %w[year month]
        )
        name = engine_instance.send(:observability_name)
        expect(name).to eq("data_drain")
      ensure
        engine_instance.instance_variable_get(:@duckdb)&.close
      end

      it "retorna unknown para clases anonimas" do
        obj = Class.new do
          include DataDrain::Observability
          attr_accessor :logger
        end.new
        obj.logger = logger
        name = obj.send(:observability_name)
        expect(name).to eq("unknown")
      end
    end
  end

  describe "con extend (class methods)" do
    let(:klass) do
      Class.new do
        extend DataDrain::Observability
        class << self
          attr_accessor :logger

          def emit(level, event, meta = {})
            safe_log(level, event, meta)
          end
        end
      end
    end

    before do
      klass.logger = logger
    end

    describe "#safe_log" do
      it "funciona con extend" do
        klass.emit(:info, "glue_runner.start", job: "my-job")
        expect(test_logger.string).to include("component=")
        expect(test_logger.string).to include("event=glue_runner.start")
      end
    end
  end
end
