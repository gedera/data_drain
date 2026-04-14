# frozen_string_literal: true

RSpec.describe DataDrain::Configuration do
  describe "defaults" do
    it "storage_mode es :local" do
      expect(described_class.new.storage_mode).to eq(:local)
    end

    it "db_host es 127.0.0.1" do
      expect(described_class.new.db_host).to eq("127.0.0.1")
    end

    it "db_port es 5432" do
      expect(described_class.new.db_port).to eq(5432)
    end

    it "batch_size es 5000" do
      expect(described_class.new.batch_size).to eq(5000)
    end

    it "throttle_delay es 0.5" do
      expect(described_class.new.throttle_delay).to eq(0.5)
    end

    it "idle_in_transaction_session_timeout es 0" do
      expect(described_class.new.idle_in_transaction_session_timeout).to eq(0)
    end

    it "logger es Logger.new($stdout)" do
      expect(described_class.new.logger).to be_a(Logger)
    end
  end

  describe "#duckdb_connection_string" do
    it "retorna URI con todos los parametros" do
      config = described_class.new
      config.db_host = "db.example.com"
      config.db_port = 5433
      config.db_user = "admin"
      config.db_pass = "secret"
      config.db_name = "mydb"

      uri = config.duckdb_connection_string
      expect(uri).to include("postgresql://")
      expect(uri).to include("admin:secret")
      expect(uri).to include("db.example.com:5433")
      expect(uri).to include("mydb")
    end

    it "incluye idle_in_transaction_session_timeout=0" do
      config = described_class.new
      config.db_user = "u"
      config.db_pass = "p"
      config.db_name = "d"

      uri = config.duckdb_connection_string
      expect(uri).to include("idle_in_transaction_session_timeout%3D0")
    end

    it "incluye idle_in_transaction_session_timeout=60000 cuando es 60000" do
      config = described_class.new
      config.db_user = "u"
      config.db_pass = "p"
      config.db_name = "d"
      config.idle_in_transaction_session_timeout = 60_000

      uri = config.duckdb_connection_string
      expect(uri).to include("idle_in_transaction_session_timeout%3D60000")
    end

    it "codifica correctamente el timeout en la query string" do
      config = described_class.new
      config.db_user = "u"
      config.db_pass = "p"
      config.db_name = "d"
      config.idle_in_transaction_session_timeout = 0

      uri = URI.parse(config.duckdb_connection_string)
      expect(URI.decode_uri_component(uri.query)).to include("idle_in_transaction_session_timeout=0")
    end
  end
end
