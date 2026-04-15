# frozen_string_literal: true

RSpec.describe DataDrain::Storage::S3 do
  let(:config) do
    DataDrain::Configuration.new.tap do |c|
      c.storage_mode = :s3
      c.aws_region = "us-east-1"
      c.aws_access_key_id = "AKIATEST123"
      c.aws_secret_access_key = "secret_test_456"
    end
  end

  let(:adapter) { described_class.new(config) }

  describe "#setup_duckdb" do
    let(:mock_conn) do
      queries = []
      Class.new do
        define_method(:query) { |q| queries << q }
        define_method(:queries) { queries }
      end.new
    end

    it "carga httpfs" do
      adapter.setup_duckdb(mock_conn)
      expect(mock_conn.queries).to include(match(/INSTALL httpfs/))
      expect(mock_conn.queries).to include(match(/LOAD httpfs/))
    end

    context "con credenciales explícitas" do
      it "crea CREATE SECRET con KEY_ID y SECRET" do
        adapter.setup_duckdb(mock_conn)
        secret_query = mock_conn.queries.find { |q| q.include?("CREATE OR REPLACE SECRET") }
        expect(secret_query).to include("TYPE S3")
        expect(secret_query).to include("KEY_ID 'AKIATEST123'")
        expect(secret_query).to include("SECRET 'secret_test_456'")
        expect(secret_query).to include("REGION 'us-east-1'")
        expect(secret_query).not_to include("credential_chain")
      end
    end

    context "sin credenciales explícitas (credential_chain)" do
      let(:config) do
        DataDrain::Configuration.new.tap do |c|
          c.storage_mode = :s3
          c.aws_region = "us-east-1"
        end
      end

      it "crea CREATE SECRET con credential_chain" do
        adapter.setup_duckdb(mock_conn)
        secret_query = mock_conn.queries.find { |q| q.include?("CREATE OR REPLACE SECRET") }
        expect(secret_query).to include("TYPE S3")
        expect(secret_query).to include("PROVIDER credential_chain")
        expect(secret_query).to include("REGION 'us-east-1'")
        expect(secret_query).not_to include("KEY_ID")
      end
    end

    it "levanta ConfigurationError si aws_region es nil" do
      config.aws_region = nil
      expect { adapter.setup_duckdb(mock_conn) }.to raise_error(DataDrain::ConfigurationError)
    end

    it "escapa comillas simples en credenciales" do
      config.aws_access_key_id = "key'with'quotes"
      adapter.setup_duckdb(mock_conn)
      secret_query = mock_conn.queries.find { |q| q.include?("KEY_ID") }
      expect(secret_query).to include("KEY_ID 'key''with''quotes'")
    end

    it "escapa comillas simples en aws_region" do
      config.aws_region = "us-east-1' OR 1=1"
      adapter.setup_duckdb(mock_conn)
      secret_query = mock_conn.queries.find { |q| q.include?("REGION") }
      expect(secret_query).to include("REGION 'us-east-1'' OR 1=1'")
    end
  end

  describe "#build_path" do
    it "retorna path s3 con bucket y folder" do
      path = adapter.build_path("my-bucket", "versions", nil)
      expect(path).to eq("s3://my-bucket/versions/**/*.parquet")
    end

    it "incluye partition_path" do
      path = adapter.build_path("my-bucket", "versions", "isp_id=42/year=2026/month=3")
      expect(path).to eq("s3://my-bucket/versions/isp_id=42/year=2026/month=3/**/*.parquet")
    end
  end

  describe "#destroy_partitions" do
    let(:s3_client) { Aws::S3::Client.new(stub_responses: true, region: "us-east-1") }

    before do
      allow(Aws::S3::Client).to receive(:new).and_return(s3_client)
    end

    it "arma prefix con folder y primera partition key" do
      s3_client.stub_responses(:list_objects_v2, lambda { |context|
        expect(context.params[:bucket]).to eq("my-bucket")
        expect(context.params[:prefix]).to eq("versions/isp_id=42/")
        { contents: [] }
      })

      adapter.destroy_partitions(
        "my-bucket", "versions", %i[isp_id year month], { isp_id: 42 }
      )
    end

    it "borra objetos que matchean el pattern completo" do
      s3_client.stub_responses(:list_objects_v2, {
                                 contents: [
                                   { key: "versions/isp_id=42/year=2026/month=3/data.parquet" },
                                   { key: "versions/isp_id=42/year=2026/month=3/metadata.parquet" }
                                 ]
                               })

      deleted_keys = []
      s3_client.stub_responses(:delete_objects, lambda { |context|
        deleted_keys.concat(context.params[:delete][:objects].map { |o| o[:key] })
        { deleted: deleted_keys.map { |k| { key: k } } }
      })

      adapter.destroy_partitions(
        "my-bucket", "versions", %i[isp_id year month], { isp_id: 42, year: 2026, month: 3 }
      )

      expect(deleted_keys).to match_array(
        ["versions/isp_id=42/year=2026/month=3/data.parquet",
         "versions/isp_id=42/year=2026/month=3/metadata.parquet"]
      )
    end

    it "retorna 0 si no hay objetos para borrar" do
      s3_client.stub_responses(:list_objects_v2, { contents: [] })

      result = adapter.destroy_partitions(
        "my-bucket", "versions", [:isp_id], { isp_id: 99 }
      )
      expect(result).to eq(0)
    end

    it "filtra objetos que no matchean el pattern" do
      s3_client.stub_responses(:list_objects_v2, {
                                 contents: [
                                   { key: "versions/isp_id=42/year=2026/month=3/data.parquet" },
                                   { key: "versions/isp_id=99/year=2026/month=3/data.parquet" }
                                 ]
                               })

      deleted_keys = []
      s3_client.stub_responses(:delete_objects, lambda { |context|
        deleted_keys.concat(context.params[:delete][:objects].map { |o| o[:key] })
        { deleted: deleted_keys.map { |k| { key: k } } }
      })

      adapter.destroy_partitions(
        "my-bucket", "versions", %i[isp_id year month], { isp_id: 42, year: 2026, month: 3 }
      )

      expect(deleted_keys).to include("versions/isp_id=42/year=2026/month=3/data.parquet")
      expect(deleted_keys).not_to include("versions/isp_id=99/year=2026/month=3/data.parquet")
    end
  end
end
