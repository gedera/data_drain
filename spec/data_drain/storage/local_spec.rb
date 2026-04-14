# frozen_string_literal: true

RSpec.describe DataDrain::Storage::Local do
  let(:config) { DataDrain::Configuration.new }
  let(:adapter) { described_class.new(config) }

  describe "#setup_duckdb" do
    it "es no-op (DuckDB nativo soporta archivos locales)" do
      mock_conn = double("duckdb_conn")
      expect { adapter.setup_duckdb(mock_conn) }.not_to raise_error
    end
  end

  describe "#prepare_export_path" do
    it "crea directorio anidado" do
      Dir.mktmpdir do |tmpdir|
        bucket = File.join(tmpdir, "data_lake")
        adapter.prepare_export_path(bucket, "versions/year=2026/month=3")
        expect(Dir.exist?(File.join(bucket, "versions/year=2026/month=3"))).to be true
      end
    end

    it "no falla si el directorio ya existe" do
      Dir.mktmpdir do |tmpdir|
        bucket = File.join(tmpdir, "data_lake")
        FileUtils.mkdir_p(File.join(bucket, "versions"))
        expect { adapter.prepare_export_path(bucket, "versions") }.not_to raise_error
      end
    end
  end

  describe "#build_path" do
    it "retorna glob pattern sin particiones" do
      path = adapter.build_path("/tmp/lake", "versions", nil)
      expect(path).to eq("/tmp/lake/versions/**/*.parquet")
    end

    it "retorna glob pattern con partition_path" do
      path = adapter.build_path("/tmp/lake", "versions", "isp_id=42/year=2026")
      expect(path).to eq("/tmp/lake/versions/isp_id=42/year=2026/**/*.parquet")
    end

    it "retorna glob pattern con partition_path vacio" do
      path = adapter.build_path("/tmp/lake", "versions", "")
      expect(path).to eq("/tmp/lake/versions/**/*.parquet")
    end
  end

  describe "#destroy_partitions" do
    it "retorna 0 si no hay carpetas para borrar" do
      Dir.mktmpdir do |tmpdir|
        result = adapter.destroy_partitions(
          tmpdir, "versions", %i[year month], { year: 2099, month: 12 }
        )
        expect(result).to eq(0)
      end
    end

    it "borra carpeta especifica con todas las keys" do
      Dir.mktmpdir do |tmpdir|
        bucket = File.join(tmpdir, "lake")
        target = File.join(bucket, "versions/year=2026/month=3")
        FileUtils.mkdir_p(target)
        other = File.join(bucket, "versions/year=2025/month=12")
        FileUtils.mkdir_p(other)

        result = adapter.destroy_partitions(
          bucket, "versions", %i[year month], { year: 2026, month: 3 }
        )

        expect(Dir.exist?(target)).to be false
        expect(Dir.exist?(other)).to be true
        expect(result).to eq(1)
      end
    end

    it "usa wildcard cuando key es nil" do
      Dir.mktmpdir do |tmpdir|
        bucket = File.join(tmpdir, "lake")
        target1 = File.join(bucket, "versions/year=2026/month=3")
        target2 = File.join(bucket, "versions/year=2026/month=4")
        FileUtils.mkdir_p(target1)
        FileUtils.mkdir_p(target2)

        result = adapter.destroy_partitions(
          bucket, "versions", %i[year month], { year: 2026 }
        )

        expect(Dir.exist?(target1)).to be false
        expect(Dir.exist?(target2)).to be false
        expect(result).to eq(2)
      end
    end

    it "borra solo el path matching completo" do
      Dir.mktmpdir do |tmpdir|
        bucket = File.join(tmpdir, "lake")
        target = File.join(bucket, "versions/year=2026/month=3")
        unrelated = File.join(bucket, "other/year=2026/month=3")
        FileUtils.mkdir_p(target)
        FileUtils.mkdir_p(unrelated)

        result = adapter.destroy_partitions(
          bucket, "versions", %i[year month], { year: 2026, month: 3 }
        )

        expect(Dir.exist?(target)).to be false
        expect(Dir.exist?(unrelated)).to be true
        expect(result).to eq(1)
      end
    end
  end
end
