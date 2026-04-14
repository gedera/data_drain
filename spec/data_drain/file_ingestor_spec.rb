# frozen_string_literal: true

RSpec.describe DataDrain::FileIngestor do
  let(:bucket)      { "tmp/test_lake" }
  let(:folder_name) { "netflow" }
  let(:csv_path)    { "tmp/test_netflow.csv" }

  describe "validación de identificadores" do
    it "rechaza folder_name con punto y coma" do
      expect do
        described_class.new(
          bucket: bucket,
          source_path: csv_path,
          folder_name: "netflow; rm -rf /"
        )
      end.to raise_error(DataDrain::ConfigurationError, /folder_name/)
    end

    it "rechaza folder_name con espacios" do
      expect do
        described_class.new(
          bucket: bucket,
          source_path: csv_path,
          folder_name: "my folder"
        )
      end.to raise_error(DataDrain::ConfigurationError, /folder_name/)
    end

    it "acepta folder_name válido" do
      expect do
        described_class.new(
          bucket: bucket,
          source_path: csv_path,
          folder_name: "my_folder_2"
        )
      end.not_to raise_error
    end
  end

  before do
    FileUtils.mkdir_p("tmp")

    File.write(csv_path, <<~CSV)
      id,isp_id,bytes,timestamp
      1,42,1024,2026-03-11 10:00:00
      2,42,2048,2026-03-11 10:05:00
      3,99,512,2026-04-01 08:00:00
    CSV
  end

  after do
    File.delete(csv_path) if File.exist?(csv_path)
  end

  it "retorna false si el archivo no existe" do
    ingestor = described_class.new(
      bucket: bucket,
      source_path: "tmp/nonexistent.csv",
      folder_name: folder_name
    )

    expect(ingestor.call).to be false
  end

  it "retorna true y limpia archivo cuando count es 0" do
    empty_csv = "tmp/empty.csv"
    File.write(empty_csv, "id,isp_id,bytes\n")

    ingestor = described_class.new(
      bucket: bucket,
      source_path: empty_csv,
      folder_name: folder_name,
      delete_after_upload: true
    )

    expect(ingestor.call).to be true
    expect(File.exist?(empty_csv)).to be false
  end

  it "delete_after_upload false no borra el archivo" do
    File.write(csv_path, <<~CSV)
      id,isp_id,bytes,timestamp
      1,42,1024,2026-03-11 10:00:00
    CSV

    ingestor = described_class.new(
      bucket: bucket,
      source_path: csv_path,
      folder_name: folder_name,
      delete_after_upload: false
    )

    ingestor.call
    expect(File.exist?(csv_path)).to be true
    File.delete(csv_path)
  end

  it "levanta Error para extension no soportada" do
    xml_path = "tmp/test.xml"
    File.write(xml_path, "<root/>")

    ingestor = described_class.new(
      bucket: bucket,
      source_path: xml_path,
      folder_name: folder_name
    )

    expect { ingestor.call }.to raise_error(DataDrain::Error, /no soportado/)
    File.delete(xml_path) if File.exist?(xml_path)
  end

  it "ingiere archivos JSON usando read_json_auto" do
    json_path = "tmp/test_netflow.json"
    File.write(json_path, <<~JSON)
      [{"id": 1, "isp_id": 42, "bytes": 1024, "timestamp": "2026-03-11 10:00:00"},
       {"id": 2, "isp_id": 99, "bytes": 2048, "timestamp": "2026-04-01 08:00:00"}]
    JSON

    ingestor = described_class.new(
      bucket: bucket,
      source_path: json_path,
      folder_name: folder_name,
      partition_keys: %w[isp_id],
      select_sql: "*, EXTRACT(YEAR FROM CAST(timestamp AS TIMESTAMP))::INT AS year, EXTRACT(MONTH FROM CAST(timestamp AS TIMESTAMP))::INT AS month",
      delete_after_upload: false
    )

    expect(ingestor.call).to be true
    expect(Dir.exist?(File.join(bucket, folder_name, "isp_id=42"))).to be true
    expect(Dir.exist?(File.join(bucket, folder_name, "isp_id=99"))).to be true
  ensure
    File.delete(json_path) if File.exist?(json_path)
  end

  it "ingiere archivos Parquet existentes" do
    parquet_path = "tmp/test_input.parquet"
    db = DuckDB::Database.open(":memory:")
    conn = db.connect
    conn.query(<<~SQL)
      CREATE TABLE t AS SELECT * FROM read_csv_auto('#{csv_path}');
      COPY t TO '#{parquet_path}' (FORMAT PARQUET);
    SQL

    ingestor = described_class.new(
      bucket: bucket,
      source_path: parquet_path,
      folder_name: folder_name,
      partition_keys: %w[isp_id],
      select_sql: "*, EXTRACT(YEAR FROM CAST(timestamp AS TIMESTAMP))::INT AS year, EXTRACT(MONTH FROM CAST(timestamp AS TIMESTAMP))::INT AS month",
      delete_after_upload: false
    )

    expect(ingestor.call).to be true
    expect(Dir.exist?(File.join(bucket, folder_name, "isp_id=42"))).to be true
  ensure
    File.delete(parquet_path) if File.exist?(parquet_path)
  end

  it "ingiere un CSV local, lo convierte a Parquet y lo particiona correctamente" do
    ingestor = described_class.new(
      bucket: bucket,
      source_path: csv_path,
      folder_name: folder_name,
      partition_keys: %w[isp_id year month],
      select_sql: "*, EXTRACT(YEAR FROM CAST(timestamp AS TIMESTAMP))::INT AS year, EXTRACT(MONTH FROM CAST(timestamp AS TIMESTAMP))::INT AS month",
      delete_after_upload: false
    )

    # Verificamos que el proceso retorne true
    expect(ingestor.call).to be true

    # Verificamos que DuckDB haya creado las particiones en el disco local
    partition_42 = File.join(bucket, folder_name, "isp_id=42", "year=2026", "month=3")
    partition_99 = File.join(bucket, folder_name, "isp_id=99", "year=2026", "month=4")

    expect(Dir.exist?(partition_42)).to be true
    expect(Dir.exist?(partition_99)).to be true

    # Verificamos que los archivos Parquet realmente existan dentro
    expect(Dir.glob("#{partition_42}/*.parquet")).not_to be_empty
    expect(Dir.glob("#{partition_99}/*.parquet")).not_to be_empty
  end
end
