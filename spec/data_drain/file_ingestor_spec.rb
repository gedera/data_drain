# frozen_string_literal: true

RSpec.describe DataDrain::FileIngestor do
  let(:bucket)      { "tmp/test_lake" }
  let(:folder_name) { "netflow" }
  let(:csv_path)    { "tmp/test_netflow.csv" }

  before do
    FileUtils.mkdir_p("tmp")

    # Generamos un CSV crudo escribiendo directamente un string,
    # sin necesidad de importar la librería CSV de Ruby.
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
