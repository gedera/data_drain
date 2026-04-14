# frozen_string_literal: true

RSpec.describe DataDrain::Engine do
  let(:bucket) { "tmp/test_lake" }
  let(:base_options) do
    {
      bucket: bucket,
      start_date: Time.new(2026, 3, 1),
      end_date: Time.new(2026, 3, 31),
      partition_keys: %w[year month]
    }
  end

  let(:engine) { described_class.new(base_options.merge(table_name: "versions")) }

  let(:mock_duckdb) { instance_double(DuckDB::Connection) }
  let(:mock_pg_conn) { instance_double(PG::Connection) }
  let(:mock_pg_result) { instance_double(PG::Result) }

  before do
    allow_any_instance_of(DuckDB::Database).to receive(:connect).and_return(mock_duckdb)
    allow(PG).to receive(:connect).and_return(mock_pg_conn)
    allow(mock_pg_conn).to receive(:close)
  end

  describe "validación de identificadores" do
    it "rechaza table_name con punto y coma (SQL injection attempt)" do
      expect do
        described_class.new(base_options.merge(table_name: "x; DROP TABLE y"))
      end.to raise_error(DataDrain::ConfigurationError, /table_name/)
    end

    it "rechaza primary_key con espacios" do
      expect do
        described_class.new(base_options.merge(table_name: "versions", primary_key: "id desc"))
      end.to raise_error(DataDrain::ConfigurationError, /primary_key/)
    end

    it "rechaza table_name con punto (schema.table)" do
      expect do
        described_class.new(base_options.merge(table_name: "public.versions"))
      end.to raise_error(DataDrain::ConfigurationError, /table_name/)
    end

    it "acepta identificador válido con guión bajo y números" do
      expect do
        described_class.new(base_options.merge(table_name: "my_table_2", primary_key: "id_2"))
      end.not_to raise_error
    end

    it "acepta identificadores con mayúsculas" do
      expect do
        described_class.new(base_options.merge(table_name: "Versions", primary_key: "Id"))
      end.not_to raise_error
    end

    it "acepta identificador que empieza con guión bajo" do
      expect do
        described_class.new(base_options.merge(table_name: "_internal_table"))
      end.not_to raise_error
    end
  end

  it "ejecuta el flujo ETL completo si la integridad es exitosa" do
    # 1. Setup
    expect(mock_duckdb).to receive(:query).with(/INSTALL postgres/).ordered
    allow(mock_duckdb).to receive(:query).with(/SET max_memory/)
    allow(mock_duckdb).to receive(:query).with(/SET temp_directory/)
    expect(mock_duckdb).to receive(:query).with(/ATTACH .* AS pg_source/).ordered

    # 2. Conteo en Postgres (Simulamos que hay 100 registros)
    expect(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).ordered.and_return([[100]])

    # 3. Exportación a Parquet
    expect(mock_duckdb).to receive(:query).with(/COPY \(/).ordered

    # 4. Verificación de Integridad (Simulamos que el Parquet también tiene 100 registros)
    expect(mock_duckdb).to receive(:query).with(/FROM read_parquet/).ordered.and_return([[100]])

    # 5. Purga en Postgres
    allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)
    allow(mock_pg_result).to receive(:cmd_tuples).and_return(100, 0) # Borra 100 en la primera iteración, 0 en la segunda (sale del loop)
    expect(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).twice.and_return(mock_pg_result)

    expect(engine.call).to be true
  end

  it "aborta la purga y retorna false si la integridad falla" do
    # Ignoramos los querys de setup
    allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)

    # Postgres dice que hay 100
    allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[100]])

    # Exportación
    allow(mock_duckdb).to receive(:query).with(/COPY \(/)

    # 💡 Parquet dice que solo hay 99 (Simulamos un error de integridad)
    allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[99]])

    # Garantizamos que NUNCA se llame a la eliminación en Postgres
    expect(mock_pg_conn).not_to receive(:exec).with(/DELETE FROM versions/)

    expect(engine.call).to be false
  end
end
