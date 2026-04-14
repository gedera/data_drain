# frozen_string_literal: true

RSpec.describe DataDrain::Validations do
  describe ".validate_identifier!" do
    it "no levanta para identificadores válidos" do
      %w[users users_v2 _table TableName].each do |id|
        expect { described_class.validate_identifier!(:x, id) }.not_to raise_error
      end
    end

    it "levanta para identificadores inválidos" do
      invalid = ["1table", "table-name", "table.name", "x; DROP", "", "foo bar"]
      invalid.each do |id|
        expect do
          described_class.validate_identifier!(:x, id)
        end.to raise_error(DataDrain::ConfigurationError)
      end
    end

    it "levanta con mensaje que incluye el nombre del campo" do
      expect do
        described_class.validate_identifier!(:table_name, "1invalid")
      end.to raise_error(DataDrain::ConfigurationError, /table_name/)
    end

    it "convierte valores no-string a string antes de validar" do
      expect { described_class.validate_identifier!(:x, 123) }.to raise_error(DataDrain::ConfigurationError)
      expect { described_class.validate_identifier!(:x, :my_table) }.not_to raise_error
    end
  end
end
