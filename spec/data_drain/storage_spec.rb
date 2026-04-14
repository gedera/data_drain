# frozen_string_literal: true

RSpec.describe DataDrain::Storage do
  describe ".adapter" do
    after do
      DataDrain::Storage.reset_adapter!
    end

    it "retorna Local cuando storage_mode es :local" do
      DataDrain.configure { |c| c.storage_mode = :local }
      adapter = DataDrain::Storage.adapter
      expect(adapter).to be_a(DataDrain::Storage::Local)
    end

    it "retorna S3 cuando storage_mode es :s3" do
      DataDrain.configure do |c|
        c.storage_mode = :s3
        c.aws_region = "us-east-1"
      end
      adapter = DataDrain::Storage.adapter
      expect(adapter).to be_a(DataDrain::Storage::S3)
    end

    it "levanta InvalidAdapterError con modo desconocido" do
      DataDrain.configure { |c| c.storage_mode = :foo }
      expect { DataDrain::Storage.adapter }.to raise_error(DataDrain::Storage::InvalidAdapterError)
    end

    it "cachea la instancia" do
      DataDrain.configure { |c| c.storage_mode = :local }
      adapter1 = DataDrain::Storage.adapter
      adapter2 = DataDrain::Storage.adapter
      expect(adapter1).to be(adapter2)
    end

    it "despues de reset_adapter! retorna nueva instancia" do
      DataDrain.configure { |c| c.storage_mode = :local }
      adapter1 = DataDrain::Storage.adapter
      DataDrain::Storage.reset_adapter!
      adapter2 = DataDrain::Storage.adapter
      expect(adapter1).not_to be(adapter2)
    end

    it "nueva instancia refleja cambio de storage_mode" do
      DataDrain.configure { |c| c.storage_mode = :local }
      adapter1 = DataDrain::Storage.adapter
      DataDrain::Storage.reset_adapter!
      DataDrain.configure do |c|
        c.storage_mode = :s3
        c.aws_region = "us-east-1"
      end
      adapter2 = DataDrain::Storage.adapter
      expect(adapter1).not_to be(adapter2)
      expect(adapter1).to be_a(DataDrain::Storage::Local)
      expect(adapter2).to be_a(DataDrain::Storage::S3)
    end
  end
end
