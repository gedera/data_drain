# frozen_string_literal: true

RSpec.describe DataDrain::Configuration do
  describe "#validate!" do
    it "no levanta con storage_mode :local" do
      config = described_class.new
      config.storage_mode = :local
      expect { config.validate! }.not_to raise_error
    end

    it "levanta con storage_mode :foo" do
      config = described_class.new
      config.storage_mode = :foo
      expect { config.validate! }.to raise_error(DataDrain::ConfigurationError, /storage_mode/)
    end

    it "levanta con storage_mode :s3 sin aws_region" do
      config = described_class.new
      config.storage_mode = :s3
      config.aws_region = nil
      expect { config.validate! }.to raise_error(DataDrain::ConfigurationError, /aws_region/)
    end

    it "no levanta con storage_mode :s3 + aws_region, sin credenciales (credential_chain)" do
      config = described_class.new
      config.storage_mode = :s3
      config.aws_region = "us-east-1"
      expect { config.validate! }.not_to raise_error
    end
  end

  describe "#validate_for_engine!" do
    it "levanta sin db_host" do
      config = described_class.new
      config.db_host = nil
      config.db_user = "u"
      config.db_name = "d"
      expect { config.validate_for_engine! }.to raise_error(DataDrain::ConfigurationError, /db_host/)
    end

    it "levanta sin db_name" do
      config = described_class.new
      config.db_user = "u"
      config.db_name = nil
      expect { config.validate_for_engine! }.to raise_error(DataDrain::ConfigurationError, /db_name/)
    end

    it "no levanta con todos los campos requeridos seteados" do
      config = described_class.new
      config.db_user = "u"
      config.db_name = "d"
      expect { config.validate_for_engine! }.not_to raise_error
    end

    it "no levanta con db_pass nil (auth peer/trust/IAM)" do
      config = described_class.new
      config.db_user = "u"
      config.db_pass = nil
      config.db_name = "d"
      expect { config.validate_for_engine! }.not_to raise_error
    end
  end
end
