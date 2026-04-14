# frozen_string_literal: true

module ConfigurationHelper
  def with_config(**overrides)
    original = DataDrain.configuration
    DataDrain.configure { |c| overrides.each { |k, v| c.send("#{k}=", v) } }
    yield
  ensure
    DataDrain.instance_variable_set(:@configuration, original)
    DataDrain::Storage.reset_adapter!
  end
end

RSpec.configure do |config|
  config.include ConfigurationHelper
  config.after(:each) do
    DataDrain.reset_configuration!
  end
end
