# frozen_string_literal: true

require "simplecov"
SimpleCov.start do
  add_filter "/spec/"
  minimum_coverage 60
end

require "data_drain"
require "fileutils"

Dir[File.expand_path("support/**/*.rb", __dir__)].sort.each { |f| require f }

RSpec.configure do |config|
  config.example_status_persistence_file_path = ".rspec_status"
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.define_derived_metadata(:integration) do |_metadata|
    skip "Integration test — requires Postgres real or external service"
  end

  # 💡 Forzamos la configuración de la gema para testing
  config.before(:suite) do
    DataDrain.configure do |c|
      c.storage_mode = :local
      c.logger       = Logger.new(nil) # Silencia los logs en consola durante los tests
    end
  end

  # 💡 Limpiamos el "Data Lake local" temporal después de cada prueba
  config.after(:each) do
    FileUtils.rm_rf("tmp/test_lake")
  end
end
