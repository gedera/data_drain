# frozen_string_literal: true

require_relative "lib/data_drain/version"

Gem::Specification.new do |spec|
  spec.name = "data_drain"
  spec.version = DataDrain::VERSION
  spec.authors = ["Gabriel"]
  spec.email = ["gab.edera@gmail.com"]

  spec.summary = "Micro-framework para drenar datos de PostgreSQL a Parquet vía DuckDB."
  spec.description = "Extrae datos transaccionales, los archiva en un Data Lake (S3/Local) " \
                     "en formato Parquet usando Hive Partitioning, y purga el origen de forma segura."
  spec.homepage = "https://github.com/gedera/data_drain"
  spec.required_ruby_version = ">= 3.0.0"

  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").reject do |f|
      (File.expand_path(f) == __FILE__) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git .github appveyor Gemfile])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # 💡 Dependencias Core de la Gema
  spec.add_dependency "activemodel", ">= 6.0"
  spec.add_dependency "aws-sdk-glue", "~> 1.0"
  spec.add_dependency "aws-sdk-s3", "~> 1.114"
  spec.add_dependency "duckdb", "~> 1.4"
  spec.add_dependency "pg", ">= 1.2"
end
