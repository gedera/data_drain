# frozen_string_literal: true

module DataDrain
  module Storage
    class S3 < Base
      # Carga la extensión httpfs en DuckDB e inyecta las credenciales de AWS.
      # Si aws_access_key_id y aws_secret_access_key están seteados, usa
      # credenciales explícitas. Si no, usa credential_chain (IAM role, env vars,
      # ~/.aws/credentials).
      # @param connection [DuckDB::Connection]
      # @raise [DataDrain::ConfigurationError] si aws_region no está configurado
      def setup_duckdb(connection)
        connection.query("INSTALL httpfs; LOAD httpfs;")
        create_s3_secret(connection)
      end

      # @param bucket [String]
      # @param folder_name [String]
      # @param partition_path [String, nil]
      # @return [String]
      def build_path(bucket, folder_name, partition_path)
        "s3://#{build_path_base(bucket, folder_name, partition_path)}/**/*.parquet"
      end

      # @param bucket [String]
      # @param folder_name [String]
      # @param partition_keys [Array<Symbol>]
      # @param partitions [Hash]
      # @return [Integer]
      def destroy_partitions(bucket, folder_name, partition_keys, partitions)
        client = s3_client
        prefix, pattern_regex = build_destroy_pattern(folder_name, partition_keys, partitions)
        objects = collect_matching_objects(client, bucket, prefix, pattern_regex)
        delete_in_batches(client, bucket, objects)
      end

      private

      # @return [Aws::S3::Client]
      def s3_client
        Aws::S3::Client.new(
          region: @config.aws_region,
          access_key_id: @config.aws_access_key_id,
          secret_access_key: @config.aws_secret_access_key
        )
      end

      # @param folder_name [String]
      # @param partition_keys [Array<Symbol>]
      # @param partitions [Hash]
      # @return [Array(String, Regexp)] prefix y pattern_regex
      def build_destroy_pattern(folder_name, partition_keys, partitions)
        regex_parts = partition_keys.map do |key|
          val = partitions[key]
          val.nil? || val.to_s.empty? ? "#{key}=[^/]+" : "#{key}=#{val}"
        end
        pattern = Regexp.new("^#{folder_name}/#{regex_parts.join("/")}")

        prefix = "#{folder_name}/"
        first_key = partition_keys.first
        prefix += "#{first_key}=#{partitions[first_key]}/" if partitions[first_key]

        [prefix, pattern]
      end

      # @param client [Aws::S3::Client]
      # @param bucket [String]
      # @param prefix [String]
      # @param pattern_regex [Regexp]
      # @return [Array<Hash>]
      def collect_matching_objects(client, bucket, prefix, pattern_regex)
        objects = []
        client.list_objects_v2(bucket: bucket, prefix: prefix).each do |response|
          response.contents.each do |obj|
            objects << { key: obj.key } if obj.key.match?(pattern_regex)
          end
        end
        objects
      end

      # @param connection [DuckDB::Connection]
      # @raise [DataDrain::ConfigurationError]
      def create_s3_secret(connection)
        region = @config.aws_region
        raise DataDrain::ConfigurationError, "aws_region es obligatorio para storage_mode=:s3" if region.nil?

        if @config.aws_access_key_id && @config.aws_secret_access_key
          connection.query(<<~SQL)
            CREATE OR REPLACE SECRET data_drain_s3 (
              TYPE S3,
              KEY_ID '#{escape_sql(@config.aws_access_key_id)}',
              SECRET '#{escape_sql(@config.aws_secret_access_key)}',
              REGION '#{escape_sql(region)}'
            );
          SQL
        else
          connection.query(<<~SQL)
            CREATE OR REPLACE SECRET data_drain_s3 (
              TYPE S3,
              PROVIDER credential_chain,
              REGION '#{escape_sql(region)}'
            );
          SQL
        end
      end

      # @param value [String]
      # @return [String]
      def escape_sql(value)
        value.to_s.gsub("'", "''")
      end

      # @param client [Aws::S3::Client]
      # @param bucket [String]
      # @param objects_to_delete [Array<Hash>]
      # @return [Integer]
      def delete_in_batches(client, bucket, objects_to_delete)
        return 0 if objects_to_delete.empty?

        deleted_count = 0
        objects_to_delete.each_slice(1000) do |batch|
          client.delete_objects(bucket: bucket, delete: { objects: batch, quiet: true })
          deleted_count += batch.size
        end
        deleted_count
      end
    end
  end
end
