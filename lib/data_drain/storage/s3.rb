# frozen_string_literal: true

module DataDrain
  module Storage
    # Implementación del adaptador de almacenamiento para Amazon S3.
    class S3 < Base
      # Carga la extensión httpfs en DuckDB e inyecta las credenciales de AWS.
      # @param connection [DuckDB::Connection]
      def setup_duckdb(connection)
        connection.query("INSTALL httpfs; LOAD httpfs;")
        connection.query("SET s3_region='#{@config.aws_region}';")
        connection.query("SET s3_access_key_id='#{@config.aws_access_key_id}';")
        connection.query("SET s3_secret_access_key='#{@config.aws_secret_access_key}';")
      end

      # @param folder_name [String]
      # @param partition_path [String, nil]
      # @return [String]
      def build_path(folder_name, partition_path)
        # En S3, el base_path actúa como el nombre del bucket
        base = File.join(@config.base_path, folder_name)
        base = File.join(base, partition_path) if partition_path && !partition_path.empty?
        "s3://#{base}/**/*.parquet"
      end

      # @param folder_name [String]
      # @param partition_keys [Array<Symbol>]
      # @param partitions [Hash]
      # @return [Integer]
      def destroy_partitions(folder_name, partition_keys, partitions)
        client = Aws::S3::Client.new(
          region: @config.aws_region,
          access_key_id: @config.aws_access_key_id,
          secret_access_key: @config.aws_secret_access_key
        )

        regex_parts = partition_keys.map do |key|
          val = partitions[key]
          val.nil? || val.to_s.empty? ? "#{key}=[^/]+" : "#{key}=#{val}"
        end
        pattern_regex = Regexp.new("^#{folder_name}/#{regex_parts.join('/')}")

        objects_to_delete = []
        prefix = "#{folder_name}/"
        first_key = partition_keys.first
        prefix += "#{first_key}=#{partitions[first_key]}/" if partitions[first_key]

        client.list_objects_v2(bucket: @config.base_path, prefix: prefix).each do |response|
          response.contents.each do |obj|
            objects_to_delete << { key: obj.key } if obj.key.match?(pattern_regex)
          end
        end

        delete_in_batches(client, objects_to_delete)
      end

      private

      # @api private
      def delete_in_batches(client, objects_to_delete)
        return 0 if objects_to_delete.empty?

        deleted_count = 0
        objects_to_delete.each_slice(1000) do |batch|
          client.delete_objects(bucket: @config.base_path, delete: { objects: batch, quiet: true })
          deleted_count += batch.size
        end
        deleted_count
      end
    end
  end
end
