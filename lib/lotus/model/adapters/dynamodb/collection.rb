require 'securerandom'
require 'aws-sdk'
require 'lotus/utils/hash'

module Lotus
  module Model
    module Adapters
      module Dynamodb
        # Acts like table, using Aws::DynamoDB::Client.
        #
        # @api private
        # @since 0.1.0
        class Collection
          include Aws::DynamoDB::Types

          # Response interface provides count and entities.
          #
          # @api private
          # @since 0.1.0
          class Response
            attr_accessor :count,
                          :entities,
                          :last_evaluated_key,
                          :consumed_capacity

            def initialize
              @count = 0
              @consumed_capacity = nil
              @entities = []
            end
          end

          # BatchResponse interface provides entities and unprocessed items
          #
          # @api private
          # @since 0.2.0
          #
          class BatchResponse
            attr_accessor :entities,
                          :unprocessed_keys,
                          :consumed_capacity

            def initialize
              @consumed_capacity = nil
              @unprocessed_keys = nil
              @entities = []
            end
          end

          # @attr_reader name [String] the name of the collection (eg. `users`)
          #
          # @since 0.1.0
          # @api private
          attr_reader :name

          # @attr_reader identity [Symbol] the primary key of the collection
          #   (eg. `:id`)
          #
          # @since 0.1.0
          # @api private
          attr_reader :identity

          # Initialize a collection.
          #
          # @param client [Aws::DynamoDB::Client] DynamoDB client
          # @param coercer [Lotus::Model::Adapters::Dynamodb::Coercer]
          # @param name [Symbol] the name of the collection (eg. `:users`)
          # @param identity [Symbol] the primary key of the collection
          #   (eg. `:id`).
          #
          # @api private
          # @since 0.1.0
          def initialize(client, coercer, name, identity)
            @client, @coercer = client, coercer
            @name, @identity = name.to_s, identity
            @key_schema = {}
          end

          # Creates a record for the given entity and returns a primary key.
          #
          # @param entity [Object] the entity to persist
          #
          # @see Lotus::Model::Adapters::Dynamodb::Command#create
          # @see http://docs.aws.amazon.com/AwsRubySDK/latest/Aws/DynamoDB/Client/V20120810.html#put_item-instance_method
          #
          # @return the primary key of the just created record.
          #
          # @api private
          # @since 0.1.0
          def create(entity)
            entity[identity] ||= SecureRandom.uuid

            @client.put_item(
              table_name: name,
              item: serialize_item(entity),
            )

            entity[identity]
          end

          # Updates the record corresponding to the given entity.
          #
          # @param entity [Object] the entity to persist
          #
          # @see Lotus::Model::Adapters::Dynamodb::Command#update
          # @see http://docs.aws.amazon.com/AwsRubySDK/latest/Aws/DynamoDB/Client/V20120810.html#update_item-instance_method
          #
          # @api private
          # @since 0.1.0
          def update(entity)
            @client.update_item(
              table_name: name,
              key: serialize_key(entity),
              attribute_updates: serialize_attributes(entity),
            )
          end

          # Deletes the record corresponding to the given entity.
          #
          # @param entity [Object] the entity to delete
          #
          # @see Lotus::Model::Adapters::Dynamodb::Command#delete
          # @see http://docs.aws.amazon.com/AwsRubySDK/latest/Aws/DynamoDB/Client/V20120810.html#delete_item-instance_method
          #
          # @api private
          # @since 0.1.0
          def delete(entity)
            @client.delete_item(
              table_name: name,
              key: serialize_key(entity),
            )
          end

          # Returns an unique record from the given collection, with the given
          # id.
          #
          # @param key [Array] the identity of the object
          #
          # @see Lotus::Model::Adapters::Dynamodb::Command#get
          # @see http://docs.aws.amazon.com/AwsRubySDK/latest/Aws/DynamoDB/Client/V20120810.html#get_item-instance_method
          #
          # @return [Hash] the serialized record
          #
          # @api private
          # @since 0.1.0
          def get(key)
            return if key.any? { |v| v.to_s == "" }
            return if key.count != key_schema.count

            response = @client.get_item(
              table_name: name,
              key: serialize_key(key),
            )

            deserialize_item(response[:item]) if response[:item]
          end

          # Returns an unique record from the given collection, with the given
          # id.
          #
          # @param keys [Array<Array>] identities
          #
          # @see Lotus::Model::Adapters::Dynamodb::Command#get
          # @see http://docs.aws.amazon.com/AwsRubySDK/latest/Aws/DynamoDB/Client/V20120810.html#get_item-instance_method
          #
          # @return [Array<Hash>] the serialized recordds
          #
          # @api private
          # @since 0.2.0
          def batch_get(keys, previous_response: nil)
            keys = keys.map { |k| [k].flatten }.uniq # ensure Array<Array>
            key_schema_count = key_schema.count

            return BatchResponse.new if keys.flatten.any? { |v| v.to_s == "" }
            return BatchResponse.new if keys.any? { |v| v.count != key_schema_count }

            response = @client.batch_get_item(
              request_items: {
                name => {
                  keys: keys.map(&method(:serialize_key))
                }
              }
            )

            deserialize_batch_response(response, previous_response: previous_response)
          end


          # Performs DynamoDB query operation.
          #
          # @param options [Hash] Aws::DynamoDB::Client options
          # @param previous_response [Response] deserialized response from a previous operation
          #
          # @see http://docs.aws.amazon.com/AwsRubySDK/latest/Aws/DynamoDB/Client/V20120810.html#query-instance_method
          #
          # @return [Array<Hash>] the serialized entities
          #
          # @api private
          # @since 0.1.0
          def query(options = {}, previous_response = nil)
            response = @client.query(options.merge(table_name: name))
            deserialize_response(response, previous_response)
          end

          # Performs DynamoDB scan operation.
          #
          # @param options [Hash] Aws::DynamoDB::Client options
          # @param previous_response [Response] deserialized response from a previous operation
          #
          # @see http://docs.aws.amazon.com/AwsRubySDK/latest/Aws/DynamoDB/Client/V20120810.html#scan-instance_method
          #
          # @return [Array<Hash>] the serialized entities
          #
          # @api private
          # @since 0.1.0
          def scan(options = {}, previous_response = nil)
            response = @client.scan(options.merge(table_name: name))
            deserialize_response(response, previous_response)
          end

          # Fetches DynamoDB table schema.
          #
          # @see http://docs.aws.amazon.com/AwsRubySDK/latest/Aws/DynamoDB/Client/V20120810.html#describe_table-instance_method
          #
          # @return [Hash] table schema definition
          #
          # @api private
          # @since 0.1.0
          def schema
            @schema ||= @client.describe_table(table_name: name).table
          end

          # Maps table key schema to hash with attribute name as key and key
          # type as value.
          #
          # @param index [String] index to check (defaults to table itself)
          #
          # @see Lotus::Model::Adapters::Dynamodb::Collection#schema
          #
          # @return [Hash] key schema definition
          #
          # @api private
          # @since 0.1.0
          def key_schema(index = nil)
            return @key_schema[index] if @key_schema[index]

            current_schema = if index
              everything = Array(schema[:local_secondary_indexes]) +
                           Array(schema[:global_secondary_indexes])
              indexes = Hash[everything.map { |i| [i[:index_name], i] }]
              indexes[index][:key_schema]
            else
              schema[:key_schema]
            end

            @key_schema[index] ||= Hash[current_schema.to_a.map do |key|
              [key[:attribute_name].to_sym, key[:key_type]]
            end]
          end

          # Checks if given column is in key schema or not.
          #
          # @param column [String] column to check
          # @param index [String] index to check (defaults to table itself)
          #
          # @see Lotus::Model::Adapters::Dynamodb::Collection#key_schema
          #
          # @return [Boolean]
          #
          # @api private
          # @since 0.1.0
          def key?(column, index = nil)
            key_schema(index).has_key?(column)
          end

          # Coerce and format attribute value to match DynamoDB type.
          #
          # @param column [String] the attribute column
          # @param value [Object] the attribute value
          #
          # @see Aws::DynamoDB::Types
          #
          # @return [Hash] the formatted attribute
          #
          # @api private
          # @since 0.1.0
          def format_attribute(column, value)
            value = @coercer.public_send(:"serialize_#{ column }", value)
            # format_attribute_value(value)
            value
          end

          # Serialize given record to have proper attributes for 'item' query.
          #
          # @param record [Hash] the serialized record
          #
          # @see Aws::DynamoDB::Types
          #
          # @return [Hash] the serialized item
          #
          # @api private
          # @since 0.1.0
          def serialize_item(record)
            Hash[record.delete_if { |_, v| v.nil? }.map do |k, v|
              # [k.to_s, format_attribute_value(v)]
              [k.to_s, v]
            end]
          end

          # Serialize given record or primary key to have proper attributes
          # for 'key' query.
          #
          # @param record [Hash,Array] the serialized record or primary key
          #
          # @see Aws::DynamoDB::Types
          #
          # @return [Hash] the serialized key
          #
          # @api private
          # @since 0.1.0
          def serialize_key(record)
            Hash[key_schema.keys.each_with_index.map do |k, idx|
              v = record.is_a?(Hash) ? record[k] : record[idx]
              [k.to_s, format_attribute(k, v)]
            end]
          end

          # Serialize given entity to exclude key schema attributes.
          #
          # @param entity [Hash] the entity
          #
          # @see Aws::DynamoDB::Types
          #
          # @return [Hash] the serialized attributes
          #
          # @api private
          # @since 0.1.0
          def serialize_attributes(entity)
            keys = key_schema.keys
            Hash[entity.reject { |k, _| keys.include?(k) }.map do |k, v|
              if v.nil?
                [k.to_s, { action: "DELETE" }]
              else
                # [k.to_s, { value: format_attribute_value(v), action: "PUT" }]
                [k.to_s, { value: v, action: "PUT" }]
              end
            end]
          end

          # Deserialize DynamoDB scan/query response.
          #
          # @param response [Hash] the serialized response
          # @param previous_response [Response] deserialized response from a previous operation
          #
          # @return [Response] the deserialized response
          #
          # @api private
          # @since 0.1.0
          def deserialize_response(response, previous_response = nil)
            current_response = previous_response || Response.new
            current_response.count += response.count

            current_response.entities += response.items.map do |item|
              deserialize_item(item)
            end if response.items

            current_response.last_evaluated_key = response.last_evaluated_key
            current_response.consumed_capacity  = response.consumed_capacity
            current_response
          end

          # Deserialize DynamoDB batch_get_item response.
          #
          # @param response [Hash] the serialized response
          # @param previous_response [BatchResponse] deserialized response from a previous operation
          #
          # @return [BatchResponse] the deserialized response
          #
          # @api private
          # @since 0.1.0
          def deserialize_batch_response(response, previous_response: nil)
            current_response = previous_response || BatchResponse.new

            current_response.entities += response
                .responses
                .values
                .flatten
                .map do |item|
                  deserialize_item(item)
                end unless response.responses.empty?

            current_response.unprocessed_keys   = response.unprocessed_keys
            current_response.consumed_capacity  = response.consumed_capacity
            current_response
          end

          # Deserialize item from DynamoDB response.
          #
          # @param item [Hash] the serialized item
          #
          # @see Aws::DynamoDB::Types
          #
          # @return [Hash] the deserialized record
          #
          # @api private
          # @since 0.1.0
          def deserialize_item(record)
            # Not entirely sure if removing "values_from_response_hash" has other
            # side effects.
            # Lotus::Utils::Hash.new(values_from_response_hash(record)).symbolize!

            Lotus::Utils::Hash.new(record).symbolize!
          end
        end
      end
    end
  end
end


__END__

Response V2:
#<struct Aws::DynamoDB::Types::ScanOutput
  items=[
    {
      "item_ids"=>#<Set: {#<BigDecimal:7f99519e96b0,'0.4E1',9(18)>, #<BigDecimal:7f99519e9570,'0.5E1',9(18)>, #<BigDecimal:7f99519e9430,'0.6E1',9(18)>}>,
      "created_at"=>#<BigDecimal:7f99519e8e18,'0.1439301354 2148638E10',27(27)>,
      "region"=>"asia",
      "uuid"=>"17655a49-3cc3-44ce-b8da-c1f1022e7e5e",
      "subtotal"=>#<BigDecimal:7f99519e8198,'0.1E3',9(18)>,
      "content"=>#<StringIO:0x007f99519e3eb8>
    }, {
      "item_ids"=>#<Set: {#<StringIO:0x007f99519e3b70>}>,
      "created_at"=>#<BigDecimal:7f99519e3828,'0.1439301354 214847E10',27(27)>,
      "region"=>"usa",
      "uuid"=>"1c089181-c95f-4078-a6b4-d73b011efe9e",
      "subtotal"=>#<BigDecimal:7f99519e3350,'0.5E1',9(18)>,
      "content"=>#<StringIO:0x007f99519e31c0>
    }, {
      "item_ids"=>#<Set: {#<BigDecimal:7f99519e2ec8,'0.1E1',9(18)>, #<BigDecimal:7f99519e2e78,'0.2E1',9(18)>, #<BigDecimal:7f99519e2d88,'0.3E1',9(18)>}>,
      "created_at"=>#<BigDecimal:7f99519e2ab8,'0.1439301354 214824E10',27(27)>,
      "region"=>"europe",
      "uuid"=>"745dff90-b793-4306-80df-70eb4e62d753",
      "subtotal"=>#<BigDecimal:7f99519e2658,'0.15E2',9(18)>,
      "content"=>#<StringIO:0x007f99519e24a0>
    }, {
      "item_ids"=>#<Set: {"2", "3", "4"}>,
      "created_at"=>#<BigDecimal:7f99519e1eb0,'0.1439301354 214839E10',27(27)>,
      "region"=>"europe",
      "uuid"=>"ff50fd2d-da07-494d-a310-fb4f4f35e806",
      "subtotal"=>#<BigDecimal:7f99519e1938,'0.1E2',9(18)>,
      "content"=>#<StringIO:0x007f99519e1500>
    }
  ],
  count=4,
  scanned_count=4,
  last_evaluated_key=nil,
  consumed_capacity=nil
>
