require 'aws-sdk'
require 'lotus/model/adapters/abstract'
require 'lotus/model/adapters/implementation'
require 'lotus/model/adapters/dynamodb/coercer'
require 'lotus/model/adapters/dynamodb/collection'
require 'lotus/model/adapters/dynamodb/command'
require 'lotus/model/adapters/dynamodb/query'
require 'lotus/model/adapters/dynamodb/response_array'

module Lotus
  module Model
    module Adapters
      # Adapter for Amazon DynamoDB.
      #
      # @api private
      # @since 0.1.0
      class DynamodbAdapter < Abstract
        include Implementation

        # Initialize the adapter.
        #
        # It takes advantage of Aws::DynamoDB::Client to perform all operations.
        #
        # @param mapper [Object] the database mapper
        #
        # @return [Lotus::Model::Adapters::DynamodbAdapter]
        #
        # @see Lotus::Model::Mapper
        # @see Lotus::Dynamodb::API_VERSION
        # @see http://docs.aws.amazon.com/AwsRubySDK/latest/Aws/DynamoDB/Client/V20120810.html
        #
        # @api private
        # @since 0.1.0
        def initialize(mapper, uri=nil)
          super

          @client = Aws::DynamoDB::Client.new(
            #api_version: Lotus::Dynamodb::API_VERSION
          )
          @collections = {}
        end

        # Creates a record in the database for the given entity.
        # It assigns the `id` attribute, in case of success.
        #
        # @param collection [Symbol] the target collection (it must be mapped)
        # @param entity [#id=] the entity to create
        #
        # @return [Object] the entity
        #
        # @api private
        # @since 0.1.0
        def create(collection, entity)
          entity.id = command(collection).create(entity)
          entity
        end

        # Updates a record in the database corresponding to the given entity.
        #
        # @param collection [Symbol] the target collection (it must be mapped)
        # @param entity [#id] the entity to update
        #
        # @return [Object] the entity
        #
        # @api private
        # @since 0.1.0
        def update(collection, entity)
          command(collection).update(entity)
        end

        # Deletes a record in the database corresponding to the given entity.
        #
        # @param collection [Symbol] the target collection (it must be mapped)
        # @param entity [#id] the entity to delete
        #
        # @api private
        # @since 0.1.0
        def delete(collection, entity)
          command(collection).delete(entity)
        end

        # Deletes all the records from the given collection.
        #
        # This works terribly slow at the moment, and this is only useful for
        # testing small collections. Consider re-creating table from scratch.
        #
        # @param collection [Symbol] the target collection (it must be mapped)
        #
        # @api private
        # @since 0.1.0
        def clear(collection)
          blk = Proc.new {}
          query(collection, blk).each { |entity| delete(collection, entity) }
        end

        # Returns an unique record from the given collection, with the given
        # id.
        #
        # @param collection [Symbol] the target collection (it must be mapped)
        # @param key [Array] the identity of the object
        #
        # @return [Object] the entity
        #
        # @api private
        # @since 0.1.0
        def find(collection, *key)
          command(collection).get(key)
        end

        # Returns an array of unique records from the given collection, with the given
        # ids.
        #
        # @param collection [Symbol] the target collection (it must be mapped)
        # @param keys [Array<Array>] an array of identities
        #
        # @return [Array<Object>] the entities
        #
        # @api private
        # @since 0.2.0
        def batch_find(collection_name, keys)
          response = nil

          while (keys && !keys.empty?)
            response = _collection(collection_name).batch_get(keys, previous_response: response)
            keys = response.unprocessed_keys
          end

          entities = _mapped_collection(collection_name).deserialize(response.try(:entities) || [])

          Dynamodb::ResponseArray.new(entities).tap do |arr|
            arr.unprocessed_keys    = response.unprocessed_keys if response.respond_to?(:unprocessed_keys)
            arr.consumed_capacity   = response.consumed_capacity if response.respond_to?(:consumed_capacity)
          end
        end

        # This method is not implemented. DynamoDB does not allow
        # table-wide sorting.
        #
        # @see http://stackoverflow.com/a/17495069
        #
        # @param collection [Symbol] the target collection (it must be mapped)
        #
        # @raise [NotImplementedError]
        #
        # @since 0.1.0
        def first(collection)
          raise NotImplementedError
        end

        # This method is not implemented. DynamoDB does not allow
        # table-wide sorting.
        #
        # @see http://stackoverflow.com/a/17495069
        #
        # @param collection [Symbol] the target collection (it must be mapped)
        #
        # @raise [NotImplementedError]
        #
        # @since 0.1.0
        def last(collection)
          raise NotImplementedError
        end

        # Fabricates a command for the given query.
        #
        # @param collection [Symbol] the target collection (it must be mapped)
        #
        # @return [Lotus::Model::Adapters::Dynamodb::Command]
        #
        # @see Lotus::Model::Adapters::Dynamodb::Command
        #
        # @api private
        # @since 0.1.0
        def command(collection)
          Dynamodb::Command.new(_collection(collection), _mapped_collection(collection))
        end

        # Fabricates a query
        #
        # @param collection [Symbol] the target collection (it must be mapped)
        # @param context [Object]
        # @param blk [Proc] a block of code to be executed in the context of
        #   the query.
        #
        # @return [Lotus::Model::Adapters::Dynamodb::Query]
        #
        # @see Lotus::Model::Adapters::Dynamodb::Query
        #
        # @api private
        # @since 0.1.0
        def query(collection, context = nil, &blk)
          Dynamodb::Query.new(_collection(collection), _mapped_collection(collection), context, &blk)
        end

        private

        # Returns a collection from the given name.
        #
        # @param name [Symbol] a name of the collection (it must be mapped)
        #
        # @return [Lotus::Model::Adapters::Dynamodb::Collection]
        #
        # @see Lotus::Model::Adapters::Dynamodb::Collection
        #
        # @api private
        # @since 0.1.0
        def _collection(name)
          @collections[name] ||= Dynamodb::Collection.new(
            @client,
            Lotus::Model::Adapters::Dynamodb::Coercer.new(_mapped_collection(name)),
            name,
            _identity(name),
          )
        end


        private

        # Check if request needs to be continued.
        #
        # @param previous_response [BatchResponse] deserialized response from a previous operation
        #
        # @return [Boolean]
        #
        # @api private
        # @since 0.2.0
        def continue?(previous_response)
          return false unless previous_response.unprocessed_keys

          true
        end
      end
    end
  end
end
