module Lotus::Model::Adapters
  module Dynamodb

    # Provides a way of communicating Dynamo-specific response information
    # to Repository clients in a way that still quacks like an Array.
    #
    # @since 0.2.0
    #
    class ResponseArray < Array
      attr_accessor :consumed_capacity
      attr_accessor :scanned_count
      attr_accessor :last_evaluated_key
      

    end
  end
end
