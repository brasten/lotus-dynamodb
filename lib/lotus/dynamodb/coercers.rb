require 'lotus/model/coercer'

module Lotus
  module Dynamodb
    module Coercers
      class IO < Lotus::Model::Coercer
        def self.dump(value)
          return if value.nil?

          value
        end

        def self.load(value)
          return if value.nil?

          StringIO.new(value)
        end
      end

      class Boolean < Lotus::Model::Coercer
        def self.dump(value)
          return if value.nil?

          value ? 1 : 0
        end

        def self.load(value)
          return if value.nil?

          value.to_i == 1
        end
      end


      class Time < Lotus::Model::Coercer
        def self.dump(value)
          return nil if value.nil?

          value.to_f
        end

        def self.load(value)
          return nil if value.nil?

          ::Time.at(value.to_f)
        end
      end

      class Date < Lotus::Model::Coercer
        def self.dump(value)
          return nil if value.nil?

          value.to_time.to_f
        end

        def self.load(value)
          return nil if value.nil?

          ::Time.at(value.to_f).to_date
        end
      end


      class DateTime < Lotus::Model::Coercer
        def self.dump(value)
          return nil if value.nil?

          value.to_time.to_f
        end

        def self.load(value)
          return nil if value.nil?

          ::Time.at(value.to_f).to_datetime
        end
      end
    end
  end
end
