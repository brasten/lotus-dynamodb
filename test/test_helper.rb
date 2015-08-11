require 'rubygems'
require 'bundler/setup'

if ENV['COVERAGE'] == 'true'
  require 'simplecov'
  require 'coveralls'

  SimpleCov.formatter = SimpleCov::Formatter::MultiFormatter[
    SimpleCov::Formatter::HTMLFormatter,
    Coveralls::SimpleCov::Formatter
  ]

  SimpleCov.start do
    command_name 'test'
    add_filter   'test'
  end
end

require 'minitest/autorun'
$:.unshift 'lib'
require 'lotus-dynamodb'

if ENV['Aws']
  Aws.config(
    # logger: Logger.new($stdout),
    # log_level: :debug,
    access_key_id: ENV['Aws_KEY'],
    secret_access_key: ENV['Aws_SECRET'],
  )
else
  uri = URI("http://localhost:8000")

  Aws.config.update(
    # logger: Logger.new($stdout),
    # log_level: :debug,
    endpoint: uri.to_s,
    region: 'us-east-1',
    access_key_id: '',
    secret_access_key: '',
  )


  Net::HTTP.new(uri.host, uri.port).delete('/')
end

def skip_for_fake_dynamo
  skip('fake_dynamo does not support this yet') unless ENV['Aws']
end

require 'fixtures'
