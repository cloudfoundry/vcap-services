require 'rubygems'
require 'sinatra'

get '/' do
  res = ''
  ENV.each do |k, v|
    if k == "VMC_SERVICES"
      res = v
    end
  end
  res
end
