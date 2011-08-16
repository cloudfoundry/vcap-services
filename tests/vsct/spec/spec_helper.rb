ENV["BUNDLE_FILE"]=File.expand_path("../../Gemfile",__FILE__)
$:.unshift File.join(File.dirname(__FILE__), "..", "lib")
require "rubygems"
require "bundler/setup"
require "cli"
require "vmc"
require "vsct"
require "rspec"
#require "webmock"
$red_count=0

module VMCStringExtensions
  alias_method :orig_red,:red
  def red
    $red_count+=1
    orig_red
  end
end

