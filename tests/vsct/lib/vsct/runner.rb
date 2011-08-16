$LOAD_PATH.unshift(File.expand_path("../../",__FILE__))
require "cli"
require 'vsct/usage'
require 'vsct/commands/servicetest'

class VMC::Cli::Runner
  alias_method :original_parse_command!,:parse_command!

  def parse_command!
    verb = @args.shift
    case verb

    when "create-test"
      usage("vmc create-test <appname>")
      set_cmd(:servicetest, :create, 1)

    when "verify-test"
      usage("vmc verify-test <appname>")
      set_cmd(:servicetest, :verify, 1)

    else
      @args.unshift(verb)
      original_parse_command!
    end
  end
end

