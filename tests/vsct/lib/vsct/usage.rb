
class VMC::Cli::Runner
  alias_method :original_command_usage,:command_usage
  def command_usage
    <<USAGE
#{original_command_usage}
  Service Test
    create-test <appname>                        Create test app & bind service
    verify-test <appname>                        Verify the app & service
USAGE
  end
end

