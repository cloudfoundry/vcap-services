require "spec_helper"

describe "VMC::Cli::Runner" do
  SPEC_APP_NAME="vsct_spec_app".freeze
  before :each do
    $red_count=0
  end

  it "should red == 1" do
    "test".red
    $red_count.should == 1
  end

  it "should red == 0" do
    "test".green
    $red_count.should == 0
  end

  it "should parse command correctly" do
    args="create-test #{SPEC_APP_NAME}"
    runner=VMC::Cli::Runner.new(args.split)
    runner.parse_command!
    runner.namespace.should == "servicetest".to_sym
    runner.action.should == "create".to_sym

    args="verify-test #{SPEC_APP_NAME}"
    runner=VMC::Cli::Runner.new(args.split)
    runner.parse_command!
    runner.namespace.should == "servicetest".to_sym
    runner.action.should == "verify".to_sym

    args="apps"
    runner=VMC::Cli::Runner.new(args.split)
    runner.parse_command!
    runner.namespace.should_not == "servicetest".to_sym
    runner.action.should_not == "verify".to_sym
  end

  it "should raise no error on creating test" do
    lambda {VMC::Cli::Command::Servicetest.new.create(SPEC_APP_NAME)}.should_not raise_error
    $red_count.should == 0
  end

  it "should raise no error on verifying test" do
    lambda {VMC::Cli::Command::Servicetest.new.verify(SPEC_APP_NAME)}.should_not raise_error
    $red_count.should == 0
  end

  after :all do
    apps_obj=VMC::Cli::Command::Apps.new({:noprompts=>true})
    apps_obj.delete_app(SPEC_APP_NAME,true)
  end

end
