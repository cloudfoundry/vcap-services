require "curb"

module VMC::Cli::Command
  class Servicetest < Base
    APP_LOCAL_PATH=File.expand_path("../../../../app",__FILE__)
    SERVICE_LIST=%W(mongodb mysql redis rabbitmq)

    def create(appname)
      display 'Creating Application: ', false
      if Dir.exist?(APP_LOCAL_PATH)
        target=VMC::Cli::Config.target_url
        manifest = {
          :name => "#{appname}",
          :staging => {
          :model => "node",
          :stack => "node"
        },
          :resources=> {
          :memory => 64
        },
          :uris => ["#{appname}.#{VMC::Cli::Config.suggest_url}"],
        :instances => 1,
        }
        client.create_app(appname, manifest)

        display 'OK'.green
        args=[appname, APP_LOCAL_PATH]
        apps_obj=Apps.new(@options)
        apps_obj.send(:upload_app_bits,*args)
        serv_obj=Services.new(@options)
        SERVICE_LIST.each do |service|
          service_name="#{service}_#{appname}"
          serv_obj.create_service_banner(service, service_name, true)
          serv_obj.bind_service_banner(service_name, appname)
        end
        sleep 5
        apps_obj.start(appname, true)
      else
        display "#{APP_LOCAL_PATH} does not exist".red
      end
    end

    def verify(appname)
      SERVICE_LIST.each do |service|
        data="abc"
        uri=form_uri(appname,"service/#{service}/#{data}")

        display "Service: #{service}:"
        display "  Post response: ",false
        contents = post_to_app(uri,data)
        code = contents.response_code
        display code==200 ? code.to_s.green : code.to_s.red
        contents.close

        display "  Get response: ",false
        contents = get_from_app(uri)
        code = contents.response_code
        display code==200 ? code.to_s.green : code.to_s.red
        display "  Data: ",false
        body=contents.body_str

        display body==data ? "Matched".green : "Not matched -- #{body}".red
        contents.close
        sleep 2
      end
    end

    private

    def post_to_app(uri, data)
      easy = Curl::Easy.new
      easy.url = uri
      easy.http_post(data)
      easy
    end

    def get_from_app(uri)
      easy = Curl::Easy.new
      easy.url = uri
      easy.http_get
      easy
    end

    def form_uri(appname,relative_path)
      target=VMC::Cli::Config.target_url
      "#{appname}.#{VMC::Cli::Config.suggest_url}/#{relative_path}"
    end

  end
end
