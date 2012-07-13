# Copyright (c) 2009-2011 VMware, Inc.
require "eventmachine"
require "em-http-request"
require "vcap/common"
require "vcap/component"
require "sinatra"
require "nats/client"
require "redis"
require "json"
require "sys/filesystem"
require "fileutils"
require "services/api"
require "services/api/const"

include Sys

module VCAP
  module Services
    module Serialization
    end
  end
end

class VCAP::Services::Serialization::Server < Sinatra::Base

  REQ_OPTS = %w(serialization_base_dir mbus port external_uri redis).map {|o| o.to_sym}
  VALID_CREDENTIAL_CHARACTERS = ("A".."Z").to_a + ("a".."z").to_a + ("0".."9").to_a

  set :show_exceptions, false
  set :method_override, true

  def initialize(opts)
    super
    missing_opts = REQ_OPTS.select {|o| !opts.has_key? o}
    raise ArgumentError, "Missing options: #{missing_opts.join(', ')}" unless missing_opts.empty?
    @opts = opts
    @logger = opts[:logger] || make_logger
    @nginx = opts[:nginx]
    @host = opts[:host]
    @port = opts[:port]
    @external_uri = opts[:external_uri]
    @cloud_controller_uri = http_uri(opts[:cloud_controller_uri] || "api.vcap.me")
    @sds_cc_uri = "#{@cloud_controller_uri}/services/v1/sds"
    @hb_interval = opts[:heartbeat_interval] || 60
    @upload_token = opts[:upload_token]
    @expire_time = opts[:expire_time] || 7200
    @purge_expired_interval = opts[:purge_expired_interval] || 1200
    @purge_num = opts[:purge_num] || 1000
    @router_start_channel  = nil
    @base_dir = opts[:serialization_base_dir]
    @sds_cc_register_json = {
      :host => @host,
      :port => (@nginx ? @nginx["nginx_port"] : @port).to_i,
      :token => @upload_token,
      :active => true,
    }.to_json
    @sds_cc_deact_json = {
      :host => @host,
      :port =>  (@nginx ? @nginx["nginx_port"] : @port).to_i,
      :token => @upload_token,
      :active => false
    }.to_json

    @cc_req_hdrs = {
      VCAP::Services::Api::SDS_UPLOAD_TOKEN_HEADER => @upload_token,
      'Content-Type' => 'application/json'
    }

    NATS.on_error do |e|
      if e.kind_of? NATS::ConnectError
        @logger.error("EXITING! NATS connection failed: #{e}")
        exit
      else
        @logger.error("NATS problem, #{e}")
      end
    end
    @nats = NATS.connect(:uri => opts[:mbus]) {
      VCAP::Component.register(
        :nats => @nats,
        :type => "SerializationDataServer",
        :index => opts[:index] || 0,
        :config => opts
      )

      on_connect_nats
    }

    z_interval = opts[:z_interval] || 30
    EM.add_periodic_timer(z_interval) do
      EM.defer { update_varz }
    end if @nats

    # Defer 5 seconds to give service a change to wake up
    EM.add_timer(5) do
      EM.defer { update_varz }
    end if @nats

    # Setup purger for expired upload files
    EM.add_periodic_timer(@purge_expired_interval) {
      EM.defer{ purge_expired }
    }

    # Setup heartbeats and exit handlers
    EM.add_periodic_timer(@hb_interval) { send_heartbeat }
    EM.next_tick { send_heartbeat }
    Kernel.at_exit do
      if EM.reactor_running?
        send_deactivation_notice(false)
      else
        EM.run { send_deactivation_notice }
      end
    end

    @router_register_json  = {
      :host => @host,
      :port => ( @nginx ? @nginx["nginx_port"] : @port),
      :uris => [ @external_uri ],
      :tags => {:components =>  "SerializationDataServer"},
    }.to_json
  end

  def http_uri(uri)
    uri = "http://#{uri}" unless (uri.index('http://') == 0 || uri.index('https://') == 0)
    uri
  end

  def on_connect_nats()
    @logger.info("Register download server uri : #{@router_register_json}")
    @nats.publish('router.register', @router_register_json)
    @router_start_channel = @nats.subscribe('router.start') { @nats.publish('router.register', @router_register_json)}
    @redis = connect_redis
  end

  def varz_details()
    varz = {}
    # check NFS disk free space
    free_space = 0
    begin
      stats = Filesystem.stat("#{@base_dir}")
      avail_blocks = stats.blocks_available
      total_blocks = stats.blocks
      free_space = format("%.2f", avail_blocks.to_f / total_blocks.to_f * 100)
    rescue => e
      @logger.error("Failed to get filesystem info of #{@base_dir}: #{e}")
    end
    varz[:nfs_free_space] = free_space

    varz
  end

  def update_varz()
    varz = varz_details
    varz.each { |k, v|
      VCAP::Component.varz[k] = v
    }
  end

  def connect_redis()
    redis_config = %w(host port password).inject({}){|res, o| res[o.to_sym] = @opts[:redis][o]; res}
    Redis.new(redis_config)
  end

  def make_logger()
    logger = Logger.new(STDOUT)
    logger.level = Logger::DEBUG
    logger
  end

  # Let cloud controller know the server is alive and where it can find us
  def send_heartbeat
    @logger.info("Sending info to cloud controller: #{@sds_cc_uri}")

    req = {
      :head => @cc_req_hdrs,
      :body => @sds_cc_register_json
    }

    http = EM::HttpRequest.new(@sds_cc_uri).post(req)

    http.callback do
      if http.response_header.status == 200
        @logger.info("Successfully registered with cloud controller")
      else
        @logger.error("Failed registering with cloud controller, status=#{http.response_header.status}")
      end
    end

    http.errback do
      @logger.error("Failed registering with cloud controller: #{http.error}")
    end
  end

  # Unregister external uri
  def send_deactivation_notice(stop_event_loop=true)
    # stop external service
    @logger.info("Sending deactivation notice to router")
    @nats.unsubscribe(@router_start_channel) if @router_start_channel
    @logger.debug("Unregister uri: #{@router_register_json}")
    @nats.publish("router.unregister", @router_register_json)
    @nats.close

    # stop internal service
    @logger.info("Sending deactivation notice to cloud controller")
    req = {
      :head => @cc_req_hdrs,
      :body => @sds_cc_deact_json
    }

    http = EM::HttpRequest.new(@sds_cc_uri).post(req)

    http.callback do
      if http.response_header.status == 200
        @logger.info("Successfully registered with cloud controller")
      else
        @logger.error("Failed deactivation with cloud controller, status=#{http.response_header.status}")
      end
    end

    http.errback do
      @logger.error("Failed deactivation notice to cloud controller: #{http.error}")
    end

    EM.stop if stop_event_loop
  end

  def redis_key(service, service_id)
    "vcap:snapshot:#{service_id}"
  end

  def redis_file_key(service, service_id)
    "vcap:serialized_file:#{service}:#{service_id}"
  end

  def redis_upload_purge_queue
    "vcap:upload_purge_queue"
  end

  def file_path(service, id, snapshot_id, file_name)
    File.join(@base_dir, "snapshots", service, id[0,2], id[2,2], id[4,2], id, snapshot_id, file_name)
  end

  def nginx_path(service, id, snapshot_id, file_name)
    File.join(@nginx["nginx_path"], "snapshots", service, id[0,2], id[2,2], id[4,2], id, snapshot_id, file_name)
  end

  def upload_file_path(service, id, token, time=nil)
    File.join(@base_dir, "uploads", service, id[0,2], id[2,2], id[4,2], id, (time||Time.now.to_i).to_s, token)
  end

  def nginx_upload_file_path(service, id, token, time=nil)
    File.join(@nginx["nginx_path"], "uploads", service, id[0,2], id[2,2], id[4,2], id, (time||Time.now.to_i).to_s, token)
  end

  def make_file_world_readable(file)
    begin
      new_permission = File.lstat(file).mode | 0444
      File.chmod(new_permission, file)
    rescue => e
      @logger.error("Fail to make the file #{file} world_readable.")
    end
  end

  def get_uploaded_data_file
    file = nil
    if @nginx
      path = params[:data_file_path]
      wrapper_class = Class.new do
        attr_accessor :path
      end
      file = wrapper_class.new
      file.path = path
    else
      file = params[:data_file][:tempfile] if params[:data_file]
    end
    file
  end

  def generate_file_token(service, service_id, file_name, length=12)
    prefix=Array.new(length) { VALID_CREDENTIAL_CHARACTERS[rand(VALID_CREDENTIAL_CHARACTERS.length)] }.join
    appendix=Digest::MD5.hexdigest(@upload_token+service+service_id+file_name+(Time.now.to_i.to_s))
    return prefix+appendix
  end

  def generate_download_url(service, service_id, token)
    url = "http://#{@host}:#{( @nginx ? @nginx["nginx_port"] : @port)}/serialized/#{service}/#{service_id}/serialized/file?token=#{token}"
  end

  def set_expire(service, service_id, token, file_path, time=nil)
    @redis.rpush(redis_upload_purge_queue, {"service" => service, "service_id" => service_id, "token" => token, "file" => file_path, "time" => (time || Time.now.to_i)}.to_json)
  end

  def register_file(service, service_id, token, file_path, time=nil)
    @redis.hset(redis_file_key(service, service_id), token, {"file" => file_path, "time" => (time || Time.now.to_i)}.to_json )
    set_expire(service, service_id, token, file_path, time)
  end

  def get_file(service, service_id, token)
    @redis.hget(redis_file_key(service, service_id), token)
  end

  def try_unregister_file(service, service_id, token, greedy=false)
    file_info_s = get_file(service, service_id, token)
    file = nil
    time = nil
    if file_info_s
      begin
        file_info = JSON.parse(file_info_s)
        file = file_info["file"]
        time = file_info["time"]
        if file && time && (greedy == true || (@expire_time > 0 && (Time.now.to_i - time.to_i) > @expire_time))
          @logger.debug("[try_unregister_file] Start to delete file #{file} for service #{service} #{service_id} and unregister it with token #{token}.")
          FileUtils.rm_rf(file)
          @redis.hdel(redis_file_key(service, service_id), token)
          @logger.debug("[try_unregister_file] Done to delete file #{file} for service #{service} #{service_id} and unregister it with token #{token}.")
          FileUtils.rm_rf(file)
          file = nil
          time = nil
        end
      rescue => e
        @logger.error("When trying to unregistering file #{file_info_s.inspect}, met error #{e.backtrace.join('|')}")
        file = nil
        # if we met exception when deleting file, keep time not nil to figure out
      end
    end
    [file, time]
  end

  def purge_expired
    expired_line = Time.now.to_i - @expire_time
    index= 0
    until index == @purge_num
      expired_file= @redis.lpop(redis_upload_purge_queue)
      if expired_file
        begin
          file = JSON.parse(expired_file)
          time = file["time"]
          if time && time.to_i < expired_line
            @logger.debug("[purge_expired] Start to delete file #{file["file"]} for service #{file["service"]} #{file["service_id"]} and unregister it with token #{file["token"]}.")
            FileUtils.rm_rf(file["file"])
            @redis.hdel(redis_file_key(file["service"], file["service_id"]), file["token"])
            @logger.debug("[purge_expired] Done to delete file #{file["file"]} for service #{file["service"]} #{file["service_id"]} and unregister it with token #{file["token"]}.")
          elsif time
            begin
              # no staled files
              @redis.lpush(redis_upload_purge_queue, expired_file)  # push back
              break
            rescue => e
              @logger.error("When push back non-expired file #{expired_file}, met error #{e.backtrace.join('|')}")
            end
          else
            @logger.error("When purging expired file #{expired_file}, timestamp is nil.")
          end
        rescue => e
          @logger.error("When purging expired file #{expired_file}, met error #{e.backtrace.join('|')}")
        end
      else
        # empty queue
        break
      end
      index += 1
    end
  end

  def authorized?
    request_header(VCAP::Services::Api::SDS_UPLOAD_TOKEN_HEADER) == @upload_token
  end

  def request_header(header)
    # This is pretty ghetto but Rack munges headers, so we need to munge them as well
    rack_hdr = "HTTP_" + header.upcase.gsub(/-/, '_')
    env[rack_hdr]
  end

  # store the uploaded file

  put "/serialized/:service/:service_id/serialized/data" do
    error(403) unless authorized?
    begin
      data_file = get_uploaded_data_file
      unless data_file && data_file.path && File.exist?(data_file.path)
        error(400)
      end
      service = params[:service]
      service_id = params[:service_id]
      @logger.debug("Upload serialized data for service=#{service}, service_id=#{service_id}")
      file_basename = File.basename(data_file.path)
      file_token = nil
      new_file_path = nil
      gen_time = Time.now.to_i

      # generate file token
      # In most cases, try once then break, but in some extreme cases, try several times
      loop {
        file_token = generate_file_token(service, service_id, file_basename)
        new_file_path = upload_file_path(service, service_id, file_token, gen_time)
        break unless File.exist?(new_file_path)
      }

      unless new_file_path && FileUtils.mkdir_p(File.dirname(new_file_path))
        @logger.error("Failed to create directory to store the uploaded file #{data_file.path}")
        error(400)
      end

      # move the file to the upload file
      FileUtils.mv(data_file.path, new_file_path)
      unless File.exist?(new_file_path)
        @logger.error("Failed to move the uploaded file #{data_file.path} to the new localtion #{new_file_path}")
        error(400)
      end

      # register the file into redis
      unless file_token && register_file(service, service_id, file_token, new_file_path, gen_time)
        @logger.error("Fail to register the uploaded file #{new_file_path} to redis: #{service} #{service_id} using token #{file_token}")
        @logger.info("Cleanup the file #{new_file_path}")
        FileUtils.rm_rf(new_file_path) if new_file_path
        error(400)
      end

      # return url
      download_url = generate_download_url(service, service_id, file_token)
      status 200
      content_type :json
      resp = {:url => download_url}
      VCAP::Services::Api::SerializedURL.new(resp).encode
    rescue => e
      @logger.error("Error when store and register the uploaded file: #{e} - #{e.backtrace.join(' | ')}")
      error(400)
    ensure
      FileUtils.rm_rf(data_file.path) if data_file && data_file.path && File.exist?(data_file.path)
    end
  end

  # download uploaded data file

  get "/serialized/:service/:service_id/serialized/file" do
    # get the token of the file
    # Is it security enough?
    token = params[:token]
    error(403) unless token
    service = params[:service]
    service_id = params[:service_id]
    # if the file is expired, unregister and delete it
    file, time = try_unregister_file(service, service_id, token, false)
    # send out the file if the file exists
    if file && File.exist?(file)
      if @nginx
        status 200
        content_type "application/octet-stream"
        @logger.info("Serve file using nginx: #{file}")
        make_file_world_readable(file)
        response["X-Accel-Redirect"] = nginx_upload_file_path(service, service_id, token, time)
      else
        @logger.info("Serve file: #{file}")
        send_file(file)
      end
    else
      @logger.info("Can't find uploaded file for service #{service}/service_id #{service_id} with token #{token}")
      error(404)
    end
  end

  delete "/serialized/:service/:service_id/serialized/file" do
    error(403) unless authorized?
    # get the token of the file
    # Is it security enough?
    token = params[:token]
    error(403) unless token
    service = params[:service]
    service_id = params[:service_id]
    file, time = try_unregister_file(service, service_id, token, true)
    unless file || time
      status 200
    else
      error(500)
    end
  end

  get "/serialized/:service/:service_id/snapshots/:snapshot_id" do
    token = params[:token]
    error(403) unless token
    service = params[:service]
    service_id = params[:service_id]
    snapshot_id = params[:snapshot_id]
    @logger.debug("Get serialized data for service=#{service}, service_id=#{service_id}, snapshot_id=#{snapshot_id}")

    key = redis_key(service, service_id)
    result = @redis.hget(key, snapshot_id)
    if not result
      @logger.info("Can't find snapshot infor for service=#{service}, service_id=#{service_id}, snapshot=#{snapshot_id}")
      error(404)
    end
    result = Yajl::Parser.parse(result)
    error(403) unless token == result["token"]
    file_name = result["file"]
    if not file_name
      @logger.error("Can't get serialized filename from redis using key:#{key}.")
      error(501)
    end

    real_path = file_path(service, service_id, snapshot_id, file_name)
    if (File.exists? real_path)
      if @nginx
        status 200
        content_type "application/octet-stream"
        path = nginx_path(service, service_id, snapshot_id, file_name)
        @logger.info("Serve file using nginx: #{real_path}")
        make_file_world_readable(real_path)
        response["X-Accel-Redirect"] = path
      else
        @logger.info("Serve file: #{real_path}")
        send_file(real_path)
      end
    else
      @logger.info("Can't find file:#{real_path}")
      error(404)
    end
  end

  not_found do
    halt 404
  end

end
