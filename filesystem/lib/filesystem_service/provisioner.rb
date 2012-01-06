# Copyright (c) 2009-2011 VMware, Inc.
$:.unshift File.join(File.dirname(__FILE__), ".")

require "filesystem_service/common"
require "filesystem_service/error"
require "uuidtools"

class VCAP::Services::Filesystem::Provisioner < VCAP::Services::Base::Provisioner

  include VCAP::Services::Filesystem::Common
  include VCAP::Services::Filesystem

  FILESYSTEM_CONFIG_FILE = File.expand_path("../../../config/filesystem_gateway.yml", __FILE__)

  def initialize(options)
    super(options)
    @backends = options[:additional_options][:backends] || get_filesystem_config
    @backend_index = rand(@backends.size)
    @logger.debug("backends: #{@backends.inspect}")
    @is_first_update_handles = true
  end

  def update_handles(handles)
    super(handles)
    # Process the handles that not on the backend
    if @is_first_update_handles
      @prov_svcs.each do |_, svc|
        # Filesystem service only need process provision handles
        if svc[:credentials]["internal"]["name"] == svc[:service_id]
          backend = get_backend(svc[:credentials]["internal"]["host"], svc[:credentials]["internal"]["export"])
          if backend
            next if File.exists?(get_instance_dir(svc[:service_id], backend))
          end
          request = ProvisionRequest.new
          request.plan = svc[:configuration]["plan"]
          provision_service(request, svc) do |msg|
            if msg["success"]
              @logger.info("Succeed to provision an instance #{svc.inspect} that not on the backend")
            else
              @logger.warn("Failed to provision an instance #{svc.inspect} that not on the backend: #{msg["response"]}")
            end
          end
        end
      end
    end
    @is_first_update_handles = false
  end

  # Only check instances orphans, there is no binding orphan of filesystem service
  def check_orphan(handles, &blk)
    @logger.debug("[#{service_description}] Check if there are orphans")
    reset_orphan_stat
    @handles_for_check_orphan = handles.deep_dup
    instances_list = []
    @backends.each do |backend|
      Dir.foreach(backend["mount"]) do |child|
        unless child == "." || child ==".."
          instances_list << child if File.directory?(File.join(backend["mount"], child))
        end
      end
    end
    nid = "gateway"
    instances_list.each do |ins|
      @staging_orphan_instances[nid] ||= []
      @staging_orphan_instances[nid] << ins unless @handles_for_check_orphan.index { |h| h["service_id"] == ins }
    end
    oi_count = @staging_orphan_instances.values.reduce(0) { |m, v| m += v.count }
    @logger.debug("Staging Orphans: Instances: #{oi_count}")
    blk.call(success)
  rescue => e
    @logger.warn(e)
    if e.instance_of? ServiceError
      blk.call(failure(e))
    else
      blk.call(internal_fail)
    end
  end

  def purge_orphan(orphan_ins_hash, orphan_bind_hash, &blk)
    # TODO: just log it now, since remove the direcotory is a dangerous operation.
    if orphan_ins_hash["gateway"] && !orphan_ins_hash["gateway"].empty?
      orphan_ins_hash["gateway"].each do |ins|
        @logger.warn("Instance #{ins} is an orphan")
      end
    else
      @logger.info("No orphons")
    end
    blk.call(success)
  rescue => e
    @logger.warn(e)
    if e.instance_of? ServiceError
      blk.call(failure(e))
    else
      blk.call(internal_fail)
    end
  end

  def provision_service(request, prov_handle=nil, &blk)
    @logger.debug("[#{service_description}] Attempting to provision instance (request=#{request.extract})")
    if prov_handle
      name = prov_handle[:service_id]
      backend = get_backend(prov_handle[:credentials]["internal"]["host"], prov_handle[:credentials]["internal"]["export"])
    else
      name = UUIDTools::UUID.random_create.to_s
      backend = get_backend
    end
    raise FilesystemError.new(FilesystemError::FILESYSTEM_GET_BACKEND_FAILED) if backend == nil
    instance_dir = get_instance_dir(name, backend)
    begin
      FileUtils.mkdir(instance_dir)
    rescue => e
      raise FilesystemError.new(FilesystemError::FILESYSTEM_CREATE_INSTANCE_DIR_FAILED, instance_dir)
    end
    begin
      FileUtils.chmod(0777, instance_dir)
    rescue => e
      raise FilesystemError.new(FilesystemError::FILESYSTEM_CHANGE_INSTANCE_DIR_PERMISSION_FAILED, instance_dir)
    end
    prov_req = ProvisionRequest.new
    prov_req.plan = request.plan
    # use old credentials to provision a service if provided.
    prov_req.credentials = prov_handle["credentials"] if prov_handle

    credentials = gen_credentials(name, backend)
    svc = {
      :data => prov_req.dup,
      :service_id => name,
      :credentials => credentials
    }
    # FIXME: workaround for inconsistant representation of bind handle and provision handle
    svc_local = {
      :configuration => prov_req.dup,
      :service_id => name,
      :credentials => credentials
    }
    @logger.debug("Provisioned #{svc.inspect}")
    @prov_svcs[svc[:service_id]] = svc_local
    blk.call(success(svc))
  rescue => e
    if e.instance_of? FilesystemError
      blk.call(failure(e))
    else
      @logger.warn(e)
      blk.call(internal_fail)
    end
  end

  def unprovision_service(instance_id, &blk)
    @logger.debug("[#{service_description}] Attempting to unprovision instance (instance id=#{instance_id}")
    svc = @prov_svcs[instance_id]
    raise FilesystemError.new(FilesystemError::FILESYSTEM_FIND_INSTANCE_FAILED, instance_id) if svc == nil
    host = svc[:credentials]["internal"]["host"]
    export = svc[:credentials]["internal"]["export"]
    backend = get_backend(host, export)
    raise FilesystemError.new(FilesystemError::FILESYSTEM_GET_BACKEND_BY_HOST_AND_EXPORT_FAILED, host, export) if backend == nil
    FileUtils.rm_rf(get_instance_dir(instance_id, backend))
    bindings = find_all_bindings(instance_id)
    bindings.each do |b|
      @prov_svcs.delete(b[:service_id])
    end
    blk.call(success())
  rescue => e
    if e.instance_of? FilesystemError
      blk.call(failure(e))
    else
      @logger.warn(e)
      blk.call(internal_fail)
    end
  end

  def bind_instance(instance_id, binding_options, bind_handle=nil, &blk)
    @logger.debug("[#{service_description}] Attempting to bind to service #{instance_id}")
    svc = @prov_svcs[instance_id]
    raise FilesystemError.new(FilesystemError::FILESYSTEM_FIND_INSTANCE_FAILED, instance_id) if svc == nil

    #FIXME options = {} currently, should parse it in future.
    request = BindRequest.new
    request.name = instance_id
    request.bind_opts = binding_options
    service_id = nil
    if bind_handle
      request.credentials = bind_handle["credentials"]
      service_id = bind_handle["service_id"]
    else
      service_id = UUIDTools::UUID.random_create.to_s
    end

    # Save binding-options in :data section of configuration
    config = svc[:configuration].clone
    config['data'] ||= {}
    config['data']['binding_options'] = binding_options
    res = {
      :service_id => service_id,
      :configuration => config,
      :credentials => svc[:credentials]
    }
    @logger.debug("[#{service_description}] Binded: #{res.inspect}")
    @prov_svcs[res[:service_id]] = res
    blk.call(success(res))
  rescue => e
    if e.instance_of? FilesystemError
      blk.call(failure(e))
    else
      @logger.warn(e)
      blk.call(internal_fail)
    end
  end

  def unbind_instance(instance_id, handle_id, binding_options, &blk)
    @logger.debug("[#{service_description}] Attempting to unbind to service #{instance_id}")
    blk.call(success())
  end

  def get_filesystem_config
    config_file = YAML.load_file(FILESYSTEM_CONFIG_FILE)
    config = VCAP.symbolize_keys(config_file)
    config[:backends]
  end

  def get_backend(host=nil, export=nil)
    if host && export
      @backends.each do |backend|
        if backend["host"] == host && backend["export"] == export
          return backend
        end
      end
      return nil
    else
      # Simple round-robin load-balancing; TODO: Something smarter?
      return nil if @backends == nil || @backends.empty?
      index = @backend_index
      @backend_index = (@backend_index + 1) % @backends.size
      return @backends[index]
    end
  end

  def get_instance_dir(name, backend)
    File.join(backend["mount"], name)
  end

  def gen_credentials(name, backend)
    credentials = {
      "internal" => {
        "name" => name,
        "host" => backend["host"],
        "export" => backend["export"],
      }
    }
  end

end
