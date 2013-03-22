# Copyright (c) 2009 - 2013 VMware, Inc.

module VCAP; module Services; module Postgresql; end; end; end

class VCAP::Services::Postgresql::Node

  XLOG_STATUS_OK = 0
  XLOG_STATUS_CHK = 1
  XLOG_STATUS_KILL = 2

  # Limitation of xlog file number when you are sensitive to the storage capacity
  # Due to a short-term peak of log output rate, there are more than 3 * checkpoint_segments + 1 segment files
  # the unneeded segment files will be deleted instead of recycled until the system gets back under this limit
  # or below the value, checkpoint (force/xlog/timeout) will just recycle them
  def xlog_file_checkpoint_limit(conn)
    conn.settings['checkpoint_segments'].to_i * 3 + 2
  end

  # Limitation of xlog file number when you are snesitive to the storage capacity
  # If we find the xlog file number exceeds this limitation in a continous (xlog_enforce_tolerance) check
  # We will try to kill the open connections and run a checkpoint
  def xlog_file_kill_limit(conn)
    conn.settings['checkpoint_segments'].to_i * 4 + 2
  end

  def xlog_file_num(data_dir)
    raise "Fail to locate the PGDATA" unless File.directory?(data_dir)
    Dir.glob(File.join(data_dir, 'pg_xlog', '*')).select { |f| File.file?(f) }.count
  end

  def xlog_status(conn, data_dir)
    file_num = xlog_file_num(data_dir)
    return XLOG_STATUS_KILL if file_num > xlog_file_kill_limit(conn)
    return XLOG_STATUS_CHK  if file_num >= xlog_file_checkpoint_limit(conn)
    return XLOG_STATUS_OK
  end

  # Use this method to enforce xlog file number
  # Usually, xlog file number are limited by checkpoint_segmentations parameters
  # http://www.postgresql.org/docs/9.2/static/wal-configuration.html
  # We will monitor this
  #  * A alert must be issued if exceeding CHECKPOINT limit
  #  * A force checkpoint is executed if exceeding CHECKPOINT limit
  #  * All open connections will be terminated only if exceeding KILL limit
  # This enforcement might lead to worse user experience, but considering we provide enough xlog file number quota and enable long-time tx killer
  # exceeding the number limitaions is rare and usually a hint of attacks or faulty configurations
  # Note: xlog enforcement might hurt performance, so make a trade-off on performance and storage overhead for xlog files
  def xlog_enforce_internal(conn, opts={})
    cur_xlog_status = opts[:xlog_status] || XLOG_STATUS_OK
    return if cur_xlog_status == XLOG_STATUS_OK
    @logger ||= create_logger
    @logger.warn("Alert: exceeding the xlog in server #{conn.host}:#{conn.port}")
    unless opts[:alert_only] || false
      if cur_xlog_status == XLOG_STATUS_KILL
        @logger.warn("Terminate alive binding users' connections")
        excluded_users = opts[:excluded_users] || []
        excluded_users += [conn.user]
        kill_alive_sessions(conn, :mode => 'exclude', :users => excluded_users)
      end
      @logger.warn("Issue a force checkpoint for xlog exceeding")
      conn.query("CHECKPOINT")
    end
  end
end
