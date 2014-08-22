#!/usr/bin/env ruby

require 'rubygems'
require 'hiredis'
require 'redis'
require 'date'
require 'optparse'

options = {}
OptionParser.new do |opts|

  opts.banner = "Usage: redist.rb [options]"

  opts.on("--src-host HOST", "Source Redis host") do |src|
    options[:src_host] = src
  end

  opts.on("--src-port PORT", String, "Source Redis port") do |src|
    options[:src_port] = src
  end

  opts.on("--dst-host HOST", String, "Destination Redis host") do |src|
    options[:dst_host] = src
  end

  opts.on("--dst-port PORT", String, "Destination Redis port") do |src|
    options[:dst_port] = src
  end

  opts.on("--op OPERATION", String, "Operation to run") do |op|
    options[:op] = op
  end

  opts.on("--expire-ttl [TTL]", Integer, "TTL when expiring keys") do |ttl|
    options[:ttl] = ttl
  end

  opts.on("--log [LOGFILE]", String, "Path to log file") do |logfile|
    options[:log] = logfile
  end

  opts.on("--progress [COUNT]", Integer, "Print progress message every COUNT keys") do |progress|
    options[:progress] = progress
  end

  opts.on("--except-prefix [x,y,z]", Array, "Used with '--op del' to determine which keys to keep") do |prefix|
    options[:prefix] = prefix
  end

end.parse!

# Validates options to make sure mix of options provided is valid.
def validate_options(opts)
  operations = ['migrate', 'persist', 'expire', 'del']
  if !opts.has_key?(:op)
    puts "You must specify an operation (e.g. --op migrate)"
    return false
  elsif !operations.include?(opts[:op])
    puts "Invalid operation: #{opts[:op]}"
  elsif opts[:op] == 'expire' && !opts.has_key?(:ttl)
    puts "You must specify '--expire-ttl TTL' when using '--op expire'"
    return false
  elsif opts[:op] == 'del' && !opts.has_key?(:prefix)
    puts "You must specify '--except-prefix PREFIX' when using '--op del'"
    return false
  end
  return true
end

# Logs a message to stdout and a file if --log was supplied.
def log(message, opts)
  ts = DateTime.now
  msg = "#{ts} - #{message}" 
  if opts.has_key?(:log)
    f = open(opts[:log], 'a')
    f.puts msg
    f.close()
  end
  puts msg
end

# Migrates a key from src redis to dst redis.
def migrate(key, src, dst, opts)
  type = src.type(key)
  begin
    case type
    when 'string'
      dst.set(key, src.get(key))
    when 'hash'
      attr = []
      src.hkeys(key).each do |f|
        attr.push(f)
        fval = src.hget(key, f) 
        attr.push(fval)
      end
      dst.hmset(key, attr)
    when 'list'
      dst.lpush(key, src.lrange(key, 0, -1))
    when 'set'
      dst.sadd(key, src.smembers(key))
    when 'zset'
      src.zrange(key, 0, -1).each do |z|
        score = src.zscore(key, z)
        dst.zadd(key, score, z)
      end
    end
  rescue
    log("type: #{type}, key: #{key}, Error: #{$!}", opts)
  end
end

# Expires a key if it does not already have a ttl.
def expire(key, src, dst, opts)
  exp_ttl = opts[:ttl]
  begin
    ttl = src.ttl(key)
    if ttl == -1
      dst.expire(key, exp_ttl)
    end
  rescue
    log("Error expiring key: #{key}, #{$1}", opts)
  end
end

# Marks a key as persistent if it did not have a ttl in the src redis.
def persist(key, src, dst, opts)
  begin
    ttl = src.ttl(key)

    # only persist keys in dst if it did not have a ttl in src
    if ttl == -1 
      dst.persist(key)
    end
  rescue
    log("Error persisting key: #{key}, #{$1}", opts)
  end
end

# Deletes a key if it does not have a matching prefix
def del(key, src, dst, opts)
  prefix = opts[:prefix]
  begin
    keep = false
    prefix.each do |pre|
      keep = keep | key.starts_with?(pre)
    end

    if !keep
      dst.del(key)
    else
      log("Keeping key: #{key}", opts)
    end
  rescue
    log("Error deleting key: #{key}, #{$1}", opts)
  end
end

# Main program driver
def redist(opts)
  src_host = opts[:src_host]
  src_port = opts[:src_port]
  dst_host = opts[:dst_host]
  dst_port = opts[:dst_port]

  total_keys = 0
  increment = opts[:progress]
  #op = expire 
  log("starting processing", opts)
  begin
    db_keys = 0
    log("Starting Database: #{db}", opts)
    src = Redis.new(:host => src_host, :port => src_port, :driver => :hiredis)
    dst = Redis.new(:host => dst_host, :port => dst_port, :driver => :hiredis)
  
    src.scan_each do |key|
      case opts[:op]
        when 'migrate'
          migrate(key, src, dst, opts)
        when 'persist'
          persist(key, src, dst, opts)
        when 'expire'
          expire(key, src, dst, opts)
        when 'del'
          del(key, src, dst, opts)
        else
          puts "Unknown operation. See 'redist.rb -h'"
      end
      db_keys += 1
      total_keys += 1

      if !increment.nil? && (db_keys % increment == 0)
        log("Processed #{db_keys} from Database #{db}", opts)
      end 
    end
    log("Done processing Database: #{db}", opts)
  rescue
    log($!, opts)
  end
  log("processing complete", opts)
end

if !validate_options(options)
  puts "ERROR: Invalid options specified. Try 'redist -h'"
else
  redist(options)
end
