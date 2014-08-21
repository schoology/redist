#!/bin/env ruby

require 'rubygems'
require 'hiredis'
require 'redis'
require 'date'

def log(message)
  ts = DateTime.now
  f = open('redist.log', 'a')
  f.puts "#{ts} - #{message}"
  f.close()
end

def migrate(key, src, dst)
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
    log("type: #{type}, key: #{key}, Error: #{$!}")
  end
end

def expire(key, src, dst)
  ttl = 259200
  begin
    dst.expire(key, ttl)
  rescue
    log("Error expiring key: #{key}, #{$1}")
  end
end

def persist(key, src, dst)
  begin
    dst.persist(key)
  rescue
    log("Error persisting key: #{key}, #{$1}")
  end
end

src_host = '127.0.0.1'
src_port = '6379'
dst_host = '127.0.0.1'
dst_port = '22121'
#dst_port = '6379'

total_keys = 0
increment = 10000
#op = expire

log("starting processing")
begin
  db_keys = 0
  db = 0
  log("Starting Database: #{db}")
  src = Redis.new(:host => src_host, :port => src_port, :db => db, :driver => :hiredis)
  dst = Redis.new(:host => dst_host, :port => dst_port, :db => db, :driver => :hiredis)
  
  src.scan_each do |key|
    persist(key, src, dst)
    db_keys += 1
    total_keys += 1

    if db_keys % increment == 0
      log("Processed #{db_keys} from Database #{db}")
    end 
  end
  log("Done processing Database: #{db}")
rescue
  log($!)
end
log("processing complete")
