package com.cheetahdigital.senderspoc.service.redisqueues.lua;

public interface RedisCommand {
  void exec(int executionCounter);
}
