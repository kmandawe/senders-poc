package com.cheetahdigital.senderspoc.service.redisqueue.lua;

public interface RedisCommand {
  void exec(int executionCounter);
}
