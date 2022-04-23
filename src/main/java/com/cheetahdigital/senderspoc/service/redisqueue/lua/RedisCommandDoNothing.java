package com.cheetahdigital.senderspoc.service.redisqueue.lua;

public class RedisCommandDoNothing implements RedisCommand {
  @Override
  public void exec(int executionCounter) {
    // do nothing here
  }
}
