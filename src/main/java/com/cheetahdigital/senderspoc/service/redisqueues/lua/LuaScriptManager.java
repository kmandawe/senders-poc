package com.cheetahdigital.senderspoc.service.redisqueues.lua;

import com.cheetahdigital.senderspoc.service.redisqueues.util.RedisUtils;
import io.vertx.core.Handler;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class LuaScriptManager {

  private final RedisAPI redisAPI;
  private final Map<LuaScript, LuaScriptState> luaScripts = new HashMap<>();

  public LuaScriptManager(RedisAPI redisAPI) {
    this.redisAPI = redisAPI;

    LuaScriptState luaGetScriptState = new LuaScriptState(LuaScript.CHECK, redisAPI);
    luaGetScriptState.loadLuaScript(new RedisCommandDoNothing(), 0);
    luaScripts.put(LuaScript.CHECK, luaGetScriptState);

    // load the MultiListLength Lua Script
    LuaScriptState luaMllenScriptState = new LuaScriptState(LuaScript.MLLEN, redisAPI);
    luaMllenScriptState.loadLuaScript(new RedisCommandDoNothing(), 0);
    luaScripts.put(LuaScript.MLLEN, luaMllenScriptState);
  }

  /**
   * If the loglevel is trace and the logoutput in luaScriptState is false, then reload the script
   * with logoutput and execute the RedisCommand. If the loglevel is not trace and the logoutput in
   * luaScriptState is true, then reload the script without logoutput and execute the RedisCommand.
   * If the loglevel is matching the luaScriptState, just execute the RedisCommand.
   *
   * @param redisCommand the redis command to execute
   * @param executionCounter current count of already passed executions
   */
  private void executeRedisCommand(RedisCommand redisCommand, int executionCounter) {
    redisCommand.exec(executionCounter);
  }

  public void handleQueueCheck(
      String lastCheckExecKey, int checkInterval, Handler<Boolean> handler) {
    List<String> keys = Collections.singletonList(lastCheckExecKey);
    List<String> arguments =
        Arrays.asList(String.valueOf(System.currentTimeMillis()), String.valueOf(checkInterval));
    executeRedisCommand(new Check(keys, arguments, redisAPI, handler), 0);
  }

  private class Check implements RedisCommand {

    private final List<String> keys;
    private final List<String> arguments;
    private final Handler<Boolean> handler;
    private final RedisAPI redisAPI;

    public Check(
        List<String> keys,
        List<String> arguments,
        RedisAPI redisAPI,
        final Handler<Boolean> handler) {
      this.keys = keys;
      this.arguments = arguments;
      this.redisAPI = redisAPI;
      this.handler = handler;
    }

    @Override
    public void exec(int executionCounter) {
      List<String> args =
          RedisUtils.toPayload(
              luaScripts.get(LuaScript.CHECK).getSha(), keys.size(), keys, arguments);
      redisAPI.evalsha(
          args,
          event -> {
            if (event.succeeded()) {
              Long value = event.result().toLong();
              if (log.isTraceEnabled()) {
                log.trace("Check lua script got result: " + value);
              }
              handler.handle(value == 1L);
            } else {
              String message = event.cause().getMessage();
              if (message != null && message.startsWith("NOSCRIPT")) {
                log.warn("Check script couldn't be found, reload it");
                log.warn("amount the script got loaded: " + executionCounter);
                if (executionCounter > 10) {
                  log.error("amount the script got loaded is higher than 10, we abort");
                } else {
                  luaScripts
                      .get(LuaScript.CHECK)
                      .loadLuaScript(
                          new Check(keys, arguments, redisAPI, handler), executionCounter);
                }
              } else {
                log.error("Check request failed.", event.cause());
              }
            }
          });
    }
  }

  public void handleMultiListLength(List<String> keys, Handler<List<Long>> handler) {
    executeRedisCommand(new MultiListLength(keys, redisAPI, handler), 0);
  }

  private class MultiListLength implements RedisCommand {

    private final List<String> keys;
    private final Handler<List<Long>> handler;
    private final RedisAPI redisAPI;

    public MultiListLength(
        List<String> keys, RedisAPI redisAPI, final Handler<List<Long>> handler) {
      this.keys = keys;
      this.redisAPI = redisAPI;
      this.handler = handler;
    }

    @Override
    public void exec(int executionCounter) {
      if (keys == null || keys.isEmpty()) {
        handler.handle(List.of());
        return;
      }
      List<String> args =
          RedisUtils.toPayload(luaScripts.get(LuaScript.MLLEN).getSha(), keys.size(), keys);
      redisAPI.evalsha(
          args,
          event -> {
            if (event.succeeded()) {
              List<Long> res = new ArrayList<>();
              for (Response response : event.result()) {
                res.add(response.toLong());
              }
              handler.handle(res);
            } else {
              String message = event.cause().getMessage();
              if (message != null && message.startsWith("NOSCRIPT")) {
                log.warn("MultiListLength script couldn't be found, reload it");
                log.warn("amount the script got loaded: {}", executionCounter);
                if (executionCounter > 10) {
                  log.error(
                      "amount the MultiListLength script got loaded is higher than 10, we abort");
                } else {
                  luaScripts
                      .get(LuaScript.MLLEN)
                      .loadLuaScript(
                          new MultiListLength(keys, redisAPI, handler), executionCounter);
                }
              } else {
                log.error("ListLength request failed.", event.cause());
              }
              event.failed();
            }
          });
    }
  }
}
