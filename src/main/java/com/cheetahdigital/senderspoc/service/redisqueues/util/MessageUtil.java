package com.cheetahdigital.senderspoc.service.redisqueues.util;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.regex.Pattern;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.FILTER;
import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.PAYLOAD;

@Slf4j
public class MessageUtil {

  public static Result<Optional<Pattern>, String> extractFilterPattern(Message<JsonObject> event) {
    JsonObject payload = event.body().getJsonObject(PAYLOAD);
    if (payload == null || payload.getString(FILTER) == null) {
      return Result.ok(Optional.empty());
    }
    String filterString = payload.getString(FILTER);
    try {
      Pattern pattern = Pattern.compile(filterString);
      return Result.ok(Optional.of(pattern));
    } catch (Exception ex) {
      log.error("Interface doesn't allow to pass stack trace. Therefore simply log it now.", ex);
      return Result.err("Error while compile regex pattern. Cause: " + ex.getMessage());
    }
  }
}
