package com.cheetahdigital.senderspoc.service.redisqueues.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;

public class GetQueuesHandler implements Handler<AsyncResult<Response>> {

  private final Message<JsonObject> event;
  private final Optional<Pattern> filterPattern;
  private final boolean countOnly;

  public GetQueuesHandler(
      Message<JsonObject> event, Optional<Pattern> filterPattern, boolean countOnly) {
    this.event = event;
    this.filterPattern = filterPattern;
    this.countOnly = countOnly;
  }

  @Override
  public void handle(AsyncResult<Response> reply) {
    if (reply.succeeded()) {
      JsonObject jsonRes = new JsonObject();
      Response queues = reply.result();
      if (filterPattern.isPresent()) {
        Pattern pattern = filterPattern.get();
        JsonArray filteredQueues = new JsonArray();
        for (int i = 0; i < queues.size(); i++) {
          String queue = queues.get(i).toString();
          if (pattern.matcher(queue).find()) {
            filteredQueues.add(queue);
          }
        }
        jsonRes.put(QUEUES, filteredQueues);
      } else {
        List<String> arrayQueues = new ArrayList<>();
        for (Response response : queues) {
          arrayQueues.add(response.toString());
        }
        jsonRes.put(QUEUES, arrayQueues);
      }
      if (countOnly) {
        event.reply(
            new JsonObject().put(STATUS, OK).put(VALUE, jsonRes.getJsonArray(QUEUES).size()));
      } else {
        event.reply(new JsonObject().put(STATUS, OK).put(VALUE, jsonRes));
      }
    } else {
      event.reply(new JsonObject().put(STATUS, ERROR));
    }
  }
}
