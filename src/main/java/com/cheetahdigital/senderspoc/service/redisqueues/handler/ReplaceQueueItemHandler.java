package com.cheetahdigital.senderspoc.service.redisqueues.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;

public class ReplaceQueueItemHandler implements Handler<AsyncResult<Response>> {
  private final Message<JsonObject> event;

  public ReplaceQueueItemHandler(Message<JsonObject> event) {
    this.event = event;
  }

  @Override
  public void handle(AsyncResult<Response> reply) {
    if (reply.succeeded()) {
      event.reply(new JsonObject().put(STATUS, OK));
    } else {
      event.reply(new JsonObject().put(STATUS, ERROR));
    }
  }
}