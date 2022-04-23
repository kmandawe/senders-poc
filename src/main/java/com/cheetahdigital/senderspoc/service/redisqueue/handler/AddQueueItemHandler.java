package com.cheetahdigital.senderspoc.service.redisqueue.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;

import static com.cheetahdigital.senderspoc.service.redisqueue.util.RedisQueueAPI.*;

public class AddQueueItemHandler implements Handler<AsyncResult<Response>> {
  private Message<JsonObject> event;

  public AddQueueItemHandler(Message<JsonObject> event) {
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
