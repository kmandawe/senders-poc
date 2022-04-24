package com.cheetahdigital.senderspoc.service.redisqueues.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;

public class GetQueueItemsHandler implements Handler<AsyncResult<Response>> {
  private final Message<JsonObject> event;
  private final Long queueItemCount;

  public GetQueueItemsHandler(Message<JsonObject> event, Long queueItemCount) {
    this.event = event;
    this.queueItemCount = queueItemCount;
  }

  @Override
  public void handle(AsyncResult<Response> reply) {
    if (reply.succeeded()) {
      Response result = reply.result();
      JsonArray countInfo = new JsonArray();
      if (result != null) {
        countInfo.add(result.size());
      }
      countInfo.add(queueItemCount);
      JsonArray values = new JsonArray();
      for (Response res : result) {
        values.add(res.toString());
      }
      event.reply(new JsonObject().put(STATUS, OK).put(VALUE, values).put(INFO, countInfo));
    } else {
      event.reply(new JsonObject().put(STATUS, ERROR));
    }
  }
}
