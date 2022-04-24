package com.cheetahdigital.senderspoc.api.sendpipeline;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.STATUS;
import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.buildEnqueueOperation;

@Slf4j
public class GetSendPipelineHandler implements Handler<RoutingContext> {
  @Override
  public void handle(RoutingContext context) {

    eventBusSend(
        context,
        buildEnqueueOperation("queue1", "a_queue_item"),
        message -> {
          JsonObject responseBody = message.result().body();
          String status = responseBody.getString(STATUS);
          log.info("Enqueue status: {}", status);
          context
              .response()
              .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
              .end(responseBody.toBuffer());
        });
  }

  private void eventBusSend(
      RoutingContext context,
      JsonObject operation,
      Handler<AsyncResult<Message<JsonObject>>> handler) {
    context.vertx().eventBus().request(getRedisquesAddress(), operation, handler);
  }

  private String getRedisquesAddress() {
    return "redis-queues";
  }
}
