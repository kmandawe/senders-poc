package com.cheetahdigital.senderspoc.api.sendpipeline;

import com.cheetahdigital.senderspoc.common.config.BrokerConfig;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.STATUS;
import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.buildEnqueueOperation;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.SP_EXECUTE_QUEUE;

@Slf4j
public class GetSendPipelineHandler implements Handler<RoutingContext> {
  @Override
  public void handle(RoutingContext context) {

    val senderId = context.pathParam("senderId");
    val batchSize = context.pathParam("batchSize");
    log.info("Executing Job for sender ID: {} and batch size: {}", senderId, batchSize);
    JsonObject payload = new JsonObject().put("senderId", senderId).put("batchSize", batchSize);
    val senderIdQueueName = SP_EXECUTE_QUEUE + "-" + senderId;
    eventBusSend(
        context,
        buildEnqueueOperation(senderIdQueueName, payload),
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
    val config = BrokerConfig.from(context.vertx().getOrCreateContext().config());
    context.vertx().eventBus().request(config.getRedisQueues().getAddress(), operation, handler);
  }
}
