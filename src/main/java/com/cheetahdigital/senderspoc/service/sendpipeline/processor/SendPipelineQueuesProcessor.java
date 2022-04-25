package com.cheetahdigital.senderspoc.service.sendpipeline.processor;

import com.cheetahdigital.senderspoc.common.config.BrokerConfig;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.*;

@Slf4j
public class SendPipelineQueuesProcessor {

  public static void registerQueuesConsumer(Vertx vertx) {
    val brokerConfig = BrokerConfig.from(vertx.getOrCreateContext().config());
    val eventBus = vertx.eventBus();
    eventBus
        .<JsonObject>consumer(brokerConfig.getRedisQueues().getProcessorAddress())
        .handler(
            message -> {
              final String queue = message.body().getString("queue");
              val payload = message.body().getString("payload");

              switch (queue) {
                case SP_EXECUTE_QUEUE:
                  val jsonPayload = new JsonObject(payload);
                  val senderId = jsonPayload.getString("senderId");
                  long startTime = System.currentTimeMillis();
                  eventBusSend(
                      vertx,
                      EB_SEGMENTATION,
                      jsonPayload,
                      resp -> {
                        JsonObject responseBody = resp.result().body();
                        String status = responseBody.getString(STATUS);
                        long elapsed = System.currentTimeMillis() - startTime;
                        log.info(
                            "Done processing job Id {} with status: {} for {}ms",
                            senderId,
                            status,
                            elapsed);
                      });
                  log.info("Processed message {} from QUEUE: {}", payload, queue);
                  message.reply(new JsonObject().put(STATUS, OK));
                  break;
                  //                case SP_SEGMENTATION_QUEUE:
                  //                  log.info("Processed message {} from QUEUE: {}", payload,
                  // queue);
                  //                  message.reply(new JsonObject().put(STATUS, OK));
                  //                  break;
                default:
                  unsupportedOperation(queue, message);
              }
            });
  }

  private static void eventBusSend(
      Vertx vertx,
      String address,
      JsonObject payload,
      Handler<AsyncResult<Message<JsonObject>>> handler) {
    val config = BrokerConfig.from(vertx.getOrCreateContext().config());
    vertx.eventBus().request(address, payload, handler);
  }

  private static void unsupportedOperation(String queue, Message<JsonObject> event) {
    JsonObject reply = new JsonObject();
    String message = "SENDPIPELINE ERROR: unrecognized queue: " + queue;
    log.error(message);
    reply.put(STATUS, ERROR);
    reply.put(MESSAGE, message);
    event.reply(reply);
  }
}
