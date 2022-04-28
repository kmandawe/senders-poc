package com.cheetahdigital.senderspoc.service.sendpipeline.processor;

import com.cheetahdigital.senderspoc.common.config.BrokerConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.LocalDateTime;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.EB_SEGMENTATION;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.SP_EXECUTE_QUEUE;
import static com.cheetahdigital.senderspoc.service.stats.SenderStatsVerticle.*;

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
                  processSegmentation(vertx, message, queue, payload);
                  break;
                default:
                  unsupportedOperation(queue, message);
              }
            });
  }

  private static void processSegmentation(
      Vertx vertx, Message<JsonObject> message, String queue, String payload) {
    val jsonPayload = new JsonObject(payload);
    val senderId = jsonPayload.getString("senderId");
    long startTime = System.currentTimeMillis();
    jsonPayload.put("startTime", startTime);
    log.info("Sending segmentation job now: {}", LocalDateTime.now());
    eventBusSend(
        vertx,
        EB_SEGMENTATION,
        jsonPayload,
        resp -> {
          if (resp.failed()) {
            val type = resp.cause().getClass();
            if (resp.cause() instanceof ReplyException) {
              ReplyException replyException = (ReplyException) resp.cause();
              log.warn(
                  "Reply exception in processing job id {} got code: {} and type {}, with message {}",
                  senderId,
                  replyException.failureCode(),
                  replyException.failureType(),
                  replyException.getMessage());
              JsonObject timedOutStatsPayload =
                  new JsonObject()
                      .put("operation", SEGMENT_TIMEDOUT)
                      .put("payload", new JsonObject().put("count", 1));
              eventBusSend(
                  vertx,
                  EB_STATS,
                  timedOutStatsPayload,
                  statsMessage -> {
                    JsonObject responseBody = statsMessage.result().body();
                    log.debug(
                        "Acknowledged timed out stats with status: {}",
                        responseBody.getString(STATUS));
                  });

            } else {
              log.error(
                  "Error in processing job id {} got error: {} with message {}",
                  senderId,
                  type,
                  resp.cause());
              JsonObject failedStatsPayload =
                  new JsonObject()
                      .put("operation", SEGMENT_FAILED)
                      .put("payload", new JsonObject().put("count", 1));
              eventBusSend(
                  vertx,
                  EB_STATS,
                  failedStatsPayload,
                  statsMessage -> {
                    JsonObject responseBody = statsMessage.result().body();
                    log.debug(
                        "Acknowledged failed stats with status: {}",
                        responseBody.getString(STATUS));
                  });
            }
          } else {
            JsonObject responseBody = resp.result().body();
            String status = responseBody.getString(STATUS);
            long elapsed = System.currentTimeMillis() - startTime;
            log.info(
                "Done processing job Id {} with status: {} for {}ms", senderId, status, elapsed);
            JsonObject successStatsPayload =
                new JsonObject()
                    .put("operation", SEGMENT_COMPLETE)
                    .put("payload", new JsonObject().put("count", 1).put("duration", elapsed));
            eventBusSend(
                vertx,
                EB_STATS,
                successStatsPayload,
                statsMessage -> {
                  JsonObject successResponseBody = statsMessage.result().body();
                  log.debug(
                      "Acknowledged complete stats with status: {}",
                      successResponseBody.getString(STATUS));
                });
          }
        });
    log.info("Processed message {} from QUEUE: {}", payload, queue);
    message.reply(new JsonObject().put(STATUS, OK));
  }

  private static void eventBusSend(
      Vertx vertx,
      String address,
      JsonObject payload,
      Handler<AsyncResult<Message<JsonObject>>> handler) {
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
