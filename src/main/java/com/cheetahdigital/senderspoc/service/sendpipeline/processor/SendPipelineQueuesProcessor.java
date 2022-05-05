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
import java.util.UUID;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.*;
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
              val jsonPayload = new JsonObject(payload);
              val senderId = jsonPayload.getString("senderId", "");
              var switchQueue = queue;
              // Remove the variable senderId suffix here to match queue name processing
              if (!senderId.isEmpty()) {
                val senderIdSuffix = "-" + senderId;
                switchQueue = switchQueue.replace(senderIdSuffix, "");
              }

              switch (switchQueue) {
                case SP_EXECUTE_QUEUE:
                  val jobId = UUID.randomUUID().toString().substring(0, 8);
                  sendStartJobToStats(vertx, payload, jobId);
                  processSegmentation(vertx, message, queue, payload, jobId);
                  break;
                case SP_RESOLVE_ATTRIBUTES:
                  processResolveAttributes(vertx, message, queue, payload);
                  break;
                default:
                  unsupportedOperation(queue, message);
              }
            });
  }

  private static void sendStartJobToStats(Vertx vertx, String payload, String jobId) {
    val jsonPayload = new JsonObject(payload);
    val senderId = jsonPayload.getString("senderId");
    JsonObject startJobPayload =
        new JsonObject()
            .put("operation", JOB_START)
            .put("payload", new JsonObject().put("senderId", senderId).put("jobId", jobId));
    eventBusSend(
        vertx,
        EB_STATS,
        startJobPayload,
        statsMessage -> {
          JsonObject responseBody = statsMessage.result().body();
          log.debug("Acknowledged job start stats with status: {}", responseBody.getString(STATUS));
        });
  }

  private static void processResolveAttributes(
      Vertx vertx, Message<JsonObject> message, String queue, String payload) {
    val jsonPayload = new JsonObject(payload);
    val senderId = jsonPayload.getString("senderId");
    long startTime = System.currentTimeMillis();
    jsonPayload.put("startTime", startTime);
    eventBusSend(
        vertx,
        EB_RESOLVE_ATTRIBUTES,
        jsonPayload,
        resp -> {
          if (resp.failed()) {
            val type = resp.cause().getClass();
            if (resp.cause() instanceof ReplyException) {
              ReplyException replyException = (ReplyException) resp.cause();
              log.warn(
                  "Reply exception in processing resolve attributes job id {} got code: {} and type {}, with message {}",
                  senderId,
                  replyException.failureCode(),
                  replyException.failureType(),
                  replyException.getMessage());

            } else {
              log.error(
                  "Error in processing job id {} got error: {} with message {}",
                  senderId,
                  type,
                  resp.cause());
            }
          } else {
            JsonObject responseBody = resp.result().body();
            String status = responseBody.getString(STATUS);
            long elapsed = System.currentTimeMillis() - startTime;
            val memberList = jsonPayload.getJsonArray("memberIds").getList();
            String memberFirst = null;
            String memberLast = null;
            if (!memberList.isEmpty()) {
              memberFirst = (String) memberList.get(0);
              memberLast = (String) memberList.get(memberList.size() - 1);
            }

            log.info(
                "EVENT-BUS:{} Done processing resolve attributes job Id {} with member {} to member {} with status: {} for {}ms",
                EB_RESOLVE_ATTRIBUTES,
                senderId,
                memberFirst,
                memberLast,
                status,
                elapsed);
          }
        });
    log.info("Processing Redis attributes calculation message {} from QUEUE: {}", payload, queue);
    message.reply(new JsonObject().put(STATUS, OK));
  }

  private static void processSegmentation(
      Vertx vertx, Message<JsonObject> message, String queue, String payload, String jobId) {
    val jsonPayload = new JsonObject(payload);
    val senderId = jsonPayload.getString("senderId");
    long startTime = System.currentTimeMillis();
    jsonPayload.put("startTime", startTime);
    jsonPayload.put("jobId", jobId);
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
                      .put("operation", SEND_TIMEDOUT)
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
                      .put("operation", SEND_FAILED)
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
                "EVENT-BUS: {} Completed sender job Id {} with status: {} for {}ms",
                EB_SEGMENTATION,
                senderId,
                status,
                elapsed);
            JsonObject successStatsPayload =
                new JsonObject()
                    .put("operation", SEND_COMPLETE)
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
    log.info("Processing Redis segmentation message {} from QUEUE: {}", payload, queue);
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
