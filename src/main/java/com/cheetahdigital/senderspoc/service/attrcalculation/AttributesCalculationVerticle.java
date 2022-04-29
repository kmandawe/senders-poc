package com.cheetahdigital.senderspoc.service.attrcalculation;

import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.impl.ConversionHelper;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Map;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.*;
import static com.cheetahdigital.senderspoc.service.stats.SenderStatsVerticle.EB_STATS;
import static com.cheetahdigital.senderspoc.service.stats.SenderStatsVerticle.JOB_BATCH_UPDATE;

@Slf4j
public class AttributesCalculationVerticle extends AbstractVerticle {
  @Override
  public void start(Promise<Void> startPromise) {
    // TODO: whole logic
    log.info("Starting {}...", AttributesCalculationVerticle.class.getSimpleName());
    vertx
        .eventBus()
        .<JsonObject>consumer(EB_RESOLVE_ATTRIBUTES)
        .handler(this::processAttributesCalculation);
    startPromise.complete();
  }

  @SuppressWarnings("unchecked")
  private void processAttributesCalculation(Message<JsonObject> message) {
    val payload = message.body();
    val senderId = payload.getString("senderId");
    val startTime = payload.getLong("startTime");
    val batch = payload.getInteger("batch");
    val memberIds = payload.getJsonArray("memberIds");
    val memberSize = memberIds.size();
    val now = System.currentTimeMillis();

    log.debug(
        "ATTRIBUTES-CALCULATION: Received at {} with startTime: {}, difference of {}ms",
        System.currentTimeMillis(),
        startTime,
        now - startTime);
    log.info("ATTRIBUTES-CALCULATION: senderId {} batch: {} ", senderId, batch);
    //
    val memberFunctionsFuture = futureForMemberFunctions(payload, senderId, batch);
    val membersSummaryFuture = futureForMembersSummary(payload, senderId, batch);

    CompositeFuture.all(List.of(memberFunctionsFuture, membersSummaryFuture))
        .onSuccess(
            compositeFuture -> {
              val memberFunctionsResult = (String) compositeFuture.resultAt(0);
              val membersSummaryResult = (Map<String, Object>) compositeFuture.resultAt(1);
              String smfPaylod = null;
              try {
                smfPaylod = buildSmfPaylod(memberFunctionsResult, membersSummaryResult, memberSize);
              } catch (InterruptedException e) {
                log.error("ATTRIBUTES-CALCULATION: Failed: ", e.getCause());
                message.reply(new JsonObject().put(STATUS, ERROR));
              }
              sendToSmf(smfPaylod);
              sendBatchCompletedToStats(senderId, 1, memberSize);
              log.info(
                  "ATTRIBUTES-CALCULATION: Completed attributes calculation for senderID {}, batch: {}",
                  senderId,
                  batch);
              message.reply(new JsonObject().put(STATUS, OK));
            });
  }

  private void sendBatchCompletedToStats(String senderId, long batchCompleted, long memberSize) {
    JsonObject batchToProcessPayload =
        new JsonObject()
            .put("operation", JOB_BATCH_UPDATE)
            .put(
                "payload",
                new JsonObject()
                    .put("senderId", senderId)
                    .put("batchCompleted", batchCompleted)
                    .put("memberSize", memberSize));
    vertx
        .eventBus()
        .<JsonObject>request(
            EB_STATS,
            batchToProcessPayload,
            statsMessage -> {
              JsonObject responseBody = statsMessage.result().body();
              log.debug(
                  "Acknowledged BatchCompleted stats with status: {}",
                  responseBody.getString(STATUS));
            });
  }

  private void sendToSmf(String smfPaylod) {
    log.info("Sending to SMF payload: {}", smfPaylod);
  }

  private String buildSmfPaylod(
      String memberFunctionsResult, Map<String, Object> membersSummaryResult, int size)
      throws InterruptedException {
    int buildTime = size / 20;
    log.info(
        "Building SMF payload memberFunctionResult {} and memberSummaryResult {} for {} members will take: {}ms",
        memberFunctionsResult,
        membersSummaryResult,
        size,
        buildTime);
    Thread.sleep(buildTime);
    return "Final SMF Payload Here";
  }

  private Future<String> futureForMemberFunctions(
      JsonObject payload, String senderId, Integer batch) {
    return Future.future(
        promise -> {
          long memberFunctionStart = System.currentTimeMillis();
          eventBusSend(
              EB_MEMBER_FUNCTIONS,
              payload,
              resp -> {
                JsonObject responseBody = resp.result().body();
                String status = responseBody.getString(STATUS);
                String result = responseBody.getString(RESULT);
                long elapsed = System.currentTimeMillis() - memberFunctionStart;
                log.info(
                    "EVENT-BUS: {} got member function result for : senderId {} batch {} with status {}, took {}ms",
                    EB_MEMBER_FUNCTIONS,
                    senderId,
                    batch,
                    status,
                    elapsed);
                promise.complete(result);
              });
        });
  }

  private Future<Map<String, Object>> futureForMembersSummary(
      JsonObject payload, String senderId, Integer batch) {
    return Future.future(
        promise -> {
          long membersSummaryStart = System.currentTimeMillis();
          eventBusSend(
              EB_MEMBERS_SUMMARY,
              payload,
              resp -> {
                JsonObject responseBody = resp.result().body();
                String status = responseBody.getString(STATUS);
                JsonObject result = responseBody.getJsonObject(RESULT);
                long elapsed = System.currentTimeMillis() - membersSummaryStart;
                log.info(
                    "EVENT-BUS: {} got member summary result for : senderId {} batch {} with status {}, took {}ms",
                    EB_MEMBERS_SUMMARY,
                    senderId,
                    batch,
                    status,
                    elapsed);
                promise.complete(ConversionHelper.fromJsonObject(result));
              });
        });
  }

  private void eventBusSend(
      String address, JsonObject payload, Handler<AsyncResult<Message<JsonObject>>> handler) {
    vertx.eventBus().request(address, payload, handler);
  }
}
