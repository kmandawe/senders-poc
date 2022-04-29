package com.cheetahdigital.senderspoc.service.attrcalculation;

import com.cheetahdigital.senderspoc.common.config.BrokerConfig;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.impl.ConversionHelper;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.*;

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
    log.info("ATTRIBUTES-CALCULATION: senderId {} batch: ", senderId, batch);
    //
    val memberFunctionsFuture = futureForMemberFunctions(payload, senderId, batch);
    val membersSummaryFuture = futureForMembersSummary(payload, senderId, batch);
    try {
      val memberFunctionsResult = memberFunctionsFuture.get();
      val membersSummaryResult = membersSummaryFuture.get();
      val smfPaylod = buildSmfPaylod(memberFunctionsResult, membersSummaryResult, memberSize);
      sendToSmf(smfPaylod);
    } catch (InterruptedException | ExecutionException e) {
      log.error("ATTRIBUTES-CALCULATION: Failed: ", e);
      message.reply(new JsonObject().put(STATUS, ERROR));
    }
    log.info(
        "ATTRIBUTES-CALCULATION: Completed attributes calculation for senderID {}, batch: {}",
        senderId,
        batch);
    message.reply(new JsonObject().put(STATUS, OK));
  }

  private void sendToSmf(String smfPaylod) {
    log.info("Sending to SMF payload: {}", smfPaylod);
  }

  private String buildSmfPaylod(
      String memberFunctionsResult, Map<String, Object> membersSummaryResult, int size)
      throws InterruptedException {
    int buildTime = size / 20;
    log.info(
        "Building SMF payload memberFunctionResult {} and memberSummaryResult{} for {} members will take: {}ms",
        memberFunctionsResult,
        membersSummaryResult,
        size,
        buildTime);
    Thread.sleep(buildTime);
    return "Final SMF Payload Here";
  }

  private CompletableFuture<String> futureForMemberFunctions(
      JsonObject payload, String senderId, Integer batch) {
    CompletableFuture<String> memberFunctionsResultFuture = new CompletableFuture<>();
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
          memberFunctionsResultFuture.complete(result);
        });
    return memberFunctionsResultFuture;
  }

  private CompletableFuture<Map<String, Object>> futureForMembersSummary(
      JsonObject payload, String senderId, Integer batch) {
    CompletableFuture<Map<String, Object>> membersSummaryResultFuture = new CompletableFuture<>();
    long memberFunctionStart = System.currentTimeMillis();
    eventBusSend(
        EB_MEMBERS_SUMMARY,
        payload,
        resp -> {
          JsonObject responseBody = resp.result().body();
          String status = responseBody.getString(STATUS);
          JsonObject result = responseBody.getJsonObject(RESULT);
          long elapsed = System.currentTimeMillis() - memberFunctionStart;
          log.info(
              "EVENT-BUS: {} got member function result for : senderId {} batch {} with status {}, took {}ms",
              EB_MEMBER_FUNCTIONS,
              senderId,
              batch,
              status,
              elapsed);
          membersSummaryResultFuture.complete(ConversionHelper.fromJsonObject(result));
        });
    return membersSummaryResultFuture;
  }

  private void eventBusSend(
      String address, JsonObject payload, Handler<AsyncResult<Message<JsonObject>>> handler) {
    vertx.eventBus().request(address, payload, handler);
  }

  private void redisQueuesSend(
      JsonObject operation, Handler<AsyncResult<Message<JsonObject>>> handler) {
    val config = BrokerConfig.from(vertx.getOrCreateContext().config());
    vertx.eventBus().request(config.getRedisQueues().getAddress(), operation, handler);
  }
}
