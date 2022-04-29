package com.cheetahdigital.senderspoc.service.membersummary;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.HashMap;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.EB_MEMBERS_SUMMARY;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.RESULT;

@Slf4j
public class MembersSummaryVerticle extends AbstractVerticle {
  @Override
  public void start(Promise<Void> startPromise) {
    log.info("Starting {}...", MembersSummaryVerticle.class.getSimpleName());
    vertx
        .eventBus()
        .<JsonObject>consumer(EB_MEMBERS_SUMMARY)
        .handler(this::processMemberFunctionsCalculation);
    startPromise.complete();
  }

  private void processMemberFunctionsCalculation(Message<JsonObject> message) {
    val payload = message.body();
    val senderId = payload.getString("senderId");
    val startTime = payload.getLong("startTime");
    val batch = payload.getInteger("batch");
    val memberIds = payload.getJsonArray("memberIds");
    val memberSize = memberIds.size();
    val now = System.currentTimeMillis();

    log.debug(
        "MEMBERS-SUMMARY-CALCULATION: Received at {} with startTime: {}, difference of {}ms",
        System.currentTimeMillis(),
        startTime,
        now - startTime);
    log.info("MEMBERS-SUMMARY-CALCULATION: senderId {} batch: ", senderId, batch);

    try {
      int buildTime = memberSize / 10;
      log.info("Executing members summary for {} members will take: {}ms", memberSize, buildTime);
      Thread.sleep(buildTime);
    } catch (InterruptedException e) {
      log.error("MEMBERS-SUMMARY-CALCULATION: Failed: ", e);
      message.reply(new JsonObject().put(STATUS, ERROR));
    }
    log.info(
        "MEMBERS-SUMMARY-CALCULATION: Completed members summary execution for senderID {}, batch: {}",
        senderId,
        batch);
    val memberSummaryResult = new HashMap<String, Object>();
    memberSummaryResult.put("M-00000001", "m01@example.com");
    message.reply(new JsonObject().put(STATUS, OK).put(RESULT, memberSummaryResult));
  }
}
