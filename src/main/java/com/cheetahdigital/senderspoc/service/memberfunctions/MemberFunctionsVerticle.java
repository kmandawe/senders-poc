package com.cheetahdigital.senderspoc.service.memberfunctions;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.EB_MEMBER_FUNCTIONS;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.RESULT;

@Slf4j
public class MemberFunctionsVerticle extends AbstractVerticle {
  @Override
  public void start(Promise<Void> startPromise) {
    log.info("Starting {}...", MemberFunctionsVerticle.class.getSimpleName());
    vertx
        .eventBus()
        .<JsonObject>consumer(EB_MEMBER_FUNCTIONS)
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
        "MEMBER-FUNCTIONS-CALCULATION: Received at {} with startTime: {}, difference of {}ms",
        System.currentTimeMillis(),
        startTime,
        now - startTime);
    log.info("MEMBER-FUNCTIONS-CALCULATION: senderId {} batch: ", senderId, batch);

    try {
      int buildTime = memberSize / 10;
      log.info("Executing member functions {} members will take: {}ms", memberSize, buildTime);
      Thread.sleep(buildTime);
    } catch (InterruptedException e) {
      log.error("MEMBER-FUNCTIONS-CALCULATION: Failed: ", e);
      message.reply(new JsonObject().put(STATUS, ERROR));
    }
    log.info(
        "MEMBER-FUNCTIONS-CALCULATION: Completed member functions execution for senderID {}, batch: {}",
        senderId,
        batch);
    message.reply(
        new JsonObject().put(STATUS, OK).put(RESULT, "{\"function_1\":\"function_1_result\"}"));
  }
}
