package com.cheetahdigital.senderspoc.service.sendpipeline;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.OK;
import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.STATUS;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.EB_SEGMENTATION;

@Slf4j
public class SegmentationVerticle extends AbstractVerticle {
  @Override
  public void start(Promise<Void> startPromise) {
    log.info("Starting {}...", SendPipelineVerticle.class.getSimpleName());
    vertx
        .eventBus()
        .<JsonObject>consumer(EB_SEGMENTATION)
        .handler(
            message -> {
              val senderId = message.body().getString("senderId");
              // TODO: REMOVE AFTER
              executeBlockingCode(
                  result -> {
                    log.info("SEGMENTATION: Completed processing senderID {}", senderId);
                    message.reply(new JsonObject().put(STATUS, OK));
                  });
            });
    startPromise.complete();
  }

  private void executeBlockingCode(Handler<AsyncResult<Message<JsonObject>>> resultHandler) {
    vertx.executeBlocking(
        event -> {
          log.debug("SEGMENTATION: Executing blocking code");
          try {
            Thread.sleep(5000);
            event.complete();
            //            event.fail("Force fail!");
          } catch (InterruptedException e) {
            log.error("SEGMENTATION: Failed: {}", e);
            event.fail(e);
          }
        },
        resultHandler);
  }
}
