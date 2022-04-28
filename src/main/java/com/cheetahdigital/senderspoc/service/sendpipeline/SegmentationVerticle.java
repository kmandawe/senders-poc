package com.cheetahdigital.senderspoc.service.sendpipeline;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.EB_SEGMENTATION;

@Slf4j
public class SegmentationVerticle extends AbstractVerticle {
  @Override
  public void start(Promise<Void> startPromise) {
    log.info("Starting {}...", SegmentationVerticle.class.getSimpleName());
    vertx
        .eventBus()
        .<JsonObject>consumer(EB_SEGMENTATION)
        .handler(
            message -> {
              val senderId = message.body().getString("senderId");
              val batchSizeParam = message.body().getString("batchSize");
              val startTime = message.body().getLong("startTime");
              val now = System.currentTimeMillis();
              val segmentSize = Integer.parseInt(senderId);
              val batchSize = Integer.parseInt(batchSizeParam);
              log.debug(
                  "SEGMENTATION: Received at {} with startTime: {}, difference of {}ms",
                  System.currentTimeMillis(),
                  startTime,
                  now - startTime);
              log.info("SEGMENTATION: senderId: {}, batchSize: {}", senderId, batchSize);
              try {
                //                Thread.sleep(5000);
                int segmentationTime = segmentSize / 10;
                log.info("SEGMENTATION: Doing segmentation and export for {}ms", segmentationTime);
                Thread.sleep(segmentationTime);
              } catch (InterruptedException e) {
                log.error("SEGMENTATION: Failed: ", e);
                message.reply(new JsonObject().put(STATUS, ERROR));
              }
              if (segmentSize == 0) {
                log.warn(
                  "SEGMENTATION: Completed segmentation, segmentation has no result");
              } else {
                var batches = 1;
                if (segmentSize > batchSize) {
                  if  (segmentSize % batchSize != 0) {
                    batches = (segmentSize / batchSize) + 1;
                  } else {
                    batches = (segmentSize / batchSize);
                  }
                }
                for (int i = 0; i < batches; i++) {
                  for (int j = 1; j <= batchSize; j++) {
                    int memberIndex = (i * batchSize) + j;
                    if (memberIndex > segmentSize) {
                      break;
                    }

                  }
                }
                log.info(
                  "SEGMENTATION: Completed segmentation for senderID {}, now processing in {} batches",
                  senderId,
                  batches);
              }
              message.reply(new JsonObject().put(STATUS, OK));
            });
    startPromise.complete();
  }
}
