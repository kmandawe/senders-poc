package com.cheetahdigital.senderspoc.service.segmentation;

import com.cheetahdigital.senderspoc.common.config.BrokerConfig;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.List;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.EB_SEGMENTATION;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.SP_RESOLVE_ATTRIBUTES;

@Slf4j
public class SegmentationVerticle extends AbstractVerticle {
  @Override
  public void start(Promise<Void> startPromise) {
    log.info("Starting {}...", SegmentationVerticle.class.getSimpleName());
    vertx
        .eventBus()
        .<JsonObject>consumer(EB_SEGMENTATION)
        .handler(this::processSegmentationAndDoBatchedAttributesCalculation);
    startPromise.complete();
  }

  private void processSegmentationAndDoBatchedAttributesCalculation(Message<JsonObject> message) {
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
      // Simulate segmentation and export here. Processing time will depend on the segment size.
      int segmentationTime = segmentSize / 10;
      log.info("SEGMENTATION: Doing segmentation and export for {}ms", segmentationTime);
      Thread.sleep(segmentationTime);
    } catch (InterruptedException e) {
      log.error("SEGMENTATION: Failed: ", e);
      message.reply(new JsonObject().put(STATUS, ERROR));
    }
    if (segmentSize == 0) {
      log.warn("SEGMENTATION: Completed segmentation, segmentation has no result");
    } else {
      int batches = getBatches(segmentSize, batchSize);
      for (int i = 0; i < batches; i++) {
        // populate memberIds and queue job for one batch
        populateBatchMembersAndQueueJob(senderId, segmentSize, batchSize, i);
      }
      log.info(
          "SEGMENTATION: Completed segmentation for senderID {}, now processing in {} batches",
          senderId,
          batches);
    }
    message.reply(new JsonObject().put(STATUS, OK));
  }

  private void populateBatchMembersAndQueueJob(
      String senderId, int segmentSize, int batchSize, int i) {
    List<String> memberIds = new ArrayList<>();
    for (int j = 1; j <= batchSize; j++) {
      int memberIndex = (i * batchSize) + j;
      if (memberIndex > segmentSize) {
        break;
      }
      val memberId = segmentSize + "-" + memberIndex;
      memberIds.add(memberId);
    }
    int batchInProcess = i + 1;
    queueJobForBatch(senderId, memberIds, batchInProcess);
  }

  private void queueJobForBatch(String senderId, List<String> memberIds, int batchInProcess) {
    JsonObject payload =
        new JsonObject()
            .put("senderId", senderId)
            .put("memberIds", memberIds)
            .put("batch", batchInProcess);
    redisQueuesSend(
        buildEnqueueOperation(SP_RESOLVE_ATTRIBUTES, payload),
        resolveAttributeMessage -> {
          JsonObject responseBody = resolveAttributeMessage.result().body();
          String status = responseBody.getString(STATUS);
          log.info(
              "Resolve attributes enqueue status: {}, senderID {}, batch {}",
              status,
              senderId,
              batchInProcess);
        });
  }

  private int getBatches(int segmentSize, int batchSize) {
    var batches = 1;
    if (segmentSize > batchSize) {
      if (segmentSize % batchSize != 0) {
        batches = (segmentSize / batchSize) + 1;
      } else {
        batches = (segmentSize / batchSize);
      }
    }
    return batches;
  }

  private void redisQueuesSend(
      JsonObject operation, Handler<AsyncResult<Message<JsonObject>>> handler) {
    val config = BrokerConfig.from(vertx.getOrCreateContext().config());
    vertx.eventBus().request(config.getRedisQueues().getAddress(), operation, handler);
  }
}
