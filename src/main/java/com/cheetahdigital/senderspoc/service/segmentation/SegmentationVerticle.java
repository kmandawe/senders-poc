package com.cheetahdigital.senderspoc.service.segmentation;

import com.cheetahdigital.senderspoc.common.config.BrokerConfig;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.List;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.EB_SEGMENTATION;
import static com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle.SP_RESOLVE_ATTRIBUTES;
import static com.cheetahdigital.senderspoc.service.stats.SenderStatsVerticle.EB_STATS;
import static com.cheetahdigital.senderspoc.service.stats.SenderStatsVerticle.JOB_BATCH_UPDATE;

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
    val jsonPayload = message.body();
    val senderId = jsonPayload.getString("senderId");
    val jobId = jsonPayload.getString("jobId");
    val batchSizeParam = jsonPayload.getString("batchSize");
    val startTime = jsonPayload.getLong("startTime");
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
      message.reply(new JsonObject().put(STATUS, OK));
      sendBatchToProcessToStats(senderId, 0, jobId);
    } else {
      int batches = getBatches(segmentSize, batchSize);
      sendBatchToProcessToStats(senderId, batches, jobId);
      List<Future> batchFutures = new ArrayList<>();
      for (int i = 0; i < batches; i++) {
        // populate memberIds and queue job for one batch
        batchFutures.add(
            populateBatchMembersAndQueueJob(senderId, segmentSize, batchSize, i, jobId));
      }
      CompositeFuture.all(batchFutures)
          .onSuccess(
              futures -> {
                log.info(
                    "SEGMENTATION: Completed segmentation for senderID {}, now processing in {} batches",
                    senderId,
                    batches);
                message.reply(new JsonObject().put(STATUS, OK));
              });
    }
  }

  private void sendBatchToProcessToStats(String senderId, long batchToProcess, String jobId) {
    JsonObject batchToProcessPayload =
        new JsonObject()
            .put("operation", JOB_BATCH_UPDATE)
            .put(
                "payload",
                new JsonObject()
                    .put("senderId", senderId)
                    .put("jobId", jobId)
                    .put("batchToProcess", batchToProcess));
    vertx
        .eventBus()
        .<JsonObject>request(
            EB_STATS,
            batchToProcessPayload,
            statsMessage -> {
              if (statsMessage.failed()) {
                log.error("Failed BatchToProcess stats: ", statsMessage.cause());
              } else {
                JsonObject responseBody = statsMessage.result().body();
                log.debug(
                    "Acknowledged BatchToProcess stats with status: {}",
                    responseBody.getString(STATUS));
              }
            });
  }

  private Future<Void> populateBatchMembersAndQueueJob(
      String senderId, int segmentSize, int batchSize, int i, String jobId) {
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
    return queueJobForBatch(senderId, memberIds, batchInProcess, jobId);
  }

  private Future<Void> queueJobForBatch(
      String senderId, List<String> memberIds, int batchInProcess, String jobId) {
    return Future.future(
        promise -> {
          JsonObject payload =
              new JsonObject()
                  .put("senderId", senderId)
                  .put("jobId", jobId)
                  .put("memberIds", memberIds)
                  .put("batch", batchInProcess);
          val senderIdQueueName = SP_RESOLVE_ATTRIBUTES + "-" + senderId;
          redisQueuesSend(
              buildEnqueueOperation(senderIdQueueName, payload),
              resolveAttributeMessage -> {
                JsonObject responseBody = resolveAttributeMessage.result().body();
                String status = responseBody.getString(STATUS);
                log.info(
                    "Resolve attributes enqueue status: {}, senderID {}, batch {}",
                    status,
                    senderId,
                    batchInProcess);
                promise.complete();
              });
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
