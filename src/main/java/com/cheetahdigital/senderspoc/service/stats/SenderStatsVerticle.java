package com.cheetahdigital.senderspoc.service.stats;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;

@Slf4j
public class SenderStatsVerticle extends AbstractVerticle {
  public static final String EB_STATS = "eb-stats";
  public static final String SEGMENT_COMPLETE = "segment-complete";
  public static final String SEGMENT_FAILED = "segment-failed";
  public static final String SEGMENT_TIMEDOUT = "segment-timedout";
  public static final String STATS_RESET = "stats-reset";
  public static final String QUERY_STATS = "query-stats";

  private AtomicLong completedSegmentations;
  private AtomicLong timedOutSegmentations;
  private AtomicLong failedSegmentations;
  private AtomicLong maxSegmentationTime;
  private long lastResetTime;
  private long lastUpdate;
  private LocalDateTime lastUpdateTime;

  @Override
  public void init(Vertx vertx, Context context) {
    completedSegmentations = new AtomicLong();
    maxSegmentationTime = new AtomicLong();
    failedSegmentations = new AtomicLong();
    timedOutSegmentations = new AtomicLong();
    lastResetTime = System.currentTimeMillis();
    lastUpdate = lastResetTime;
    lastUpdateTime = LocalDateTime.now();
    super.init(vertx, context);
  }

  @Override
  public void start(Promise<Void> startPromise) {
    log.info("Starting {}...", SenderStatsVerticle.class.getSimpleName());
    vertx
        .eventBus()
        .<JsonObject>consumer(EB_STATS)
        .handler(
            message -> {
              val operation = message.body().getString("operation");
              val payload = message.body().getJsonObject("payload");
              switch (operation) {
                case SEGMENT_COMPLETE:
                  val count = payload.getLong("count");
                  val duration = payload.getLong("duration");
                  updateStats(count, duration);
                  log.debug(
                      "Processed segment complete with count {} and duration {}", count, duration);
                  message.reply(new JsonObject().put(STATUS, OK));
                  break;
                case SEGMENT_FAILED:
                  val failedCount = payload.getLong("count");
                  updateFailedStats(failedCount);
                  log.debug("Processed segment failed with count {}", failedCount);
                  message.reply(new JsonObject().put(STATUS, OK));
                  break;
                case SEGMENT_TIMEDOUT:
                  val timedOutCount = payload.getLong("count");
                  updateTimedOutStats(timedOutCount);
                  log.debug("Processed segment timed out with count {}", timedOutCount);
                  message.reply(new JsonObject().put(STATUS, OK));
                  break;
                case STATS_RESET:
                  resetStats();
                  log.debug("Stats reset successfully at {}", LocalDateTime.now());
                  message.reply(new JsonObject().put(STATUS, OK));
                  break;
                case QUERY_STATS:
                  message.reply(getStats());
                  break;
                default:
                  unsupportedOperation(operation, message);
              }
            });
    startPromise.complete();
  }

  private synchronized JsonObject getStats() {
    return new JsonObject()
        .put("completed_segments", completedSegmentations.get())
        .put("timedout_segments", timedOutSegmentations.get())
        .put("failed_segments", failedSegmentations.get())
        .put("max_segmentation_time", maxSegmentationTime.get())
        .put("last_reset_to_last_update", lastUpdate - lastResetTime)
        .put("last_update_time", lastUpdateTime.toString());
  }

  private synchronized void resetStats() {
    completedSegmentations.set(0);
    timedOutSegmentations.set(0);
    failedSegmentations.set(0);
    maxSegmentationTime.set(0);
    lastResetTime = System.currentTimeMillis();
    lastUpdate = lastResetTime;
    lastUpdateTime = LocalDateTime.now();
  }

  private synchronized void updateStats(Long count, Long duration) {
    completedSegmentations.addAndGet(count);
    if (maxSegmentationTime.get() < duration) {
      maxSegmentationTime.set(duration);
    }
    lastUpdate = System.currentTimeMillis();
    lastUpdateTime = LocalDateTime.now();
  }

  private synchronized void updateFailedStats(Long count) {
    failedSegmentations.addAndGet(count);
    lastUpdate = System.currentTimeMillis();
    lastUpdateTime = LocalDateTime.now();
  }

  private void updateTimedOutStats(Long timedOutCount) {
    timedOutSegmentations.addAndGet(timedOutCount);
    lastUpdate = System.currentTimeMillis();
    lastUpdateTime = LocalDateTime.now();
  }

  private void unsupportedOperation(String operation, Message<JsonObject> event) {
    JsonObject reply = new JsonObject();
    String message = "SENDER STATS ERROR: unrecognized operation: " + operation;
    log.error(message);
    reply.put(STATUS, ERROR);
    reply.put(MESSAGE, message);
    event.reply(reply);
  }
}
