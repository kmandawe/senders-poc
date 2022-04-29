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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.*;

@Slf4j
public class SenderStatsVerticle extends AbstractVerticle {
  public static final String EB_STATS = "eb-stats";
  public static final String SEND_COMPLETE = "send-complete";
  public static final String SEND_FAILED = "send-failed";
  public static final String SEND_TIMEDOUT = "send-timedout";
  public static final String STATS_RESET = "stats-reset";
  public static final String QUERY_STATS = "query-stats";
  public static final String JOB_START = "job-start";
  public static final String JOB_BATCH_UPDATE = "job-batch";

  private AtomicLong completedSends;
  private AtomicLong timedOutSends;
  private AtomicLong failedSends;
  private AtomicLong maxSendTime;
  private long lastResetTime;
  private long lastUpdate;
  private LocalDateTime lastUpdateTime;
  private Map<String, SenderJob> senderJobs;

  @Override
  public void init(Vertx vertx, Context context) {
    completedSends = new AtomicLong();
    maxSendTime = new AtomicLong();
    failedSends = new AtomicLong();
    timedOutSends = new AtomicLong();
    lastResetTime = System.currentTimeMillis();
    lastUpdate = lastResetTime;
    lastUpdateTime = LocalDateTime.now();
    senderJobs = new HashMap<>();
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
                case SEND_COMPLETE:
                  val count = payload.getLong("count");
                  val duration = payload.getLong("duration");
                  updateStats(count, duration);
                  log.debug(
                      "Processed send complete with count {} and duration {}", count, duration);
                  message.reply(new JsonObject().put(STATUS, OK));
                  break;
                case SEND_FAILED:
                  val failedCount = payload.getLong("count");
                  updateFailedStats(failedCount);
                  log.debug("Processed send failed with count {}", failedCount);
                  message.reply(new JsonObject().put(STATUS, OK));
                  break;
                case SEND_TIMEDOUT:
                  val timedOutCount = payload.getLong("count");
                  updateTimedOutStats(timedOutCount);
                  log.debug("Processed send timed out with count {}", timedOutCount);
                  message.reply(new JsonObject().put(STATUS, OK));
                  break;
                case STATS_RESET:
                  resetStats();
                  log.debug("Stats reset successfully at {}", LocalDateTime.now());
                  message.reply(new JsonObject().put(STATUS, OK));
                  break;
                case JOB_START:
                  createNewJob(payload);
                  log.debug("Stats created a new job successfully at {}", LocalDateTime.now());
                  message.reply(new JsonObject().put(STATUS, OK));
                  break;
                case JOB_BATCH_UPDATE:
                  updateJobBatch(payload);
                  log.debug("Job batch update successful at {}", LocalDateTime.now());
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

  private synchronized void updateJobBatch(JsonObject payload) {
    val senderId = payload.getString("senderId");
    val batchToProcess = payload.getLong("batchToProcess");
    val batchCompleted = payload.getLong("batchCompleted");
    val senderJob = senderJobs.get(senderId);
    if (batchToProcess != null) {
      if (batchToProcess == 0) {
        senderJob.getBatchToProcess().set(batchToProcess);
        senderJob.setEndTimeMillis(System.currentTimeMillis());
        senderJob.setStatus("completed");
      } else {
        senderJob.getBatchToProcess().set(batchToProcess);
        senderJob.setStatus("executing");
      }
    }
    if (batchCompleted != null) {
      val memberSize = payload.getLong("memberSize");
      senderJob.getMembersProcessed().addAndGet(memberSize);
      long completed = senderJob.getBatchCompleted().addAndGet(batchCompleted);
      if (completed == senderJob.getBatchToProcess().get()) {
        senderJob.setEndTimeMillis(System.currentTimeMillis());
        senderJob.setStatus("completed");
      }
    }
  }

  private synchronized void createNewJob(JsonObject payload) {
    val senderId = payload.getString("senderId");
    val senderJob =
        SenderJob.builder()
            .senderId(senderId)
            .status("submitted")
            .startTimeMillis(System.currentTimeMillis())
            .membersProcessed(new AtomicLong())
            .batchToProcess(new AtomicLong())
            .batchCompleted(new AtomicLong())
            .build();
    senderJobs.put(senderId, senderJob);
  }

  private synchronized JsonObject getStats() {
    return new JsonObject()
        .put("completed_sends", completedSends.get())
        .put("timedout_sends", timedOutSends.get())
        .put("failed_sends", failedSends.get())
        .put("max_send_time", maxSendTime.get())
        .put("last_reset_to_last_update", lastUpdate - lastResetTime)
        .put("last_update_time", lastUpdateTime.toString())
        .put("sender_jobs", senderJobs.toString());
  }

  private synchronized void resetStats() {
    completedSends.set(0);
    timedOutSends.set(0);
    failedSends.set(0);
    maxSendTime.set(0);
    lastResetTime = System.currentTimeMillis();
    lastUpdate = lastResetTime;
    lastUpdateTime = LocalDateTime.now();
    senderJobs.clear();
  }

  private synchronized void updateStats(Long count, Long duration) {
    completedSends.addAndGet(count);
    if (maxSendTime.get() < duration) {
      maxSendTime.set(duration);
    }
    lastUpdate = System.currentTimeMillis();
    lastUpdateTime = LocalDateTime.now();
  }

  private synchronized void updateFailedStats(Long count) {
    failedSends.addAndGet(count);
    lastUpdate = System.currentTimeMillis();
    lastUpdateTime = LocalDateTime.now();
  }

  private void updateTimedOutStats(Long timedOutCount) {
    timedOutSends.addAndGet(timedOutCount);
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
