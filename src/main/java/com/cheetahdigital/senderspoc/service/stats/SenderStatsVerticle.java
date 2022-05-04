package com.cheetahdigital.senderspoc.service.stats;

import com.cheetahdigital.senderspoc.common.config.BrokerConfig;
import com.cheetahdigital.senderspoc.db.DBPools;
import com.cheetahdigital.senderspoc.db.DbResponse;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.impl.RedisClient;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.templates.SqlTemplate;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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

  private static final int DB_POOL_MAX_SIZE = 4;
  private static final String REDIS_SENDER_STATS_KEY = "redis-senders:stats";
  private static final String REDIS_SENDER_STATS_FIELD = "stats";
  public static final String COMPLETED_SENDS = "completed_sends";
  public static final String MAX_SEND_TIME = "max_send_time";
  public static final String LAST_UPDATE_TIME = "last_update_time";
  public static final String LAST_RESET_TIME = "last_reset_time";
  public static final String FAILED_SENDS = "failed_sends";
  public static final String TIMEDOUT_SENDS = "timed_out_sends";

  private Pool db;
  private RedisAPI redisAPI;

  @Override
  public void init(Vertx vertx, Context context) {
    BrokerConfig brokerConfig = BrokerConfig.from(context.config());
    this.db = DBPools.createMySQLPool(brokerConfig, vertx, DB_POOL_MAX_SIZE);
    val redisConfig = brokerConfig.getRedisQueues();
    // Address of the redis mod
    RedisClient redisClient =
        new RedisClient(
            vertx,
            new RedisOptions()
                .setConnectionString(
                    "redis://" + redisConfig.getRedisHost() + ":" + redisConfig.getRedisPort())
                .setPassword(redisConfig.getRedisAuth())
                .setMaxPoolSize(redisConfig.getRedisMaxPoolSize())
                .setMaxPoolWaiting(redisConfig.getRedisMaxWaitSize()));

    this.redisAPI = RedisAPI.api(redisClient);
    createIfNoStats();
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
                  updateStats(count, duration, message);
                  break;
                case SEND_FAILED:
                  val failedCount = payload.getLong("count");
                  updateFailedStats(failedCount, message);
                  break;
                case SEND_TIMEDOUT:
                  val timedOutCount = payload.getLong("count");
                  updateTimedOutStats(timedOutCount, message);
                  break;
                case STATS_RESET:
                  resetStats(message);
                  break;
                case JOB_START:
                  createNewJob(payload, message);
                  break;
                case JOB_BATCH_UPDATE:
                  updateJobBatch(payload, message);
                  break;
                case QUERY_STATS:
                  getStats(message);
                  break;
                default:
                  unsupportedOperation(operation, message);
              }
            });
    startPromise.complete();
  }

  private synchronized void updateJobBatch(JsonObject payload, Message<JsonObject> message) {
    val senderId = payload.getString("senderId");
    val jobId = payload.getString("jobId");
    val batchToProcess = payload.getLong("batchToProcess");
    val batchCompleted = payload.getLong("batchCompleted");

    db.withTransaction(
            client ->
                queryForSenderJob(client, jobId)
                    .compose(
                        senderJobs ->
                            performSenderJobUpdate(
                                client,
                                senderId,
                                senderJobs,
                                batchToProcess,
                                batchCompleted,
                                payload)))
        .onComplete(
            txn -> {
              if (txn.succeeded()) {
                val successMessage = txn.result();
                log.info(
                    "Job batch update done with message {} at {}",
                    successMessage,
                    LocalDateTime.now());
                message.reply(new JsonObject().put(STATUS, OK));
              } else {
                Throwable cause = txn.cause();
                log.error("Job batch update error: ", cause);
                message.reply(new JsonObject().put(STATUS, ERROR));
              }
            });
  }

  private synchronized void createNewJob(JsonObject payload, Message<JsonObject> message) {
    val senderId = payload.getString("senderId");
    val jobId = payload.getString("jobId");
    val timeNow = LocalDateTime.now();
    val senderJob =
        SenderJob.builder()
            .jobId(jobId)
            .senderId(Integer.parseInt(senderId))
            .status("submitted")
            .startTime(timeNow)
            .lastUpdateTime(timeNow)
            .membersProcessed(0L)
            .batchToProcess(0L)
            .batchCompleted(0L)
            .build();
    SqlTemplate.forUpdate(
            db,
            "INSERT into sender_jobs (job_id, sender_id, status, start_time, last_update_time,"
                + " members_processed, batch_to_process, batch_completed) VALUES (#{job_id}, #{sender_id}, #{status},"
                + " #{start_time}, #{last_update_time}, #{members_processed}, #{batch_to_process}, #{batch_completed})")
        .mapFrom(SenderJob.class)
        .execute(senderJob)
        .onFailure(
            DbResponse.errorHandler(
                message, "Failed inserting Sender Job with senderId: " + senderId))
        .onSuccess(
            resp -> {
              log.info(
                  "Stats created a new job with senderId {} successfully at {}",
                  senderId,
                  LocalDateTime.now());
              message.reply(new JsonObject().put(STATUS, OK));
            });
  }

  private synchronized void getStats(Message<JsonObject> message) {
    redisAPI
        .hget(REDIS_SENDER_STATS_KEY, REDIS_SENDER_STATS_FIELD)
        .onFailure(DbResponse.errorHandler(message, "Failed retrieving Sender Stats from Redis!"))
        .onSuccess(
            redisResp ->
                queryForSenderJobs(db)
                    .onFailure(
                        DbResponse.errorHandler(message, "Failed retrieving Sender Jobs from DB!"))
                    .onSuccess(
                        resp -> {
                          JsonObject stats;
                          val timeNow = LocalDateTime.now();
                          if (redisResp == null) {
                            val senderStatsNew =
                                SenderStats.builder()
                                    .completedSends(0L)
                                    .timedOutSends(0L)
                                    .failedSends(0L)
                                    .maxSendTime(0L)
                                    .lastResetTime(timeNow)
                                    .lastUpdateTime(timeNow)
                                    .build();
                            stats = senderStatsNew.toJsonObject();
                          } else {
                            val redisStatsJson = new JsonObject(redisResp.toString());
                            val lastUpdateString = redisStatsJson.getString(LAST_UPDATE_TIME);
                            val lastResetString = redisStatsJson.getString(LAST_RESET_TIME);
                            stats =
                                new JsonObject()
                                    .put(
                                        COMPLETED_SENDS, redisStatsJson.getInteger(COMPLETED_SENDS))
                                    .put(TIMEDOUT_SENDS, redisStatsJson.getInteger(TIMEDOUT_SENDS))
                                    .put(FAILED_SENDS, redisStatsJson.getInteger(FAILED_SENDS))
                                    .put(MAX_SEND_TIME, redisStatsJson.getInteger(MAX_SEND_TIME))
                                    .put(
                                        "last_reset_to_last_update",
                                        ChronoUnit.MILLIS.between(
                                            LocalDateTime.parse(
                                                lastResetString,
                                                DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                                            LocalDateTime.parse(
                                                lastUpdateString,
                                                DateTimeFormatter.ISO_LOCAL_DATE_TIME)))
                                    .put(
                                        LAST_UPDATE_TIME,
                                        redisStatsJson.getString(LAST_UPDATE_TIME));
                          }
                          List<SenderJob> senderJobList = new ArrayList<>();
                          resp.forEach(senderJobList::add);
                          stats.put("sender_jobs", new JsonArray(senderJobList).toString());
                          message.reply(stats);
                        }));
  }

  private synchronized void resetStats(Message<JsonObject> message) {

    val timeNow = LocalDateTime.now();
    val senderStatsNew =
        SenderStats.builder()
            .completedSends(0L)
            .timedOutSends(0L)
            .failedSends(0L)
            .maxSendTime(0L)
            .lastResetTime(timeNow)
            .lastUpdateTime(timeNow)
            .build();

    redisAPI
        .hset(
            List.of(
                REDIS_SENDER_STATS_KEY,
                REDIS_SENDER_STATS_FIELD,
                senderStatsNew.toJsonObject().toString()))
        .onFailure(DbResponse.errorHandler(message, "Unable to reset Sender Stats to Redis"))
        .onSuccess(
            res -> {
              log.debug("Stats reset successfully at {}", LocalDateTime.now());
              message.reply(new JsonObject().put(STATUS, OK));
            });
  }

  private synchronized void createIfNoStats() {
    redisAPI
        .hget(REDIS_SENDER_STATS_KEY, REDIS_SENDER_STATS_FIELD)
        .onFailure(error -> log.error("Cannot get Sender stats from Redis ", error))
        .onSuccess(
            redisResp -> {
              if (redisResp == null) {
                val timeNow = LocalDateTime.now();
                val senderStatsNew =
                    SenderStats.builder()
                        .completedSends(0L)
                        .timedOutSends(0L)
                        .failedSends(0L)
                        .maxSendTime(0L)
                        .lastResetTime(timeNow)
                        .lastUpdateTime(timeNow)
                        .build();

                redisAPI
                    .hset(
                        List.of(
                            REDIS_SENDER_STATS_KEY,
                            REDIS_SENDER_STATS_FIELD,
                            senderStatsNew.toJsonObject().toString()))
                    .onFailure(error -> log.error("Error resetting stats: ", error))
                    .onSuccess(
                        res -> log.info("Stats reset successfully at {}", LocalDateTime.now()));
              } else {
                log.info("Already have Sender Stats in Redis {}", redisResp);
              }
            });
  }

  private synchronized void updateStats(Long count, Long duration, Message<JsonObject> message) {
    redisAPI
        .hget(REDIS_SENDER_STATS_KEY, REDIS_SENDER_STATS_FIELD)
        .onFailure(DbResponse.errorHandler(message, "Unable to retrieve Sender Stats from Redis"))
        .onSuccess(
            resp -> {
              val statsJson = new JsonObject(resp.toString());
              val newCompletedSends = statsJson.getInteger(COMPLETED_SENDS) + count;
              statsJson.put(COMPLETED_SENDS, newCompletedSends);
              val maxSendTime = statsJson.getLong(MAX_SEND_TIME);
              if (maxSendTime < duration) {
                statsJson.put(MAX_SEND_TIME, duration);
              }
              statsJson.put(LAST_UPDATE_TIME, LocalDateTime.now().toString());
              redisAPI
                  .hset(
                      List.of(
                          REDIS_SENDER_STATS_KEY, REDIS_SENDER_STATS_FIELD, statsJson.toString()))
                  .onFailure(
                      DbResponse.errorHandler(message, "Unable to update Sender Stats to Redis"))
                  .onSuccess(
                      res -> {
                        log.info(
                            "Processed send complete with count {} and duration {}",
                            count,
                            duration);
                        message.reply(new JsonObject().put(STATUS, OK));
                      });
            });
  }

  private synchronized void updateFailedStats(Long failedCount, Message<JsonObject> message) {
    redisAPI
        .hget(REDIS_SENDER_STATS_KEY, REDIS_SENDER_STATS_FIELD)
        .onFailure(DbResponse.errorHandler(message, "Unable to retrieve Sender Stats from Redis"))
        .onSuccess(
            resp -> {
              val statsJson = JsonObject.mapFrom(resp.toString());
              val newFailedSends = statsJson.getInteger(FAILED_SENDS) + failedCount;
              statsJson.put(FAILED_SENDS, newFailedSends);
              statsJson.put(LAST_UPDATE_TIME, LocalDateTime.now().toString());

              redisAPI
                  .hset(
                      List.of(
                          REDIS_SENDER_STATS_KEY, REDIS_SENDER_STATS_FIELD, statsJson.toString()))
                  .onFailure(
                      DbResponse.errorHandler(message, "Unable to update Sender Stats to Redis"))
                  .onSuccess(
                      res -> {
                        log.info("Processed send failed with count {}", failedCount);
                        message.reply(new JsonObject().put(STATUS, OK));
                      });
            });
  }

  private void updateTimedOutStats(Long timedOutCount, Message<JsonObject> message) {
    redisAPI
        .hget(REDIS_SENDER_STATS_KEY, REDIS_SENDER_STATS_FIELD)
        .onFailure(DbResponse.errorHandler(message, "Unable to retrieve Sender Stats from Redis"))
        .onSuccess(
            resp -> {
              val statsJson = JsonObject.mapFrom(resp.toString());
              val newTimedOutSends = statsJson.getInteger(TIMEDOUT_SENDS) + timedOutCount;
              statsJson.put(TIMEDOUT_SENDS, newTimedOutSends);
              statsJson.put(LAST_UPDATE_TIME, LocalDateTime.now().toString());

              redisAPI
                  .hset(
                      List.of(
                          REDIS_SENDER_STATS_KEY, REDIS_SENDER_STATS_FIELD, statsJson.toString()))
                  .onFailure(
                      DbResponse.errorHandler(message, "Unable to update Sender Stats to Redis"))
                  .onSuccess(
                      res -> {
                        log.debug("Processed send timed out with count {}", timedOutCount);
                        message.reply(new JsonObject().put(STATUS, OK));
                      });
            });
  }

  private void unsupportedOperation(String operation, Message<JsonObject> event) {
    JsonObject reply = new JsonObject();
    String message = "SENDER STATS ERROR: unrecognized operation: " + operation;
    log.error(message);
    reply.put(STATUS, ERROR);
    reply.put(MESSAGE, message);
    event.reply(reply);
  }

  private Future<String> performSenderJobUpdate(
      SqlConnection client,
      String senderId,
      RowSet<SenderJob> senderJobs,
      Long batchToProcess,
      Long batchCompleted,
      JsonObject payload) {
    return Future.future(
        promise -> {
          if (!senderJobs.iterator().hasNext()) {
            log.warn("Sender Job for senderId" + senderId + " not available!");
            promise.fail("Sender Job for senderId" + senderId + " not available!");
          } else {
            val senderJob = senderJobs.iterator().next();
            val senderUpdated = senderJob.toBuilder().build();
            if (batchToProcess != null) {
              if (batchToProcess == 0) {
                senderUpdated.setBatchToProcess(batchToProcess);
                senderUpdated.setEndTime(LocalDateTime.now());
                senderUpdated.setStatus("completed");
              } else {
                senderUpdated.setBatchToProcess(batchToProcess);
                senderUpdated.setStatus("executing");
              }
            }
            if (batchCompleted != null) {
              val memberSize = payload.getLong("memberSize");
              val membersProcessed = senderUpdated.getMembersProcessed();
              senderUpdated.setMembersProcessed(membersProcessed + memberSize);
              val completed = senderUpdated.getBatchCompleted();
              senderUpdated.setBatchCompleted(completed + batchCompleted);
              if (Objects.equals(
                  senderUpdated.getBatchCompleted(), senderUpdated.getBatchToProcess())) {
                senderUpdated.setEndTime(LocalDateTime.now());
                senderUpdated.setStatus("completed");
              }
            }

            // Check if with updates and do DB update
            if (!senderUpdated.equals(senderJob)) {
              senderUpdated.setLastUpdateTime(LocalDateTime.now());
              val sqlResponse =
                  SqlTemplate.forUpdate(
                          client,
                          "UPDATE sender_jobs "
                              + "SET status = #{status}, start_time = #{start_time},"
                              + " end_time = #{end_time}, message = #{message}, error_message = #{error_message},"
                              + " members_processed = #{members_processed}, batch_to_process = #{batch_to_process},"
                              + " batch_completed = #{batch_completed}"
                              + " WHERE job_id = #{job_id}")
                      .mapFrom(SenderJob.class)
                      .execute(senderUpdated)
                      .map("Successfully updated Sender Job.");
              promise.complete(sqlResponse.result());
            } else {
              promise.complete("No changes to Sender Job, update not executed");
            }
          }
        });
  }

  private Future<RowSet<SenderJob>> queryForSenderJob(SqlConnection client, String jobId) {
    return SqlTemplate.forQuery(client, "SELECT * FROM sender_jobs WHERE job_id = #{jobId}")
        .mapTo(SenderJob.class)
        .execute(Collections.singletonMap("jobId", jobId));
  }

  private Future<RowSet<SenderJob>> queryForSenderJobs(Pool client) {
    return SqlTemplate.forQuery(client, "SELECT * FROM sender_jobs")
        .mapTo(SenderJob.class)
        .execute(Collections.emptyMap());
  }
}
