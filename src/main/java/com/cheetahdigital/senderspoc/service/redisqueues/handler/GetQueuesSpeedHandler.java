package com.cheetahdigital.senderspoc.service.redisqueues.handler;

import com.cheetahdigital.senderspoc.service.redisqueues.util.QueueHandlerUtil;
import com.cheetahdigital.senderspoc.service.redisqueues.util.QueueStatisticsCollector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.ERROR;
import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.STATUS;

/**
 * Retrieves in it's AsyncResult handler the speed summary of the queues matching the given filter
 * and returns the same in the event completion.
 */
public class GetQueuesSpeedHandler implements Handler<AsyncResult<Response>> {

  private final Message<JsonObject> event;
  private final Optional<Pattern> filterPattern;
  private final QueueStatisticsCollector queueStatisticsCollector;

  public GetQueuesSpeedHandler(
      Message<JsonObject> event,
      Optional<Pattern> filterPattern,
      QueueStatisticsCollector queueStatisticsCollector) {
    this.event = event;
    this.filterPattern = filterPattern;
    this.queueStatisticsCollector = queueStatisticsCollector;
  }

  @Override
  public void handle(AsyncResult<Response> handleQueues) {
    if (handleQueues.succeeded()) {
      // apply the given filter in order to have only the queues for which we ar interested
      List<String> queues = QueueHandlerUtil.filterQueues(handleQueues.result(), filterPattern);
      queueStatisticsCollector.getQueuesSpeed(event, queues);
    } else {
      event.reply(new JsonObject().put(STATUS, ERROR));
    }
  }
}
