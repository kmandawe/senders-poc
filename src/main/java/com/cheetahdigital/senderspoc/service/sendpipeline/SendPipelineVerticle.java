package com.cheetahdigital.senderspoc.service.sendpipeline;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.OK;
import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.STATUS;

@Slf4j
public class SendPipelineVerticle extends AbstractVerticle {
  @Override
  public void start(Promise<Void> startPromise) {
    log.info("Starting {}...", SendPipelineVerticle.class.getSimpleName());
    vertx
        .eventBus()
        .<JsonObject>consumer(getProcessorAddress())
        .handler(
            message -> {
              final String queue = message.body().getString("queue");
              final String payload = message.body().getString("payload");
              log.info("Processed message {} from QUEUE: {}", payload, queue);
              message.reply(new JsonObject().put(STATUS, OK));
            });
    startPromise.complete();
  }

  private String getProcessorAddress() {
    return "redis-queues-processor";
  }
}
