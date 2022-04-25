package com.cheetahdigital.senderspoc.service.sendpipeline.processor;

import com.cheetahdigital.senderspoc.common.config.BrokerConfig;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.OK;
import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.STATUS;

@Slf4j
public class SendPipelineQueuesProcessor {

  public static void registerQueuesConsumer(Vertx vertx) {
    val brokerConfig = BrokerConfig.from(vertx.getOrCreateContext().config());
    vertx
        .eventBus()
        .<JsonObject>consumer(brokerConfig.getRedisQueues().getProcessorAddress())
        .handler(
            message -> {
              final String queue = message.body().getString("queue");
              final String payload = message.body().getString("payload");
              log.info("APP 1: Processed message {} from QUEUE: {}", payload, queue);
              message.reply(new JsonObject().put(STATUS, OK));
            });
  }
}
