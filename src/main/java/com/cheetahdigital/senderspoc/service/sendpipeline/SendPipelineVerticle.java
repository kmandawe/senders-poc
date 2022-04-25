package com.cheetahdigital.senderspoc.service.sendpipeline;

import com.cheetahdigital.senderspoc.service.sendpipeline.processor.SendPipelineQueuesProcessor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.OK;
import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.STATUS;

@Slf4j
public class SendPipelineVerticle extends AbstractVerticle {
  public static final String SEGMENTATION_QUEUE = "segmentation_queue";

  @Override
  public void start(Promise<Void> startPromise) {
    log.info("Starting {}...", SendPipelineVerticle.class.getSimpleName());
    SendPipelineQueuesProcessor.registerQueuesConsumer(vertx);
    startPromise.complete();
  }
}
