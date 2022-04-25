package com.cheetahdigital.senderspoc.service.sendpipeline;

import com.cheetahdigital.senderspoc.service.sendpipeline.processor.SendPipelineQueuesProcessor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SendPipelineVerticle extends AbstractVerticle {
  public static final String SP_EXECUTE_QUEUE = "sp-execute-queue";
  public static final String EB_SEGMENTATION = "eb-segmentation";

  @Override
  public void start(Promise<Void> startPromise) {
    log.info("Starting {}...", SendPipelineVerticle.class.getSimpleName());
    SendPipelineQueuesProcessor.registerQueuesConsumer(vertx);
    startPromise.complete();
  }
}
