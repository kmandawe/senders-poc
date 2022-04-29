package com.cheetahdigital.senderspoc.service.sendpipeline;

import com.cheetahdigital.senderspoc.service.sendpipeline.processor.SendPipelineQueuesProcessor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SendPipelineVerticle extends AbstractVerticle {
  public static final String SP_EXECUTE_QUEUE = "sp-execute-queue";
  public static final String SP_RESOLVE_ATTRIBUTES = "sp-resolve-attributes";
  public static final String EB_SEGMENTATION = "eb-segmentation";
  public static final String EB_RESOLVE_ATTRIBUTES = "eb-resolve-attributes";
  public static final String EB_MEMBER_FUNCTIONS = "eb-member-functions";
  public static final String EB_MEMBERS_SUMMARY = "eb-members-summary";
  public static final String RESULT = "eb-member-functions";

  @Override
  public void start(Promise<Void> startPromise) {
    log.info("Starting {}...", SendPipelineVerticle.class.getSimpleName());
    SendPipelineQueuesProcessor.registerQueuesConsumer(vertx);
    startPromise.complete();
  }
}
