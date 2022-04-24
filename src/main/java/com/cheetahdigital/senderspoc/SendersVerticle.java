package com.cheetahdigital.senderspoc;

import com.cheetahdigital.senderspoc.api.RestApiVerticle;
import com.cheetahdigital.senderspoc.service.redisqueues.RedisQueuesVerticle;
import com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle;
import com.cheetahdigital.senderspoc.service.versioninfo.VersionInfoVerticle;
import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SendersVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) {
    var startTimeMillis = System.currentTimeMillis();
    deployVerticle(VersionInfoVerticle.class, startPromise, 1, false, startTimeMillis)
        .compose(
            next ->
                deployVerticle(
                    RedisQueuesVerticle.class,
                    startPromise,
                    processors() / 4,
                    false,
                    startTimeMillis))
        .compose(
            next ->
                deployVerticle(SendPipelineVerticle.class, startPromise, 2, false, startTimeMillis))
        .compose(
            next ->
                deployVerticle(
                    RestApiVerticle.class, startPromise, processors() / 4, true, startTimeMillis));
  }

  private Future<String> deployVerticle(
      Class<? extends Verticle> verticleClass,
      Promise<Void> startPromise,
      int instances,
      boolean completeOnSuccess,
      long startTime) {
    return vertx
        .deployVerticle(verticleClass.getName(), new DeploymentOptions().setInstances(instances))
        .onFailure(startPromise::fail)
        .onSuccess(
            id -> {
              log.info("Deployed {} with {}", verticleClass.getSimpleName(), id);
              if (completeOnSuccess) {
                long duration = System.currentTimeMillis() - startTime;
                log.info("###################################################");
                log.info("###### Senders Microservice started in {}ms ######", duration);
                log.info("###################################################");;
                startPromise.complete();
              }
            });
  }

  private int processors() {
    return Math.max(1, Runtime.getRuntime().availableProcessors());
  }
}
