package com.cheetahdigital.senderspoc;

import com.cheetahdigital.senderspoc.api.RestApiVerticle;
import com.cheetahdigital.senderspoc.common.config.ConfigLoader;
import com.cheetahdigital.senderspoc.service.attrcalculation.AttributesCalculationVerticle;
import com.cheetahdigital.senderspoc.service.memberfunctions.MemberFunctionsVerticle;
import com.cheetahdigital.senderspoc.service.membersummary.MembersSummaryVerticle;
import com.cheetahdigital.senderspoc.service.redisqueues.RedisQueuesVerticle;
import com.cheetahdigital.senderspoc.service.segmentation.SegmentationVerticle;
import com.cheetahdigital.senderspoc.service.sendpipeline.SendPipelineVerticle;
import com.cheetahdigital.senderspoc.service.stats.SenderStatsVerticle;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.concurrent.atomic.AtomicReference;

import static com.cheetahdigital.senderspoc.common.config.ConfigLoader.REDISQUEUES_CONFIG;

@Slf4j
public class SendersVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) {
    var startTimeMillis = System.currentTimeMillis();
    AtomicReference<JsonObject> brokerConfig = new AtomicReference<>();
    ConfigLoader.load(vertx)
        .onFailure(startPromise::fail)
        .onSuccess(
            configuration -> {
              log.info("Retrieved configuration {}", configuration);
              log.info("Current Application Version is: {}", configuration.getVersion());
              brokerConfig.set(JsonObject.mapFrom(configuration));
            })
        .compose(
            next ->
                deployVerticle(
                    RedisQueuesVerticle.class,
                    brokerConfig.get().getJsonObject(REDISQUEUES_CONFIG),
                    startPromise,
                    2,
                    false,
                    startTimeMillis))
        .compose(
            next ->
                deployVerticle(
                    SendPipelineVerticle.class,
                    brokerConfig.get(),
                    startPromise,
                    2,
                    false,
                    startTimeMillis))
        .compose(
            next -> {
              val segmentationConfig = brokerConfig.get().getJsonObject("segmentation");
              val instances = segmentationConfig.getInteger("instances");
              return deployVerticle(
                  SegmentationVerticle.class,
                  startPromise,
                  new DeploymentOptions()
                      .setInstances(instances)
                      .setConfig(brokerConfig.get())
                      .setWorker(true)
                      .setWorkerPoolSize(instances)
                      .setWorkerPoolName("senders-segmentation-worker"),
                  false,
                  startTimeMillis);
            })
        .compose(
            next ->
                deployVerticle(
                    SenderStatsVerticle.class,
                    brokerConfig.get(),
                    startPromise,
                    1,
                    false,
                    startTimeMillis))
        .compose(
            next ->
                deployVerticle(
                    AttributesCalculationVerticle.class,
                    startPromise,
                    new DeploymentOptions()
                        // TODO: dynamic
                        .setInstances(4)
                        .setConfig(brokerConfig.get())
                        .setWorker(true)
                        // TODO: dynamic
                        .setWorkerPoolSize(4)
                        .setWorkerPoolName("attributes-calculation-worker"),
                    false,
                    startTimeMillis))
        .compose(
            next ->
                deployVerticle(
                    MemberFunctionsVerticle.class,
                    startPromise,
                    new DeploymentOptions()
                        // TODO: dynamic
                        .setInstances(4)
                        .setConfig(brokerConfig.get())
                        .setWorker(true)
                        // TODO: dynamic
                        .setWorkerPoolSize(4)
                        .setWorkerPoolName("member-functions-worker"),
                    false,
                    startTimeMillis))
        .compose(
            next ->
                deployVerticle(
                    MembersSummaryVerticle.class,
                    startPromise,
                    new DeploymentOptions()
                        // TODO: dynamic
                        .setInstances(4)
                        .setConfig(brokerConfig.get())
                        .setWorker(true)
                        // TODO: dynamic
                        .setWorkerPoolSize(4)
                        .setWorkerPoolName("members-summary-worker"),
                    false,
                    startTimeMillis))
        .compose(
            next ->
                deployVerticle(
                    RestApiVerticle.class,
                    brokerConfig.get(),
                    startPromise,
                    2,
                    true,
                    startTimeMillis));
  }

  private Future<String> deployVerticle(
      Class<? extends Verticle> verticleClass,
      JsonObject config,
      Promise<Void> startPromise,
      int instances,
      boolean completeOnSuccess,
      long startTime) {
    val deploymentOptions = new DeploymentOptions().setConfig(config).setInstances(instances);
    return deployVerticle(
        verticleClass, startPromise, deploymentOptions, completeOnSuccess, startTime);
  }

  private Future<String> deployVerticle(
      Class<? extends Verticle> verticleClass,
      Promise<Void> startPromise,
      DeploymentOptions deploymentOptions,
      boolean completeOnSuccess,
      long startTime) {
    return vertx
        .deployVerticle(verticleClass.getName(), deploymentOptions)
        .onFailure(startPromise::fail)
        .onSuccess(
            id -> {
              log.info("Deployed {} with {}", verticleClass.getSimpleName(), id);
              if (completeOnSuccess) {
                long duration = System.currentTimeMillis() - startTime;
                log.info("###################################################");
                log.info("###### Senders Microservice started in {}ms ######", duration);
                log.info("###################################################");
                startPromise.complete();
              }
            });
  }

  private int processors() {
    return Math.max(1, Runtime.getRuntime().availableProcessors());
  }
}
