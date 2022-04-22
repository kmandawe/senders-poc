package com.cheetahdigital.senderspoc;

import com.cheetahdigital.senderspoc.api.RestApiVerticle;
import com.cheetahdigital.senderspoc.service.versioninfo.VersionInfoVerticle;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SendersVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) {
    vertx
        .deployVerticle(VersionInfoVerticle.class.getName())
        .onFailure(startPromise::fail)
        .onSuccess(
            id ->
                log.info("Deployed {} with ID: {}", VersionInfoVerticle.class.getSimpleName(), id))
        .compose(next -> deployRestApiVerticle(startPromise));
  }

  private Future<String> deployRestApiVerticle(Promise<Void> startPromise) {
    return vertx
        .deployVerticle(
            RestApiVerticle.class.getName(), new DeploymentOptions().setInstances(processors()))
        .onFailure(startPromise::fail)
        .onSuccess(
            id -> {
              log.info("Deployed {} with {}", RestApiVerticle.class.getSimpleName(), id);
              startPromise.complete();
            });
  }

  private int processors() {
    return Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
  }
}
