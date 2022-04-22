package com.cheetahdigital.senderspoc.service.versioninfo;

import com.cheetahdigital.senderspoc.common.config.ConfigLoader;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class VersionInfoVerticle extends AbstractVerticle {
  private static final Logger LOG = LoggerFactory.getLogger(VersionInfoVerticle.class);

  @Override
  public void start(Promise<Void> startPromise) {
    ConfigLoader.load(vertx)
        .onFailure(startPromise::fail)
        .onSuccess(
            configuration -> {
              log.info("Current Application Version is: {}", configuration.getVersion());
              startPromise.complete();
            });
  }
}
