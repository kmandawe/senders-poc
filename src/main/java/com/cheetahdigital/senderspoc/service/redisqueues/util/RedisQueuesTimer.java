package com.cheetahdigital.senderspoc.service.redisqueues.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;

@Slf4j
public class RedisQueuesTimer {
  private final Vertx vertx;
  private Random random;

  public RedisQueuesTimer(Vertx vertx) {
    this.vertx = vertx;
    this.random = new Random();
  }

  /**
   * Delay an operation by providing a delay in milliseconds. This method completes the {@link
   * Future} any time between immediately and the delay. When 0 provided as delay, the {@link
   * Future} is resolved immediately.
   *
   * @param delayMs the delay in milliseconds
   * @return A {@link Future} which completes after the delay
   */
  public Future<Void> executeDelayedMax(long delayMs) {
    Promise<Void> promise = Promise.promise();

    if (delayMs > 0) {
      int delay = random.nextInt((int) (delayMs + 1)) + 1;
      log.debug("starting timer with a delay of " + delay + "ms");
      vertx.setTimer(delay, delayed -> promise.complete());
    } else {
      promise.complete();
    }

    return promise.future();
  }
}
