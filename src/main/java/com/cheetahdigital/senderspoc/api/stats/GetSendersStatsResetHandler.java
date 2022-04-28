package com.cheetahdigital.senderspoc.api.stats;

import com.cheetahdigital.senderspoc.api.util.RestApiUtil;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.STATUS;
import static com.cheetahdigital.senderspoc.service.stats.SenderStatsVerticle.EB_STATS;
import static com.cheetahdigital.senderspoc.service.stats.SenderStatsVerticle.STATS_RESET;

@Slf4j
public class GetSendersStatsResetHandler implements Handler<RoutingContext> {
  @Override
  public void handle(RoutingContext context) {
    JsonObject payload = new JsonObject().put("operation", STATS_RESET);
    RestApiUtil.eventBusSend(
        context,
        EB_STATS,
        payload,
        message -> {
          JsonObject responseBody = message.result().body();
          log.info("Reset status: {}", responseBody.getString(STATUS));
          context
              .response()
              .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
              .end(responseBody.toBuffer());
        });
  }
}
