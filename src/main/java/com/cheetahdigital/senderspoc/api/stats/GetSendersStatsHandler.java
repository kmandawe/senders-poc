package com.cheetahdigital.senderspoc.api.stats;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import static com.cheetahdigital.senderspoc.service.stats.SenderStatsVerticle.EB_STATS;
import static com.cheetahdigital.senderspoc.service.stats.SenderStatsVerticle.QUERY_STATS;

@Slf4j
public class GetSendersStatsHandler implements Handler<RoutingContext> {
  @Override
  public void handle(RoutingContext context) {
    JsonObject payload = new JsonObject().put("operation", QUERY_STATS);
    eventBusSend(
        context,
        EB_STATS,
        payload,
        message -> {
          JsonObject responseBody = message.result().body();
          log.info("Current stats: {}", responseBody.toString());
          context
              .response()
              .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
              .end(responseBody.toBuffer());
        });
  }

  private void eventBusSend(
      RoutingContext context,
      String address,
      JsonObject payload,
      Handler<AsyncResult<Message<JsonObject>>> handler) {
    context.vertx().eventBus().request(address, payload, handler);
  }
}
