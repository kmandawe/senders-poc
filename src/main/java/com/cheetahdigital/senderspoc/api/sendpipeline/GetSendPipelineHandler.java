package com.cheetahdigital.senderspoc.api.sendpipeline;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public class GetSendPipelineHandler implements Handler<RoutingContext> {
  @Override
  public void handle(RoutingContext context) {
    final JsonObject response = new JsonObject().put("message", "Hello from Send Pipeline!");
    context
        .response()
        .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
        .end(response.toBuffer());
  }
}
