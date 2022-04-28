package com.cheetahdigital.senderspoc.api.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.UtilityClass;

@UtilityClass
public class RestApiUtil {
  public void eventBusSend(
      RoutingContext context,
      String address,
      JsonObject payload,
      Handler<AsyncResult<Message<JsonObject>>> handler) {
    context.vertx().eventBus().request(address, payload, handler);
  }
}
