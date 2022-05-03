package com.cheetahdigital.senderspoc.db;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.ERROR;
import static com.cheetahdigital.senderspoc.service.redisqueues.util.RedisQueuesAPI.STATUS;

@Slf4j
public class DbResponse {

  public static Handler<Throwable> errorHandler(Message<JsonObject> message, String errorMessage) {
    return error -> {
      log.error("Failure: ", error);
      val response = new JsonObject().put(STATUS, ERROR).put("message", errorMessage);
      message.reply(response);
    };
  }

  public static void notFoundResponse(Message<JsonObject> message, String errorMessage) {
    log.error("Failure: ", errorMessage);
    val response = new JsonObject().put(STATUS, ERROR).put("message", errorMessage);
    message.reply(response);
  }

  public static Handler<Throwable> errorHandler(RoutingContext context, String message) {
    return error -> {
      log.error("Failure: ", error);
      context
          .response()
          .setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code())
          .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
          .end(
              new JsonObject()
                  .put("message", message)
                  .put("path", context.normalizedPath())
                  .toBuffer());
    };
  }

  public static void notFoundResponse(RoutingContext context, String message) {
    context
        .response()
        .setStatusCode(HttpResponseStatus.NOT_FOUND.code())
        .end(
            new JsonObject()
                .put("message", message)
                .put("path", context.normalizedPath())
                .toBuffer());
  }
}
