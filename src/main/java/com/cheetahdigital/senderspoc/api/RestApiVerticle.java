package com.cheetahdigital.senderspoc.api;

import com.cheetahdigital.senderspoc.api.sendpipeline.SendPipelineRestApi;
import com.cheetahdigital.senderspoc.common.config.BrokerConfig;
import com.cheetahdigital.senderspoc.common.config.ConfigLoader;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestApiVerticle extends AbstractVerticle {
  @Override
  public void start(Promise<Void> startPromise) {
    ConfigLoader.load(vertx)
        .onFailure(startPromise::fail)
        .onSuccess(
            configuration -> {
              log.info("Retrieved configuration {}", configuration);
              startHttpServerAndAttachRoutes(startPromise, configuration);
            });
  }

  private void startHttpServerAndAttachRoutes(
      final Promise<Void> startPromise, final BrokerConfig configuration) {

    final Router restApi = Router.router(vertx);
    restApi.route().handler(BodyHandler.create()).failureHandler(handleFailure());

    SendPipelineRestApi.attach(restApi);

    var port = configuration.getServerConfig().getPort();

    vertx
        .createHttpServer()
        .requestHandler(restApi)
        .exceptionHandler(error -> log.error("HTTP Server error: ", error))
        .listen(
            port,
            http -> {
              if (http.succeeded()) {
                startPromise.complete();
                log.info("HTTP server started on port {}", port);
              } else {
                startPromise.fail(http.cause());
              }
            });
  }

  private Handler<RoutingContext> handleFailure() {
    return errorContext -> {
      if (errorContext.response().ended()) {
        // Ignore completed response
        return;
      }
      log.error("Route Error:", errorContext.failure());
      errorContext
          .response()
          .setStatusCode(500)
          .end(new JsonObject().put("message", "Something went wrong :(").toBuffer());
    };
  }
}
