package com.cheetahdigital.senderspoc.service.redisqueue.handler;

import com.cheetahdigital.senderspoc.service.redisqueue.util.RedisQueueAPI;
import com.cheetahdigital.senderspoc.service.redisqueue.util.RedisQueueConfiguration;
import com.cheetahdigital.senderspoc.service.redisqueue.util.Result;
import com.cheetahdigital.senderspoc.service.redisqueue.util.StatusCode;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.cheetahdigital.senderspoc.service.redisqueue.util.HttpServerRequestUtil.*;
import static com.cheetahdigital.senderspoc.service.redisqueue.util.RedisQueueAPI.*;

@Slf4j
public class RedisQueueHttpRequestHandler implements Handler<HttpServerRequest> {

  private final Router router;
  private final EventBus eventBus;

  private static final String APPLICATION_JSON = "application/json";
  private static final String CONTENT_TYPE = "content-type";
  private static final String LOCKED_PARAM = "locked";
  private static final String UNLOCK_PARAM = "unlock";
  private static final String BULK_DELETE_PARAM = "bulkDelete";
  private static final String EMPTY_QUEUES_PARAM = "emptyQueues";
  private static final String DELETED = "deleted";

  private final String redisquesAddress;
  private final String userHeader;
  private final boolean enableQueueNameDecoding;
  private final int queueSpeedIntervalSec;

  public static void init(Vertx vertx, RedisQueueConfiguration modConfig) {
    log.info("Enable http request handler: " + modConfig.getHttpRequestHandlerEnabled());
    if (modConfig.getHttpRequestHandlerEnabled()) {
      if (modConfig.getHttpRequestHandlerPort() != null
          && modConfig.getHttpRequestHandlerUserHeader() != null) {
        RedisQueueHttpRequestHandler handler = new RedisQueueHttpRequestHandler(vertx, modConfig);
        // in Vert.x 2x 100-continues was activated per default, in vert.x 3x it is off per default.
        HttpServerOptions options = new HttpServerOptions().setHandle100ContinueAutomatically(true);
        vertx
            .createHttpServer(options)
            .requestHandler(handler)
            .listen(
                modConfig.getHttpRequestHandlerPort(),
                result -> {
                  if (result.succeeded()) {
                    log.info(
                        "Successfully started http request handler on port "
                            + modConfig.getHttpRequestHandlerPort());
                  } else {
                    log.error("Unable to start http request handler.", result.cause());
                  }
                });
      } else {
        log.error(
            "Configured to enable http request handler but no port configuration and/or user header configuration provided");
      }
    }
  }

  private RedisQueueHttpRequestHandler(Vertx vertx, RedisQueueConfiguration modConfig) {
    this.router = Router.router(vertx);
    this.eventBus = vertx.eventBus();
    this.redisquesAddress = modConfig.getAddress();
    this.userHeader = modConfig.getHttpRequestHandlerUserHeader();
    this.enableQueueNameDecoding = modConfig.getEnableQueueNameDecoding();
    this.queueSpeedIntervalSec = modConfig.getQueueSpeedIntervalSec();

    final String prefix = modConfig.getHttpRequestHandlerPrefix();

    /*
     * List endpoints
     */
    router.get(prefix).handler(this::listEndpoints);

    /*
     * Get configuration
     */
    router.get(prefix + "/configuration").handler(this::getConfiguration);

    /*
     * Set configuration
     */
    router.post(prefix + "/configuration").handler(this::setConfiguration);

    /*
     * Get monitor information
     */
    router.get(prefix + "/monitor").handler(this::getMonitorInformation);

    /*
     * Get statistic information
     */
    router.get(prefix + "/statistics").handler(this::getQueuesStatistics);

    /*
     * Get queue speed information
     */
    router.get(prefix + "/speed").handler(this::getQueuesSpeed);

    /*
     * Enqueue or LockedEnqueue
     */
    router.putWithRegex(prefix + "/enqueue/([^/]+)/").handler(this::enqueueOrLockedEnqueue);

    /*
     * List queue items
     */
    router.getWithRegex(prefix + "/monitor/[^/]+").handler(this::listQueueItems);

    /*
     * List or count queues
     */
    router.get(prefix + "/queues").handler(this::listOrCountQueues);

    /*
     * List or count queue items
     */
    router.getWithRegex(prefix + "/queues/[^/]+").handler(this::listOrCountQueueItems);

    /*
     * Delete all queue items
     */
    router.deleteWithRegex(prefix + "/queues/[^/]+").handler(this::deleteAllQueueItems);

    /*
     * Bulk delete queues
     */
    router.post(prefix + "/queues").handler(this::bulkDeleteQueues);

    /*
     * Get single queue item
     */
    router.getWithRegex(prefix + "/queues/([^/]+)/[0-9]+").handler(this::getSingleQueueItem);

    /*
     * Replace single queue item
     */
    router.putWithRegex(prefix + "/queues/([^/]+)/[0-9]+").handler(this::replaceSingleQueueItem);

    /*
     * Delete single queue item
     */
    router.deleteWithRegex(prefix + "/queues/([^/]+)/[0-9]+").handler(this::deleteQueueItem);

    /*
     * Add queue item
     */
    router.postWithRegex(prefix + "/queues/([^/]+)/").handler(this::addQueueItem);

    /*
     * Get all locks
     */
    router.get(prefix + "/locks").handler(this::getAllLocks);

    /*
     * Add lock
     */
    router.putWithRegex(prefix + "/locks/[^/]+").handler(this::addLock);

    /*
     * Get single lock
     */
    router.getWithRegex(prefix + "/locks/[^/]+").handler(this::getSingleLock);

    /*
     * Delete all locks
     */
    router.delete(prefix + "/locks").handler(this::deleteAllLocks);

    /*
     * Bulk create / delete locks
     */
    router.post(prefix + "/locks").handler(this::bulkPutOrDeleteLocks);

    /*
     * Delete single lock
     */
    router.deleteWithRegex(prefix + "/locks/[^/]+").handler(this::deleteSingleLock);

    router.routeWithRegex(".*").handler(this::respondMethodNotAllowed);
  }

  @Override
  public void handle(HttpServerRequest request) {
    router.handle(request);
  }

  private void respondMethodNotAllowed(RoutingContext ctx) {
    respondWith(StatusCode.METHOD_NOT_ALLOWED, ctx.request());
  }

  private void listEndpoints(RoutingContext ctx) {
    JsonObject result = new JsonObject();
    JsonArray items = new JsonArray();
    items.add("locks/");
    items.add("queues/");
    items.add("monitor/");
    items.add("configuration/");
    result.put(lastPart(ctx.request().path()), items);
    ctx.response().putHeader(CONTENT_TYPE, APPLICATION_JSON);
    ctx.response().end(result.encode());
  }

  private void enqueueOrLockedEnqueue(RoutingContext ctx) {
    decodedQueueNameOrRespondWithBadRequest(ctx, lastPart(ctx.request().path()))
        .ifPresent(
            queue ->
                ctx.request()
                    .bodyHandler(
                        buffer -> {
                          try {
                            String strBuffer = encodePayload(buffer.toString());
                            eventBus.request(
                                redisquesAddress,
                                buildEnqueueOrLockedEnqueueOperation(
                                    queue, strBuffer, ctx.request()),
                                (Handler<AsyncResult<Message<JsonObject>>>)
                                    reply -> {
                                      if (reply.failed()) {
                                        log.warn(
                                            "Received failed message for enqueueOrLockedEnqueue. But will continue anyway",
                                            reply.cause());
                                        // We should respond with 'HTTP 5xx' here. But we don't, to
                                        // keep backward compatibility.
                                        // Why we should? See:
                                        // "https://softwareengineering.stackexchange.com/a/190535"
                                      }
                                      checkReply(
                                          reply.result(), ctx.request(), StatusCode.BAD_REQUEST);
                                    });
                          } catch (Exception ex) {
                            respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), ctx.request());
                          }
                        }));
  }

  private JsonObject buildEnqueueOrLockedEnqueueOperation(
      String queue, String message, HttpServerRequest request) {
    if (evaluateUrlParameterToBeEmptyOrTrue(LOCKED_PARAM, request)) {
      return buildLockedEnqueueOperation(queue, message, extractUser(request));
    } else {
      return buildEnqueueOperation(queue, message);
    }
  }

  private void getAllLocks(RoutingContext ctx) {
    String filter = ctx.request().params().get(FILTER);
    eventBus.request(
        redisquesAddress,
        buildGetAllLocksOperation(filter),
        (Handler<AsyncResult<Message<JsonObject>>>)
            reply -> {
              if (reply.failed()) {
                log.warn(
                    "Received failed message for getAllLocksOperation. Lets run into NullPointerException now",
                    reply.cause());
                // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward
                // compatibility.
              }
              final JsonObject body = reply.result().body();
              if (OK.equals(body.getString(STATUS))) {
                jsonResponse(ctx.response(), body.getJsonObject(VALUE));
              } else {
                String errorType = body.getString(ERROR_TYPE);
                if (BAD_INPUT.equalsIgnoreCase(errorType)) {
                  if (body.getString(MESSAGE) != null) {
                    respondWith(StatusCode.BAD_REQUEST, body.getString(MESSAGE), ctx.request());
                  } else {
                    respondWith(StatusCode.BAD_REQUEST, ctx.request());
                  }
                } else {
                  respondWith(StatusCode.NOT_FOUND, ctx.request());
                }
              }
            });
  }

  private void addLock(RoutingContext ctx) {
    decodedQueueNameOrRespondWithBadRequest(ctx, lastPart(ctx.request().path()))
        .ifPresent(
            queue ->
                eventBus.request(
                    redisquesAddress,
                    buildPutLockOperation(queue, extractUser(ctx.request())),
                    (Handler<AsyncResult<Message<JsonObject>>>)
                        reply -> {
                          if (reply.failed()) {
                            log.warn(
                                "Received failed message for addLockOperation. Lets run into NullPointerException now",
                                reply.cause());
                            // IMO we should respond with 'HTTP 5xx' here. But we don't, to keep
                            // backward compatibility.
                            // Nevertheless. Lets run into NullPointerException by calling method
                            // below.
                          }
                          checkReply(reply.result(), ctx.request(), StatusCode.BAD_REQUEST);
                        }));
  }

  private void getSingleLock(RoutingContext ctx) {
    decodedQueueNameOrRespondWithBadRequest(ctx, lastPart(ctx.request().path()))
        .ifPresent(
            queue ->
                eventBus.request(
                    redisquesAddress,
                    buildGetLockOperation(queue),
                    (Handler<AsyncResult<Message<JsonObject>>>)
                        reply -> {
                          final HttpServerResponse response = ctx.response();
                          if (reply.failed()) {
                            log.warn(
                                "Received failed message for getSingleLockOperation. Lets run into NullPointerException now",
                                reply.cause());
                            // IMO we should respond with 'HTTP 5xx' here. But we don't, to keep
                            // backward compatibility.
                          }
                          if (OK.equals(reply.result().body().getString(STATUS))) {
                            response.putHeader(CONTENT_TYPE, APPLICATION_JSON);
                            response.end(reply.result().body().getString(VALUE));
                          } else {
                            response.setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
                            response.setStatusMessage(StatusCode.NOT_FOUND.getStatusMessage());
                            response.end(NO_SUCH_LOCK);
                          }
                        }));
  }

  private Optional<String> decodedQueueNameOrRespondWithBadRequest(
      RoutingContext ctx, String encodedQueueName) {
    if (enableQueueNameDecoding) {
      String decodedQueueName;
      try {
        decodedQueueName = java.net.URLDecoder.decode(encodedQueueName, StandardCharsets.UTF_8);
      } catch (IllegalArgumentException ex) {
        respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), ctx.request());
        return Optional.empty();
      }
      return Optional.of(decodedQueueName);
    } else {
      return Optional.of(encodedQueueName);
    }
  }

  private void deleteSingleLock(RoutingContext ctx) {
    decodedQueueNameOrRespondWithBadRequest(ctx, lastPart(ctx.request().path()))
        .ifPresent(
            queue ->
                eventBus.request(
                    redisquesAddress,
                    buildDeleteLockOperation(queue),
                    (Handler<AsyncResult<Message<JsonObject>>>)
                        reply -> {
                          if (reply.failed()) {
                            log.warn(
                                "Received failed message for deleteSingleLockOperation. Lets run into NullPointerException now",
                                reply.cause());
                            // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward
                            // compatibility.
                            // Nevertheless. Lets run into NullPointerException by calling below
                            // method.
                          }
                          checkReply(
                              reply.result(), ctx.request(), StatusCode.INTERNAL_SERVER_ERROR);
                        }));
  }

  private void deleteAllLocks(RoutingContext ctx) {
    eventBus.request(
        redisquesAddress,
        buildDeleteAllLocksOperation(),
        (Handler<AsyncResult<Message<JsonObject>>>)
            reply -> {
              if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                JsonObject result = new JsonObject();
                result.put(DELETED, reply.result().body().getLong(VALUE));
                jsonResponse(ctx.response(), result);
              } else {
                respondWith(
                    StatusCode.INTERNAL_SERVER_ERROR, "Error deleting all locks", ctx.request());
              }
            });
  }

  private void bulkPutOrDeleteLocks(RoutingContext ctx) {
    ctx.request()
        .bodyHandler(
            buffer -> {
              try {
                Result<JsonArray, String> result =
                    extractNonEmptyJsonArrayFromBody(LOCKS, buffer.toString());
                if (result.isErr()) {
                  respondWith(StatusCode.BAD_REQUEST, result.getErr(), ctx.request());
                  return;
                }

                if (evaluateUrlParameterToBeEmptyOrTrue(BULK_DELETE_PARAM, ctx.request())) {
                  bulkDeleteLocks(ctx, result.getOk());
                } else {
                  bulkPutLocks(ctx, result.getOk());
                }
              } catch (Exception ex) {
                respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), ctx.request());
              }
            });
  }

  private void bulkDeleteLocks(RoutingContext ctx, JsonArray locks) {
    eventBus.request(
        redisquesAddress,
        buildBulkDeleteLocksOperation(locks),
        (Handler<AsyncResult<Message<JsonObject>>>)
            reply -> {
              if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                JsonObject result = new JsonObject();
                result.put(DELETED, reply.result().body().getLong(VALUE));
                jsonResponse(ctx.response(), result);
              } else {
                String errorType = reply.result().body().getString(ERROR_TYPE);
                if (BAD_INPUT.equalsIgnoreCase(errorType)) {
                  respondWith(
                      StatusCode.BAD_REQUEST,
                      reply.result().body().getString(MESSAGE),
                      ctx.request());
                } else {
                  respondWith(
                      StatusCode.INTERNAL_SERVER_ERROR, "Error bulk deleting locks", ctx.request());
                }
              }
            });
  }

  private void bulkPutLocks(RoutingContext ctx, JsonArray locks) {
    eventBus.request(
        redisquesAddress,
        buildBulkPutLocksOperation(locks, extractUser(ctx.request())),
        (Handler<AsyncResult<Message<JsonObject>>>)
            reply -> {
              if (reply.failed()) {
                log.warn("Problem while bulkPutLocks", reply.cause());
                // Continue, only to keep backward compatibility.
              }
              if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                respondWith(StatusCode.OK, ctx.request());
              } else {
                final JsonObject body = reply.result().body();
                final String errorType = body.getString(ERROR_TYPE);
                if (BAD_INPUT.equalsIgnoreCase(errorType)) {
                  respondWith(StatusCode.BAD_REQUEST, body.getString(MESSAGE), ctx.request());
                } else {
                  respondWith(StatusCode.INTERNAL_SERVER_ERROR, ctx.request());
                }
              }
            });
  }

  private void getQueueItemsCount(RoutingContext ctx) {
    decodedQueueNameOrRespondWithBadRequest(ctx, lastPart(ctx.request().path()))
        .ifPresent(
            queue ->
                eventBus.request(
                    redisquesAddress,
                    buildGetQueueItemsCountOperation(queue),
                    (Handler<AsyncResult<Message<JsonObject>>>)
                        reply -> {
                          if (reply.failed()) {
                            log.warn("Failed to getQueueItemsCount", reply.cause());
                            // Continue, only to keep backward compatibility.
                          }
                          if (reply.succeeded()
                              && OK.equals(reply.result().body().getString(STATUS))) {
                            JsonObject result = new JsonObject();
                            result.put(COUNT, reply.result().body().getLong(VALUE));
                            jsonResponse(ctx.response(), result);
                          } else {
                            respondWith(
                                StatusCode.INTERNAL_SERVER_ERROR,
                                "Error gathering count of active queue items",
                                ctx.request());
                          }
                        }));
  }

  private void getConfiguration(RoutingContext ctx) {
    eventBus.request(
        redisquesAddress,
        buildGetConfigurationOperation(),
        (Handler<AsyncResult<Message<JsonObject>>>)
            reply -> {
              if (reply.failed()) {
                log.warn("Failed to getConfiguration.", reply.cause());
                // Continue, only to keep backward compatibility.
              }
              if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                jsonResponse(ctx.response(), reply.result().body().getJsonObject(VALUE));
              } else {
                String error = "Error gathering configuration";
                log.error(error);
                respondWith(StatusCode.INTERNAL_SERVER_ERROR, error, ctx.request());
              }
            });
  }

  private void setConfiguration(RoutingContext ctx) {
    ctx.request()
        .bodyHandler(
            (Buffer buffer) -> {
              try {
                JsonObject configurationValues = new JsonObject(buffer.toString());
                eventBus.request(
                    redisquesAddress,
                    buildSetConfigurationOperation(configurationValues),
                    (Handler<AsyncResult<Message<JsonObject>>>)
                        reply -> {
                          if (reply.failed()) {
                            log.error("Failed to setConfiguration.", reply.cause());
                            respondWith(
                                StatusCode.INTERNAL_SERVER_ERROR,
                                reply.cause().getMessage(),
                                ctx.request());
                          } else {
                            if (OK.equals(reply.result().body().getString(STATUS))) {
                              respondWith(StatusCode.OK, ctx.request());
                            } else {
                              respondWith(
                                  StatusCode.BAD_REQUEST,
                                  reply.result().body().getString(MESSAGE),
                                  ctx.request());
                            }
                          }
                        });
              } catch (Exception ex) {
                respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), ctx.request());
              }
            });
  }

  private void getMonitorInformation(RoutingContext ctx) {
    final boolean emptyQueues =
        evaluateUrlParameterToBeEmptyOrTrue(EMPTY_QUEUES_PARAM, ctx.request());
    final int limit = extractLimit(ctx);
    String filter = ctx.request().params().get(FILTER);
    eventBus.request(
        redisquesAddress,
        buildGetQueuesItemsCountOperation(filter),
        (Handler<AsyncResult<Message<JsonObject>>>)
            reply -> {
              if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                JsonArray queuesArray = reply.result().body().getJsonArray(QUEUES);
                if (queuesArray != null && !queuesArray.isEmpty()) {
                  List<JsonObject> queuesList = queuesArray.getList();
                  queuesList = sortJsonQueueArrayBySize(queuesList);
                  if (!emptyQueues) {
                    queuesList = filterJsonQueueArrayNotEmpty(queuesList);
                  }
                  if (limit > 0) {
                    queuesList = limitJsonQueueArray(queuesList, limit);
                  }
                  JsonObject resultObject = new JsonObject();
                  resultObject.put(QUEUES, queuesList);
                  jsonResponse(ctx.response(), resultObject);
                } else {
                  // there was no result, we as well return an empty result
                  JsonObject resultObject = new JsonObject();
                  resultObject.put(QUEUES, new JsonArray());
                  jsonResponse(ctx.response(), resultObject);
                }
              } else {
                String error = "Error gathering names of active queues";
                log.error(error);
                respondWith(StatusCode.INTERNAL_SERVER_ERROR, error, ctx.request());
              }
            });
  }

  private void listOrCountQueues(RoutingContext ctx) {
    if (evaluateUrlParameterToBeEmptyOrTrue(COUNT, ctx.request())) {
      getQueuesCount(ctx);
    } else {
      listQueues(ctx);
    }
  }

  private void getQueuesCount(RoutingContext ctx) {
    String filter = ctx.request().params().get(FILTER);
    eventBus.request(
        redisquesAddress,
        buildGetQueuesCountOperation(filter),
        (Handler<AsyncResult<Message<JsonObject>>>)
            reply -> {
              if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                JsonObject result = new JsonObject();
                result.put(COUNT, reply.result().body().getLong(VALUE));
                jsonResponse(ctx.response(), result);
              } else {
                String error =
                    "Error gathering count of active queues. Cause: "
                        + reply.result().body().getString(MESSAGE);
                String errorType = reply.result().body().getString(ERROR_TYPE);
                if (BAD_INPUT.equalsIgnoreCase(errorType)) {
                  respondWith(StatusCode.BAD_REQUEST, error, ctx.request());
                } else {
                  respondWith(
                      StatusCode.INTERNAL_SERVER_ERROR,
                      "Error gathering count of active queues",
                      ctx.request());
                }
              }
            });
  }

  private void listQueues(RoutingContext ctx) {
    String filter = ctx.request().params().get(FILTER);
    eventBus.request(
        redisquesAddress,
        buildGetQueuesOperation(filter),
        (Handler<AsyncResult<Message<JsonObject>>>)
            reply -> {
              if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                jsonResponse(ctx.response(), reply.result().body().getJsonObject(VALUE));
              } else {
                String error =
                    "Unable to list active queues. Cause: "
                        + reply.result().body().getString(MESSAGE);
                String errorType = reply.result().body().getString(ERROR_TYPE);
                if (BAD_INPUT.equalsIgnoreCase(errorType)) {
                  respondWith(StatusCode.BAD_REQUEST, error, ctx.request());
                } else {
                  respondWith(StatusCode.INTERNAL_SERVER_ERROR, error, ctx.request());
                }
              }
            });
  }

  private void listOrCountQueueItems(RoutingContext ctx) {
    if (evaluateUrlParameterToBeEmptyOrTrue(COUNT, ctx.request())) {
      getQueueItemsCount(ctx);
    } else {
      listQueueItems(ctx);
    }
  }

  private void listQueueItems(RoutingContext ctx) {
    decodedQueueNameOrRespondWithBadRequest(ctx, lastPart(ctx.request().path()))
        .ifPresent(
            queue -> {
              String limitParam = null;
              if (ctx.request() != null && ctx.request().params().contains(LIMIT)) {
                limitParam = ctx.request().params().get(LIMIT);
              }
              eventBus.request(
                  redisquesAddress,
                  buildGetQueueItemsOperation(queue, limitParam),
                  (Handler<AsyncResult<Message<JsonObject>>>)
                      reply -> {
                        if (reply.failed()) {
                          log.error(
                              "Received failed message for listQueueItemsOperation", reply.cause());
                          respondWith(StatusCode.INTERNAL_SERVER_ERROR, ctx.request());
                          return;
                        }
                        final JsonObject replyBody = reply.result().body();
                        if (OK.equals(replyBody.getString(STATUS))) {
                          List<Object> list = replyBody.getJsonArray(VALUE).getList();
                          JsonArray items = new JsonArray();
                          for (Object item : list.toArray()) {
                            items.add((String) item);
                          }
                          JsonObject result = new JsonObject().put(queue, items);
                          jsonResponse(ctx.response(), result);
                        } else {
                          ctx.response().setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
                          ctx.response().end(replyBody.getString(MESSAGE));
                          log.warn(
                              "Error in routerMatcher.getWithRegEx. Command = '"
                                  + (replyBody.getString("command") == null
                                      ? "<null>"
                                      : replyBody.getString("command"))
                                  + "'.");
                        }
                      });
            });
  }

  private void addQueueItem(RoutingContext ctx) {
    decodedQueueNameOrRespondWithBadRequest(ctx, part(ctx.request().path(), 1))
        .ifPresent(
            queue ->
                ctx.request()
                    .bodyHandler(
                        buffer -> {
                          try {
                            String strBuffer = encodePayload(buffer.toString());
                            eventBus.request(
                                redisquesAddress,
                                buildAddQueueItemOperation(queue, strBuffer),
                                (Handler<AsyncResult<Message<JsonObject>>>)
                                    reply -> {
                                      if (reply.failed()) {
                                        log.warn(
                                            "Received failed message for addQueueItemOperation. Lets run into NullPointerException now",
                                            reply.cause());
                                        // IMO we should respond with 'HTTP 5xx'. But we don't, to
                                        // keep backward compatibility.
                                        // Nevertheless. Lets run into NullPointerException by
                                        // calling method below.
                                      }
                                      checkReply(
                                          reply.result(), ctx.request(), StatusCode.BAD_REQUEST);
                                    });
                          } catch (Exception ex) {
                            respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), ctx.request());
                          }
                        }));
  }

  private void getSingleQueueItem(RoutingContext ctx) {
    final String requestPath = ctx.request().path();
    decodedQueueNameOrRespondWithBadRequest(
            ctx, lastPart(requestPath.substring(0, requestPath.length() - 2)))
        .ifPresent(
            queue -> {
              final int index = Integer.parseInt(lastPart(requestPath));
              eventBus.request(
                  redisquesAddress,
                  buildGetQueueItemOperation(queue, index),
                  (Handler<AsyncResult<Message<JsonObject>>>)
                      reply -> {
                        if (reply.failed()) {
                          log.warn(
                              "Received failed message for getSingleQueueItemOperation. Lets run into NullPointerException now",
                              reply.cause());
                          // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward
                          // compatibility.
                        }
                        final JsonObject replyBody = reply.result().body();
                        final HttpServerResponse response = ctx.response();
                        if (OK.equals(replyBody.getString(STATUS))) {
                          response.putHeader(CONTENT_TYPE, APPLICATION_JSON);
                          response.end(decode(replyBody.getString(VALUE)));
                        } else {
                          response.setStatusCode(StatusCode.NOT_FOUND.getStatusCode());
                          response.setStatusMessage(StatusCode.NOT_FOUND.getStatusMessage());
                          response.end("Not Found");
                        }
                      });
            });
  }

  private void replaceSingleQueueItem(RoutingContext ctx) {
    final HttpServerRequest request = ctx.request();
    decodedQueueNameOrRespondWithBadRequest(ctx, part(request.path(), 2))
        .ifPresent(
            queue ->
                checkLocked(
                    queue,
                    request,
                    voidEvent -> {
                      final int index = Integer.parseInt(lastPart(request.path()));
                      request.bodyHandler(
                          buffer -> {
                            try {
                              String strBuffer = encodePayload(buffer.toString());
                              eventBus.request(
                                  redisquesAddress,
                                  buildReplaceQueueItemOperation(queue, index, strBuffer),
                                  (Handler<AsyncResult<Message<JsonObject>>>)
                                      reply -> {
                                        if (reply.failed()) {
                                          log.warn(
                                              "Received failed message for replaceSingleQueueItemOperation. Lets run into NullPointerException now",
                                              reply.cause());
                                          // IMO we should respond with 'HTTP 5xx'. But we don't, to
                                          // keep backward compatibility.
                                          // Nevertheless. Lets run into NullPointerException by
                                          // calling method below.
                                        }
                                        checkReply(reply.result(), request, StatusCode.NOT_FOUND);
                                      });
                            } catch (Exception ex) {
                              log.warn(
                                  "Undocumented exception caught while replaceSingleQueueItem. But assume its the clients bad ;)",
                                  ex);
                              respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), request);
                            }
                          });
                    }));
  }

  private void deleteQueueItem(RoutingContext ctx) {
    final HttpServerRequest request = ctx.request();
    decodedQueueNameOrRespondWithBadRequest(ctx, part(request.path(), 2))
        .ifPresent(
            queue -> {
              final int index = Integer.parseInt(lastPart(request.path()));
              checkLocked(
                  queue,
                  request,
                  event ->
                      eventBus.request(
                          redisquesAddress,
                          buildDeleteQueueItemOperation(queue, index),
                          (Handler<AsyncResult<Message<JsonObject>>>)
                              reply -> {
                                if (reply.failed()) {
                                  log.warn(
                                      "Received failed message for deleteQueueItemOperation. Lets run into NullPointerException now",
                                      reply.cause());
                                  // IMO we should respond with 'HTTP 5xx'. But we don't, to keep
                                  // backward compatibility.
                                  // Nevertheless. Lets run into NullPointerException by calling
                                  // method below.
                                }
                                checkReply(reply.result(), request, StatusCode.NOT_FOUND);
                              }));
            });
  }

  private void deleteAllQueueItems(RoutingContext ctx) {
    final HttpServerRequest request = ctx.request();
    boolean unlock = evaluateUrlParameterToBeEmptyOrTrue(UNLOCK_PARAM, request);
    final String queue = lastPart(request.path());
    eventBus.request(
        redisquesAddress,
        buildDeleteAllQueueItemsOperation(queue, unlock),
        (Handler<AsyncResult<Message<JsonObject>>>)
            reply -> {
              if (reply.failed()) {
                log.warn("Received failed message for deleteAllQueueItemsOperation", reply.cause());
                respondWith(
                    StatusCode.INTERNAL_SERVER_ERROR,
                    "Error deleting all queue items of queue " + queue,
                    request);
                return;
              }

              final long deletedQueueCount = reply.result().body().getLong(VALUE);
              if (deletedQueueCount == 0) {
                respondWith(StatusCode.NOT_FOUND, request);
              } else {
                respondWith(StatusCode.OK, request);
              }
            });
  }

  private void bulkDeleteQueues(RoutingContext ctx) {
    final HttpServerRequest request = ctx.request();
    if (evaluateUrlParameterToBeEmptyOrTrue(BULK_DELETE_PARAM, request)) {
      request.bodyHandler(
          buffer -> {
            try {
              Result<JsonArray, String> result =
                  extractNonEmptyJsonArrayFromBody(QUEUES, buffer.toString());
              if (result.isErr()) {
                respondWith(StatusCode.BAD_REQUEST, result.getErr(), request);
                return;
              }
              eventBus.request(
                  redisquesAddress,
                  buildBulkDeleteQueuesOperation(result.getOk()),
                  (Handler<AsyncResult<Message<JsonObject>>>)
                      reply -> {
                        if (reply.failed()) {
                          log.warn(
                              "Failed to bulkDeleteQueues. Lets run into NullPointerException now",
                              reply.cause());
                          // IMO we should respond with 'HTTP 5xx'. But we don't, to keep backward
                          // compatibility.
                          // Nevertheless. Lets run into NullPointerException in else case below
                          // now.
                        }
                        if (reply.succeeded()
                            && OK.equals(reply.result().body().getString(STATUS))) {
                          JsonObject resultObj = new JsonObject();
                          resultObj.put(DELETED, reply.result().body().getLong(VALUE));
                          jsonResponse(ctx.response(), resultObj);
                        } else {
                          final JsonObject body = reply.result().body();
                          String errorType = body.getString(ERROR_TYPE);
                          if (BAD_INPUT.equalsIgnoreCase(errorType)) {
                            if (body.getString(MESSAGE) != null) {
                              respondWith(StatusCode.BAD_REQUEST, body.getString(MESSAGE), request);
                            } else {
                              respondWith(StatusCode.BAD_REQUEST, request);
                            }
                          } else {
                            respondWith(
                                StatusCode.INTERNAL_SERVER_ERROR,
                                "Error bulk deleting queues",
                                request);
                          }
                        }
                      });

            } catch (Exception ex) {
              respondWith(StatusCode.BAD_REQUEST, ex.getMessage(), request);
            }
          });
    } else {
      respondWith(
          StatusCode.BAD_REQUEST,
          "Unsupported operation. Add '"
              + BULK_DELETE_PARAM
              + "' parameter for bulk deleting queues",
          request);
    }
  }

  private void respondWith(
      StatusCode statusCode, String responseMessage, HttpServerRequest request) {
    final HttpServerResponse response = request.response();
    log.info("Responding with status code " + statusCode + " and message: " + responseMessage);
    response.setStatusCode(statusCode.getStatusCode());
    response.setStatusMessage(statusCode.getStatusMessage());
    response.end(responseMessage);
  }

  private void respondWith(StatusCode statusCode, HttpServerRequest request) {
    respondWith(statusCode, statusCode.getStatusMessage(), request);
  }

  private String lastPart(String source) {
    String[] tokens = source.split("/");
    return tokens[tokens.length - 1];
  }

  private String part(String source, int pos) {
    String[] tokens = source.split("/");
    return tokens[tokens.length - pos];
  }

  private void jsonResponse(HttpServerResponse response, JsonObject object) {
    response.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    response.end(object.encode());
  }

  private String extractUser(HttpServerRequest request) {
    String user = request.headers().get(userHeader);
    if (user == null) {
      user = "Unknown";
    }
    return user;
  }

  private void checkLocked(
      String queue, final HttpServerRequest request, final Handler<Void> handler) {
    request.pause();
    eventBus.request(
        redisquesAddress,
        buildGetLockOperation(queue),
        (Handler<AsyncResult<Message<JsonObject>>>)
            reply -> {
              request.resume();
              if (NO_SUCH_LOCK.equals(reply.result().body().getString(STATUS))) {
                final HttpServerResponse response = request.response();
                response.setStatusCode(StatusCode.CONFLICT.getStatusCode());
                response.setStatusMessage("Queue must be locked to perform this operation");
                response.end("Queue must be locked to perform this operation");
              } else {
                handler.handle(null);
              }
            });
  }

  private void checkReply(
      Message<JsonObject> reply, HttpServerRequest request, StatusCode statusCode) {
    final HttpServerResponse response = request.response();
    if (OK.equals(reply.body().getString(STATUS))) {
      response.end();
    } else {
      response.setStatusCode(statusCode.getStatusCode());
      response.setStatusMessage(statusCode.getStatusMessage());
      response.end(statusCode.getStatusMessage());
    }
  }

  private int extractLimit(RoutingContext ctx) {
    String limitParam = ctx.request().params().get(LIMIT);
    try {
      return Integer.parseInt(limitParam);
    } catch (NumberFormatException ex) {
      if (limitParam != null) {
        log.warn("Non-numeric limit parameter value used: " + limitParam);
      }
      return Integer.MAX_VALUE;
    }
  }

  private List<JsonObject> sortJsonQueueArrayBySize(List<JsonObject> queueList) {
    queueList.sort(
        (left, right) ->
            right.getLong(MONITOR_QUEUE_SIZE).compareTo(left.getLong(MONITOR_QUEUE_SIZE)));
    return queueList;
  }

  private List<JsonObject> filterJsonQueueArrayNotEmpty(List<JsonObject> queueList) {
    return queueList.stream()
        .filter(queue -> queue.getLong(MONITOR_QUEUE_SIZE) > 0)
        .collect(Collectors.toList());
  }

  private List<JsonObject> limitJsonQueueArray(List<JsonObject> queueList, int limit) {
    return queueList.stream().limit(limit).collect(Collectors.toList());
  }

  private void getQueuesStatistics(RoutingContext ctx) {
    final boolean emptyQueues =
        evaluateUrlParameterToBeEmptyOrTrue(EMPTY_QUEUES_PARAM, ctx.request());
    final int limit = extractLimit(ctx);
    String filter = ctx.request().params().get(FILTER);
    eventBus.request(
        redisquesAddress,
        buildGetQueuesStatisticsOperation(filter),
        (Handler<AsyncResult<Message<JsonObject>>>)
            reply -> {
              if (reply.succeeded() && OK.equals(reply.result().body().getString(STATUS))) {
                JsonArray queuesArray = reply.result().body().getJsonArray(QUEUES);
                if (queuesArray != null && !queuesArray.isEmpty()) {
                  List<JsonObject> queuesList = queuesArray.getList();
                  queuesList = sortJsonQueueArrayBySize(queuesList);
                  if (!emptyQueues) {
                    queuesList = filterJsonQueueArrayNotEmpty(queuesList);
                  }
                  if (limit > 0) {
                    queuesList = limitJsonQueueArray(queuesList, limit);
                  }
                  JsonObject resultObject = new JsonObject();
                  resultObject.put(QUEUES, queuesList);
                  jsonResponse(ctx.response(), resultObject);
                } else {
                  // there was no result, we as well return an empty result
                  JsonObject resultObject = new JsonObject();
                  resultObject.put(QUEUES, new JsonArray());
                  jsonResponse(ctx.response(), resultObject);
                }
              } else {
                String error = "Error gathering names of active queues";
                log.error(error);
                respondWith(StatusCode.INTERNAL_SERVER_ERROR, error, ctx.request());
              }
            });
  }

  private void getQueuesSpeed(RoutingContext ctx) {
    String filter = ctx.request().params().get(FILTER);
    eventBus.request(
        redisquesAddress,
        buildGetQueuesSpeedOperation(filter),
        (Handler<AsyncResult<Message<JsonObject>>>)
            reply -> {
              if (reply.succeeded()
                  && reply.result().body().getString(RedisQueueAPI.STATISTIC_QUEUE_SPEED) != null) {
                long speed = reply.result().body().getLong(RedisQueueAPI.STATISTIC_QUEUE_SPEED);
                JsonObject resultObject = new JsonObject();
                resultObject.put(RedisQueueAPI.STATISTIC_QUEUE_SPEED, speed);
                resultObject.put(
                    RedisQueueAPI.STATISTIC_QUEUE_SPEED_INTERVAL_UNIT, queueSpeedIntervalSec);
                jsonResponse(ctx.response(), resultObject);
              } else {
                // there was no result, we as well return an empty result
                JsonObject resultObject = new JsonObject();
                resultObject.put(RedisQueueAPI.STATISTIC_QUEUE_SPEED, 0L);
                jsonResponse(ctx.response(), resultObject);
              }
            });
  }
}
