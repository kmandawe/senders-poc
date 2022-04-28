package com.cheetahdigital.senderspoc.common.config;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class ConfigLoader {

  public static final String SEGMENTATION_THREADS = "SEGMENTATION_THREADS";
  public static final Integer SEGMENTATION_THREADS_DEFAULT = 30;
  public static final String SERVER_PORT = "SERVER_PORT";
  public static final String CONFIG_FILE = "application.yml";
  public static final String INSTANCES = "instances";
  public static final String SERVER = "server";
  public static final String PORT = "port";

  static final List<String> EXPOSED_ENVIRONMENT_VARIABLES = Arrays.asList(SERVER_PORT);

  /***** RedisQueues Configuration *****/
  public static final String REDISQUEUES_CONFIG = "redisQueues";
  public static final String REDISQUEUES_ADDRESS = "address";
  public static final String REDISQUEUES_HTTP_ENABLED = "httpRequestHandlerEnabled";
  public static final String REDISQUEUES_PROCESSOR_ADDRESS = "processorAddress";

  /***** Segmentation Configuration *****/
  public static final String SEGMENTATION_CONFIG = "segmentation";

  public static Future<BrokerConfig> load(Vertx vertx) {

    final var exposedKeys = new JsonArray();
    EXPOSED_ENVIRONMENT_VARIABLES.forEach(exposedKeys::add);
    log.debug("Fetch configuration for {}", exposedKeys.encode());

    var envStore =
        new ConfigStoreOptions()
            .setType("env")
            .setConfig(new JsonObject().put("keys", exposedKeys));

    var propertyStore =
        new ConfigStoreOptions().setType("sys").setConfig(new JsonObject().put("cache", false));

    var yamlStore =
        new ConfigStoreOptions()
            .setType("file")
            .setFormat("yaml")
            .setConfig(new JsonObject().put("path", CONFIG_FILE));

    var retriever =
        ConfigRetriever.create(
            vertx,
            new ConfigRetrieverOptions()
                .addStore(yamlStore)
                .addStore(propertyStore)
                .addStore(envStore));
    return retriever.getConfig().map(BrokerConfig::from);
  }
}
