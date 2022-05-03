package com.cheetahdigital.senderspoc.common.config;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class ConfigLoader {

  public static final String SEGMENTATION_THREADS = "SEGMENTATION_THREADS";
  public static final Integer SEGMENTATION_THREADS_DEFAULT = 30;
  public static final String ATTRIBUTES_CALCULATION_THREADS = "ATTRIBUTES_CALCULATION_THREADS";
  public static final Integer ATTRIBUTES_CALCULATION_THREADS_DEFAULT = 10;
  public static final String MEMBER_FUNCTIONS_THREADS = "MEMBER_FUNCTIONS_THREADS";
  public static final Integer MEMBER_FUNCTIONS_THREADS_DEFAULT = 20;
  public static final String MEMBERS_SUMMARY_THREADS = "MEMBERS_SUMMARY_THREADS";
  public static final Integer MEMBERS_SUMMARY_THREADS_DEFAULT = 20;
  public static final String CONFIG_FILE = "application.yml";
  public static final String INSTANCES = "instances";
  public static final String SERVER = "server";
  public static final String PORT = "port";

  /***** Database Configuration *****/
  public static final String DATABASE = "database";
  public static final String DB = "db";
  public static final String DB_CONFIG = "db";
  public static final String DB_DATABASE = "DB_DATABASE";
  public static final String DB_HOST = "DB_HOST";
  public static final String DB_PASSWORD = "DB_PASSWORD";
  public static final String DB_PORT = "DB_PORT";
  public static final String DB_USER = "DB_USER";
  public static final String HOST = "host";
  public static final String PASSWORD = "password";
  public static final String USER = "user";

  /***** Server Configuration *****/
  public static final String SERVER_PORT = "SERVER_PORT";

  static final List<String> EXPOSED_ENVIRONMENT_VARIABLES =
      Arrays.asList(DB_DATABASE, DB_HOST, DB_PASSWORD, DB_PORT, DB_USER, SERVER_PORT);

  /***** RedisQueues Configuration *****/
  public static final String REDISQUEUES_CONFIG = "redisQueues";

  public static final String REDISQUEUES_ADDRESS = "address";
  public static final String REDISQUEUES_REDISHOST = "redisHost";
  public static final String REDISQUEUES_REDISPORT = "redisPort";
  public static final String REDISQUEUES_REDISAUTH = "redisAuth";
  public static final String REDISQUEUES_REDISMAXPOOLSIZE = "redisMaxPoolSize";
  public static final String REDISQUEUES_REDISMAXWAITSIZE = "redisMaxWaitSize";
  public static final String REDISQUEUES_HTTP_ENABLED = "httpRequestHandlerEnabled";
  public static final String REDISQUEUES_PROCESSOR_ADDRESS = "processorAddress";

  /***** Segmentation Configuration *****/
  public static final String SEGMENTATION_CONFIG = "segmentation";

  /***** Attributes Calculation Configuration *****/
  public static final String ATTRIBUTES_CALCULATION_CONFIG = "attributesCalculation";

  /***** Member Functions Configuration *****/
  public static final String MEMBER_FUNCTIONS_CONFIG = "memberFunctions";

  /***** Members Summary Configuration *****/
  public static final String MEMBERS_SUMMARY_CONFIG = "membersSummary";

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
