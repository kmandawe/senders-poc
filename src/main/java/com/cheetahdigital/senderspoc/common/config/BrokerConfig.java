package com.cheetahdigital.senderspoc.common.config;

import io.vertx.core.json.JsonObject;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import lombok.val;

import java.util.Objects;
import java.util.Optional;

import static com.cheetahdigital.senderspoc.common.config.ConfigLoader.*;

@Builder
@Value
@ToString
public class BrokerConfig {
  ServerConfig server;
  DbConfig db;
  RedisQueuesConfig redisQueues;
  String version;
  SegmentationConfig segmentation;
  AttributesCalculationConfig attributesCalculation;
  MemberFunctionsConfig memberFunctions;
  MembersSummaryConfig membersSummary;

  public static BrokerConfig from(final JsonObject config) {
    final String version = config.getString("version");
    if (Objects.isNull(version)) {
      throw new RuntimeException("version is not configured in config file!");
    }
    return BrokerConfig.builder()
        .server(parseServerConfig(config))
        .db(parseDbConfig(config))
        .version(version)
        .redisQueues(parseRedisQueuesConfig(config))
        .segmentation(parseSegmentationConfig(config))
        .attributesCalculation(parseAttributesCalculationConfig(config))
        .memberFunctions(parseMemberFunctionsConfig(config))
        .membersSummary(parseMembersSummaryConfig(config))
        .build();
  }

  private static ServerConfig parseServerConfig(final JsonObject config) {
    final Integer portProperties =
        config.getJsonObject("server", config) != null
            ? config.getJsonObject(SERVER).getInteger(PORT)
            : null;
    final Integer serverPort =
        Optional.ofNullable(config.getInteger(SERVER_PORT)).orElse(portProperties);
    if (Objects.isNull(serverPort)) {
      throw new RuntimeException(SERVER_PORT + " not configured!");
    }
    return ServerConfig.builder().port(serverPort).build();
  }

  private static RedisQueuesConfig parseRedisQueuesConfig(final JsonObject config) {
    val redisQueuesConfig = config.getJsonObject(REDISQUEUES_CONFIG);
    var httpEnabled = false;
    if (redisQueuesConfig != null) {
      httpEnabled = redisQueuesConfig.getBoolean(REDISQUEUES_HTTP_ENABLED);
    }
    return RedisQueuesConfig.builder()
        .redisHost(redisQueuesConfig.getString(REDISQUEUES_REDISHOST))
        .redisPort(redisQueuesConfig.getInteger(REDISQUEUES_REDISPORT))
        .redisAuth(redisQueuesConfig.getString(REDISQUEUES_REDISAUTH))
        .redisMaxPoolSize(redisQueuesConfig.getInteger(REDISQUEUES_REDISMAXPOOLSIZE))
        .redisMaxWaitSize(redisQueuesConfig.getInteger(REDISQUEUES_REDISMAXWAITSIZE))
        .address(redisQueuesConfig.getString(REDISQUEUES_ADDRESS))
        .httpRequestHandlerEnabled(httpEnabled)
        .processorAddress(redisQueuesConfig.getString(REDISQUEUES_PROCESSOR_ADDRESS))
        .build();
  }

  private static SegmentationConfig parseSegmentationConfig(final JsonObject config) {
    val instancesFromProperties =
        config.getJsonObject(SEGMENTATION_CONFIG) != null
            ? config.getJsonObject(SEGMENTATION_CONFIG).getInteger(INSTANCES)
            : null;
    var instances =
        Optional.ofNullable(config.getInteger(SEGMENTATION_THREADS))
            .orElse(instancesFromProperties);
    if (instances == null) {
      instances = SEGMENTATION_THREADS_DEFAULT;
    }
    return SegmentationConfig.builder().instances(instances).build();
  }

  private static AttributesCalculationConfig parseAttributesCalculationConfig(
      final JsonObject config) {
    val instancesFromProperties =
        config.getJsonObject(ATTRIBUTES_CALCULATION_CONFIG) != null
            ? config.getJsonObject(ATTRIBUTES_CALCULATION_CONFIG).getInteger(INSTANCES)
            : null;
    var instances =
        Optional.ofNullable(config.getInteger(ATTRIBUTES_CALCULATION_THREADS))
            .orElse(instancesFromProperties);
    if (instances == null) {
      instances = ATTRIBUTES_CALCULATION_THREADS_DEFAULT;
    }
    return AttributesCalculationConfig.builder().instances(instances).build();
  }

  private static MemberFunctionsConfig parseMemberFunctionsConfig(final JsonObject config) {
    val instancesFromProperties =
        config.getJsonObject(MEMBER_FUNCTIONS_CONFIG) != null
            ? config.getJsonObject(MEMBER_FUNCTIONS_CONFIG).getInteger(INSTANCES)
            : null;
    var instances =
        Optional.ofNullable(config.getInteger(MEMBER_FUNCTIONS_THREADS))
            .orElse(instancesFromProperties);
    if (instances == null) {
      instances = MEMBER_FUNCTIONS_THREADS_DEFAULT;
    }
    return MemberFunctionsConfig.builder().instances(instances).build();
  }

  private static MembersSummaryConfig parseMembersSummaryConfig(final JsonObject config) {
    val instancesFromProperties =
        config.getJsonObject(MEMBERS_SUMMARY_CONFIG) != null
            ? config.getJsonObject(MEMBERS_SUMMARY_CONFIG).getInteger(INSTANCES)
            : null;
    var instances =
        Optional.ofNullable(config.getInteger(MEMBERS_SUMMARY_THREADS))
            .orElse(instancesFromProperties);
    if (instances == null) {
      instances = MEMBERS_SUMMARY_THREADS_DEFAULT;
    }
    return MembersSummaryConfig.builder().instances(instances).build();
  }

  private static DbConfig parseDbConfig(final JsonObject config) {
    final String dbHostProperties =
        config.getJsonObject(DB, config) != null ? config.getJsonObject(DB).getString(HOST) : null;
    final String dbHost = Optional.ofNullable(config.getString(DB_HOST)).orElse(dbHostProperties);

    final Integer dbPortProperties =
        config.getJsonObject(DB, config) != null ? config.getJsonObject(DB).getInteger(PORT) : null;
    final Integer dbPort = Optional.ofNullable(config.getInteger(DB_PORT)).orElse(dbPortProperties);

    final String dbDatabaseProperties =
        config.getJsonObject(DB, config) != null
            ? config.getJsonObject(DB).getString(DATABASE)
            : null;
    final String dbDatabase =
        Optional.ofNullable(config.getString(DB_DATABASE)).orElse(dbDatabaseProperties);

    final String dbUserProperties =
        config.getJsonObject(DB, config) != null ? config.getJsonObject(DB).getString(USER) : null;
    final String dbUser = Optional.ofNullable(config.getString(DB_USER)).orElse(dbUserProperties);

    final String dbPasswordProperties =
        config.getJsonObject(DB, config) != null
            ? config.getJsonObject(DB).getString(PASSWORD)
            : null;
    final String dbPassword =
        Optional.ofNullable(config.getString(DB_PASSWORD)).orElse(dbPasswordProperties);

    return DbConfig.builder()
        .host(dbHost)
        .port(dbPort)
        .database(dbDatabase)
        .user(dbUser)
        .password(dbPassword)
        .build();
  }
}
