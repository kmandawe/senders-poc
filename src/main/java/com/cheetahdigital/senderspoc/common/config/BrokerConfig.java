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
  RedisQueuesConfig redisQueues;
  String version;

  public static BrokerConfig from(final JsonObject config) {
    final String version = config.getString("version");
    if (Objects.isNull(version)) {
      throw new RuntimeException("version is not configured in config file!");
    }
    return BrokerConfig.builder()
        .server(parseServerConfig(config))
        .version(version)
        .redisQueues(parseRedisQueuesConfig(config))
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
        .address(redisQueuesConfig.getString(REDISQUEUES_ADDRESS))
        .httpRequestHandlerEnabled(httpEnabled)
        .processorAddress(redisQueuesConfig.getString(REDISQUEUES_PROCESSOR_ADDRESS))
        .build();
  }
}
