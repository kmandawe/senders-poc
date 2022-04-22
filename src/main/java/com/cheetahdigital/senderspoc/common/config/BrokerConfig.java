package com.cheetahdigital.senderspoc.common.config;

import io.vertx.core.json.JsonObject;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

import java.util.Objects;
import java.util.Optional;

import static com.cheetahdigital.senderspoc.common.config.ConfigLoader.*;

@Builder
@Value
@ToString
public class BrokerConfig {
  ServerConfig serverConfig;
  String version;

  public static BrokerConfig from(final JsonObject config) {
    final String version = config.getString("version");
    if (Objects.isNull(version)) {
      throw new RuntimeException("version is not configured in config file!");
    }
    return BrokerConfig.builder().serverConfig(parseServerConfig(config)).version(version).build();
  }

  private static ServerConfig parseServerConfig(final JsonObject config) {
    final Integer portProperties =
        config.getJsonObject("server") != null
            ? config.getJsonObject(SERVER).getInteger(PORT)
            : null;
    final Integer serverPort =
        Optional.ofNullable(config.getInteger(SERVER_PORT)).orElse(portProperties);
    if (Objects.isNull(serverPort)) {
      throw new RuntimeException(SERVER_PORT + " not configured!");
    }
    return ServerConfig.builder().port(serverPort).build();
  }
}
