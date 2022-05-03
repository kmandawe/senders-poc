package com.cheetahdigital.senderspoc.db;

import com.cheetahdigital.senderspoc.common.config.BrokerConfig;
import io.vertx.core.Vertx;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;

public class DBPools {

  public static Pool createMySQLPool(
      final BrokerConfig configuration, final Vertx vertx, int maxSize) {
    final var connectionOptions =
        new MySQLConnectOptions()
            .setHost(configuration.getDb().getHost())
            .setPort(configuration.getDb().getPort())
            .setDatabase(configuration.getDb().getDatabase())
            .setUser(configuration.getDb().getUser())
            .setPassword(configuration.getDb().getPassword());

    final var poolOptions = new PoolOptions().setMaxSize(maxSize);

    return MySQLPool.pool(vertx, connectionOptions, poolOptions);
  }
}
