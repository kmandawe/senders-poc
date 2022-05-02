package com.cheetahdigital.senderspoc.db;

import com.cheetahdigital.senderspoc.common.config.DbConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class FlyWayMigration {

  public static Future<Void> migrate(final Vertx vertx, final DbConfig dbConfig) {
    log.debug("DB Config {}", dbConfig);
    return vertx
        .<Void>executeBlocking(
            promise -> {
              // Flyway migration is blocking => uses JDBC
              execute(dbConfig);
              promise.complete();
            })
        .onFailure(err -> log.error("Failed to migrate db schema with error: ", err));
  }

  private static void execute(DbConfig dbConfig) {
    var database = "mysql";
    final String jdbcUrl =
        String.format(
            "jdbc:%s://%s:%d/%s",
            database, dbConfig.getHost(), dbConfig.getPort(), dbConfig.getDatabase());
    log.debug("Migrating DB schema using jdbc url: {}", jdbcUrl);

    final Flyway flyway =
        Flyway.configure()
            .dataSource(jdbcUrl, dbConfig.getUser(), dbConfig.getPassword())
            .schemas(dbConfig.getDatabase())
            .defaultSchema(dbConfig.getDatabase())
            .load();

    var current = Optional.ofNullable(flyway.info().current());
    current.ifPresent(info -> log.info("db schema is at version {}", info.getVersion()));

    var pendingMigrations = flyway.info().pending();
    log.debug("Pending migrations are: {}", printMigrations(pendingMigrations));

    flyway.migrate();
  }

  private static String printMigrations(MigrationInfo[] pendingMigrations) {
    if (Objects.isNull(pendingMigrations)) {
      return "[]";
    }
    return Arrays.stream(pendingMigrations)
        .map(each -> each.getVersion() + " - " + each.getDescription())
        .collect(Collectors.joining(",", "[", "]"));
  }
}
