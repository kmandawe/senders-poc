package com.cheetahdigital.senderspoc.api.stats;

import io.vertx.ext.web.Router;

public class SenderStatsRestApi {
  public static void attach(Router parent) {
    parent.get("/senders/stats").handler(new GetSenderStatsHandler());
    parent.get("/senders/stats/reset").handler(new GetSenderStatsResetHandler());
  }
}
