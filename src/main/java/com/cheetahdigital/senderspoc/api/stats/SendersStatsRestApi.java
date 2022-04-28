package com.cheetahdigital.senderspoc.api.stats;

import io.vertx.ext.web.Router;

public class SendersStatsRestApi {
  public static void attach(Router parent) {
    parent.get("/senders/stats").handler(new GetSendersStatsHandler());
    parent.get("/senders/stats/reset").handler(new GetSendersStatsResetHandler());
  }
}
