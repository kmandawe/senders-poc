package com.cheetahdigital.senderspoc.api.sendpipeline;

import io.vertx.ext.web.Router;

public class SendPipelineRestApi {
  public static void attach(Router parent) {
    parent.get("/sendpipeline/:senderId/:batchSize").handler(new GetSendPipelineHandler());
  }
}
