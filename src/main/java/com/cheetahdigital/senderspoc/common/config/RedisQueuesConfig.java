package com.cheetahdigital.senderspoc.common.config;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;

@Value
@Builder
@ToString
public class RedisQueuesConfig {
  boolean httpRequestHandlerEnabled;
  @Builder.Default
  String processorAddress = "redis-queues-processor";
}
