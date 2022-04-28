package com.cheetahdigital.senderspoc.common.config;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;

@Value
@Builder
@ToString
public class SegmentationConfig {
  Integer instances;
}
