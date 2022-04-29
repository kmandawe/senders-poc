package com.cheetahdigital.senderspoc.service.stats;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicLong;

@Setter
@Getter
@Builder
public class SenderJob {
  private String senderId;
  private String status;
  private long startTimeMillis;
  private long endTimeMillis;
  private String message;
  private String errorMessage;
  private AtomicLong membersProcessed;
  private AtomicLong batchToProcess;
  private AtomicLong batchCompleted;

  @Override
  public String toString() {
    return "{"
        + "senderId='"
        + senderId
        + '\''
        + ", status='"
        + status
        + '\''
        + ", startTimeMillis="
        + startTimeMillis
        + ", endTimeMillis="
        + endTimeMillis
        + ", message='"
        + message
        + '\''
        + ", errorMessage='"
        + errorMessage
        + '\''
        + ", membersProcessed="
        + membersProcessed
        + ", completionTime="
        + (endTimeMillis - startTimeMillis)
        + ", batchToProcess="
        + batchToProcess
        + ", batchCompleted="
        + batchCompleted
        + '}';
  }
}
