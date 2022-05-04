package com.cheetahdigital.senderspoc.service.stats;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.vertx.core.json.JsonObject;
import lombok.*;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder(toBuilder = true)
@EqualsAndHashCode
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class SenderJob {
  private String jobId;
  private Integer senderId;
  private String status;

  @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
  private LocalDateTime startTime;

  @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
  private LocalDateTime endTime;

  @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
  private LocalDateTime lastUpdateTime;

  private String message;
  private String errorMessage;
  private Long membersProcessed;
  private Long batchToProcess;
  private Long batchCompleted;
  private Long completionTime;

  public Long getCompletionTime() {
    this.completionTime = -1L;
    if (endTime != null) {
      this.completionTime = ChronoUnit.MILLIS.between(startTime, endTime);
    }
    return this.completionTime;
  }

  public JsonObject toJsonObject() {
    return JsonObject.mapFrom(this);
  }

  @Override
  public String toString() {
    var completionTime = -1L;
    if (endTime != null) {
      completionTime = ChronoUnit.MILLIS.between(startTime, endTime);
    }
    return "{"
        + "jobId='"
        + jobId
        + '\''
        + ", senderId='"
        + senderId
        + '\''
        + ", status='"
        + status
        + '\''
        + ", startTime="
        + startTime
        + ", endTimes="
        + endTime
        + ", message='"
        + message
        + '\''
        + ", errorMessage='"
        + errorMessage
        + '\''
        + ", membersProcessed="
        + membersProcessed
        + ", completionTime="
        + (completionTime)
        + ", batchToProcess="
        + batchToProcess
        + ", batchCompleted="
        + batchCompleted
        + '}';
  }
}
