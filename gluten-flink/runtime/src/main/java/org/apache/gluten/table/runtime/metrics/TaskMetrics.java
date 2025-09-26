package org.apache.gluten.table.runtime.metrics;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.github.zhztheplayer.velox4j.query.SerialTask;

public class TaskMetrics {

  private final String keyOperatorType = "operatorType";
  private final String sourceOperatorName = "TableScan";
  private final String sinkOperatorName = "TableWrite";
  private final String keyInputRows = "rawInputRows";
  private final String keyInputBytes = "rawInputBytes";
  private long sourceRecordsOut = 0;
  private long sourceBytesOut = 0;
  private long sinkRecordsIn = 0;
  private long sinkBytesIn = 0;

  private static final TaskMetrics instance = new TaskMetrics();

  private TaskMetrics() {}

  public static TaskMetrics getInstance() {
    return instance;
  }

  public long getSourceRecordsOut() {
    return sourceRecordsOut;
  }

  public long getSourceBytesOut() {
    return sourceBytesOut;
  }

  public long getSinkRecordsIn() {
    return sinkRecordsIn;
  }

  public long getSinkBytesIn() {
    return sinkBytesIn;
  }

  public void updateMetrics(SerialTask task, List<String> planIds) {
    for (String planId : planIds) {
      try {
        ObjectNode planStats = task.collectStats().planStats(planId);
        JsonNode jsonNode = planStats.get(keyOperatorType);
        if (jsonNode.asText().equals(sourceOperatorName)) {
          sourceRecordsOut = planStats.get(keyInputRows).asInt();
          sourceBytesOut = planStats.get(keyInputBytes).asInt();
        } else if (jsonNode.asText().equals(sinkOperatorName)) {
          sinkRecordsIn = planStats.get(keyInputRows).asInt();
          sinkBytesIn = planStats.get(keyInputBytes).asInt();
        }
      } catch (Exception e) {

      }
    }
  }
}