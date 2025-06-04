package org.apache.gluten.table.runtime.plan;

import java.io.Serializable;

public class PlanEvent implements Serializable {
  private String operatorId;
  private String planNode;
  private String sourceConnetSplit;

  public PlanEvent(String operatorId, String planNode, String sourceConnString) {
    this.operatorId = operatorId;
    this.planNode = planNode;
    this.sourceConnetSplit = sourceConnString;
  }

  public String getOperatorId() {
    return this.operatorId;
  }

  public String getPlanNode() {
    return planNode;
  }

  public String getSourceConnectSplit() {
    return sourceConnetSplit;
  }
}