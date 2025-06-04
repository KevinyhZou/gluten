package org.apache.gluten.table.runtime.plan;

import java.io.Serializable;

import com.google.common.eventbus.Subscribe;

public class PlanChainingHandler implements Serializable {

  private final SupportsPlanChaining handler;

  public PlanChainingHandler() {
    this(null);
  }

  public PlanChainingHandler(SupportsPlanChaining handler) {
    this.handler = handler;
  }

  @Subscribe
  public void handle(PlanEvent plan) {
    if (handler != null) {
      handler.apply(plan);
    }
  }
}
