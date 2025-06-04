package org.apache.gluten.table.runtime.plan;

import java.io.Serializable;

public interface SupportsPlanChaining extends Serializable {

  default void setPlanChainingHandler(PlanChainingHandler handler) {}

  default void apply(PlanEvent plan) {}
}