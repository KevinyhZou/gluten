package org.apache.gluten.extension

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, GetJsonObject}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec}

case class ConvertGetJsonObjectsHaveCommonExpr(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan) : LogicalPlan = {
    plan
  }

  private def visitPlan(plan: LogicalPlan) : LogicalPlan = plan.transform {
    case p: ProjectExec => plan
    case f: FilterExec => plan
    case _ => plan
  }

  private def optimize(expr: Expression) : Expression = expr match {
    case g @ GetJsonObject(json, path) =>

      g
    case _ => expr
  }

}
