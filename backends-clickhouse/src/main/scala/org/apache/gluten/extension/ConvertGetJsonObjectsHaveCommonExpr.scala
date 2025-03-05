/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.extension

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Expression, GetJsonObject, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.ObjectType

import scala.collection.Map

case class ConvertGetJsonObjectsHaveCommonExpr(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (canBeOptimized(plan)) {
      visitPlan(plan)
    } else {
      plan
    }
  }

  private def canBeOptimized(plan: LogicalPlan): Boolean = {
    var getJsonObjectNumber = 0
    plan match {
      case p: Project =>
        p.projectList.foreach {
          case a: Alias if a.child.isInstanceOf[GetJsonObject] =>
            getJsonObjectNumber += 1
          case _ =>
        }
      case _ =>
    }
    getJsonObjectNumber > 1
  }

  private def visitPlan(plan: LogicalPlan): LogicalPlan = plan.transform {
    case p: Project =>
      val newProjectList = p.projectList.map(c => optimize(c).asInstanceOf[NamedExpression])
      Project(newProjectList, p.child)
    case _ => plan
  }

  private def optimize(expr: Expression): Expression = expr match {
    case a: Alias if a.child.isInstanceOf[GetJsonObject] =>
      val newChild = optimize(a.child)
      a.withNewChildren(Seq.apply(newChild))
    case _ @GetJsonObject(json, path) =>
      val castType = ObjectType(classOf[Map[String, Object]])
      val castExpr = Cast(json, castType)
      val objectElementExpr = GetJsonObject(castExpr, path)
      objectElementExpr
    case _ => expr
  }
}
