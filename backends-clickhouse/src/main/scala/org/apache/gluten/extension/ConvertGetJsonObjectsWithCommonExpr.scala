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

import org.apache.gluten.extension.ConvertGetJsonObjectsWithCommonExpr.OPT_BY_CH_OBJECT_TYPE

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types._

case class ConvertGetJsonObjectsWithCommonExpr(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (canBeOptimized(plan)) {
      visitPlan(plan)
    } else {
      plan
    }
  }

  private def canBeOptimized(plan: LogicalPlan): Boolean = {
    var getJsonObjectNumber = 0
    var commonExpr = null.asInstanceOf[Expression]
    plan match {
      case p: Project =>
        p.projectList.foreach {
          case a: Alias =>
            a.child match {
              case g @ GetJsonObject(j, _) =>
                if (commonExpr == null) {
                  commonExpr = j
                  getJsonObjectNumber = 1
                } else if (commonExpr.eq(j)) {
                  getJsonObjectNumber += 1
                }
              case _ =>
            }
          case _ =>
        }
      case _ =>
    }
    getJsonObjectNumber > 1
  }

  private def visitPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case p: Project =>
      val projectsWithCommonExpr = commonExpressionByTryCast(p.projectList)
      optimizeByCommonExpression(projectsWithCommonExpr, p) match {
        case Some(a) => a
        case None => p
      }
    case _ => plan
  }

  private def optimizeByCommonExpression(
      projectsWithCommonExpr: Seq[Expression],
      p: LogicalPlan): Option[LogicalPlan] = {
    var commonExpr = null.asInstanceOf[Expression]
    var commonExprNumber = 0

    def f(e: Expression): Unit = e match {
      case a: Alias if a.child.isInstanceOf[GetJsonObject] =>
        f(a.child)
      case g @ GetJsonObject(j, _) =>
        if (commonExpr == null) {
          commonExpr = j
          commonExprNumber = 1
        } else if (commonExpr.eq(j)) {
          commonExprNumber += 1
        }
      case _ =>
    }
    projectsWithCommonExpr.foreach(p => f(p))
    if (commonExprNumber > 1) {
      val alias = Alias(commonExpr, commonExpr.toString())()
      val commonExprProject = Project(Seq.apply(alias), p.children.head)
      def f1(e: Expression): Expression = e match {
        case a: Alias if a.child.isInstanceOf[GetJsonObject] =>
          val newChild = f1(a.child)
          a.withNewChildren(Seq.apply(newChild))
        case g: GetJsonObject =>
          g.withNewChildren(Seq.apply(alias.toAttribute, g.path))
      }
      val newProjects = projectsWithCommonExpr.map(e => f1(e).asInstanceOf[NamedExpression])
      Option.apply(Project(newProjects, commonExprProject))
    } else {
      Option.empty
    }
  }

  private def commonExpressionByTryCast(exprs: Seq[NamedExpression]): Seq[NamedExpression] = {
    var castExpr = null.asInstanceOf[TryCast]
    def f(e: Expression): Expression = e match {
      case a: Alias if a.child.isInstanceOf[GetJsonObject] =>
        val newChild = f(a.child)
        a.withNewChildren(Seq.apply(newChild))
      case g @ GetJsonObject(json, path) if !json.isInstanceOf[TryCast] =>
        if (castExpr == null) {
          castExpr = TryCast(json, DataTypes.StringType)
          castExpr.setTagValue(OPT_BY_CH_OBJECT_TYPE, true)
        } else {
          castExpr.withNewChildren(Seq.apply(json))
        }
        val objectElementExpr = GetJsonObject(castExpr, path)
        objectElementExpr.setTagValue(OPT_BY_CH_OBJECT_TYPE, true)
        objectElementExpr
      case _ => e
    }
    exprs.map(c => f(c).asInstanceOf[NamedExpression])
  }
}

object ConvertGetJsonObjectsWithCommonExpr {
  val OPT_BY_CH_OBJECT_TYPE: TreeNodeTag[Boolean] = TreeNodeTag[Boolean]("opt_by_ch_object_type")
}
