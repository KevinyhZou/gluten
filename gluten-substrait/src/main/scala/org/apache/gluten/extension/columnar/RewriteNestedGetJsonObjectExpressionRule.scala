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
package org.apache.gluten.extension.columnar

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.GetJsonObjectExpressionTransformer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StringType

class RewriteNestedGetJsonObjectExpressionRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      plan.resolved
      && GlutenConfig.get.enableGluten
      && GlutenConfig.get.enableRewriteNestedGetJsonObject
    ) {
      visitPlan(plan)
    } else {
      plan
    }
  }

  private def visitPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case p: Project =>
      var newProjectList = Seq.empty[NamedExpression]
      p.projectList.foreach {
        case a: Alias if a.child.isInstanceOf[GetJsonObject] =>
          newProjectList :+= optimizeNestedFunctions(a).asInstanceOf[NamedExpression]
        case p =>
          newProjectList :+= p
      }
      val newChild = visitPlan(p.child)
      Project(newProjectList, newChild)
    case f: Filter =>
      val newCond = optimizeNestedFunctions(f.condition)
      val newChild = visitPlan(f.child)
      Filter(newCond, newChild)
    case other =>
      val children = other.children.map(visitPlan)
      plan.withNewChildren(children)
  }

  private def optimizeNestedFunctions(
      expr: Expression,
      path: String = "",
      isNested: Boolean = false,
      originalPaths: Seq[String] = Seq.empty[String]): Expression = {

    def getPathLiteral(path: Expression): Option[String] = path match {
      case l: Literal if l.dataType.isInstanceOf[StringType] =>
        Option.apply(l.value.toString)
      case _ =>
        Option.empty
    }

    expr match {
      case g: GetJsonObject =>
        var paths = originalPaths
        val gPath = getPathLiteral(g.path).orNull
        paths :+= gPath
        var newPath = ""
        if (gPath != null) {
          newPath = gPath.replace("$", "") + path
        }
        val res = optimizeNestedFunctions(g.json, newPath, isNested = true, originalPaths = paths)
        if (gPath != null) {
          res
        } else {
          var newChildren = Seq.empty[Expression]
          newChildren :+= res
          newChildren :+= g.path
          val newExpr = g.withNewChildren(newChildren)
          if (path.nonEmpty) {
            GetJsonObject(newExpr, Literal.apply("$" + path))
          } else {
            newExpr
          }
        }
      case _ =>
        val newChildren = expr.children.map(x => optimizeNestedFunctions(x, path))
        val newExpr = expr.withNewChildren(newChildren)
        if (isNested && path.nonEmpty) {
          val pathExpr = Literal.apply("$" + path)
          val newGetJsonObjectExpr = GetJsonObject(newExpr, pathExpr)
          if (originalPaths.size > 1) {
            newGetJsonObjectExpr.setTagValue(
              GetJsonObjectExpressionTransformer.TAG_GET_JSON_OBJECT_REWRITE,
              true)
            newGetJsonObjectExpr.setTagValue(
              GetJsonObjectExpressionTransformer.TAG_GET_JSON_OBJECT_ORIGINAL_PATHS,
              originalPaths)
            newGetJsonObjectExpr
          } else {
            newGetJsonObjectExpr
          }
        } else {
          newExpr
        }
    }
  }
}
