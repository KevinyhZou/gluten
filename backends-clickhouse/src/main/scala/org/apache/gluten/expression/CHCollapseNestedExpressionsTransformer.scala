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
package org.apache.gluten.expression

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.substrait.expression.{ExpressionNode, ScalarFunctionNode}
import org.apache.gluten.substrait.expression.ExpressionBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType

import java.util

case class CHCollapseNestedExpressionsTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: Expression)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: Object): ExpressionNode = {
    if (canBeOptimized(original)) {
      val functionMap = args.asInstanceOf[util.HashMap[String, java.lang.Long]]
      val newExprNode = doTransform(original, functionMap)
      logDebug("The new expression node: " + newExprNode.toProtobuf)
      newExprNode
    } else {
      super.doTransform(args)
    }
  }

  def getExpressionName(expr: Expression): Option[String] = expr match {
    case _: And => ExpressionMappings.expressionsMap.get(classOf[And])
    case _: Or => ExpressionMappings.expressionsMap.get(classOf[Or])
    case _ => Option.empty[String]
  }

  private def canBeOptimized(expr: Expression): Boolean = {
    var exprCall = expr
    expr match {
      case a: Alias => exprCall = a.child
      case _ =>
    }
    val exprName = getExpressionName(exprCall)
    exprName match {
      case None =>
        exprCall match {
          case _: LeafExpression => false
          case _ => exprCall.children.exists(c => canBeOptimized(c))
        }
      case Some(f) =>
        GlutenConfig.get.getSupportedCollapsedExpressions.split(",").exists(c => c.equals(f))
    }
  }

  def doTransform(
      expr: Expression,
      functionMap: util.Map[String, java.lang.Long]): ExpressionNode = {

    var name = Option.empty[String]
    var dataType = null.asInstanceOf[DataType]
    var children = Seq.empty[Expression]

    def f(e: Expression, parent: Option[Expression] = Option.empty): Unit = {
      parent match {
        case None =>
          name = getExpressionName(e)
          dataType = e.dataType
          e match {
            case a: And if canBeOptimized(a) =>
              f(a.left, Option.apply(a))
              f(a.right, Option.apply(a))
            case o: Or if canBeOptimized(o) =>
              f(o.left, Option.apply(o))
              f(o.right, Option.apply(o))
            case _ =>
          }
        case Some(_: And) =>
          e match {
            case a: And if canBeOptimized(a) =>
              f(a.left, Option.apply(a))
              f(a.right, Option.apply(a))
            case _ =>
              children +:= e
          }
        case Some(_: Or) =>
          e match {
            case o: Or if canBeOptimized(o) =>
              f(o.left, Option.apply(o))
              f(o.right, Option.apply(o))
            case _ =>
              children +:= e
          }
      }
    }
    f(expr)
    val funcName: String = ConverterUtils.makeFuncName(substraitExprName, children.map(_.dataType))
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, funcName)
    val childNodes = new util.ArrayList[ExpressionNode]()
    children.map(c => doTransform(c, functionMap)).foreach(c => childNodes.add(c))
    val typeNode = ConverterUtils.getTypeNode(dataType, expr.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childNodes, typeNode)
  }
}
