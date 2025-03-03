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

import org.apache.gluten.substrait.expression.{ExpressionNode, ScalarFunctionNode}
import org.apache.gluten.substrait.expression.ExpressionBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType

import java.util
import java.util.stream.Collectors

case class CHCollapseNestedExpressionsTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: Expression)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: Object): ExpressionNode = {
    val exprNode = super.doTransform(args)
    val functionMap = args.asInstanceOf[util.HashMap[String, java.lang.Long]]
    if (canBeOptimized(exprNode, functionMap)) {
      val newExprNode = doTransform(exprNode, functionMap)
      logDebug("The new expression node: " + newExprNode.toProtobuf)
      newExprNode
    } else {
      exprNode
    }
  }

  private def getExpressionName(
      expr: Option[ExpressionNode],
      functionMap: util.Map[String, java.lang.Long]): Option[String] = expr match {
    case Some(s: ScalarFunctionNode) =>
      var exprName = Option.empty[String]
      val functionId = s.getFunctionId
      val newFunctionMap = new util.HashMap[java.lang.Long, String]()
      functionMap.entrySet().stream().forEach(x => newFunctionMap.put(x.getValue, x.getKey))
      val functionName = newFunctionMap.getOrDefault(functionId, null)
      if (functionName == null) {
        return Option.empty[String]
      } else if (functionName.startsWith("and")) {
        exprName = Option.apply("and")
      } else if (functionName.startsWith("or")) {
        exprName = Option.apply("or")
      }
      exprName
    case _ => Option.empty[String]
  }

  private def canBeOptimized(
      expr: ExpressionNode,
      functionMap: util.Map[String, java.lang.Long]): Boolean = expr match {
    case s: ScalarFunctionNode =>
      val exprName = getExpressionName(Option.apply(s), functionMap)
      exprName match {
        case Some(name) => CollapsedExpressionMappings.supported(name)
        case _ if !s.getExpressionNodes.isEmpty =>
          s.getExpressionNodes.stream().anyMatch(x => canBeOptimized(x, functionMap))
        case _ => false
      }
    case _ => false
  }

  private def doTransform(
      expr: ExpressionNode,
      functionMap: java.util.HashMap[String, java.lang.Long]): ExpressionNode = expr match {
    case s: ScalarFunctionNode =>
      var resultExpr = expr
      var name = getExpressionName(Option.apply(s), functionMap)
      var children = Seq.empty[ExpressionNode]
      var dataType = s.getTypeNode
      def f(
          e: ExpressionNode,
          parent: Option[ExpressionNode] = Option.empty[ExpressionNode]): Unit = {
        parent match {
          case None if e.isInstanceOf[ScalarFunctionNode] =>
            name = getExpressionName(Option.apply(e), functionMap)
            dataType = e.asInstanceOf[ScalarFunctionNode].getTypeNode
          case _ =>
        }
        getExpressionName(Option.apply(e), functionMap) match {
          case Some("and") if canBeOptimized(e, functionMap) =>
            getExpressionName(parent, functionMap) match {
              case Some("and") | None =>
                val childNodes = e.asInstanceOf[ScalarFunctionNode].getExpressionNodes
                childNodes.forEach(c => f(c, parent = Option.apply(e)))
              case _ =>
                children +:= doTransform(e, functionMap)
            }
          case Some("or") if canBeOptimized(e, functionMap) =>
            getExpressionName(parent, functionMap) match {
              case Some("or") | None =>
                val childNodes = e.asInstanceOf[ScalarFunctionNode].getExpressionNodes
                childNodes.forEach(c => f(c, parent = Option.apply(e)))
              case _ =>
                children +:= doTransform(e, functionMap)
            }
          case _ =>
            if (parent.nonEmpty || !e.isInstanceOf[ScalarFunctionNode]) {
              children +:= doTransform(e, functionMap)
            } else {
              val s = e.asInstanceOf[ScalarFunctionNode]
              children = Seq.empty[ExpressionNode]
              val exprNewChildren = s.getExpressionNodes
                .stream()
                .map[ExpressionNode](p => doTransform(p, functionMap))
                .collect(Collectors.toList[ExpressionNode])
              resultExpr = ExpressionBuilder.makeScalarFunction(
                s.getFunctionId,
                exprNewChildren,
                s.getTypeNode)
            }
        }
      }
      f(expr)
      if (name.isDefined) {
        val childrenList = new util.ArrayList[ExpressionNode]()
        var childrenDataTypes = Seq.empty[DataType]
        children.foreach {
          case s: ScalarFunctionNode =>
            childrenList.add(s)
            childrenDataTypes +:= ConverterUtils.parseFromSubstraitType(s.getTypeNode.toProtobuf)._1
          case x => childrenList.add(x)
        }
        val funcName: String =
          ConverterUtils.makeFuncName(substraitExprName, childrenDataTypes)
        logInfo("funcName:" + funcName)
        val functionId = ExpressionBuilder.newScalarFunction(functionMap, funcName)
        ExpressionBuilder.makeScalarFunction(functionId, childrenList, dataType)
      } else {
        resultExpr
      }
    case _ => expr
  }
}
