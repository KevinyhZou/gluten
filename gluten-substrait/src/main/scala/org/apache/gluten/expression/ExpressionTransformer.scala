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

import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.DataType

import java.util

import scala.collection.JavaConverters._

// ==== Expression transformer basic interface start ====

trait ExpressionTransformer {
  def substraitExprName: String
  def children: Seq[ExpressionTransformer]
  def original: Expression
  def dataType: DataType = original.dataType
  def nullable: Boolean = original.nullable

  def doTransform(args: java.lang.Object): ExpressionNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    // TODO: the funcName seems can be simplified to `substraitExprName`
    val funcName: String =
      ConverterUtils.makeFuncName(substraitExprName, original.children.map(_.dataType))
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, funcName)
    val childNodes = children.map(_.doTransform(args)).asJava
    val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childNodes, typeNode)
  }
}

trait LeafExpressionTransformer extends ExpressionTransformer {
  final override def children: Seq[ExpressionTransformer] = Nil
}

trait UnaryExpressionTransformer extends ExpressionTransformer {
  def child: ExpressionTransformer
  final override def children: Seq[ExpressionTransformer] = child :: Nil
}

trait BinaryExpressionTransformer extends ExpressionTransformer {
  def left: ExpressionTransformer
  def right: ExpressionTransformer
  final override def children: Seq[ExpressionTransformer] = left :: right :: Nil
}

// ==== Expression transformer basic interface end ====

case class GenericExpressionTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: Expression)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: Object): ExpressionNode = {
    val OPT_BY_CH_OBJECT_TYPE: TreeNodeTag[Boolean] = TreeNodeTag[Boolean]("opt_by_ch_object_type")
    original.getTagValue(OPT_BY_CH_OBJECT_TYPE) match {
      case Some(p) if p =>
        logInfo("set options here")
        val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
        val funcName: String =
          ConverterUtils.makeFuncName(substraitExprName, original.children.map(_.dataType))
        val functionId = ExpressionBuilder.newScalarFunction(functionMap, funcName)
        val childNodes = children.map(_.doTransform(args)).asJava
        val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
        val options = new util.HashMap[String, String]()
        options.put(OPT_BY_CH_OBJECT_TYPE.name, "true")
        ExpressionBuilder.makeScalarFunction(functionId, childNodes, typeNode, options)
      case None =>
        super.doTransform(args)
    }
  }
}

object GenericExpressionTransformer {
  def apply(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): GenericExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, child :: Nil, original)
  }
}

case class LiteralTransformer(original: Literal) extends LeafExpressionTransformer {
  override def substraitExprName: String = "literal"
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    ExpressionBuilder.makeLiteral(original.value, original.dataType, original.nullable)
  }
}
object LiteralTransformer {
  def apply(v: Any): LiteralTransformer = {
    LiteralTransformer(Literal(v))
  }
}
