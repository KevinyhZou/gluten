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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

abstract class CHCollapsedExpression(children: Seq[Expression] = Seq.empty, name: String = "")
  extends Expression {

  override def toString: String = s"$name(${children.mkString(", ")})"

  override def eval(input: InternalRow): Any = null

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = null

}

case class CHAnd(dataType: DataType, children: Seq[Expression], name: String, nullable: Boolean)
  extends CHCollapsedExpression(children, name) {
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}

case class CHOr(dataType: DataType, children: Seq[Expression], name: String, nullable: Boolean)
  extends CHCollapsedExpression(children, name) {
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}

object CHCollapsedExpression {

  def sigAnd: Sig = Sig[CHAnd]("CHAnd")
  def sigOr: Sig = Sig[CHOr]("CHOr")

  def supported(name: String): Boolean = {
    GlutenConfig.get.getSupportedCollapsedExpressions.split(",").exists(p => p.equals(name))
  }

  def genCollapsedExpression(
      dataType: DataType,
      children: Seq[Expression],
      name: String,
      nullable: Boolean): Option[CHCollapsedExpression] = name match {
    case "and" => Option.apply(CHAnd(dataType, children, name, nullable))
    case "or" => Option.apply(CHOr(dataType, children, name, nullable))
    case _ => Option.empty
  }

}
