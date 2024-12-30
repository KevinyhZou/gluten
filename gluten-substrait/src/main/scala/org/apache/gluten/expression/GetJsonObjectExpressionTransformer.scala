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
import org.apache.gluten.expression.GetJsonObjectExpressionTransformer.{TAG_GET_JSON_OBJECT_ORIGINAL_PATHS, TAG_GET_JSON_OBJECT_REWRITE}
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import java.util

import scala.collection.JavaConverters._

case class GetJsonObjectExpressionTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: Expression)
  extends ExpressionTransformer {

  override def doTransform(args: Object): ExpressionNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val funcName: String =
      ConverterUtils.makeFuncName(substraitExprName, original.children.map(_.dataType))
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, funcName)
    val childNodes = children.map(_.doTransform(args)).asJava
    val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
    if (original.getTagValue(TAG_GET_JSON_OBJECT_REWRITE).isDefined) {
      val originalPaths = original.getTagValue(TAG_GET_JSON_OBJECT_ORIGINAL_PATHS)
      val exprOptions = new java.util.HashMap[String, java.util.List[String]]()
      val rewriteOptions = exprOptions.computeIfAbsent(
        TAG_GET_JSON_OBJECT_REWRITE.name,
        x => new java.util.ArrayList[String]())
      val originalPathOptions = exprOptions.computeIfAbsent(
        TAG_GET_JSON_OBJECT_ORIGINAL_PATHS.name,
        x => new util.ArrayList[String]())
      rewriteOptions.add(String.valueOf(true))
      if (originalPaths.isDefined) {
        originalPaths.get.foreach(p => originalPathOptions.add(p))
      }
      ExpressionBuilder.makeScalarFunction(functionId, childNodes, typeNode, exprOptions)
    } else {
      super.doTransform(args)
    }
  }
}

object GetJsonObjectExpressionTransformer {
  val TAG_GET_JSON_OBJECT_REWRITE: TreeNodeTag[Boolean] =
    TreeNodeTag[Boolean]("getJsonObjectRewrite")
  val TAG_GET_JSON_OBJECT_ORIGINAL_PATHS: TreeNodeTag[Seq[String]] =
    TreeNodeTag[Seq[String]]("getJsonObjectOriginalPaths")
}
