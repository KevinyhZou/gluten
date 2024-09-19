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
package org.apache.spark.sql.hive

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, GetArrayItem, GetMapValue, GetStructField, NamedExpression}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.hive.HiveTableScanExecTransformer.TEXT_INPUT_FORMAT_CLASS
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

object HiveTableScanNestedColumnPruning extends Logging {

  def supportNestedColumnPruning(projectExec: ProjectExec): Boolean = {
    if (BackendsApiManager.getSparkPlanExecApiInstance.supportHiveTableScanNestedColumnPruning()) {
      projectExec.child match {
        case HiveTableScanExecTransformer(_, relation, _, _) =>
          // Only support for hive json format. ORC, Parquet is already supported by `FileSourceScanExec` and hive text format will fallback to valina to execute for nested field right now.
          relation.tableMeta.storage.inputFormat match {
            case Some(inputFormat)
                if TEXT_INPUT_FORMAT_CLASS.isAssignableFrom(Utils.classForName(inputFormat)) =>
              relation.tableMeta.storage.serde match {
                case Some("org.openx.data.jsonserde.JsonSerDe") | Some(
                      "org.apache.hive.hcatalog.data.JsonSerDe") =>
                  return true
                case _ =>
              }
            case _ =>
          }
        case _ =>
      }
    }
    false
  }

  def apply(projectExec: ProjectExec): SparkPlan = {
    val projectPlan = projectExec.child
    val newOutputAttrs = HiveTableScanNestedColumnPruning.genNewScanOutputs(
      projectPlan.output,
      projectExec.projectList)
    val newProjectLists = HiveTableScanNestedColumnPruning
      .genNewProjectExpressions(projectExec.projectList, newOutputAttrs)
    val newProjectPlan = HiveTableScanExecTransformer.apply(projectPlan, newOutputAttrs)
    ProjectExecTransformer(newProjectLists, newProjectPlan)
  }

  private def fieldExists(
      fieldName: String,
      fieldType: DataType,
      subFieldName: String,
      subFieldType: DataType,
      e: Expression): Boolean = {
    e match {
      case gf: GetStructField =>
        if (
          subFieldName != null && (!gf.extractFieldName.equals(subFieldName) || !gf.dataType.equals(
            subFieldType))
        ) {
          false
        } else {
          fieldExists(fieldName, fieldType, null, null, gf.child)
        }
      case GetArrayItem(c, _, _) =>
        fieldExists(fieldName, fieldType, subFieldName, subFieldType, c)
      case GetMapValue(c, _, _) =>
        fieldExists(fieldName, fieldType, subFieldName, subFieldType, c)
      case attr: Attribute =>
        if (fieldType != null) {
          attr.name.equals(fieldName) && attr.dataType.equals(fieldType)
        } else {
          attr.name.equals(fieldName)
        }
      case _ =>
        false
    }
  }

  private def genNewProjectExpressions(
      projectList: Seq[NamedExpression],
      outputs: Seq[Attribute]): Seq[NamedExpression] = {

    def getOrdinal(d: DataType, fieldName: String): Int = {
      d match {
        case st: StructType =>
          for (i <- st.indices) {
            if (st(i).name.equals(fieldName)) {
              return i
            } else {
              val ordinal = getOrdinal(st(i).dataType, fieldName)
              if (ordinal >= 0) {
                return ordinal
              }
            }
          }
        case at: ArrayType =>
          return getOrdinal(at.elementType, fieldName)
        case mt: MapType =>
          return getOrdinal(mt.valueType, fieldName)
        case _ =>
      }
      -1
    }

    def convertExpression(
        e: Expression,
        outputs: Seq[Attribute],
        getRoot: Boolean = false): Expression = {
      e match {
        case ga @ GetArrayItem(c, _, _) =>
          val newChild = convertExpression(c, outputs, getRoot)
          if (getRoot) {
            return newChild
          } else {
            return GetArrayItem(newChild, ga.ordinal, ga.failOnError)
          }
        case gm @ GetMapValue(c, _, _) =>
          val newChild = convertExpression(c, outputs, getRoot)
          if (getRoot) {
            return newChild
          } else {
            return GetMapValue(newChild, gm.key, gm.failOnError)
          }
        case gs @ GetStructField(c, _, _) =>
          val newChild = convertExpression(c, outputs, getRoot)
          if (getRoot) {
            return newChild;
          } else {
            val rootAttr = outputs.head
            rootAttr match {
              case a: Attribute =>
                val ordinal = getOrdinal(a.dataType, gs.extractFieldName)
                if (ordinal >= 0)
                  return GetStructField(newChild, ordinal, gs.name)
                else
                  return GetStructField(newChild, gs.ordinal, gs.name)
              case _ =>
                return GetStructField(newChild, gs.ordinal, gs.name)
            }
          }
        case attr: Attribute =>
          for (o <- outputs) {
            if (
              fieldExists(attr.name, fieldType = null, subFieldName = null, subFieldType = null, o)
            ) {
              val newAttr = attr.withDataType(o.dataType)
              return newAttr
            }
          }
      }
      e
    }

    var newProjectList = Seq.empty[NamedExpression]
    for (expr <- projectList) {
      expr match {
        case alias @ Alias(child, _) =>
          val rootAttr = convertExpression(child, outputs, getRoot = true)
          val newChild = convertExpression(child, Seq.apply(rootAttr.asInstanceOf[Attribute]))
          val newAlias = alias.withNewChildren(Seq.apply(newChild)).asInstanceOf[Alias]
          logDebug("The new generated project expression:" + newAlias.toJSON)
          newProjectList :+= newAlias
        case _ =>
          newProjectList :+= expr
      }
    }
    newProjectList
  }

  private def genNewScanOutputs(
      outputs: Seq[Attribute],
      projectList: Seq[NamedExpression]): Seq[Attribute] = {

    def projectExists(
        attrName: String,
        attrType: DataType,
        fieldName: String,
        fieldType: DataType): Boolean = {
      if (fieldName == null) {
        return false
      }
      var exists = false
      for (p <- projectList) {
        if (exists)
          return true
        else
          p match {
            case Alias(child, _) =>
              exists = fieldExists(attrName, attrType, fieldName, fieldType, child)
            case a: Attribute => exists = a.name.equals(fieldName) && a.dataType.equals(fieldType)
            case _ =>
          }
      }
      exists
    }

    def genOutputType(
        fieldName: String,
        fieldType: DataType,
        subFieldName: String,
        subFieldType: DataType): Option[DataType] = {
      if (subFieldName != null && projectExists(fieldName, fieldType, subFieldName, subFieldType)) {
        Option.apply(subFieldType)
      } else {
        var dt = subFieldType
        if (dt == null) {
          dt = fieldType
        }
        dt match {
          case st: StructType =>
            var structFields = Seq.empty[StructField]
            for (i <- st.fields.indices) {
              val f = st.fields(i)
              val newFieldType = genOutputType(fieldName, fieldType, f.name, f.dataType)
              if (newFieldType.nonEmpty) {
                val structField = StructField(f.name, newFieldType.get)
                structFields :+= structField
              }
            }
            if (structFields.nonEmpty) {
              val newStructType = StructType(structFields.toArray)
              Option.apply(newStructType)
            } else {
              Option.empty[StructType]
            }
          case a: ArrayType =>
            val newElementType =
              genOutputType(fieldName, fieldType, subFieldName = "", a.elementType)
            var newArrayType = Option.empty[ArrayType]
            if (newElementType.nonEmpty) {
              newArrayType = Option.apply(ArrayType(newElementType.get, a.containsNull))
            }
            newArrayType
          case m: MapType =>
            val newValueType = genOutputType(fieldName, fieldType, subFieldName = "", m.valueType)
            var newMapType = Option.empty[MapType]
            if (newValueType.nonEmpty) {
              newMapType = Option.apply(MapType(m.keyType, newValueType.get, m.valueContainsNull))
            }
            newMapType
          case _ =>
            Option.empty[DataType]
        }
      }
    }

    var newOutputs = Seq.empty[Attribute]
    for (outputAttr <- outputs) {
      val newOutputType = genOutputType(
        outputAttr.name,
        outputAttr.dataType,
        subFieldName = null,
        subFieldType = null)
      if (newOutputType.nonEmpty) {
        val newOutputAttr = AttributeReference(outputAttr.name, newOutputType.get)()
        logDebug("The new generated output attribute: " + newOutputAttr.toJSON)
        newOutputs :+= newOutputAttr
      } else {
        val originOutputAttr = AttributeReference(outputAttr.name, outputAttr.dataType)()
        logDebug("Use the original attribute:" + originOutputAttr.toJSON)
        newOutputs :+= originOutputAttr
      }
    }
    newOutputs
  }
}
