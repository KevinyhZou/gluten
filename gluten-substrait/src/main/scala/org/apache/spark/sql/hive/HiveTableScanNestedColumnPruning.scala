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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{FilterExec, LeafExecNode, ProjectExec, SparkPlan}
import org.apache.spark.sql.hive.HiveTableScanExecTransformer.TEXT_INPUT_FORMAT_CLASS
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.util.SchemaUtils._
import org.apache.spark.util.Utils

object HiveTableScanNestedColumnPruning extends Logging {
  import org.apache.spark.sql.catalyst.expressions.SchemaPruning._

  def supportNestedColumnPruning(projectExec: ProjectExec): Boolean = {
    if (BackendsApiManager.getSparkPlanExecApiInstance.supportHiveTableScanNestedColumnPruning()) {
      projectExec.child match {
        case HiveTableScanExecTransformer(_, relation, _, _) =>
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

  def apply(plan: SparkPlan): SparkPlan = {
    plan match {
      case ProjectExec(projectList, child) =>
        child match {
          case h: HiveTableScanExecTransformer =>
            val newPlan = prunePhysicalColumns(
              h.relation,
              projectList,
              Seq.empty[Expression],
              (prunedDataSchema, prunedMetadataSchema) => {
                buildNewHiveTableScan(h, prunedDataSchema, prunedMetadataSchema)
              }
            )
            if (newPlan.nonEmpty) {
              return newPlan.get
            }
          case _ =>
        }
      case _ =>
    }
    plan
  }

  private def prunePhysicalColumns(
      relation: HiveTableRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      leafNodeBuilder: (StructType, StructType) => LeafExecNode): Option[SparkPlan] = {
    val (normalizedProjects, normalizedFilters) =
      normalizeAttributeRefNames(relation.output, projects, filters)
    val requestedRootFields = identifyRootFields(normalizedProjects, normalizedFilters)
    // If requestedRootFields includes a nested field, continue. Otherwise,
    // return op
    if (requestedRootFields.exists { root: RootField => !root.derivedFromAtt }) {
      val prunedDataSchema = pruneSchema(relation.tableMeta.dataSchema, requestedRootFields)
      val metaFieldNames = relation.tableMeta.schema.fieldNames
      val metadataSchema = relation.output.collect {
        case attr: AttributeReference if metaFieldNames.contains(attr.name) => attr
      }.toStructType
      val prunedMetadataSchema = if (metadataSchema.nonEmpty) {
        pruneSchema(metadataSchema, requestedRootFields)
      } else {
        metadataSchema
      }
      // If the data schema is different from the pruned data schema
      // OR
      // the metadata schema is different from the pruned metadata schema, continue.
      // Otherwise, return None.
      if (
        countLeaves(relation.tableMeta.dataSchema) > countLeaves(prunedDataSchema) ||
        countLeaves(metadataSchema) > countLeaves(prunedMetadataSchema)
      ) {
        val leafNode = leafNodeBuilder(prunedDataSchema, prunedMetadataSchema)
        val projectionOverSchema = ProjectionOverSchema(
          prunedDataSchema.merge(prunedMetadataSchema),
          AttributeSet(relation.output))
        Some(
          buildNewProjection(
            projects,
            normalizedProjects,
            normalizedFilters,
            leafNode,
            projectionOverSchema))
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * Normalizes the names of the attribute references in the given projects and filters to reflect
   * the names in the given logical relation. This makes it possible to compare attributes and
   * fields by name. Returns a tuple with the normalized projects and filters, respectively.
   */
  private def normalizeAttributeRefNames(
      output: Seq[AttributeReference],
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): (Seq[NamedExpression], Seq[Expression]) = {
    val normalizedAttNameMap = output.map(att => (att.exprId, att.name)).toMap
    val normalizedProjects = projects
      .map(_.transform {
        case att: AttributeReference if normalizedAttNameMap.contains(att.exprId) =>
          att.withName(normalizedAttNameMap(att.exprId))
      })
      .map { case expr: NamedExpression => expr }
    val normalizedFilters = filters.map(_.transform {
      case att: AttributeReference if normalizedAttNameMap.contains(att.exprId) =>
        att.withName(normalizedAttNameMap(att.exprId))
    })
    (normalizedProjects, normalizedFilters)
  }

  /** Builds the new output [[Project]] Spark SQL operator that has the `leafNode`. */
  private def buildNewProjection(
      projects: Seq[NamedExpression],
      normalizedProjects: Seq[NamedExpression],
      filters: Seq[Expression],
      leafNode: LeafExecNode,
      projectionOverSchema: ProjectionOverSchema): ProjectExec = {
    // Construct a new target for our projection by rewriting and
    // including the original filters where available
    val projectionChild =
      if (filters.nonEmpty) {
        val projectedFilters = filters.map(_.transformDown {
          case projectionOverSchema(expr) => expr
        })
        val newFilterCondition = projectedFilters.reduce(And)
        FilterExec(newFilterCondition, leafNode)
      } else {
        leafNode
      }

    // Construct the new projections of our Project by
    // rewriting the original projections
    val newProjects =
      normalizedProjects.map(_.transformDown { case projectionOverSchema(expr) => expr }).map {
        case expr: NamedExpression => expr
      }

    if (log.isDebugEnabled) {
      logDebug(s"New projects:\n${newProjects.map(_.treeString).mkString("\n")}")
    }
    ProjectExec(restoreOriginalOutputNames(newProjects, projects.map(_.name)), projectionChild)
  }

  private def buildNewHiveTableScan(
      hiveTableScan: HiveTableScanExecTransformer,
      prunedDataSchema: StructType,
      prunedMetadataSchema: StructType): HiveTableScanExecTransformer = {
    val relation = hiveTableScan.relation
    val partitionSchema = relation.tableMeta.partitionSchema
    val prunedBaseSchema = StructType(
      prunedDataSchema.fields.filter(
        f => partitionSchema.fieldNames.contains(f.name, f)) ++ partitionSchema.fields)
    val finalSchema = prunedBaseSchema.merge(prunedMetadataSchema)
    val prunedOutput = getPrunedOutput(relation.output, finalSchema)
    val finalOutput =
      prunedOutput.filter(p => hiveTableScan.requestedAttributes.exists(x => x.name.equals(p.name)))
    HiveTableScanExecTransformer(
      hiveTableScan.requestedAttributes,
      relation,
      hiveTableScan.partitionPruningPred,
      finalOutput)(hiveTableScan.session)
  }

  // Prune the given output to make it consistent with `requiredSchema`.
  private def getPrunedOutput(
      output: Seq[AttributeReference],
      requiredSchema: StructType): Seq[AttributeReference] = {
    // We need to update the data type of the output attributes to use the pruned ones.
    // so that references to the original relation's output are not broken
    val nameAttributeMap = output.map(att => (att.name, att)).toMap
    requiredSchema.toAttributes
      .map {
        case att if nameAttributeMap.contains(att.name) =>
          nameAttributeMap(att.name).withDataType(att.dataType)
        case att => att
      }
  }

  /**
   * Counts the "leaf" fields of the given dataType. Informally, this is the number of fields of
   * non-complex data type in the tree representation of [[DataType]].
   */
  private def countLeaves(dataType: DataType): Int = {
    dataType match {
      case array: ArrayType => countLeaves(array.elementType)
      case map: MapType => countLeaves(map.keyType) + countLeaves(map.valueType)
      case struct: StructType =>
        struct.map(field => countLeaves(field.dataType)).sum
      case _ => 1
    }
  }
}
