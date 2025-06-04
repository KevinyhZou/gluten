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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.gluten.rexnode.Utils;
import org.apache.gluten.table.runtime.operators.GlutenChainedOperator;
import org.apache.gluten.table.runtime.operators.GlutenSingleInputOperator;
import org.apache.gluten.table.runtime.plan.PlanChainingHandler;
import org.apache.gluten.table.runtime.plan.SupportsPlanChaining;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.rexnode.RexNodeConverter;

import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.plan.FilterNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.common.SchemaPruning;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.gluten.util.PlanNodeIdGenerator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

/** Gluten Stream {@link ExecNode} for Calc to use {@link GlutenSingleInputOperator}. */
@ExecNodeMetadata(
        name = "stream-exec-calc",
        version = 1,
        producedTransformations = CommonExecCalc.CALC_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecCalc extends CommonExecCalc implements StreamExecNode<RowData> {

    public StreamExecCalc(
            ReadableConfig tableConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecCalc.class),
                ExecNodeContext.newPersistedConfig(StreamExecCalc.class, tableConfig),
                projection,
                condition,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecCalc(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_PROJECTION) List<RexNode> projection,
            @JsonProperty(FIELD_NAME_CONDITION) @Nullable RexNode condition,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                projection,
                condition,
                TableStreamOperator.class,
                true, // retainHeader
                inputProperties,
                outputType,
                description);
    }

    private List<List<Integer>> getProjectionFieldIndex(RexNode node) {
        List<List<Integer>> indexList = new ArrayList<>();
        if (node instanceof RexFieldAccess) {
                RexFieldAccess field = (RexFieldAccess) node;
                int fIndex = field.getField().getIndex();
                List<List<Integer>> fList = getProjectionFieldIndex(field.getReferenceExpr());
                for (List<Integer> f : fList) {
                        List<Integer> newList = new ArrayList<>();
                        newList.addAll(f);
                        newList.add(fIndex);
                        indexList.add(newList);
                }
        } else if (node instanceof RexInputRef) {
                RexInputRef inputRef = (RexInputRef) node;
                List<Integer> l = List.of(inputRef.getIndex());
                indexList.add(l);
        } else if (node instanceof RexCall) {
                RexCall call = (RexCall) node;
                List<RexNode> ops = call.getOperands();
                for (RexNode op : ops) {
                        List<List<Integer>> opIndexes = getProjectionFieldIndex(op);
                        indexList.addAll(opIndexes);
                }
        }
        return indexList;
    }

    private List<RowField> requestedFields(RowType type, List<List<Integer>> projects) {
        List<RowField> requestFields = new ArrayList<>();
        for (int i = 0; i < projects.size(); ++i) {
                Stack<String> fieldNames = new Stack<>();
                List<Integer> project = projects.get(i);
                List<LogicalType> logicalTypes = type.getChildren();
                List<String> childNames = type.getFieldNames();
                LogicalType leafLogicalType = null;
                for (int j = 0; j < project.size(); j++) {
                        int fieldIndex = project.get(j);
                        LogicalType lt = logicalTypes.get(fieldIndex);
                        String name = childNames.get(fieldIndex);
                        fieldNames.push(name);
                        if (j == project.size() - 1) {
                                leafLogicalType = lt;
                        } else {
                                RowType leftRowType = (RowType) lt;
                                logicalTypes = leftRowType.getChildren();
                                childNames = leftRowType.getFieldNames(); 
                        }
                }
                if (leafLogicalType != null && !fieldNames.empty()) {
                        LogicalType lt = leafLogicalType;
                        RowField requestField = null;
                        while (!fieldNames.empty()) {
                                String fieldName = fieldNames.pop();
                                requestField = new RowField(fieldName, lt);
                                RowType fieldRowType = new RowType(List.of(requestField));
                                lt = fieldRowType;
                        }
                        if (requestField != null) {
                                requestFields.add(requestField);
                        }
                }
        }
        return requestFields;
    }


    @SuppressWarnings("unchecked")
    private SourceTransformation<RowData, ?, ?> pushDownProjectionToTableScan(SourceTransformation<RowData, ?, ?> transformation) {
        Source<RowData, ?, ?> source = transformation.getSource();
        TypeInformation<RowData> sourceOutType = null;
        if (source instanceof ResultTypeQueryable) {
                sourceOutType = ((ResultTypeQueryable<RowData>) source).getProducedType();
        }
        DataType newSourceOutType = null;
        if (sourceOutType != null) {
                InternalTypeInfo<RowData> rowTypeInfo = (InternalTypeInfo<RowData>) sourceOutType;
                List<List<Integer>> fieldIndexList = new ArrayList<>();
                for (RexNode p : projection) {
                        fieldIndexList.addAll(getProjectionFieldIndex(p));
                }
                if (condition != null && condition instanceof RexCall) {
                        fieldIndexList.addAll(getProjectionFieldIndex(condition));
                }
                RowType outputSchema = rowTypeInfo.toRowType();
                List<RowField> requestFields = requestedFields(outputSchema, fieldIndexList);
                RowType pruningSchema = SchemaPruning.pruneSchema(outputSchema, requestFields);
                newSourceOutType = TypeConversions.fromLogicalToDataType(pruningSchema);
        }
        if (source instanceof SupportsProjectionPushDown) {
                ((SupportsProjectionPushDown)source).applyProjection(null, newSourceOutType);
        }
        return transformation;
    }

    @Override
    public Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        // --- Begin Gluten-specific code changes ---
        boolean kafkaSource = false;
        if (inputTransform instanceof SourceTransformation) {
                SourceTransformation<RowData, ?, ?> sourceTrans = (SourceTransformation<RowData, ?, ?>) inputTransform;
                inputTransform = pushDownProjectionToTableScan(sourceTrans);
                Class<?> sourceClazz = sourceTrans.getSource().getClass();
                if (sourceClazz.getSimpleName().equals("GlutenKafkaSource")) {
                        kafkaSource = true;
                }
        }
        io.github.zhztheplayer.velox4j.type.RowType inputType =
                (io.github.zhztheplayer.velox4j.type.RowType)
                        LogicalTypeConverter.toVLType(inputEdge.getOutputType());
        List<String> inNames = Utils.getNamesFromRowType(inputEdge.getOutputType());
        PlanNode filter = null;
        if (condition != null) {
            filter = new FilterNode(
                    PlanNodeIdGenerator.newId(),
                    List.of(),
                    RexNodeConverter.toTypedExpr(condition, inNames));
        }
        List<TypedExpr> projectExprs = RexNodeConverter.toTypedExpr(projection, inNames);
        PlanNode project = new ProjectNode(
                PlanNodeIdGenerator.newId(),
                filter == null ? List.of() : List.of(filter),
                Utils.getNamesFromRowType(getOutputType()),
                projectExprs);
        io.github.zhztheplayer.velox4j.type.RowType outputType =
                (io.github.zhztheplayer.velox4j.type.RowType)
                        LogicalTypeConverter.toVLType(getOutputType());
        final GlutenSingleInputOperator calOperator;
        if (kafkaSource) {
                SourceTransformation<RowData, ?, ?> sourceTrans = (SourceTransformation<RowData, ?, ?>) inputTransform;
                Source<RowData, ?, ? > source = sourceTrans.getSource();
                calOperator = new GlutenChainedOperator(project,
                                PlanNodeIdGenerator.newId(),
                                inputType,
                                outputType);
                if (source instanceof SupportsPlanChaining) {
                        ((SupportsPlanChaining) source).setPlanChainingHandler(
                                new PlanChainingHandler((GlutenChainedOperator) calOperator));
                }
        } else {
                calOperator = new GlutenSingleInputOperator(
                                project,
                                PlanNodeIdGenerator.newId(),
                                inputType,
                                outputType);
        }
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                new TransformationMetadata("gluten-calc", "Gluten cal operator"),
                calOperator,
                InternalTypeInfo.of(getOutputType()),
                inputTransform.getParallelism(),
                false);
        // --- End Gluten-specific code changes ---
    }
}
