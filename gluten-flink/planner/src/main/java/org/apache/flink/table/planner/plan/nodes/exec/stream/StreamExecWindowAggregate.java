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

import org.apache.gluten.rexnode.AggregateCallConverter;
import org.apache.gluten.table.runtime.operators.GlutenVectorOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TimeWindowNode;
import io.github.zhztheplayer.velox4j.plan.TimeWindowNode.WindowParameters;
import io.github.zhztheplayer.velox4j.window.WindowFunction;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} for window table-valued based aggregate.
 *
 * <p>The differences between {@link StreamExecWindowAggregate} and {@link
 * StreamExecGroupWindowAggregate} is that, this node is translated from window TVF syntax, but the
 * other is from the legacy GROUP WINDOW FUNCTION syntax. In the long future, {@link
 * StreamExecGroupWindowAggregate} will be dropped.
 */
@ExecNodeMetadata(
    name = "stream-exec-window-aggregate",
    version = 1,
    consumedOptions = "table.local-time-zone",
    producedTransformations = StreamExecWindowAggregate.WINDOW_AGGREGATE_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecWindowAggregate extends StreamExecWindowAggregateBase {
  private static final Logger LOG = LoggerFactory.getLogger(StreamExecWindowAggregate.class);
  public static final String WINDOW_AGGREGATE_TRANSFORMATION = "gluten-window-aggregate";

  private static final long WINDOW_AGG_MEMORY_RATIO = 100;

  public static final String FIELD_NAME_WINDOWING = "windowing";
  public static final String FIELD_NAME_NAMED_WINDOW_PROPERTIES = "namedWindowProperties";

  @JsonProperty(FIELD_NAME_GROUPING)
  private final int[] grouping;

  @JsonProperty(FIELD_NAME_AGG_CALLS)
  private final AggregateCall[] aggCalls;

  @JsonProperty(FIELD_NAME_WINDOWING)
  private final WindowingStrategy windowing;

  @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
  private final NamedWindowProperty[] namedWindowProperties;

  @JsonProperty(FIELD_NAME_NEED_RETRACTION)
  private final boolean needRetraction;

  public StreamExecWindowAggregate(
      ReadableConfig tableConfig,
      int[] grouping,
      AggregateCall[] aggCalls,
      WindowingStrategy windowing,
      NamedWindowProperty[] namedWindowProperties,
      Boolean needRetraction,
      InputProperty inputProperty,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecWindowAggregate.class),
        ExecNodeContext.newPersistedConfig(StreamExecWindowAggregate.class, tableConfig),
        grouping,
        aggCalls,
        windowing,
        namedWindowProperties,
        needRetraction,
        Collections.singletonList(inputProperty),
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecWindowAggregate(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
      @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
      @JsonProperty(FIELD_NAME_WINDOWING) WindowingStrategy windowing,
      @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES) NamedWindowProperty[] namedWindowProperties,
      @Nullable @JsonProperty(FIELD_NAME_NEED_RETRACTION) Boolean needRetraction,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(id, context, persistedConfig, inputProperties, outputType, description);
    this.grouping = checkNotNull(grouping);
    this.aggCalls = checkNotNull(aggCalls);
    this.windowing = checkNotNull(windowing);
    this.namedWindowProperties = checkNotNull(namedWindowProperties);
    this.needRetraction = Optional.ofNullable(needRetraction).orElse(false);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    final ExecEdge inputEdge = getInputEdges().get(0);
    final Transformation<RowData> inputTransform =
        (Transformation<RowData>) inputEdge.translateToPlan(planner);
    final RowType inputRowType = (RowType) inputEdge.getOutputType();
    final RowDataKeySelector selector =
        KeySelectorUtil.getRowDataSelector(
            planner.getFlinkContext().getClassLoader(),
            grouping,
            InternalTypeInfo.of(inputRowType));
    // --- Begin Gluten-sepcific code changes ---
    io.github.zhztheplayer.velox4j.type.RowType inputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(inputRowType);
    io.github.zhztheplayer.velox4j.type.RowType outputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            LogicalTypeConverter.toVLType(getOutputType());
    List<String> inputFieldNames = inputRowType.getFieldNames();
    List<FieldAccessTypedExpr> groupKeys = new ArrayList<>();
    for (int keyIndex : grouping) {
      LogicalType keyType = inputRowType.getTypeAt(keyIndex);
      String keyName = inputFieldNames.get(keyIndex);
      FieldAccessTypedExpr keyField =
          FieldAccessTypedExpr.create(LogicalTypeConverter.toVLType(keyType), keyName);
      groupKeys.add(keyField);
    }
    List<WindowFunction> functions = AggregateCallConverter.toFunctions(aggCalls, inputType);
    List<String> colNames =
        outputType.getNames().stream()
            .skip(grouping.length)
            .limit(aggCalls.length)
            .collect(Collectors.toList());

    TimeWindowNode windowNode =
        new TimeWindowNode(
            PlanNodeIdGenerator.newId(),
            groupKeys,
            colNames,
            functions,
            List.of(new EmptyNode(inputType)),
            getWindowType(windowing.getWindow()),
            getWindowParameters(windowing));

    final OneInputStreamOperator windowOperator =
        new GlutenVectorOneInputOperator(
            new StatefulPlanNode(windowNode.getId(), windowNode),
            PlanNodeIdGenerator.newId(),
            inputType,
            Map.of(windowNode.getId(), outputType));
    // --- End Gluten-specific code changes ---
    final OneInputTransformation<RowData, RowData> transform =
        ExecNodeUtil.createOneInputTransformation(
            inputTransform,
            createTransformationMeta(WINDOW_AGGREGATE_TRANSFORMATION, config),
            SimpleOperatorFactory.of(windowOperator),
            InternalTypeInfo.of(getOutputType()),
            inputTransform.getParallelism(),
            WINDOW_AGG_MEMORY_RATIO,
            false);

    // set KeyType and Selector for state
    transform.setStateKeySelector(selector);
    transform.setStateKeyType(selector.getProducedType());
    return transform;
  }

  private int getWindowType(WindowSpec spec) {
    return spec instanceof TumblingWindowSpec ? 0 : spec instanceof HoppingWindowSpec ? 1 : 2;
  }

  private WindowParameters getWindowParameters(WindowingStrategy strategy) {
    WindowParameters parameters = null;
    boolean isEventTime = strategy.isRowtime();
    int timeFieldIndex = -1;
    if (isEventTime && strategy instanceof TimeAttributeWindowingStrategy) {
      timeFieldIndex = ((TimeAttributeWindowingStrategy) strategy).getTimeAttributeIndex();
    }
    WindowSpec window = strategy.getWindow();
    if (window instanceof TumblingWindowSpec) {
      TumblingWindowSpec spec = (TumblingWindowSpec) window;
      parameters =
          new WindowParameters(
              spec.getSize().getSeconds(),
              spec.getOffset() == null ? 0 : spec.getOffset().getSeconds(),
              -1L,
              -1L,
              isEventTime,
              timeFieldIndex);
    } else if (window instanceof HoppingWindowSpec) {
      HoppingWindowSpec spec = (HoppingWindowSpec) window;
      parameters =
          new WindowParameters(
              spec.getSize().getSeconds(),
              spec.getOffset() == null ? 0 : spec.getOffset().getSeconds(),
              spec.getSlide().getSeconds(),
              -1L,
              isEventTime,
              timeFieldIndex);
    } else if (window instanceof SessionWindowSpec) {
      SessionWindowSpec spec = (SessionWindowSpec) window;
      parameters =
          new WindowParameters(
              -1L, -1L, -1L, spec.getGap().getSeconds(), isEventTime, timeFieldIndex);
    } else {
      throw new FlinkRuntimeException("not supported:" + window.getClass().getName());
    }
    return parameters;
  }
}
