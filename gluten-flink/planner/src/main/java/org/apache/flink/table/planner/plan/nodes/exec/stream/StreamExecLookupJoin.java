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

import org.apache.gluten.rexnode.RexConversionContext;
import org.apache.gluten.rexnode.RexNodeConverter;
import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.table.runtime.operators.GlutenVectorOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;
import org.apache.gluten.util.ReflectUtils;
import org.apache.gluten.velox.IndexLookupJoinBuilder;

import io.github.zhztheplayer.velox4j.connector.Assignment;
import io.github.zhztheplayer.velox4j.connector.ColumnHandle;
import io.github.zhztheplayer.velox4j.connector.ColumnType;
import io.github.zhztheplayer.velox4j.connector.FileSystemColumnHandle;
import io.github.zhztheplayer.velox4j.connector.FileSystemIndexTableHandle;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.join.JoinType;
import io.github.zhztheplayer.velox4j.plan.IndexLookupCondition;
import io.github.zhztheplayer.velox4j.plan.IndexLookupJoinNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil.FieldRefLookupKey;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil.LookupKey;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.sources.CsvTableSource.CsvLookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** {@link StreamExecNode} for temporal table join that implemented by lookup. */
@ExecNodeMetadata(
    name = "stream-exec-lookup-join",
    version = 1,
    producedTransformations = CommonExecLookupJoin.LOOKUP_JOIN_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecLookupJoin extends CommonExecLookupJoin
    implements StreamExecNode<RowData>, MultipleTransformationTranslator<RowData> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamExecLookupJoin.class);
  public static final String FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE = "requireUpsertMaterialize";
  public static final String FIELD_NAME_LOOKUP_KEY_CONTAINS_PRIMARY_KEY =
      "lookupKeyContainsPrimaryKey";

  public static final String STATE_NAME = "lookupJoinState";

  @JsonProperty(FIELD_NAME_LOOKUP_KEY_CONTAINS_PRIMARY_KEY)
  private final boolean lookupKeyContainsPrimaryKey;

  @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE)
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private final boolean upsertMaterialize;

  @Nullable
  @JsonProperty(FIELD_NAME_STATE)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final List<StateMetadata> stateMetadataList;

  public StreamExecLookupJoin(
      ReadableConfig tableConfig,
      FlinkJoinType joinType,
      @Nullable RexNode preFilterCondition,
      @Nullable RexNode remainingJoinCondition,
      TemporalTableSourceSpec temporalTableSourceSpec,
      Map<Integer, LookupJoinUtil.LookupKey> lookupKeys,
      @Nullable List<RexNode> projectionOnTemporalTable,
      @Nullable RexNode filterOnTemporalTable,
      boolean lookupKeyContainsPrimaryKey,
      boolean upsertMaterialize,
      @Nullable LookupJoinUtil.AsyncLookupOptions asyncLookupOptions,
      @Nullable LookupJoinUtil.RetryLookupOptions retryOptions,
      ChangelogMode inputChangelogMode,
      InputProperty inputProperty,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecLookupJoin.class),
        ExecNodeContext.newPersistedConfig(StreamExecLookupJoin.class, tableConfig),
        joinType,
        preFilterCondition,
        remainingJoinCondition,
        temporalTableSourceSpec,
        lookupKeys,
        projectionOnTemporalTable,
        filterOnTemporalTable,
        lookupKeyContainsPrimaryKey,
        upsertMaterialize,
        asyncLookupOptions,
        retryOptions,
        inputChangelogMode,
        // serialize state meta only when upsert materialize is enabled
        upsertMaterialize
            ? StateMetadata.getOneInputOperatorDefaultMeta(tableConfig, STATE_NAME)
            : null,
        Collections.singletonList(inputProperty),
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecLookupJoin(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_JOIN_TYPE) FlinkJoinType joinType,
      @JsonProperty(FIELD_NAME_PRE_FILTER_CONDITION) @Nullable RexNode preFilterCondition,
      @JsonProperty(FIELD_NAME_REMAINING_JOIN_CONDITION) @Nullable RexNode remainingJoinCondition,
      @JsonProperty(FIELD_NAME_TEMPORAL_TABLE) TemporalTableSourceSpec temporalTableSourceSpec,
      @JsonProperty(FIELD_NAME_LOOKUP_KEYS) Map<Integer, LookupJoinUtil.LookupKey> lookupKeys,
      @JsonProperty(FIELD_NAME_PROJECTION_ON_TEMPORAL_TABLE) @Nullable
          List<RexNode> projectionOnTemporalTable,
      @JsonProperty(FIELD_NAME_FILTER_ON_TEMPORAL_TABLE) @Nullable RexNode filterOnTemporalTable,
      @JsonProperty(FIELD_NAME_LOOKUP_KEY_CONTAINS_PRIMARY_KEY) boolean lookupKeyContainsPrimaryKey,
      @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE) boolean upsertMaterialize,
      @JsonProperty(FIELD_NAME_ASYNC_OPTIONS) @Nullable
          LookupJoinUtil.AsyncLookupOptions asyncLookupOptions,
      @JsonProperty(FIELD_NAME_RETRY_OPTIONS) @Nullable
          LookupJoinUtil.RetryLookupOptions retryOptions,
      @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE) @Nullable ChangelogMode inputChangelogMode,
      @JsonProperty(FIELD_NAME_STATE) @Nullable List<StateMetadata> stateMetadataList,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(
        id,
        context,
        persistedConfig,
        joinType,
        preFilterCondition,
        remainingJoinCondition,
        temporalTableSourceSpec,
        lookupKeys,
        projectionOnTemporalTable,
        filterOnTemporalTable,
        asyncLookupOptions,
        retryOptions,
        inputChangelogMode,
        inputProperties,
        outputType,
        description);
    this.lookupKeyContainsPrimaryKey = lookupKeyContainsPrimaryKey;
    this.upsertMaterialize = upsertMaterialize;
    this.stateMetadataList = stateMetadataList;
  }

  @Override
  public Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    return createJoinTransformation(
        planner, config, upsertMaterialize, lookupKeyContainsPrimaryKey);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  protected Transformation<RowData> createJoinTransformation(
      PlannerBase planner,
      ExecNodeConfig config,
      boolean upsertMaterialize,
      boolean lookupKeyContainsPrimaryKey) {
    final TemporalTableSourceSpec temporalTableSourceSpec =
        (TemporalTableSourceSpec)
            ReflectUtils.getObjectField(
                CommonExecLookupJoin.class, this, "temporalTableSourceSpec");
    final Map<Integer, LookupJoinUtil.LookupKey> lookupKeys =
        (Map) ReflectUtils.getObjectField(CommonExecLookupJoin.class, this, "lookupKeys");
    final LookupJoinUtil.AsyncLookupOptions asyncLookupOptions =
        (LookupJoinUtil.AsyncLookupOptions)
            ReflectUtils.getObjectField(CommonExecLookupJoin.class, this, "asyncLookupOptions");
    final LookupJoinUtil.RetryLookupOptions retryOptions =
        (LookupJoinUtil.RetryLookupOptions)
            ReflectUtils.getObjectField(CommonExecLookupJoin.class, this, "retryOptions");
    final FlinkJoinType joinType =
        (FlinkJoinType) ReflectUtils.getObjectField(CommonExecLookupJoin.class, this, "joinType");
    RelOptTable temporalTable =
        temporalTableSourceSpec.getTemporalTable(
            planner.getFlinkContext(), ShortcutUtils.unwrapTypeFactory(planner));
    // validate whether the node is valid and supported.
    ReflectUtils.invokeObjectMethod(
        CommonExecLookupJoin.class,
        this,
        "validate",
        new Class<?>[] {RelOptTable.class},
        new Object[] {temporalTable});
    final ExecEdge inputEdge = getInputEdges().get(0);
    RowType inputRowType = (RowType) inputEdge.getOutputType();
    RowType tableSourceRowType = FlinkTypeFactory.toLogicalRowType(temporalTable.getRowType());
    RowType resultRowType = (RowType) getOutputType();
    validateLookupKeyType(lookupKeys, inputRowType, tableSourceRowType);
    boolean isAsyncEnabled = null != asyncLookupOptions;
    ResultRetryStrategy retryStrategy =
        retryOptions != null ? retryOptions.toRetryStrategy() : null;

    UserDefinedFunction lookupFunction =
        LookupJoinUtil.getLookupFunction(
            temporalTable,
            lookupKeys.keySet(),
            planner.getFlinkContext().getClassLoader(),
            isAsyncEnabled,
            retryStrategy);
    UserDefinedFunctionHelper.prepareInstance(config, lookupFunction);

    // boolean isLeftOuterJoin = joinType == FlinkJoinType.LEFT;
    if (isAsyncEnabled) {
      assert lookupFunction instanceof AsyncTableFunction;
    }
    Transformation<RowData> inputTransformation =
        (Transformation<RowData>) inputEdge.translateToPlan(planner);
    io.github.zhztheplayer.velox4j.type.RowType joinInputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(inputRowType);
    io.github.zhztheplayer.velox4j.type.RowType joinOutputType =
        (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(resultRowType);
    IndexLookupJoinNode joinNode =
        new IndexLookupJoinNode(
            PlanNodeIdGenerator.newId(),
            getVeloxJoinType(joinType),
            getLeftKeys(inputRowType, lookupKeys),
            getRightKeys(tableSourceRowType, lookupKeys),
            getJoinConditions(tableSourceRowType),
            null,
            getLeftTable(inputTransformation),
            getRightTable(lookupFunction, lookupKeys, isAsyncEnabled),
            joinOutputType);
    GlutenVectorOneInputOperator joinOperator =
        new GlutenVectorOneInputOperator(
            new StatefulPlanNode(joinNode.getId(), joinNode),
            PlanNodeIdGenerator.newId(),
            joinInputType,
            Map.of(joinNode.getId(), joinOutputType));
    StreamOperatorFactory<RowData> operatorFactory =
        new GlutenOneInputOperatorFactory(joinOperator);
    return ExecNodeUtil.createOneInputTransformation(
        inputTransformation,
        createTransformationMeta(LOOKUP_JOIN_TRANSFORMATION, config),
        operatorFactory,
        InternalTypeInfo.of(resultRowType),
        inputTransformation.getParallelism(),
        false);
  }

  private JoinType getVeloxJoinType(FlinkJoinType joinType) {
    switch (joinType) {
      case INNER:
        return JoinType.INNER;
      case LEFT:
        return JoinType.LEFT;
      case RIGHT:
        return JoinType.RIGHT;
      default:
        String errMsg = String.format("join type: {} not supported.", joinType.name());
        throw new FlinkRuntimeException(errMsg);
    }
  }

  private List<FieldAccessTypedExpr> getLeftKeys(
      RowType inRowType, Map<Integer, LookupKey> lookupKeys) {
    List<LogicalType> fieldTypes = inRowType.getChildren();
    List<String> fieldNames = inRowType.getFieldNames();
    List<FieldAccessTypedExpr> leftKeys = new ArrayList<>();
    for (LookupKey key : lookupKeys.values()) {
      if (key instanceof FieldRefLookupKey) {
        int keyIndex = ((FieldRefLookupKey) key).index;
        String fieldName = fieldNames.get(keyIndex);
        FieldAccessTypedExpr leftKey =
            FieldAccessTypedExpr.create(
                LogicalTypeConverter.toVLType(fieldTypes.get(keyIndex)), fieldName);
        leftKeys.add(leftKey);
      }
    }
    return leftKeys;
  }

  private List<FieldAccessTypedExpr> getRightKeys(
      RowType tableSourceRowType, Map<Integer, LookupKey> lookupKeys) {
    List<LogicalType> fieldTypes = tableSourceRowType.getChildren();
    List<String> fieldNames = tableSourceRowType.getFieldNames();
    List<FieldAccessTypedExpr> rightKeys = new ArrayList<>();
    for (Integer key : lookupKeys.keySet()) {
      if (lookupKeys.get(key) instanceof FieldRefLookupKey) {
        String fieldName = fieldNames.get(key);
        FieldAccessTypedExpr rightKey =
            FieldAccessTypedExpr.create(
                LogicalTypeConverter.toVLType(fieldTypes.get(key)), fieldName);
        rightKeys.add(rightKey);
      }
    }
    return rightKeys;
  }

  private List<IndexLookupCondition> getJoinConditions(RowType inputRowType) {
    final RexNode filter =
        (RexNode)
            ReflectUtils.getObjectField(CommonExecLookupJoin.class, this, "filterOnTemporalTable");
    if (filter == null) {
      LOG.info("return null here");
      return new ArrayList<>();
    }
    List<String> inNames = inputRowType.getFieldNames();
    RexConversionContext conversionContext = new RexConversionContext(inNames);
    TypedExpr filterExpr = RexNodeConverter.toTypedExpr(filter, conversionContext);
    List<IndexLookupCondition> conditions = List.of(IndexLookupJoinBuilder.build(filterExpr));
    LOG.info("conditions.size:" + conditions.size());
    return conditions;
  }

  @SuppressWarnings("rawtypes")
  private PlanNode getLeftTable(Transformation<RowData> inputTrans) {
    if (inputTrans instanceof OneInputTransformation) {
      OneInputTransformation oneInputTrans = (OneInputTransformation) inputTrans;
      GlutenVectorOneInputOperator oneInputOperator =
          (GlutenVectorOneInputOperator) oneInputTrans.getOperator();
      PlanNode planNode = oneInputOperator.getPlanNode().getNode();
      return planNode;
    } else {
      String errMsg =
          String.format("Transformation: %s not supported", inputTrans.getClass().getName());
      throw new FlinkRuntimeException(errMsg);
    }
  }

  @SuppressWarnings("deprecation")
  private TableScanNode getRightTable(
      UserDefinedFunction lookupFunction, Map<Integer, LookupKey> lookupKeys, boolean asyncLookup) {
    if (lookupFunction instanceof CsvLookupFunction) {
      CsvLookupFunction csvLookupFunc = (CsvLookupFunction) lookupFunction;
      Object csvInputConfig =
          ReflectUtils.getObjectField(CsvLookupFunction.class, csvLookupFunc, "config");
      String[] fieldNames =
          (String[])
              ReflectUtils.getObjectField(csvInputConfig.getClass(), csvInputConfig, "fieldNames");
      DataType[] fieldTypes =
          (DataType[])
              ReflectUtils.getObjectField(csvInputConfig.getClass(), csvInputConfig, "fieldTypes");
      List<RowField> fields = new ArrayList<>();
      for (int i = 0; i < fieldNames.length; ++i) {
        RowField field = new RowField(fieldNames[i], fieldTypes[i].getLogicalType());
        fields.add(field);
      }
      Map<String, String> tableParameters =
          Map.of(
              "path",
                  (String)
                      ReflectUtils.getObjectField(
                          csvInputConfig.getClass(), csvInputConfig, "path"),
              "format", "csv",
              "csv.field.delimiter",
                  (String)
                      ReflectUtils.getObjectField(
                          csvInputConfig.getClass(), csvInputConfig, "fieldDelim"));
      FileSystemIndexTableHandle tableHandle =
          new FileSystemIndexTableHandle(
              "connector-filesystem",
              "lookupTable",
              (io.github.zhztheplayer.velox4j.type.RowType)
                  LogicalTypeConverter.toVLType(new RowType(fields)),
              lookupKeys.keySet().stream().mapToInt(Integer::intValue).toArray(),
              asyncLookup,
              tableParameters);
      RowType outputType =
          (RowType)
              TypeConversions.fromLegacyInfoToDataType(csvLookupFunc.getResultType())
                  .getLogicalType();
      io.github.zhztheplayer.velox4j.type.RowType vlRowType =
          (io.github.zhztheplayer.velox4j.type.RowType) LogicalTypeConverter.toVLType(outputType);
      List<Assignment> columnHandles = new ArrayList<>();
      for (int i = 0; i < vlRowType.size(); i++) {
        final String name = vlRowType.getNames().get(i);
        final Type type = vlRowType.getChildren().get(i);
        ColumnHandle column = new FileSystemColumnHandle(name, ColumnType.REGULAR, type, List.of());
        Assignment assignment = new Assignment(name, column);
        columnHandles.add(assignment);
      }
      TableScanNode scanNode =
          new TableScanNode(PlanNodeIdGenerator.newId(), vlRowType, tableHandle, columnHandles);
      return scanNode;
    } else {
      String errMsg =
          String.format(
              "lookup function %s currently not supported.", lookupFunction.getClass().getName());
      throw new FlinkRuntimeException(errMsg);
    }
  }

  @Override
  protected Transformation<RowData> createSyncLookupJoinWithState(
      Transformation<RowData> inputTransformation,
      RelOptTable temporalTable,
      ExecNodeConfig config,
      ClassLoader classLoader,
      Map<Integer, LookupKey> allLookupKeys,
      TableFunction<?> syncLookupFunction,
      RelBuilder relBuilder,
      RowType inputRowType,
      RowType tableSourceRowType,
      RowType resultRowType,
      boolean isLeftOuterJoin,
      boolean isObjectReuseEnabled,
      boolean lookupKeyContainsPrimaryKey) {
    throw new UnsupportedOperationException("Unimplemented method 'createSyncLookupJoinWithState'");
  }
}
