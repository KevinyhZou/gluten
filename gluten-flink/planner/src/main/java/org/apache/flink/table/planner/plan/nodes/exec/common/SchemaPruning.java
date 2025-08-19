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
package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class SchemaPruning {

  public static RowType pruneSchema(RowType schema, List<RowField> requestFields) {
    List<LogicalType> rowTypes =
        requestFields.stream()
            .map(x -> (LogicalType) (new RowType(List.of(x))))
            .collect(Collectors.toList());
    RowType mergedSchema = (RowType) (rowTypes.stream().reduce(null, (x, y) -> mergeType(x, y)));
    Map<String, RowField> requestFieldsMap = fieldsMap(requestFields);
    List<RowField> schemaFields =
        schema.getFields().stream()
            .filter(x -> requestFieldsMap.containsKey(x.getName()))
            .collect(Collectors.toList());
    List<RowField> mergedFields = mergedSchema.getFields();
    List<RowField> mergedDataFields = new ArrayList<>();
    for (RowField sf : schemaFields) {
      boolean found = false;
      for (RowField mf : mergedFields) {
        if (sf.getName().equals(mf.getName())) {
          mergedDataFields.add(mf);
          found = true;
        } else {
          continue;
        }
      }
      if (!found) {
        mergedDataFields.add(sf);
      }
    }
    RowType mergedDataSchema = new RowType(mergedDataFields);
    return (RowType) sortLeftFieldsByRight(mergedDataSchema, new RowType(schemaFields));
  }

  private static LogicalType sortLeftFieldsByRight(LogicalType left, LogicalType right) {
    if (left.equals(right)) {
      return left;
    } else if (left instanceof ArrayType && right instanceof ArrayType) {
      LogicalType leftElementType = ((ArrayType) left).getElementType();
      LogicalType rightElementType = ((ArrayType) right).getElementType();
      return new ArrayType(sortLeftFieldsByRight(leftElementType, rightElementType));
    } else if (left instanceof MapType && right instanceof MapType) {
      LogicalType leftKeyType = ((MapType) left).getKeyType();
      LogicalType leftValueType = ((MapType) left).getValueType();
      LogicalType rightKeyType = ((MapType) left).getKeyType();
      LogicalType rightValueType = ((MapType) right).getValueType();
      return new MapType(
          sortLeftFieldsByRight(leftKeyType, rightKeyType),
          sortLeftFieldsByRight(leftValueType, rightValueType));
    } else if (left instanceof RowType && right instanceof RowType) {
      List<RowField> leftFields = ((RowType) left).getFields();
      HashMap<String, RowField> leftMap = new HashMap<>();
      leftFields.stream().forEach(x -> leftMap.put(x.getName(), x));
      List<RowField> rightFields = ((RowType) right).getFields();
      List<RowField> newSortedFields = new ArrayList<>();
      rightFields.forEach(
          x -> {
            if (leftMap.containsKey(x.getName())) {
              RowField resolvedLeftField = leftMap.get(x.getName());
              LogicalType resolvedLeftDataType = resolvedLeftField.getType();
              LogicalType rightDataType = x.getType();
              LogicalType sortedType = sortLeftFieldsByRight(resolvedLeftDataType, rightDataType);
              RowField newSortedField = new RowField(x.getName(), sortedType);
              newSortedFields.add(newSortedField);
            }
          });
      return new RowType(newSortedFields);
    } else {
      return left;
    }
  }

  private static LogicalType mergeType(LogicalType t1, LogicalType t2) {
    if (t1 == null) {
      return t2;
    }
    if (t2 == null) {
      return t1;
    }
    BiFunction<RowType, RowType, RowType> f = SchemaPruning::mergeRowType;
    return mergeInternal(t1, t2, f);
  }

  private static LogicalType mergeInternal(
      LogicalType left, LogicalType right, BiFunction<RowType, RowType, RowType> f) {
    if (left instanceof ArrayType && right instanceof ArrayType) {
      LogicalType leftElementType = ((ArrayType) left).getElementType();
      LogicalType rightElementType = ((ArrayType) right).getElementType();
      LogicalType mergedType = mergeInternal(leftElementType, rightElementType, f);
      return new ArrayType(mergedType);
    } else if (left instanceof MapType && right instanceof MapType) {
      LogicalType leftKeyType = ((MapType) left).getKeyType();
      LogicalType leftValueType = ((MapType) left).getValueType();
      LogicalType rightKeyType = ((MapType) right).getKeyType();
      LogicalType rightValueType = ((MapType) right).getValueType();
      LogicalType mergedKeyType = mergeInternal(leftKeyType, rightKeyType, f);
      LogicalType mergedValueType = mergeInternal(leftValueType, rightValueType, f);
      return new MapType(mergedKeyType, mergedValueType);
    } else if (left instanceof RowType && right instanceof RowType) {
      return f.apply((RowType) left, (RowType) right);
    } else if (left.equals(right)) {
      return left;
    } else {
      String errMsg =
          String.format(
              "Do not support merge %s and %s",
              left.getClass().getName(), right.getClass().getName());
      throw new RuntimeException(errMsg);
    }
  }

  private static RowType mergeRowType(RowType s1, RowType s2) {
    List<RowField> leftFields = s1.getFields();
    List<RowField> rightFields = s2.getFields();
    List<RowField> newFields = new ArrayList<>();

    Map<String, RowField> rightMapped = fieldsMap(rightFields);
    leftFields.stream()
        .forEach(
            lf -> {
              String leftName = lf.getName();
              LogicalType leftType = lf.getType();
              if (rightMapped.containsKey(leftName)) {
                RowField rf = rightMapped.get(leftName);
                LogicalType rightType = rf.getType();
                LogicalType newType = mergeType(leftType, rightType);
                RowField newField = new RowField(leftName, newType);
                newFields.add(newField);
              } else {
                newFields.add(lf);
              }
            });

    Map<String, RowField> leftMapped = fieldsMap(leftFields);
    rightFields.stream()
        .filter(f -> !leftMapped.containsKey(f.getName()))
        .forEach(f -> newFields.add(f));

    return new RowType(newFields);
  }

  private static Map<String, RowField> fieldsMap(List<RowField> fields) {
    Map<String, RowField> map = new HashMap<>();
    fields.stream().forEach(s -> map.put(s.getName(), s));
    return map;
  }
}
