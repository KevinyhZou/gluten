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
package org.apache.gluten.velox;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.plan.BetweenIndexLookupCondition;
import io.github.zhztheplayer.velox4j.plan.IndexLookupCondition;
import io.github.zhztheplayer.velox4j.type.ArrayType;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.SmallIntType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.variant.ArrayValue;
import io.github.zhztheplayer.velox4j.variant.BigIntValue;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
import io.github.zhztheplayer.velox4j.variant.SmallIntValue;
import io.github.zhztheplayer.velox4j.variant.Variant;

import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.List;

public class IndexLookupJoinBuilder {

  public static IndexLookupCondition build(TypedExpr expr) {
    if (expr instanceof CallTypedExpr) {
      CallTypedExpr callExpr = (CallTypedExpr) expr;
      if (callExpr.getFunctionName() == "between") {
        List<TypedExpr> inputs = callExpr.getInputs();
        if (inputs.size() != 3) {
          throw new FlinkRuntimeException(
              "Can not parse the between expression to velox join conditions, as inputs number is not 3");
        }
        if (!(inputs.get(0) instanceof FieldAccessTypedExpr)) {
          throw new FlinkRuntimeException(
              "Can not parse the between expression to velox join conditions, as the first input is not a field access expression, which not supported.");
        }
        FieldAccessTypedExpr keyColumnExpr = (FieldAccessTypedExpr) inputs.get(0);
        BetweenIndexLookupCondition lookupCondition =
            new BetweenIndexLookupCondition(
                "between",
                keyColumnExpr,
                castIndexConditionInputExpr(inputs.get(1), keyColumnExpr.getReturnType()),
                castIndexConditionInputExpr(inputs.get(2), keyColumnExpr.getReturnType()));
        return lookupCondition;
      } else if (callExpr.getFunctionName() == "equals") {

      } else if (callExpr.getFunctionName().equals("greater")) {

      } else if (callExpr.getFunctionName().equals("callExpr")) {

      }
    }
    return null;
  }

  private static TypedExpr removeCasTypedExpr(TypedExpr expr) {
    TypedExpr convertTypedExpr = expr;
    while (convertTypedExpr instanceof CastTypedExpr) {
      CastTypedExpr castExpr = (CastTypedExpr) convertTypedExpr;
      if (castExpr.getInputs().size() != 1) {
        throw new FlinkRuntimeException("The inputs number of cast expression should be 1.");
      }
      convertTypedExpr = castExpr.getInputs().get(0);
    }
    return convertTypedExpr;
  }

  private static TypedExpr castIndexConditionInputExpr(TypedExpr expr, Type indexType) {
    TypedExpr convertTypedExpr = removeCasTypedExpr(expr);
    Type exprType = convertTypedExpr.getReturnType();
    if (convertTypedExpr instanceof FieldAccessTypedExpr) {
      if (exprType.equals(indexType)) {
        return convertTypedExpr;
      } else if (exprType instanceof ArrayType) {
        ArrayType arrayType = (ArrayType) exprType;
        List<Type> elementTypes = arrayType.getChildren();
        if (elementTypes.size() > 0 && elementTypes.get(0).equals(indexType)) {
          return convertTypedExpr;
        }
      } else {
        throw new FlinkRuntimeException(
            "Can not cast the join condition expression as expr type not matched.");
      }
    }
    if (!(convertTypedExpr instanceof ConstantTypedExpr)) {
      throw new FlinkRuntimeException(
          "Can not cast the join condition expression as the expr is not constant.");
    }
    ConstantTypedExpr constantExpr = (ConstantTypedExpr) convertTypedExpr;
    if (exprType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) exprType;
      Type elementType = arrayType.getChildren().get(0);
      if (elementType.equals(indexType)) {
        return constantExpr;
      }
      if ((elementType instanceof IntegerType)
          || (elementType instanceof BigIntType)
          || (elementType instanceof SmallIntType)) {
        return castConstantArrayConditionInput(constantExpr, elementType, indexType);
      } else {
        throw new FlinkRuntimeException(
            "Incompatible condition input type:" + elementType.getClass().getName());
      }
    }
    if (constantExpr.getReturnType().equals(indexType)) {
      return constantExpr;
    }
    if ((exprType instanceof IntegerType)
        || (exprType instanceof BigIntType)
        || (exprType instanceof SmallIntType)) {
      return castConstantConditionInput(constantExpr, exprType, indexType);
    } else {
      throw new FlinkRuntimeException("Not supported type:" + exprType.getClass().getName());
    }
  }

  private static TypedExpr castConstantArrayConditionInput(
      ConstantTypedExpr expr, Type srcType, Type dstType) {
    if (srcType.equals(dstType)) {
      return expr;
    }
    ArrayValue arrayValue = (ArrayValue) expr.getValue();
    List<Variant> arrayValues = arrayValue.getArray();
    List<Variant> castedArrayValues = new ArrayList<>();
    for (Variant v : arrayValues) {
      if (v instanceof IntegerValue) {
        int t = ((IntegerValue) v).getValue();
        castedArrayValues.add(getVariant(dstType, t));
      } else if (v instanceof BigIntValue) {
        long t = ((BigIntValue) v).getValue();
        castedArrayValues.add(getVariant(dstType, t));
      } else if (v instanceof SmallIntValue) {
        int t = ((SmallIntValue) v).getValue();
        castedArrayValues.add(getVariant(dstType, t));
      } else {
        throw new FlinkRuntimeException("Value type not supported:" + v.getClass().getName());
      }
    }
    return ConstantTypedExpr.create(new ArrayValue(castedArrayValues));
  }

  private static TypedExpr castConstantConditionInput(
      ConstantTypedExpr expr, Type srcType, Type dstType) {
    if (srcType.equals(dstType)) {
      return expr;
    }
    Variant value = expr.getValue();
    Variant castedValue = null;
    if (value instanceof IntegerValue) {
      int t = ((IntegerValue) value).getValue();
      castedValue = getVariant(dstType, t);
    } else if (value instanceof BigIntValue) {
      long t = ((BigIntValue) value).getValue();
      castedValue = getVariant(dstType, t);
    } else if (value instanceof SmallIntValue) {
      int t = ((SmallIntValue) value).getValue();
      castedValue = getVariant(dstType, t);
    } else {
      throw new FlinkRuntimeException("Value type not supported:" + value.getClass().getName());
    }
    return ConstantTypedExpr.create(castedValue);
  }

  private static <T> Variant getVariant(Type type, T t) {
    if (type instanceof IntegerType) {
      IntegerValue intValue = new IntegerValue((int) t);
      return intValue;
    } else if (type instanceof BigIntType) {
      BigIntValue bigIntValue = new BigIntValue((long) t);
      return bigIntValue;
    } else if (type instanceof SmallIntType) {
      SmallIntValue smallIntValue = new SmallIntValue((int) t);
      return smallIntValue;
    } else {
      throw new FlinkRuntimeException("Type not supported:" + type.getClass().getName());
    }
  }
}
