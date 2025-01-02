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
package org.apache.gluten.substrait.expression;

import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.Expression;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.FunctionOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScalarFunctionNode implements ExpressionNode, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ScalarFunctionNode.class);
  private final Long functionId;
  private final List<ExpressionNode> expressionNodes = new ArrayList<>();
  private final TypeNode typeNode;
  private Map<String, List<String>> functionOptions = new HashMap<>();

  ScalarFunctionNode(
      Long functionId,
      List<ExpressionNode> expressionNodes,
      TypeNode typeNode,
      Map<String, List<String>> options) {
    this(functionId, expressionNodes, typeNode);
    this.functionOptions = options;
  }

  ScalarFunctionNode(Long functionId, List<ExpressionNode> expressionNodes, TypeNode typeNode) {
    this.functionId = functionId;
    this.expressionNodes.addAll(expressionNodes);
    this.typeNode = typeNode;
  }

  @Override
  public Expression toProtobuf() {
    Expression.ScalarFunction.Builder scalarBuilder = Expression.ScalarFunction.newBuilder();
    scalarBuilder.setFunctionReference(functionId.intValue());
    for (ExpressionNode expressionNode : expressionNodes) {
      FunctionArgument.Builder functionArgument = FunctionArgument.newBuilder();
      functionArgument.setValue(expressionNode.toProtobuf());
      scalarBuilder.addArguments(functionArgument.build());
    }
    for (String optionKey : functionOptions.keySet()) {
      List<String> optionValues = functionOptions.get(optionKey);
      LOG.info("optionKey:{}, optionValues:{}", optionKey, optionValues);
      FunctionOption.Builder functionOptionBuilder = FunctionOption.newBuilder();
      functionOptionBuilder.setName(optionKey);
      for (int i = 0; i < optionValues.size(); ++i) {
        String optionValue = optionValues.get(i);
        functionOptionBuilder.addPreference(optionValue);
      }
      scalarBuilder.addOptions(functionOptionBuilder);
    }
    scalarBuilder.setOutputType(typeNode.toProtobuf());

    Expression.Builder builder = Expression.newBuilder();
    builder.setScalarFunction(scalarBuilder.build());
    return builder.build();
  }
}
