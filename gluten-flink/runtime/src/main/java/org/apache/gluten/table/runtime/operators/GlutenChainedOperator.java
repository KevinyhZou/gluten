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

package org.apache.gluten.table.runtime.operators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.gluten.table.runtime.plan.PlanEvent;
import org.apache.gluten.table.runtime.plan.SupportsPlanChaining;

import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.KafkaConnectorSplit;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.type.RowType;

public class GlutenChainedOperator extends GlutenSingleInputOperator implements SupportsPlanChaining {

  private static final Map<PlanNode, ConnectorSplit> plans = new HashMap<>();
  private boolean running = false;

  public GlutenChainedOperator(PlanNode plan, String id, RowType inputType, RowType outputType) {
    super(plan, id, inputType, outputType);
  }

  @Override
  public void open() throws Exception {
    super.open();
    plans.put(glutenPlan, null);
  }

  private void startTask() {
    if (running) {
      return;
    }
    for (PlanNode plan : plans.keySet()) {
      if (plan instanceof TableScanNode) {
        TableScanNode tableScan = (TableScanNode) plan;
        glutenPlan.setSources(List.of(tableScan));
        query = new Query(glutenPlan, Config.empty(), ConnectorConfig.empty());
        task = session.queryOps().execute(query);
        task.addSplit(tableScan.getId(), plans.get(plan));
        task.start();
        running = true;
        break;
      }
    }
  }

  @Override
  public boolean queueAsSource() {
    return false;
  }

  @Override
  public void processElement(StreamRecord<RowData> element) {
    if (!running) {
      startTask();
    }
    RowData rowData = element.getValue();
    if (rowData != null) {
      output.collect(outElement.replace(rowData));
    }
    try {
      Thread.sleep(100);
    } catch (Exception ignore) {

    }
  }

  @Override
  public void close() throws Exception {
    running = false;
    plans.clear();
    if (task != null) {
      task.stop();
    }
    super.close();
  }

  @Override
  public void apply(PlanEvent event) {
    if (event.getOperatorId().equals("connector-kafka")) {
      TableScanNode tableScanNode = Serde.fromJson(event.getPlanNode(), TableScanNode.class);
      KafkaConnectorSplit kafkaConnectorSplit = Serde.fromJson(event.getSourceConnectSplit(), KafkaConnectorSplit.class);
      plans.put(tableScanNode, kafkaConnectorSplit);
    }
  }
}