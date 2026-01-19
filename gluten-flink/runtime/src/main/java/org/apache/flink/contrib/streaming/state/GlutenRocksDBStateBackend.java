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
package org.apache.flink.contrib.streaming.state;

import org.apache.gluten.table.runtime.config.VeloxSessionConfig;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.OperatorStateBackend;

import java.io.IOException;

@SuppressWarnings("deprecation")
public class GlutenRocksDBStateBackend extends RocksDBStateBackend {

  public GlutenRocksDBStateBackend(String checkpointDataUri) throws IOException {
    super(checkpointDataUri, true);
  }

  @Override
  public RocksDBStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
    return this;
  }

  @Override
  public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
      KeyedStateBackendParameters<K> parameters) throws IOException {
    RocksDBKeyedStateBackend<K> stateBackend =
        (RocksDBKeyedStateBackend<K>) super.createKeyedStateBackend(parameters);
    VeloxSessionConfig.getSessionConfig().setKeyedStateBackend(stateBackend);
    return stateBackend;
  }

  @Override
  public OperatorStateBackend createOperatorStateBackend(OperatorStateBackendParameters parameters)
      throws IOException {
    final boolean asyncSnapshots = true;
    return new DefaultOperatorStateBackendBuilder(
            parameters.getEnv().getUserCodeClassLoader().asClassLoader(),
            parameters.getEnv().getExecutionConfig(),
            asyncSnapshots,
            parameters.getStateHandles(),
            parameters.getCancelStreamRegistry())
        .build();
  }
}
