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
package org.apache.gluten.table.runtime.stream.custom;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaToHiveE2ETest {

  private static final int KAFKA_PORT = 19092;

  @RegisterExtension
  static final SharedKafkaTestResource KAFKA =
      new SharedKafkaTestResource()
          .withBrokerProperty("host.name", "127.0.0.1")
          .withBrokers(1)
          .registerListener(new PlainListener().onPorts(KAFKA_PORT));

  @Test
  void testInsertFromKafkaToHive(@TempDir Path tempDir) throws Exception {
    String topic = "kafka_to_hive_" + UUID.randomUUID().toString().replace("-", "");
    KAFKA.getKafkaTestUtils().createTopic(topic, 1, (short) 1);
    KAFKA
        .getKafkaTestUtils()
        .produceRecords(
            List.of(
                jsonRecord(topic, "{\"id\":1,\"name\":\"alice\"}"),
                jsonRecord(topic, "{\"id\":2,\"name\":\"bob\"}"),
                jsonRecord(topic, "{\"id\":3,\"name\":\"carol\"}")));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().inStreamingMode().build());

    createKafkaSource(tEnv, topic);
    Path sinkPath = tempDir.resolve("hive_sink");
    createHiveCatalogAndSink(tEnv, sinkPath);

    org.apache.flink.table.api.TableResult insertResult =
        tEnv.executeSql(
            "INSERT INTO hive_catalog.`default`.hive_sink "
                + "SELECT id, name FROM default_catalog.default_database.kafka_source");
    JobClient jobClient = insertResult.getJobClient().orElseThrow();
    jobClient.getJobExecutionResult().get(60000, TimeUnit.MILLISECONDS);

    assertThat(readHiveTextRows(sinkPath)).containsExactlyInAnyOrder("1,alice", "2,bob", "3,carol");
  }

  private static ProducerRecord<byte[], byte[]> jsonRecord(String topic, String value) {
    return new ProducerRecord<>(topic, value.getBytes(StandardCharsets.UTF_8));
  }

  private void createKafkaSource(StreamTableEnvironment tEnv, String topic) {
    tEnv.executeSql(
        "CREATE TABLE kafka_source ("
            + " id INT,"
            + " name STRING"
            + ") WITH ("
            + " 'connector' = 'kafka',"
            + " 'topic' = '"
            + topic
            + "',"
            + " 'properties.bootstrap.servers' = '"
            + "127.0.0.1:"
            + KAFKA_PORT
            + "',"
            + " 'properties.group.id' = 'kafka-to-hive-e2e',"
            + " 'properties.broker.address.family' = 'v4',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'scan.bounded.mode' = 'latest-offset',"
            + " 'format' = 'json'"
            + ")");
  }

  private void createHiveCatalogAndSink(StreamTableEnvironment tEnv, Path sinkPath) {
    HiveCatalog hiveCatalog = HiveTestUtils.createHiveCatalog("hive_catalog", "2.3.9");
    tEnv.registerCatalog("hive_catalog", hiveCatalog);

    tEnv.useCatalog("hive_catalog");
    tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
    tEnv.executeSql(
        "CREATE TABLE hive_sink (id INT, name STRING) "
            + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
            + "STORED AS TEXTFILE LOCATION '"
            + sinkPath.toUri()
            + "'");
    tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
    tEnv.useCatalog("default_catalog");
  }

  private static List<String> readHiveTextRows(Path sinkPath) throws Exception {
    try (java.util.stream.Stream<Path> paths = Files.walk(sinkPath)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(path -> !path.getFileName().toString().startsWith("."))
          .filter(path -> !path.getFileName().toString().startsWith("_"))
          .flatMap(
              path -> {
                try {
                  return Files.lines(path, StandardCharsets.UTF_8);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              })
          .collect(java.util.stream.Collectors.toList());
    }
  }
}
