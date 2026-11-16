/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.pipeline.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.tableinput.TableInputMeta;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SparkJdbcInputHandlerIT {
  private static SparkSession spark;

  @BeforeAll
  static void start() throws Exception {
    HopEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    try (var connection =
            DriverManager.getConnection("jdbc:h2:mem:spark_partition_it;DB_CLOSE_DELAY=-1");
        var statement = connection.createStatement()) {
      statement.execute("CREATE TABLE T (ID INT, VAL VARCHAR(10))");
      statement.execute(
          "INSERT INTO T VALUES (NULL, 'null'), (-1, 'low'), (5, 'mid'), (200, 'high')");
    }
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("jdbc-partition-it")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "localhost")
            .getOrCreate();
  }

  @AfterAll
  static void stop() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void readsAllRowsExactlyOnceAcrossPartitions() throws Exception {
    Dataset<Row> result = read(4, "0", "100");
    assertEquals(4, result.javaRDD().getNumPartitions());
    assertEquals(
        List.of("high", "low", "mid", "null"),
        result.select("VAL").collectAsList().stream().map(r -> r.getString(0)).sorted().toList());
  }

  @Test
  void equalBoundsReadOnce() throws Exception {
    Dataset<Row> result = read(4, "5", "5");
    assertEquals(1, result.javaRDD().getNumPartitions());
    assertEquals(4, result.count());
  }

  private Dataset<Row> read(int count, String lower, String upper) throws Exception {
    var provider = new MemoryMetadataProvider();
    provider
        .getSerializer(DatabaseMeta.class)
        .save(
            new DatabaseMeta(
                "h2", "H2", "Native", "", "mem:spark_partition_it;DB_CLOSE_DELAY=-1", "", "", ""));
    var meta = new TableInputMeta();
    meta.setConnection("h2");
    meta.setSql("SELECT ID, VAL FROM T");
    meta.setUseNamedParameters(false);
    meta.setJdbcPartitionColumn("ID");
    meta.setJdbcLowerBound(lower);
    meta.setJdbcUpperBound(upper);
    meta.setJdbcNumPartitions(Integer.toString(count));
    var transform = new TransformMeta("input", meta);
    transform.setTransformPluginId("TableInput");
    var pipeline = new PipelineMeta();
    pipeline.setMetadataProvider(provider);
    pipeline.addTransform(transform);
    var result = new HashMap<String, Dataset<Row>>();
    new SparkJdbcInputHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "spark",
            new SparkPipelineRunConfiguration(),
            provider,
            "{}",
            pipeline,
            transform,
            result,
            spark,
            new RowMeta(),
            List.of(),
            null);
    return result.get("input");
  }
}
