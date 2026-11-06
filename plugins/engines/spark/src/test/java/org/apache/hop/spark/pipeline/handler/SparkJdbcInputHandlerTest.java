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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transforms.tableinput.TableInputMeta;
import org.junit.jupiter.api.Test;

/**
 * When the native partitioned JDBC read applies. The real reads are exercised by {@link
 * SparkJdbcInputHandlerIT}, so this class stays free of any Spark session.
 */
class SparkJdbcInputHandlerTest {

  private TableInputMeta partitionedMeta() {
    TableInputMeta meta = new TableInputMeta();
    meta.setConnection("some-connection");
    meta.setSql("SELECT id, val FROM T");
    meta.setUseNamedParameters(false);
    meta.setJdbcPartitionColumn("id");
    meta.setJdbcLowerBound("0");
    meta.setJdbcUpperBound("1000");
    meta.setJdbcNumPartitions("4");
    return meta;
  }

  @Test
  void nativeReadWhenConfiguredAndNoIncomingInput() {
    assertTrue(
        SparkJdbcInputHandler.isNativePartitionedRead(partitionedMeta(), new Variables(), false),
        "a partitioned Table Input with no incoming hop is read natively");
  }

  @Test
  void fallbackWhenIncomingInput() {
    assertFalse(
        SparkJdbcInputHandler.isNativePartitionedRead(partitionedMeta(), new Variables(), true),
        "an incoming hop means Table Input consumes parameters, not a partitioned source");
  }

  @Test
  void fallbackWhenNotConfigured() {
    TableInputMeta meta = new TableInputMeta();
    meta.setConnection("some-connection");
    meta.setSql("SELECT id, val FROM T");
    assertFalse(SparkJdbcInputHandler.isNativePartitionedRead(meta, new Variables(), false));
  }

  @Test
  void fallbackForPerRowExecution() {
    TableInputMeta meta = partitionedMeta();
    meta.setExecuteEachInputRow(true);
    assertFalse(
        SparkJdbcInputHandler.isNativePartitionedRead(meta, new Variables(), false),
        "per-row execution cannot be expressed as a single partitioned read");
  }

  @Test
  void fallbackForNamedParameters() {
    TableInputMeta meta = partitionedMeta();
    meta.setUseNamedParameters(true);
    assertFalse(SparkJdbcInputHandler.isNativePartitionedRead(meta, new Variables(), false));
  }

  @Test
  void fallbackForRowLimit() {
    TableInputMeta meta = partitionedMeta();
    meta.setRowLimit("10");
    assertFalse(
        SparkJdbcInputHandler.isNativePartitionedRead(meta, new Variables(), false),
        "the row limit applies to the whole read, not to each partition");
  }

  @Test
  void fallbackWhenPartitionCountIsNotAPositiveNumber() {
    TableInputMeta meta = partitionedMeta();
    meta.setJdbcNumPartitions("0");
    assertFalse(SparkJdbcInputHandler.isNativePartitionedRead(meta, new Variables(), false));
  }

  @Test
  void variablesAreResolvedBeforeTheDecision() {
    TableInputMeta meta = partitionedMeta();
    meta.setJdbcPartitionColumn("${PARTITION_COLUMN}");
    meta.setJdbcNumPartitions("${PARTITION_COUNT}");
    Variables variables = new Variables();
    variables.setVariable("PARTITION_COLUMN", "id");
    variables.setVariable("PARTITION_COUNT", "8");
    assertTrue(SparkJdbcInputHandler.isNativePartitionedRead(meta, variables, false));

    assertFalse(
        SparkJdbcInputHandler.isNativePartitionedRead(meta, new Variables(), false),
        "unresolved variables leave the partition column empty, so the read is not partitioned");
  }
}
