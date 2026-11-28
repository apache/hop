/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.pipeline.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.junit.jupiter.api.Test;

class SourcePartitionsTest {

  private static int partitions(String value) {
    IVariables variables = new Variables();
    variables.setVariable("PARTITIONS", "4");
    SparkPipelineRunConfiguration configuration = new SparkPipelineRunConfiguration();
    configuration.setSourcePartitions(value);
    return SparkGenericTransformHandler.sourcePartitions(variables, configuration);
  }

  @Test
  void aSourceStaysInOnePartitionUnlessConfigured() {
    assertEquals(1, partitions(null));
    assertEquals(1, partitions(""));
    assertEquals(1, partitions("0"));
    assertEquals(1, partitions("many"));
    assertEquals(8, partitions(" 8 "));
    assertEquals(4, partitions("${PARTITIONS}"));
    assertEquals(1, SparkGenericTransformHandler.sourcePartitions(new Variables(), null));
  }
}
