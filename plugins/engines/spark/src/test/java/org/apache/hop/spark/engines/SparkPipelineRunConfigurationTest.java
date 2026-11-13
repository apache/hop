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

package org.apache.hop.spark.engines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.spark.util.SparkConst;
import org.junit.jupiter.api.Test;

class SparkPipelineRunConfigurationTest {

  @Test
  void defaultsAndClone() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    assertEquals(SparkConst.PLUGIN_ID, config.getEnginePluginId());
    assertEquals("local[*]", config.getSparkMaster());

    config.setSparkMaster("spark://host:7077");
    config.setSparkAppName("test-app");
    config.setDriverMemory("1g");
    config.setPathSchemeMap("s3=s3a\nminio=s3a");
    config.setGenericTransformRunMode("DRIVER_ONLY");
    config.setHadoopConfDir("/etc/hadoop/conf");

    SparkPipelineRunConfiguration copy = config.clone();
    assertNotSame(config, copy);
    assertEquals("spark://host:7077", copy.getSparkMaster());
    assertEquals("test-app", copy.getSparkAppName());
    assertEquals("1g", copy.getDriverMemory());
    assertEquals("/etc/hadoop/conf", copy.getHadoopConfDir());
    assertEquals("s3=s3a\nminio=s3a", copy.getPathSchemeMap());
    assertEquals("DRIVER_ONLY", copy.getGenericTransformRunMode());
    assertEquals("DISTRIBUTED", new SparkPipelineRunConfiguration().getGenericTransformRunMode());
  }

  /** A run configuration saved before these options existed has them switched on. */
  @Test
  void optionsMissingFromTheJsonAreOn() throws Exception {
    HopEnvironment.init();
    JsonParser parser =
        new JsonFactory()
            .createParser(
                "{\"name\":\"old\",\"engineRunConfiguration\":{\""
                    + SparkConst.PLUGIN_ID
                    + "\":{\"sparkMaster\":\"local[2]\"}}}");
    parser.nextToken();
    PipelineRunConfiguration runConfiguration =
        new JsonMetadataParser<>(PipelineRunConfiguration.class, new MemoryMetadataProvider())
            .loadJsonObject(PipelineRunConfiguration.class, parser);
    SparkPipelineRunConfiguration configuration =
        (SparkPipelineRunConfiguration) runConfiguration.getEngineRunConfiguration();

    assertTrue(configuration.isFuseGenericTransforms());
    assertTrue(configuration.isInternalRowConversion());
  }
}
