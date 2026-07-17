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

    SparkPipelineRunConfiguration copy = config.clone();
    assertNotSame(config, copy);
    assertEquals("spark://host:7077", copy.getSparkMaster());
    assertEquals("test-app", copy.getSparkAppName());
    assertEquals("1g", copy.getDriverMemory());
    assertEquals("s3=s3a\nminio=s3a", copy.getPathSchemeMap());
  }
}
