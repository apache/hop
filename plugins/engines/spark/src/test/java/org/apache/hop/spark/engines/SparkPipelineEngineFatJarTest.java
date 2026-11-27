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

package org.apache.hop.spark.engines;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SparkPipelineEngineFatJarTest {

  @Test
  void localMastersRunInThisJvm() {
    assertFalse(SparkPipelineEngine.needsFatJar("local"));
    assertFalse(SparkPipelineEngine.needsFatJar("local[*]"));
    assertFalse(SparkPipelineEngine.needsFatJar(" local[4] "));
    assertFalse(SparkPipelineEngine.needsFatJar("local[4,2]"));
    assertFalse(SparkPipelineEngine.needsFatJar(""));
    assertFalse(SparkPipelineEngine.needsFatJar(null));
  }

  @Test
  void clusterMastersNeedAFatJar() {
    assertTrue(SparkPipelineEngine.needsFatJar("spark://spark:7077"));
    assertTrue(SparkPipelineEngine.needsFatJar("yarn"));
    assertTrue(SparkPipelineEngine.needsFatJar("k8s://https://kubernetes:6443"));
    assertTrue(SparkPipelineEngine.needsFatJar("local-cluster[2,1,1024]"));
  }
}
