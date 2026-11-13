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

package org.apache.hop.spark.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformInitThread;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.spark.util.SparkConst;
import org.junit.jupiter.api.Test;

class SparkPartitionBundleExtensionPointTest {

  private static TransformInitThread initThread(BaseTransformData data) {
    TransformMetaDataCombi combi = new TransformMetaDataCombi();
    combi.transformName = "write";
    combi.transform = mock(ITransform.class);
    combi.data = data;
    return new TransformInitThread(combi, null, LogChannel.GENERAL);
  }

  @Test
  void aTransformOnASparkPartitionWritesItsOwnBundle() {
    BaseTransformData data = new BaseTransformData() {};
    IVariables pipeline = new Variables();
    pipeline.setVariable(SparkConst.VAR_PARTITION_NR, "5");
    new SparkPartitionBundleExtensionPoint()
        .callExtensionPoint(LogChannel.GENERAL, pipeline, initThread(data));
    assertTrue(data.isBeamContext());
    assertEquals(5, data.getBeamBundleNr());
  }

  @Test
  void otherPipelinesAreLeftAlone() {
    BaseTransformData data = new BaseTransformData() {};
    new SparkPartitionBundleExtensionPoint()
        .callExtensionPoint(LogChannel.GENERAL, new Variables(), initThread(data));
    assertFalse(data.isBeamContext());
  }
}
