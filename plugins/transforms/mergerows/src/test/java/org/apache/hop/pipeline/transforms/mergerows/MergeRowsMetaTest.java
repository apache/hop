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
 *
 */

package org.apache.hop.pipeline.transforms.mergerows;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class MergeRowsMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<MergeRowsMeta> testMetaClass = MergeRowsMeta.class;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  public void testSerialization() throws Exception {
    MergeRowsMeta meta =
        TransformSerializationTestUtil.testSerialization("/merge-rows.xml", MergeRowsMeta.class);

    Assert.assertEquals(1, meta.getKeyFields().size());
    Assert.assertEquals("id", meta.getKeyFields().get(0));
    Assert.assertEquals("flagfield", meta.getFlagField());
    Assert.assertEquals("source1", meta.getReferenceTransform());
    Assert.assertEquals("source2", meta.getCompareTransform());
    Assert.assertEquals(2, meta.getValueFields().size());
    Assert.assertEquals("name", meta.getValueFields().get(0));
    Assert.assertEquals("score", meta.getValueFields().get(1));
  }
}
