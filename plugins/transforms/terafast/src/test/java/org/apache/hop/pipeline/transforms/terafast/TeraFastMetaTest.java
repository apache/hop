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

package org.apache.hop.pipeline.transforms.terafast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TeraFastMetaTest {
  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testSerialization() throws Exception {
    TransformSerializationTestUtil.testSerialization("/terafast-transform.xml", TeraFastMeta.class);
  }

  @Test
  void testSetDefault() {
    TeraFastMeta meta = new TeraFastMeta();
    meta.setDefault();
    assertEquals(TeraFastMeta.DEFAULT_FASTLOAD_PATH, meta.getFastloadPath());
    assertEquals(TeraFastMeta.DEFAULT_DATA_FILE, meta.getDataFile());
    assertEquals(TeraFastMeta.DEFAULT_SESSIONS, meta.getSessions());
    assertEquals(TeraFastMeta.DEFAULT_ERROR_LIMIT, meta.getErrorLimit());
    assertEquals(TeraFastMeta.DEFAULT_TARGET_TABLE, meta.getTargetTable());
    assertTrue(meta.isTruncateTable());
    assertTrue(meta.isVariableSubstitution());
    assertTrue(meta.isUseControlFile());
  }

  @Test
  void testClone() {
    TeraFastMeta meta = new TeraFastMeta();
    meta.setDefault();
    meta.setFastloadPath("/custom/fastload");
    meta.setTableFieldList(Arrays.asList("a", "b"));
    meta.setStreamFieldList(Arrays.asList("x", "y"));
    TeraFastMeta clone = (TeraFastMeta) meta.clone();
    assertNotNull(clone);
    assertEquals(meta.getFastloadPath(), clone.getFastloadPath());
    assertEquals(meta.getTableFieldList(), clone.getTableFieldList());
    assertEquals(meta.getStreamFieldList(), clone.getStreamFieldList());
    assertEquals(meta.getSessions(), clone.getSessions());
  }

  @Test
  void testGetFieldsDoesNotModifyInputRowMeta() throws Exception {
    TeraFastMeta meta = new TeraFastMeta();
    IRowMeta inputRowMeta = new RowMeta();
    int originalSize = inputRowMeta.size();
    meta.getFields(
        inputRowMeta, "origin", null, null, new Variables(), (IHopMetadataProvider) null);
    assertEquals(originalSize, inputRowMeta.size());
  }

  @Test
  void testGetRequiredFieldsWhenUsingControlFile() throws Exception {
    TeraFastMeta meta = new TeraFastMeta();
    meta.setUseControlFile(true);
    assertNull(meta.getRequiredFields(new Variables()));
  }
}
