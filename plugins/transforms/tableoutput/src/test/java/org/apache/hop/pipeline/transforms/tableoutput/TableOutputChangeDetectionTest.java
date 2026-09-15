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

package org.apache.hop.pipeline.transforms.tableoutput;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Issue #8022 is not limited to the four transforms in the report: Table Output has the same
 * shallow TableOutputMeta.clone() and the same in-place {@code info.getFields().clear()} in its
 * dialog, so editing the field mapping is invisible to the delegate's before/after comparison too.
 */
class TableOutputChangeDetectionTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void editingTheFieldMappingIsNotDetected() throws Exception {
    TableOutputMeta meta = new TableOutputMeta();
    meta.setSpecifyFields(true);
    TableOutputField field = new TableOutputField();
    field.setFieldStream("a");
    field.setFieldDatabase("col_a");
    meta.getFields().add(field);

    TransformMeta transformMeta = new TransformMeta("Table output", meta);
    transformMeta.setTransformPluginId("TableOutput");

    TransformMeta before = (TransformMeta) transformMeta.clone();

    // TableOutputDialog.getInfo(): info.getFields().clear() then re-add from the grid.
    meta.getFields().clear();
    TableOutputField remapped = new TableOutputField();
    remapped.setFieldStream("a");
    remapped.setFieldDatabase("REMAPPED_COLUMN");
    meta.getFields().add(remapped);

    TransformMeta after = (TransformMeta) transformMeta.clone();

    System.out.println("before snapshot already remapped: " + before.getXml().contains("REMAPPED"));

    assertTrue(
        !before.getXml().equals(after.getXml()),
        "remapping a Table Output column must be detected");
  }
}
