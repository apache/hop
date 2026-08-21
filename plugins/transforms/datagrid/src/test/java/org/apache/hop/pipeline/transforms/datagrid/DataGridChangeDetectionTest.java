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

package org.apache.hop.pipeline.transforms.datagrid;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Reproduces issue #8022 for the Data Grid transform, including the asymmetry reported in the
 * comments: the "Data" tab is detected, the "Meta" tab is not.
 *
 * <p>DataGridMeta's copy constructor copies both list <i>references</i>, so a clone shares them
 * with the live meta. DataGridDialog.getMetaInfo() mutates the shared field list in place
 * (invisible to the delegate's before/after XML compare) while getDataInfo() assigns a brand new
 * list (visible).
 */
class DataGridChangeDetectionTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  private static TransformMeta dataGridTransform() {
    DataGridMeta meta = new DataGridMeta();
    DataGridFieldMeta field = new DataGridFieldMeta();
    field.setName("a");
    field.setType("String");
    meta.getDataGridFields().add(field);

    DataGridDataMeta line = new DataGridDataMeta();
    line.getDatalines().add("row-1");
    meta.getDataLines().add(line);

    TransformMeta transformMeta = new TransformMeta("Data grid", meta);
    transformMeta.setTransformPluginId("DataGrid");
    return transformMeta;
  }

  private static boolean delegateSeesChange(TransformMeta before, TransformMeta after)
      throws Exception {
    return !before.getXml().equals(after.getXml());
  }

  @Test
  void editingTheMetaTabIsNotDetected() throws Exception {
    TransformMeta transformMeta = dataGridTransform();
    TransformMeta before = (TransformMeta) transformMeta.clone();

    // DataGridDialog.getMetaInfo(): clear + refill the *live* list.
    DataGridMeta live = (DataGridMeta) transformMeta.getTransform();
    live.getDataGridFields().clear();
    DataGridFieldMeta renamed = new DataGridFieldMeta();
    renamed.setName("RENAMED_BY_USER");
    renamed.setType("String");
    live.getDataGridFields().add(renamed);

    TransformMeta after = (TransformMeta) transformMeta.clone();

    System.out.println(
        "meta list shared with snapshot: "
            + (((DataGridMeta) before.getTransform()).getDataGridFields()
                == live.getDataGridFields()));
    System.out.println("before snapshot already renamed: " + before.getXml().contains("RENAMED"));

    assertTrue(delegateSeesChange(before, after), "renaming a Data Grid field must be detected");
  }

  @Test
  void editingTheDataTabIsDetected() throws Exception {
    TransformMeta transformMeta = dataGridTransform();
    TransformMeta before = (TransformMeta) transformMeta.clone();

    // DataGridDialog.getDataInfo(): build a new list and hand it over via the setter.
    DataGridMeta live = (DataGridMeta) transformMeta.getTransform();
    List<DataGridDataMeta> data = new ArrayList<>();
    DataGridDataMeta line = new DataGridDataMeta();
    line.getDatalines().add("EDITED_BY_USER");
    data.add(line);
    live.setDataLines(data);

    TransformMeta after = (TransformMeta) transformMeta.clone();

    assertTrue(delegateSeesChange(before, after), "editing Data Grid data is detected today");
    assertFalse(
        before.getXml().contains("EDITED_BY_USER"),
        "the setter replaced the list, so the snapshot kept the old rows");
  }
}
