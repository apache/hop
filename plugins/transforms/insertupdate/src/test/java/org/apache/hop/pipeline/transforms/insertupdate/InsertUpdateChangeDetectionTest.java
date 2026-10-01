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

package org.apache.hop.pipeline.transforms.insertupdate;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Reproduces issue #8022 for Insert/Update. InsertUpdateMeta.clone() is a shallow Object.clone(),
 * so the nested InsertUpdateLookupField - which owns the schema, the table and both grids - is
 * shared with the "before" snapshot the delegate compares against. Only the three properties that
 * live directly on InsertUpdateMeta (connection, commit size, "do not perform any updates") survive
 * the shallow copy as independent values, which is exactly the split the reporter describes.
 */
class InsertUpdateChangeDetectionTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  private static TransformMeta insertUpdateTransform() {
    InsertUpdateMeta meta = new InsertUpdateMeta();
    meta.setConnection("db");
    meta.setCommitSize("100");
    meta.getInsertUpdateLookupField().setSchemaName("public");
    meta.getInsertUpdateLookupField().setTableName("customers");
    meta.getInsertUpdateLookupField().getLookupKeys().add(new InsertUpdateKeyField());

    TransformMeta transformMeta = new TransformMeta("Insert / Update", meta);
    transformMeta.setTransformPluginId("InsertUpdate");
    return transformMeta;
  }

  private static boolean delegateSeesChange(TransformMeta before, TransformMeta after)
      throws Exception {
    return !before.getXml().equals(after.getXml());
  }

  @Test
  void changingTheTargetTableIsNotDetected() throws Exception {
    TransformMeta transformMeta = insertUpdateTransform();
    TransformMeta before = (TransformMeta) transformMeta.clone();

    // InsertUpdateDialog.getInfo(): setters on the *shared* nested lookup object.
    InsertUpdateMeta live = (InsertUpdateMeta) transformMeta.getTransform();
    live.getInsertUpdateLookupField().setSchemaName("reporting");
    live.getInsertUpdateLookupField().setTableName("orders");
    live.getInsertUpdateLookupField().getLookupKeys().clear();

    TransformMeta after = (TransformMeta) transformMeta.clone();

    System.out.println(
        "nested lookup object shared with snapshot: "
            + (((InsertUpdateMeta) before.getTransform()).getInsertUpdateLookupField()
                == live.getInsertUpdateLookupField()));
    System.out.println(
        "before snapshot already says 'orders': " + before.getXml().contains("orders"));

    assertTrue(delegateSeesChange(before, after), "changing schema/table/keys must be detected");
  }

  @Test
  void changingTheCommitSizeIsDetected() throws Exception {
    TransformMeta transformMeta = insertUpdateTransform();
    TransformMeta before = (TransformMeta) transformMeta.clone();

    // A property that lives directly on InsertUpdateMeta, so the shallow copy holds its own value.
    ((InsertUpdateMeta) transformMeta.getTransform()).setCommitSize("5000");

    TransformMeta after = (TransformMeta) transformMeta.clone();

    assertTrue(delegateSeesChange(before, after), "commit size is detected today");
  }
}
