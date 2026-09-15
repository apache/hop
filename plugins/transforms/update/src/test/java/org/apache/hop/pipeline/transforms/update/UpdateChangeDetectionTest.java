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

package org.apache.hop.pipeline.transforms.update;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Reproduces issue #8022 for the Update transform: UpdateMeta.clone() is a shallow Object.clone()
 * so the nested UpdateLookupField (schema, table, lookup keys, update fields) is shared with the
 * "before" snapshot that HopGuiPipelineTransformDelegate compares against.
 */
class UpdateChangeDetectionTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  private static TransformMeta updateTransform() {
    UpdateMeta meta = new UpdateMeta();
    meta.setConnection("db");
    meta.setCommitSize("100");
    meta.getLookupField().setSchemaName("public");
    meta.getLookupField().setTableName("customers");
    meta.getLookupField().getLookupKeys().add(new UpdateKeyField());

    TransformMeta transformMeta = new TransformMeta("Update", meta);
    transformMeta.setTransformPluginId("Update");
    return transformMeta;
  }

  private static boolean delegateSeesChange(TransformMeta before, TransformMeta after)
      throws Exception {
    return !before.getXml().equals(after.getXml());
  }

  @Test
  void changingTheTargetTableIsNotDetected() throws Exception {
    TransformMeta transformMeta = updateTransform();
    TransformMeta before = (TransformMeta) transformMeta.clone();

    // UpdateDialog.getInfo(): setters on the *shared* nested lookup object.
    UpdateMeta live = (UpdateMeta) transformMeta.getTransform();
    live.getLookupField().setSchemaName("reporting");
    live.getLookupField().setTableName("orders");
    live.getLookupField().getLookupKeys().clear();

    TransformMeta after = (TransformMeta) transformMeta.clone();

    System.out.println(
        "nested lookup object shared with snapshot: "
            + (((UpdateMeta) before.getTransform()).getLookupField() == live.getLookupField()));

    assertTrue(delegateSeesChange(before, after), "changing schema/table/keys must be detected");
  }

  @Test
  void changingTheCommitSizeIsDetected() throws Exception {
    TransformMeta transformMeta = updateTransform();
    TransformMeta before = (TransformMeta) transformMeta.clone();

    ((UpdateMeta) transformMeta.getTransform()).setCommitSize("5000");

    TransformMeta after = (TransformMeta) transformMeta.clone();

    assertTrue(delegateSeesChange(before, after), "commit size is detected today");
  }
}
