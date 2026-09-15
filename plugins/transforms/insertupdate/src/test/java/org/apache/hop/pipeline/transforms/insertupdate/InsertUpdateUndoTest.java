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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The shallow clone behind issue #8022 also corrupts undo. HopGuiPipelineTransformDelegate stores
 * the same aliased "before" snapshot as the undo entry, and HopGuiPipelineUndoDelegate restores it
 * with replaceMeta(). Anything that lives behind the aliased reference was never captured, so undo
 * rolls back the top-level properties and silently keeps the edited ones.
 */
class InsertUpdateUndoTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void undoRestoresTheWholeTransform() throws Exception {
    InsertUpdateMeta meta = new InsertUpdateMeta();
    meta.setConnection("db");
    meta.setCommitSize("100");
    meta.getInsertUpdateLookupField().setSchemaName("public");
    meta.getInsertUpdateLookupField().setTableName("customers");

    TransformMeta transformMeta = new TransformMeta("Insert / Update", meta);
    transformMeta.setTransformPluginId("InsertUpdate");

    // The delegate's snapshot, which also becomes the undo entry's "previous" state.
    TransformMeta before = (TransformMeta) transformMeta.clone();

    // The user edits both a top-level property and the target table in one dialog session.
    InsertUpdateMeta live = (InsertUpdateMeta) transformMeta.getTransform();
    live.setCommitSize("5000");
    live.getInsertUpdateLookupField().setTableName("orders");

    // The XML diff sees the commit size, so the edit *is* detected and an undo entry is recorded.
    TransformMeta after = (TransformMeta) transformMeta.clone();
    org.junit.jupiter.api.Assertions.assertNotEquals(
        before.getXml(), after.getXml(), "precondition: this edit is detected");

    // HopGuiPipelineUndoDelegate, case ChangeTransform.
    transformMeta.replaceMeta((TransformMeta) before.clone());

    InsertUpdateMeta restored = (InsertUpdateMeta) transformMeta.getTransform();
    System.out.println("commit size after undo: " + restored.getCommitSize());
    System.out.println(
        "table name  after undo: " + restored.getInsertUpdateLookupField().getTableName());

    assertEquals("100", restored.getCommitSize(), "commit size is rolled back");
    assertEquals(
        "customers",
        restored.getInsertUpdateLookupField().getTableName(),
        "the target table must be rolled back too");
  }
}
