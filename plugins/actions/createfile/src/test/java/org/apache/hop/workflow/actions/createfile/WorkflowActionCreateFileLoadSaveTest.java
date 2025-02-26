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
package org.apache.hop.workflow.actions.createfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.Test;

class WorkflowActionCreateFileLoadSaveTest {

  @Test
  void testSerialization() throws Exception {
    ActionCreateFile action =
        ActionSerializationTestUtil.testSerialization(
            "/create-file-action.xml", ActionCreateFile.class);

    assertFalse(action.isAddFilenameToResult());
    assertTrue(action.isFailIfFileExists());
    assertEquals("${TARGET_FOLDER}/three/hop-config.json", action.getFilename());
  }

  @Test
  void testClone() throws Exception {
    ActionCreateFile action =
        ActionSerializationTestUtil.testSerialization(
            "/create-file-action.xml", ActionCreateFile.class);

    ActionCreateFile clone = (ActionCreateFile) action.clone();

    assertEquals(clone.isAddFilenameToResult(), action.isAddFilenameToResult());
    assertEquals(clone.isFailIfFileExists(), action.isFailIfFileExists());
    assertEquals(clone.getFilename(), action.getFilename());
  }
}
