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
 *
 */

package org.apache.hop.workflow.actions.checkfilelocked;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ActionCheckFilesLockedTest {

  @Test
  void testSerialization() throws Exception {
    HopClientEnvironment.init();
    MemoryMetadataProvider provider = new MemoryMetadataProvider();

    ActionCheckFilesLocked action =
        ActionSerializationTestUtil.testSerialization(
            "/check-files-locked-action.xml", ActionCheckFilesLocked.class, provider);

    Assertions.assertFalse(action.isArgFromPrevious());
    Assertions.assertTrue(action.isIncludeSubfolders());
    Assertions.assertEquals(2, action.getCheckedFiles().size());
    Assertions.assertEquals("/tmp/folder1", action.getCheckedFiles().get(0).getName());
    Assertions.assertEquals(".*\\.txt$", action.getCheckedFiles().get(0).getWildcard());
    Assertions.assertEquals("/tmp/folder2", action.getCheckedFiles().get(1).getName());
    Assertions.assertEquals(".*\\.zip$", action.getCheckedFiles().get(1).getWildcard());
  }

  /**
   * Resource export clones every action before serializing it. A clone that loses its name or
   * plugin id ends up in the export with an empty name and without its type, and the exported
   * workflow then fails to load on a remote server. See issue #5644.
   */
  @Test
  void cloneKeepsNamePluginIdAndFields() {
    ActionCheckFilesLocked action = new ActionCheckFilesLocked("Check files locked");
    action.setDescription("a description");
    action.setPluginId("CHECK_FILES_LOCKED");
    action.setIncludeSubfolders(true);
    action.getCheckedFiles().add(new ActionCheckFilesLocked.CheckedFile("/tmp/file", ".*\\.txt$"));

    ActionCheckFilesLocked copy = (ActionCheckFilesLocked) action.clone();

    Assertions.assertEquals("Check files locked", copy.getName());
    Assertions.assertEquals("a description", copy.getDescription());
    Assertions.assertEquals("CHECK_FILES_LOCKED", copy.getPluginId());
    Assertions.assertTrue(copy.isIncludeSubfolders());
    Assertions.assertEquals(1, copy.getCheckedFiles().size());
    Assertions.assertEquals("/tmp/file", copy.getCheckedFiles().get(0).getName());
  }
}
