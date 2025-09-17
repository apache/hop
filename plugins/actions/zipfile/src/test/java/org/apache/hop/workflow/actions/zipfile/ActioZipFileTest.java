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

package org.apache.hop.workflow.actions.zipfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.Test;

class ActioZipFileTest {

  @Test
  void testSerialization() throws Exception {
    HopClientEnvironment.init();
    MemoryMetadataProvider provider = new MemoryMetadataProvider();

    ActionZipFile action =
        ActionSerializationTestUtil.testSerialization(
            "/zip-files-action.xml", ActionZipFile.class, provider);

    assertFalse(action.isSpecifyFormat());
    assertTrue(action.isAddDate());
    assertTrue(action.isAddFileToResult());
    assertTrue(action.isAddTime());
    assertEquals(0, action.getAfterZip());
    assertEquals(1, action.getCompressionRate());
    assertFalse(action.isCreateMoveToDirectory());
    assertTrue(action.isCreateParentFolder());
    assertEquals(2, action.getIfZipFileExists());
    assertTrue(action.isIncludingSubFolders());
    assertFalse(action.isFromPrevious());
    assertEquals("${java.io.tmpdir}/zip-action/", action.getSourceDirectory());
    assertEquals("2 : project/file.txt", action.getStoredSourcePathDepth());
    assertEquals("${PRM_YEAR}.*.txt", action.getWildCard());
    assertEquals("${java.io.tmpdir}/zip-action-archive/${PRM_YEAR}.zip", action.getZipFilename());
  }
}
