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
 */

package org.apache.hop.workflow.actions.movefiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActionMoveFilesTest {
  @BeforeEach
  void beforeEach() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    ActionMoveFiles action =
        ActionSerializationTestUtil.testSerialization(
            "/action-move-files.xml", ActionMoveFiles.class);

    assertTrue(action.isMoveEmptyFolders());
    assertTrue(action.isArgFromPrevious());
    assertTrue(action.isIncludeSubfolders());
    assertTrue(action.isAddResultFilenames());
    assertTrue(action.isDestinationIsAFile());
    assertTrue(action.isCreateDestinationFolder());
    assertTrue(action.isAddDate());
    assertTrue(action.isAddTime());
    assertTrue(action.isSpecifyFormat());
    assertTrue(action.isAddDateBeforeExtension());
    assertTrue(action.isDoNotKeepFolderStructure());
    assertTrue(action.isCreateMoveToFolder());
    assertTrue(action.isAddMovedDate());
    assertTrue(action.isAddMovedTime());
    assertTrue(action.isSpecifyMoveFormat());
    assertTrue(action.isAddMovedDateBeforeExtension());
    assertTrue(action.isSimulate());
    assertEquals("dt-format", action.getDateTimeFormat());
    assertEquals("10", action.getNrErrorsLessThan());
    assertEquals("success_if_no_errors", action.getSuccessCondition());
    assertEquals("overwrite_file", action.getIfFileExists());
    assertEquals("destination-folder", action.getDestinationFolder());
    assertEquals("do_nothing", action.getIfMovedFileExists());
    assertEquals("moved-dt-format", action.getMovedDateTimeFormat());

    assertEquals(2, action.getFilesToMove().size());
    ActionMoveFiles.FileToMove f = action.getFilesToMove().getFirst();
    assertEquals("sourceFolder1", f.getSourceFileFolder());
    assertEquals("destinationFolder1", f.getDestinationFileFolder());
    assertEquals("wildcard1", f.getWildcard());
    f = action.getFilesToMove().getLast();
    assertEquals("sourceFolder2", f.getSourceFileFolder());
    assertEquals("destinationFolder2", f.getDestinationFileFolder());
    assertEquals("wildcard2", f.getWildcard());
  }
}
