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

package org.apache.hop.workflow.actions.copymoveresultfilenames;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.Test;

class ActionCopyMoveResultFilenamesTest {
  @Test
  void testSerializationRoundTrip() throws Exception {
    ActionCopyMoveResultFilenames action =
        ActionSerializationTestUtil.testSerialization(
            "/action-copy-move-result-filenames.xml", ActionCopyMoveResultFilenames.class);

    assertEquals("folder", action.getFolderName());
    assertTrue(action.isSpecifyWildcard());
    assertEquals("wildcard", action.getWildcard());
    assertEquals("exclusionWildcard", action.getWildcardExclude());
    assertEquals("destination", action.getDestinationFolder());
    assertEquals("10", action.getNrErrorsLessThan());
    assertEquals("success_when_at_least", action.getSuccessCondition());
    assertTrue(action.isAddDate());
    assertTrue(action.isAddTime());
    assertTrue(action.isSpecifyFormat());
    assertEquals("dt_format", action.getDateTimeFormat());
    assertEquals(ActionCopyMoveResultFilenames.ActionType.MOVE, action.getAction());
    assertTrue(action.isAddDateBeforeExtension());
    assertTrue(action.isOverwriteFile());
    assertTrue(action.isCreateDestinationFolder());
    assertTrue(action.isRemovedSourceFilename());
    assertTrue(action.isAddDestinationFilename());
  }
}
