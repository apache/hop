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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ActionCopyMoveResultFilenames} */
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

  @Test
  void checkRequiresDestinationFolderWhenCopying() {
    ActionCopyMoveResultFilenames action = new ActionCopyMoveResultFilenames();
    action.setAction(ActionCopyMoveResultFilenames.ActionType.COPY);

    List<String> errors = errors(check(action));

    assertEquals(1, errors.size());
    assertTrue(errors.getFirst().contains("destinationFolder cannot be null or blank"));
    assertFalse(errors.getFirst().contains("filename"));
  }

  @Test
  void checkRequiresDestinationFolderWhenMoving() {
    ActionCopyMoveResultFilenames action = new ActionCopyMoveResultFilenames();
    action.setAction(ActionCopyMoveResultFilenames.ActionType.MOVE);

    List<String> errors = errors(check(action));

    assertEquals(1, errors.size());
    assertTrue(errors.getFirst().contains("destinationFolder cannot be null or blank"));
  }

  @Test
  void checkAcceptsADestinationFolder() {
    ActionCopyMoveResultFilenames action = new ActionCopyMoveResultFilenames();
    action.setAction(ActionCopyMoveResultFilenames.ActionType.COPY);
    action.setDestinationFolder("/tmp/hop-result");

    List<ICheckResult> remarks = check(action);

    assertTrue(errors(remarks).isEmpty());
    assertTrue(
        remarks.stream().anyMatch(remark -> remark.getText().contains("destinationFolder is OK")));
    assertTrue(remarks.stream().noneMatch(remark -> remark.getText().contains("filename")));
  }

  @Test
  void checkSkipsDestinationFolderWhenDeleting() {
    ActionCopyMoveResultFilenames action = new ActionCopyMoveResultFilenames();
    action.setAction(ActionCopyMoveResultFilenames.ActionType.DELETE);

    List<ICheckResult> remarks = check(action);

    assertTrue(remarks.isEmpty());
  }

  private static List<ICheckResult> check(ActionCopyMoveResultFilenames action) {
    List<ICheckResult> remarks = new ArrayList<>();
    action.check(remarks, null, action, null);
    return remarks;
  }

  private static List<String> errors(List<ICheckResult> remarks) {
    List<String> errors = new ArrayList<>();
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        errors.add(remark.getText());
      }
    }
    return errors;
  }
}
