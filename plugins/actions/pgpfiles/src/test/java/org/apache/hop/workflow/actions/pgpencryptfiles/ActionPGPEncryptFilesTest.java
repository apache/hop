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

package org.apache.hop.workflow.actions.pgpencryptfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActionPGPEncryptFilesTest {
  @BeforeEach
  void beforeEach() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    ActionPGPEncryptFiles action =
        ActionSerializationTestUtil.testSerialization(
            "/action-pgp-encrypt-files.xml", ActionPGPEncryptFiles.class);

    assertEquals("pgp-location", action.getGpgLocation());
    assertTrue(action.isArgFromPrevious());
    assertTrue(action.isAddResultFileNames());
    assertTrue(action.isDestinationIsAFile());
    assertTrue(action.isAddDate());
    assertTrue(action.isAddTime());
    assertTrue(action.isSpecifyFormat());
    assertEquals("date-time-format", action.getDateTimeFormat());
    assertEquals("10", action.getNrErrorsLessThan());
    assertEquals("success_when_at_least", action.getSuccessCondition());
    assertTrue(action.isAddDateBeforeExtension());
    assertTrue(action.isDoNotKeepFolderStructure());
    assertEquals("move_file", action.getIfFileExists());
    assertEquals("dest-folder", action.getDestinationFolder());
    assertEquals("overwrite_file", action.getIfMovedFileExists());
    assertEquals("date-time-format", action.getMovedDateTimeFormat());
    assertTrue(action.isCreateMoveToFolder());
    assertTrue(action.isAddMovedDate());
    assertTrue(action.isAddMovedTime());
    assertTrue(action.isSpecifyMoveFormat());
    assertTrue(action.isAddMovedDateBeforeExtension());
    assertTrue(action.isAsciiMode());
    assertEquals(3, action.getPgpFiles().size());
    ActionPGPEncryptFiles.PgpFile f = action.getPgpFiles().getFirst();
    assertEquals(ActionPGPEncryptFiles.ActionType.ENCRYPT, f.getActionType());
    assertEquals("folder1", f.getSourceFileFolder());
    assertEquals("user1", f.getUserId());
    assertEquals("target1", f.getDestinationFileFolder());
    assertEquals("wildcard1", f.getWildcard());
    f = action.getPgpFiles().get(1);
    assertEquals(ActionPGPEncryptFiles.ActionType.SIGN, f.getActionType());
    assertEquals("folder2", f.getSourceFileFolder());
    assertEquals("user2", f.getUserId());
    assertEquals("target2", f.getDestinationFileFolder());
    assertEquals("wildcard2", f.getWildcard());
    f = action.getPgpFiles().getLast();
    assertEquals(ActionPGPEncryptFiles.ActionType.SIGN_AND_ENCRYPT, f.getActionType());
    assertEquals("folder3", f.getSourceFileFolder());
    assertEquals("user3", f.getUserId());
    assertEquals("target3", f.getDestinationFileFolder());
    assertEquals("wildcard3", f.getWildcard());
  }
}
