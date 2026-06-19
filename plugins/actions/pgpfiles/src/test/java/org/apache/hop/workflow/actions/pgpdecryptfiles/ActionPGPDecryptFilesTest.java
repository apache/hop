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

package org.apache.hop.workflow.actions.pgpdecryptfiles;

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

class ActionPGPDecryptFilesTest {
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
    ActionPGPDecryptFiles action =
        ActionSerializationTestUtil.testSerialization(
            "/action-pgp-decrypt-files.xml", ActionPGPDecryptFiles.class);

    assertEquals("pgp-location", action.getGpgLocation());
    assertTrue(action.isArgFromPrevious());
    assertTrue(action.isIncludeSubFolders());
    assertTrue(action.isAddResultFilenames());
    assertTrue(action.isDestinationIsAFile());
    assertTrue(action.isCreateDestinationFolder());
    assertTrue(action.isAddDate());
    assertTrue(action.isAddTime());
    assertTrue(action.isSpecifyFormat());
    assertEquals("dt-format", action.getDateTimeFormat());
    assertEquals("10", action.getNrErrorsLessThan());
    assertEquals("success_if_no_errors", action.getSuccessCondition());
    assertTrue(action.isAddDateBeforeExtension());
    assertTrue(action.isDoNotKeepFolderStructure());
    assertEquals("move_file", action.getIfFileExists());
    assertEquals("dest-folder", action.getDestinationFolder());
    assertEquals("move_file", action.getIfFileExists());
    assertEquals("move-to-format", action.getMovedDateTimeFormat());
    assertTrue(action.isCreateMoveToFolder());
    assertTrue(action.isAddMovedDate());
    assertTrue(action.isAddMovedTime());
    assertTrue(action.isSpecifyMoveFormat());
    assertTrue(action.isAddMovedDateBeforeExtension());
    assertEquals(3, action.getFilesToDecrypt().size());
    ActionPGPDecryptFiles.FileToDecrypt f = action.getFilesToDecrypt().getFirst();
    assertEquals("source1", f.getSourceFileFolder());
    assertEquals("destination1", f.getDestinationFileFolder());
    assertEquals("wildcard1", f.getWildcard());
    assertEquals("pass1", f.getPassphrase());

    f = action.getFilesToDecrypt().get(1);
    assertEquals("source2", f.getSourceFileFolder());
    assertEquals("destination2", f.getDestinationFileFolder());
    assertEquals("wildcard2", f.getWildcard());
    assertEquals("pass2", f.getPassphrase());

    f = action.getFilesToDecrypt().getLast();
    assertEquals("source3", f.getSourceFileFolder());
    assertEquals("destination3", f.getDestinationFileFolder());
    assertEquals("wildcard3", f.getWildcard());
    assertEquals("pass3", f.getPassphrase());
  }
}
