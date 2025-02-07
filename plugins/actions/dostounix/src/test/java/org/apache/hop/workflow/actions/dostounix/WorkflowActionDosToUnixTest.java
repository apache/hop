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
package org.apache.hop.workflow.actions.dostounix;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.Test;

class WorkflowActionDosToUnixTest {

  @Test
  void testNewSerialization() throws Exception {
    ActionDosToUnix meta =
        ActionSerializationTestUtil.testSerialization(
            "/dos-unix-converter-action.xml", ActionDosToUnix.class);

    assertEquals("success_if_no_errors", meta.getSuccessCondition());
    assertEquals("nothing", meta.getResultFilenames());
    assertEquals("10", meta.getNrErrorsLessThan());
    assertFalse(meta.isArgFromPrevious());
    assertTrue(meta.isIncludeSubFolders());
  }

  @Test
  public void testClone() throws Exception {
    ActionDosToUnix meta =
        ActionSerializationTestUtil.testSerialization(
            "/dos-unix-converter-action.xml", ActionDosToUnix.class);

    ActionDosToUnix clone = (ActionDosToUnix) meta.clone();

    assertEquals(clone.getSuccessCondition(), meta.getSuccessCondition());
    assertEquals(clone.getResultFilenames(), meta.getResultFilenames());
    assertEquals(clone.getNrErrorsLessThan(), meta.getNrErrorsLessThan());
    assertEquals(clone.isArgFromPrevious(), meta.isArgFromPrevious());
    assertEquals(clone.isIncludeSubFolders(), meta.isIncludeSubFolders());
  }
}
