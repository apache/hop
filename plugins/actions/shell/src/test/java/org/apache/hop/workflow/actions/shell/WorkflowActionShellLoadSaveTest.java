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

package org.apache.hop.workflow.actions.shell;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.junit.jupiter.api.Test;

class WorkflowActionShellLoadSaveTest {
  @Test
  void testSerialization() throws Exception {
    ActionShell meta =
        ActionSerializationTestUtil.testSerialization("/shell-action.xml", ActionShell.class);

    assertEquals("${PROJECT_HOME}/0002-shell-test.sh", meta.getFilename());
    assertEquals("${PROJECT_HOME}", meta.getWorkDirectory());
    assertEquals(1, meta.getArguments().size());
  }

  @Test
  void testClone() throws Exception {
    ActionShell meta =
        ActionSerializationTestUtil.testSerialization("/shell-action.xml", ActionShell.class);

    ActionShell clone = (ActionShell) meta.clone();
    assertEquals(clone.getFilename(), meta.getFilename());
    assertEquals(clone.getWorkDirectory(), meta.getWorkDirectory());
    assertEquals(clone.getArguments().size(), meta.getArguments().size());
  }
}
