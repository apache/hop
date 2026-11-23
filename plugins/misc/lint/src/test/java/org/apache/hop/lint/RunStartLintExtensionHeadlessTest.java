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

package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.lang.reflect.Method;
import org.apache.hop.core.Const;
import org.apache.hop.ui.testing.WithoutSwt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * hop-run fires the GUI's "before start" extension point for every workflow it runs. Without SWT
 * the lint handlers must leave it alone: touching HopGui there fails with a NoClassDefFoundError.
 */
class RunStartLintExtensionHeadlessTest {

  private String runtime;

  @BeforeEach
  void runLikeHopRun() {
    runtime = System.getProperty(Const.HOP_PLATFORM_RUNTIME);
    System.setProperty(Const.HOP_PLATFORM_RUNTIME, "Run");
  }

  @AfterEach
  void restore() {
    if (runtime == null) {
      System.clearProperty(Const.HOP_PLATFORM_RUNTIME);
    } else {
      System.setProperty(Const.HOP_PLATFORM_RUNTIME, runtime);
    }
  }

  @Test
  void aWorkflowStartsWithoutSwt() throws Exception {
    assertRunsWithoutSwt(WorkflowBeforeStartLintExtension.class.getName());
  }

  @Test
  void aPipelineStartsWithoutSwt() throws Exception {
    assertRunsWithoutSwt(PipelineBeforeStartLintExtension.class.getName());
  }

  private static void assertRunsWithoutSwt(String className) throws Exception {
    try (WithoutSwt withoutSwt = new WithoutSwt()) {
      Class<?> extensionClass = Class.forName(className, true, withoutSwt);
      Object extension = extensionClass.getConstructor().newInstance();
      Method call =
          extensionClass.getMethod(
              "callExtensionPoint",
              Class.forName("org.apache.hop.core.logging.ILogChannel", false, withoutSwt),
              Class.forName("org.apache.hop.core.variables.IVariables", false, withoutSwt),
              Object.class);
      assertDoesNotThrow(() -> call.invoke(extension, null, null, null));
    }
  }
}
