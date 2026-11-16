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

package org.apache.hop.testing.xp;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Method;
import org.apache.hop.core.Const;
import org.apache.hop.ui.testing.WithoutSwt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Working out a transform's fields asks for the active unit test in every run, also in hop-run,
 * hop-server and on Spark executors. Without SWT that must answer "none" without loading
 * TestingGuiPlugin, which needs SWT to load.
 */
class UnitTestLookupHeadlessTest {

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
  void thereIsNoActiveUnitTestWithoutAGui() throws Exception {
    try (WithoutSwt withoutSwt = new WithoutSwt()) {
      Class<?> pipelineMetaClass =
          Class.forName("org.apache.hop.pipeline.PipelineMeta", true, withoutSwt);
      Object pipelineMeta = pipelineMetaClass.getConstructor().newInstance();
      Method lookup =
          Class.forName("org.apache.hop.testing.util.DataSetConst", true, withoutSwt)
              .getMethod("getActiveUnitTest", pipelineMetaClass);
      assertNull(lookup.invoke(null, pipelineMeta));
    }
  }

  @Test
  void aTransformsFieldsAreWorkedOutWithoutSwt() throws Exception {
    try (WithoutSwt withoutSwt = new WithoutSwt()) {
      Class<?> pipelineMetaClass =
          Class.forName("org.apache.hop.pipeline.PipelineMeta", true, withoutSwt);
      Class<?> transformMetaClass =
          Class.forName("org.apache.hop.pipeline.transform.TransformMeta", true, withoutSwt);
      Class<?> rowMetaClass = Class.forName("org.apache.hop.core.row.IRowMeta", true, withoutSwt);
      Class<?> variablesClass =
          Class.forName("org.apache.hop.core.variables.IVariables", true, withoutSwt);
      Class<?> metadataClass =
          Class.forName("org.apache.hop.metadata.api.IHopMetadataProvider", true, withoutSwt);
      Class<?> extensionClass =
          Class.forName(
              "org.apache.hop.pipeline.PipelineMeta$GetFieldsExtension", true, withoutSwt);
      Object extension =
          extensionClass
              .getConstructor(
                  pipelineMetaClass,
                  transformMetaClass,
                  rowMetaClass,
                  String.class,
                  rowMetaClass.arrayType(),
                  transformMetaClass,
                  variablesClass,
                  metadataClass)
              .newInstance(
                  pipelineMetaClass.getConstructor().newInstance(),
                  transformMetaClass.getConstructor().newInstance(),
                  null,
                  "transform",
                  null,
                  null,
                  null,
                  null);
      Class<?> xpClass =
          Class.forName(PipelineGetFieldsExtensionPoint.class.getName(), true, withoutSwt);
      Object xp = xpClass.getConstructor().newInstance();
      Method call =
          xpClass.getMethod(
              "callExtensionPoint",
              Class.forName("org.apache.hop.core.logging.ILogChannel", true, withoutSwt),
              variablesClass,
              extensionClass);
      assertDoesNotThrow(() -> call.invoke(xp, null, null, extension));
    }
  }
}
