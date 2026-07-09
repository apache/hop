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

package org.apache.hop.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for issue #7062: "Load of Unittest does not work if unit test base path is
 * edited".
 *
 * <p>When the unit-test base path is a sub-folder (e.g. {@code ${PROJECT_HOME}/unittests}) that
 * does not contain the pipeline, the stored pipeline filename stays absolute. On Windows an
 * absolute path is {@code C:\...} / {@code C:/...}, which used to be misclassified as relative, so
 * the base path was prepended and the unit-test dropdown never matched the open pipeline.
 */
class PipelineUnitTestBasePathTest {

  /** Baseline: base path == project root, the pipeline is stored as a relative "./" path. */
  @Test
  void relativePathUnderProjectRootResolvesBackToPipeline() {
    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "/home/me/project");

    PipelineUnitTest test = new PipelineUnitTest();
    test.setBasePath("${PROJECT_HOME}");
    test.setPipelineFilename("./test.hpl");

    assertEquals("/home/me/project/./test.hpl", test.calculateCompletePipelineFilename(variables));
  }

  /**
   * The core of issue #7062: a Windows drive-letter absolute path must be returned untouched even
   * when the base path is a sub-folder, so it can match the open pipeline again.
   */
  @Test
  void windowsAbsolutePipelinePathIsReturnedUnchanged() {
    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "C:/Users/me/project");

    PipelineUnitTest test = new PipelineUnitTest();
    test.setBasePath("${PROJECT_HOME}/unittests");
    String windowsAbsolute = "C:/Users/me/project/test.hpl";
    test.setPipelineFilename(windowsAbsolute);

    String complete = test.calculateCompletePipelineFilename(variables);
    assertEquals(
        windowsAbsolute,
        complete,
        "Windows absolute pipeline path must round-trip unchanged (base path must not be prepended)");
  }

  /** Windows paths with backslashes must be handled the same way. */
  @Test
  void windowsBackslashAbsolutePipelinePathIsReturnedUnchanged() {
    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "C:\\Users\\me\\project");

    PipelineUnitTest test = new PipelineUnitTest();
    test.setBasePath("${PROJECT_HOME}\\unittests");
    String windowsAbsolute = "C:\\Users\\me\\project\\test.hpl";
    test.setPipelineFilename(windowsAbsolute);

    assertEquals(windowsAbsolute, test.calculateCompletePipelineFilename(variables));
  }

  /** POSIX absolute paths kept working (regression guard). */
  @Test
  void posixAbsolutePipelinePathIsReturnedUnchanged() {
    IVariables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "/home/me/project");

    PipelineUnitTest test = new PipelineUnitTest();
    test.setBasePath("${PROJECT_HOME}/unittests");
    test.setPipelineFilename("/home/me/project/test.hpl");

    assertEquals("/home/me/project/test.hpl", test.calculateCompletePipelineFilename(variables));
  }
}
