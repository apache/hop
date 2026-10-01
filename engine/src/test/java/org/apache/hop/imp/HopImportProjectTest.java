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
package org.apache.hop.imp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The folder {@code hop-import --project} registers a new project at, and what happens when it
 * cannot register one (issue #8516).
 */
class HopImportProjectTest {

  private HopImport hopImport;
  private IVariables variables;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    hopImport = new HopImport();
    variables = new Variables();
    set("variables", variables);
    set("log", new LogChannel("HopImportProjectTest"));
  }

  /**
   * A relative output folder is pinned down before it is stored: the next {@code hop-import
   * --project} can run from any working directory.
   */
  @Test
  void relativeOutputFolderBecomesAnAbsoluteProjectHome() throws Exception {
    set("outputFolderName", "target/an-import-folder");

    String home = projectHomeToStore("file:///tmp/an-import-folder");

    assertEquals("/tmp/an-import-folder", home);
  }

  /** An absolute folder is stored as it was given, rather than as a VFS URI. */
  @Test
  void absoluteOutputFolderIsStoredAsGiven() throws Exception {
    set("outputFolderName", "/tmp/an-import-folder/");

    assertEquals("/tmp/an-import-folder/", projectHomeToStore("file:///tmp/an-import-folder"));
  }

  /**
   * A folder written with a variable stays portable: the ProjectHome extension point resolves it on
   * every read, so freezing it here would tie the project to this machine.
   */
  @Test
  void variableInTheOutputFolderSurvives() throws Exception {
    variables.setVariable("IMPORT_TARGET", "/tmp");
    set("outputFolderName", "${IMPORT_TARGET}/an-import-folder");

    assertEquals(
        "${IMPORT_TARGET}/an-import-folder", projectHomeToStore("file:///tmp/an-import-folder"));
  }

  /** A folder on a VFS location keeps its scheme: s3://bucket/folder is not /bucket/folder. */
  @Test
  void schemeOfANonLocalFolderIsKept() throws Exception {
    set("outputFolderName", "relative-so-it-gets-resolved");

    assertEquals("ram:///an-import-folder", projectHomeToStore("ram:///an-import-folder"));
  }

  /** Without a {@code --project} there is nothing to register and nothing to fail on. */
  @Test
  void noProjectRequestedAlwaysSucceeds() throws Exception {
    assertTrue(registerTargetProject("file:///tmp/an-import-folder"));
  }

  /**
   * Nothing registers the project without the projects plugin, which used to be logged and
   * forgotten: the import ran and the process still exited 0. Fail the run instead.
   */
  @Test
  void unregisteredProjectFailsTheRun() throws Exception {
    set("projectName", "Never Registered");
    set("outputFolderName", "/tmp/an-import-folder");

    assertFalse(registerTargetProject("file:///tmp/an-import-folder"));
  }

  private String projectHomeToStore(String validatedOutputFolder) throws Exception {
    return (String) invoke("projectHomeToStore", validatedOutputFolder);
  }

  private boolean registerTargetProject(String validatedOutputFolder) throws Exception {
    return (Boolean) invoke("registerTargetProject", validatedOutputFolder);
  }

  private Object invoke(String methodName, String validatedOutputFolder) throws Exception {
    Method method = HopImport.class.getDeclaredMethod(methodName, String.class);
    method.setAccessible(true);
    try {
      return method.invoke(hopImport, validatedOutputFolder);
    } catch (InvocationTargetException e) {
      throw (Exception) e.getCause();
    }
  }

  private void set(String fieldName, Object value) throws Exception {
    Field field = HopImport.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(hopImport, value);
  }
}
