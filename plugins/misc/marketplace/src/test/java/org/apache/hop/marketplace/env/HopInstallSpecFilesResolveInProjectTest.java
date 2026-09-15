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

package org.apache.hop.marketplace.env;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A relative env file reference configured on a lifecycle environment must be anchored at the
 * project home. Anchoring it at {@code user.dir} points it into the Hop installation directory
 * (issue #8012).
 */
class HopInstallSpecFilesResolveInProjectTest {

  private static IVariables vars() {
    IVariables variables = new Variables();
    variables.initializeFrom(null);
    return variables;
  }

  private static IVariables variablesWithProjectHome(String home) {
    IVariables variables = vars();
    if (home != null) {
      variables.setVariable("PROJECT_HOME", home);
    }
    return variables;
  }

  @Test
  void relativeReferenceIsAnchoredAtTheProjectHome() {
    assertEquals(
        "/home/me/projects/sales/config/hop-env.yaml",
        HopInstallSpecFiles.resolveInProject(
            "config/hop-env.yaml", vars(), "/home/me/projects/sales"));
  }

  @Test
  void bareFilenameIsAnchoredToo() {
    assertEquals(
        "/home/me/projects/sales/hop-env.yaml",
        HopInstallSpecFiles.resolveInProject("hop-env.yaml", vars(), "/home/me/projects/sales"));
  }

  @Test
  void projectHomeArgumentMayItselfBeAVariable() {
    IVariables variables = variablesWithProjectHome("/home/me/projects/sales");
    assertEquals(
        "/home/me/projects/sales/config/hop-env.yaml",
        HopInstallSpecFiles.resolveInProject("config/hop-env.yaml", variables, "${PROJECT_HOME}"));
  }

  @Test
  void projectHomeVariableIsUsedWhenNoHomeIsPassed() {
    IVariables variables = variablesWithProjectHome("/home/me/projects/sales");
    assertEquals(
        "/home/me/projects/sales/config/hop-env.yaml",
        HopInstallSpecFiles.resolveInProject("config/hop-env.yaml", variables, null));
  }

  @Test
  void referenceUsingProjectHomeExplicitlyIsResolvedAndLeftAlone() {
    IVariables variables = variablesWithProjectHome("/home/me/projects/sales");
    assertEquals(
        "/home/me/projects/sales/hop-env.yaml",
        HopInstallSpecFiles.resolveInProject(
            "${PROJECT_HOME}/hop-env.yaml", variables, "/home/me/projects/sales"));
  }

  @Test
  void trailingSeparatorOnTheProjectHomeIsNotDoubled() {
    assertEquals(
        "/home/me/projects/sales/hop-env.yaml",
        HopInstallSpecFiles.resolveInProject("hop-env.yaml", vars(), "/home/me/projects/sales/"));
  }

  @Test
  void absoluteReferencesAreNeverAnchored() {
    assertEquals(
        "/etc/hop/hop-env.yaml",
        HopInstallSpecFiles.resolveInProject(
            "/etc/hop/hop-env.yaml", vars(), "/home/me/projects/sales"));
    assertEquals(
        "C:\\hop\\hop-env.yaml",
        HopInstallSpecFiles.resolveInProject(
            "C:\\hop\\hop-env.yaml", vars(), "/home/me/projects/sales"));
    assertEquals(
        "\\\\server\\share\\hop-env.yaml",
        HopInstallSpecFiles.resolveInProject(
            "\\\\server\\share\\hop-env.yaml", vars(), "/home/me/projects/sales"));
  }

  @Test
  void vfsReferencesAreNeverAnchored() {
    assertEquals(
        "s3://bucket/hop-env.yaml",
        HopInstallSpecFiles.resolveInProject(
            "s3://bucket/hop-env.yaml", vars(), "/home/me/projects/sales"));
    assertEquals(
        "file:///etc/hop/hop-env.yaml",
        HopInstallSpecFiles.resolveInProject(
            "file:///etc/hop/hop-env.yaml", vars(), "/home/me/projects/sales"));
  }

  @Test
  void withoutAProjectHomeTheReferenceIsHandedOnUnchanged() {
    // No project home anywhere: keep the previous behaviour rather than invent a base.
    assertEquals(
        "config/hop-env.yaml",
        HopInstallSpecFiles.resolveInProject("config/hop-env.yaml", vars(), null));
    assertEquals(
        "config/hop-env.yaml",
        HopInstallSpecFiles.resolveInProject("config/hop-env.yaml", vars(), "   "));
  }

  @Test
  void aRelativeProjectHomeIsNoBetterThanNone() {
    assertEquals(
        "hop-env.yaml",
        HopInstallSpecFiles.resolveInProject("hop-env.yaml", vars(), "projects/sales"));
  }

  @Test
  void blankReferencesStayBlank() {
    assertNull(HopInstallSpecFiles.resolveInProject(null, vars(), "/home/me"));
    assertEquals("", HopInstallSpecFiles.resolveInProject("   ", vars(), "/home/me").trim());
  }

  @Test
  void nullVariablesAreTolerated() {
    assertEquals(
        "/home/me/hop-env.yaml",
        HopInstallSpecFiles.resolveInProject("hop-env.yaml", null, "/home/me"));
  }

  /**
   * The defect itself: a project relative reference handed straight to VFS is looked up under
   * {@code user.dir} — the Hop install for a launched Hop GUI — so the file the user configured is
   * not found, while the same reference anchored at the project home is.
   */
  @Test
  void relativeReferenceIsOnlyFoundOnceAnchoredAtTheProject(@TempDir Path projectHome)
      throws Exception {
    Path config = Files.createDirectories(projectHome.resolve("config"));
    Files.writeString(
        config.resolve("hop-env.yaml"),
        "version: \"1.0\"\nhopVersion: \"2.19.0\"\n",
        StandardCharsets.UTF_8);
    IVariables variables = vars();

    assertFalse(
        HopInstallSpecFiles.exists(
            HopInstallSpecFiles.resolve("config/hop-env.yaml", variables), variables),
        "a relative reference resolves against user.dir, not the project");
    assertTrue(
        HopInstallSpecFiles.exists(
            HopInstallSpecFiles.resolveInProject(
                "config/hop-env.yaml", variables, projectHome.toString()),
            variables));
  }

  @Test
  void isRelativeRecognisesSchemesDrivesAndRoots() {
    assertTrue(HopInstallSpecFiles.isRelative("hop-env.yaml"));
    assertTrue(HopInstallSpecFiles.isRelative("config/hop-env.yaml"));
    assertTrue(HopInstallSpecFiles.isRelative("../hop-env.yaml"));
    assertFalse(HopInstallSpecFiles.isRelative("/hop-env.yaml"));
    assertFalse(HopInstallSpecFiles.isRelative("D:/hop/hop-env.yaml"));
    assertFalse(HopInstallSpecFiles.isRelative("hdfs://nn:8020/hop-env.yaml"));
    assertFalse(HopInstallSpecFiles.isRelative(null));
  }
}
