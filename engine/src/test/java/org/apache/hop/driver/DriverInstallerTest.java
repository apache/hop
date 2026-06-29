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

package org.apache.hop.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.driver.DriverResolver.ResolvedArtifact;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DriverInstallerTest {

  private String original;

  @BeforeEach
  void capture() {
    original = System.getProperty(Const.HOP_SHARED_JDBC_FOLDERS);
  }

  @AfterEach
  void restore() {
    if (original == null) {
      System.clearProperty(Const.HOP_SHARED_JDBC_FOLDERS);
    } else {
      System.setProperty(Const.HOP_SHARED_JDBC_FOLDERS, original);
    }
  }

  private static void setFolders(String value) {
    if (value == null) {
      System.clearProperty(Const.HOP_SHARED_JDBC_FOLDERS);
    } else {
      System.setProperty(Const.HOP_SHARED_JDBC_FOLDERS, value);
    }
  }

  @Test
  void installFolderDefaultsToLibJdbcWhenUnset() {
    setFolders(null);
    assertEquals(new File("lib/jdbc").getAbsoluteFile(), DriverInstaller.defaultInstallFolder());
  }

  @Test
  void installFolderSkipsLibJdbcWhenAnotherFolderIsConfigured() {
    setFolders("lib/jdbc,some/other/folder");
    assertEquals(
        new File("some/other/folder").getAbsoluteFile(), DriverInstaller.defaultInstallFolder());
  }

  @Test
  void installFolderPicksFirstNonLibJdbcRegardlessOfOrder() {
    setFolders("custom/a,lib/jdbc");
    assertEquals(new File("custom/a").getAbsoluteFile(), DriverInstaller.defaultInstallFolder());
  }

  @Test
  void installFolderFallsBackToLibJdbcWhenItIsTheOnlyEntry() {
    setFolders("lib/jdbc");
    assertEquals(new File("lib/jdbc").getAbsoluteFile(), DriverInstaller.defaultInstallFolder());
  }

  @Test
  void installFolderTreatsAbsoluteLibJdbcAsBundled() {
    setFolders("/opt/hop/lib/jdbc,/opt/hop/extra-drivers");
    assertEquals(
        new File("/opt/hop/extra-drivers").getAbsoluteFile(),
        DriverInstaller.defaultInstallFolder());
  }

  @Test
  void findInstalledReturnsOnlyMatchingArtifactVersions(@TempDir File dir) throws Exception {
    Files.createFile(new File(dir, "ojdbc11-23.5.0.24.07.jar").toPath());
    Files.createFile(new File(dir, "ojdbc11-21.0.0.jar").toPath());
    Files.createFile(new File(dir, "ojdbc8-1.0.jar").toPath()); // different artifact
    Files.createFile(new File(dir, "ojdbc11-notes.txt").toPath()); // not a jar
    setFolders(dir.getAbsolutePath());

    List<DriverInstaller.InstalledDriver> found = DriverInstaller.findInstalled("ojdbc11");

    assertEquals(2, found.size());
    assertTrue(found.stream().anyMatch(d -> "23.5.0.24.07".equals(d.version())));
    assertTrue(found.stream().anyMatch(d -> "21.0.0".equals(d.version())));
  }

  @Test
  void removeOtherVersionsDeletesDifferentVersionsOnly(@TempDir File dir) throws Exception {
    File oldVersion = new File(dir, "ojdbc11-21.0.0.jar");
    File unrelated = new File(dir, "unrelated-1.0.jar");
    File keep = new File(dir, "ojdbc11-23.5.0.24.07.jar");
    Files.createFile(oldVersion.toPath());
    Files.createFile(unrelated.toPath());
    Files.createFile(keep.toPath());

    DriverInstaller.removeOtherVersions(dir, new ResolvedArtifact("ojdbc11", "23.5.0.24.07", keep));

    assertFalse(oldVersion.exists(), "the old version should be removed");
    assertTrue(keep.exists(), "the version being installed should be kept");
    assertTrue(unrelated.exists(), "unrelated jars must be left alone");
  }
}
