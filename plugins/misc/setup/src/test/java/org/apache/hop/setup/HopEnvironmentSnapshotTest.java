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

package org.apache.hop.setup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class HopEnvironmentSnapshotTest {

  @Test
  void existingFoldersPreferEnvironment() {
    assertEquals(
        "/data/hop", HopEnvironmentSnapshot.existingFolder("/data/hop", "./config", "./config"));
  }

  @Test
  void existingFoldersPreferExplicitProperty() {
    assertEquals(
        "/opt/hop/config",
        HopEnvironmentSnapshot.existingFolder(null, "/opt/hop/config", "./config"));
  }

  @Test
  void existingFoldersUseRelativeInstallFallback() {
    assertEquals("./config", HopEnvironmentSnapshot.existingFolder(null, null, "./config"));
    assertEquals("./audit", HopEnvironmentSnapshot.existingFolder("", "", "./audit"));
  }

  @Test
  void userOptionsKeepsPlainValues() {
    assertEquals("-Xmx4096m", HopEnvironmentSnapshot.userOptions("-Xmx4096m"));
    assertNull(HopEnvironmentSnapshot.userOptions(null));
  }

  @Test
  void userOptionsDiscardsLauncherExpandedValue() {
    assertNull(
        HopEnvironmentSnapshot.userOptions(
            "-Xmx2048m -DHOP_SHARED_JDBC_FOLDERS=\"C:\\java\\hop\\jdbc-shared\""
                + " -DHOP_PLATFORM_OS=Windows -DHOP_PLATFORM_RUNTIME=GUI"
                + " --add-opens java.base/java.lang=ALL-UNNAMED"));
  }
}
