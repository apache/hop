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

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import java.util.Set;
import org.apache.hop.ui.core.widget.ControlSpaceKeyAdapter.VarCategory;
import org.junit.jupiter.api.Test;

class ControlSpaceKeyAdapterTest {

  private static final Set<String> EMPTY = Set.of();

  @Test
  void secretValuesAreMasked() {
    assertTrue(ControlSpaceKeyAdapter.isSecret("MY_PASSWORD"));
    assertTrue(ControlSpaceKeyAdapter.isSecret("db.secret.key"));
    assertTrue(ControlSpaceKeyAdapter.isSecret("API_TOKEN"));
    assertTrue(ControlSpaceKeyAdapter.isSecret("passwd_file"));
  }

  @Test
  void nonSecretValuesAreNotMasked() {
    assertFalse(ControlSpaceKeyAdapter.isSecret("PROJECT_HOME"));
    assertFalse(ControlSpaceKeyAdapter.isSecret("Internal.Pipeline.Filename.Name"));
    assertFalse(ControlSpaceKeyAdapter.isSecret(null));
  }

  @Test
  void deprecatedWinsOverEveryOtherCategory() {
    // A deprecated variable is shown as deprecated even when it is also internal.
    assertEquals(
        VarCategory.DEPRECATED,
        ControlSpaceKeyAdapter.categorize(
            "Internal.Job.Filename.Directory",
            Set.of("Internal.Job.Filename.Directory"),
            EMPTY,
            EMPTY,
            EMPTY,
            new Properties()));
  }

  @Test
  void pluginVariablesAreProjectScopedAndWinOverInternal() {
    assertEquals(
        VarCategory.PROJECT,
        ControlSpaceKeyAdapter.categorize(
            "PROJECT_HOME", EMPTY, Set.of("PROJECT_HOME"), EMPTY, EMPTY, new Properties()));
    // Plugin contribution is checked before the Internal.* rule.
    assertEquals(
        VarCategory.PROJECT,
        ControlSpaceKeyAdapter.categorize(
            "Internal.Custom", EMPTY, Set.of("Internal.Custom"), EMPTY, EMPTY, new Properties()));
  }

  @Test
  void internalVariablesAreDetectedByPrefix() {
    assertEquals(
        VarCategory.INTERNAL,
        ControlSpaceKeyAdapter.categorize(
            "Internal.Pipeline.Filename.Name", EMPTY, EMPTY, EMPTY, EMPTY, new Properties()));
  }

  @Test
  void systemPropertiesAndSystemSettingsAreSystemScoped() {
    Properties props = new Properties();
    props.setProperty("user.home", "/home/hop");
    assertEquals(
        VarCategory.SYSTEM,
        ControlSpaceKeyAdapter.categorize("user.home", EMPTY, EMPTY, EMPTY, EMPTY, props));
    assertEquals(
        VarCategory.SYSTEM,
        ControlSpaceKeyAdapter.categorize(
            "HOP_SYSTEM_SETTING",
            EMPTY,
            EMPTY,
            Set.of("HOP_SYSTEM_SETTING"),
            EMPTY,
            new Properties()));
  }

  @Test
  void registeredApplicationVariablesAreApplicationScoped() {
    assertEquals(
        VarCategory.APPLICATION,
        ControlSpaceKeyAdapter.categorize(
            "HOP_CONFIG_FOLDER",
            EMPTY,
            EMPTY,
            EMPTY,
            Set.of("HOP_CONFIG_FOLDER"),
            new Properties()));
  }

  @Test
  void unknownVariablesFallBackToCustom() {
    assertEquals(
        VarCategory.CUSTOM,
        ControlSpaceKeyAdapter.categorize(
            "MY_OWN_VARIABLE", EMPTY, EMPTY, EMPTY, EMPTY, new Properties()));
  }
}
