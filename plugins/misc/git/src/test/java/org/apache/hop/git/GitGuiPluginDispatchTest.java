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

package org.apache.hop.git;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.junit.jupiter.api.Test;

/**
 * The explorer toolbar and context menu reach {@link GitGuiPlugin} through the {@link GuiRegistry},
 * which builds an instance of its own - one with no repository - when the registry has none for
 * those widgets. These tests cover the registration which prevents that.
 */
class GitGuiPluginDispatchTest {

  /** The lookup BaseGuiWidgets performs before it falls back to constructing an instance. */
  private static Object lookup(String hopGuiId, String instanceId) {
    return GuiRegistry.getInstance()
        .findGuiPluginObject(hopGuiId, GitGuiPlugin.class.getName(), instanceId);
  }

  @Test
  void widgetLookupFindsTheInstanceHoldingTheRepository() {
    GitGuiPlugin plugin = new GitGuiPlugin();
    String hopGuiId = "GitGuiPluginDispatchTest-gui";
    String instanceId = "GitGuiPluginDispatchTest-toolbar";

    plugin.registerAsGuiPluginObject(hopGuiId, instanceId);

    assertSame(
        plugin,
        lookup(hopGuiId, instanceId),
        "Toolbar and menu items must be dispatched to the instance which owns the UIGit handle, "
            + "not to one the GuiRegistry builds for itself");
  }

  @Test
  void eachSetOfWidgetsNeedsItsOwnRegistration() {
    GitGuiPlugin plugin = new GitGuiPlugin();
    String hopGuiId = "GitGuiPluginDispatchTest-gui-2";

    plugin.registerAsGuiPluginObject(hopGuiId, "GitGuiPluginDispatchTest-toolbar-2");

    // The explorer toolbar, the explorer context menu and the status toolbar each get a random
    // instance id, so registering one of them says nothing about the others.
    assertNull(
        lookup(hopGuiId, "GitGuiPluginDispatchTest-menu-2"),
        "An unregistered set of widgets falls back to a fresh instance with no repository");
  }

  @Test
  void registrationsOfDifferentSessionsDoNotOverlap() {
    GitGuiPlugin first = new GitGuiPlugin();
    GitGuiPlugin second = new GitGuiPlugin();
    String instanceId = "GitGuiPluginDispatchTest-shared-toolbar";

    first.registerAsGuiPluginObject("GitGuiPluginDispatchTest-session-a", instanceId);
    second.registerAsGuiPluginObject("GitGuiPluginDispatchTest-session-b", instanceId);

    assertSame(first, lookup("GitGuiPluginDispatchTest-session-a", instanceId));
    assertSame(second, lookup("GitGuiPluginDispatchTest-session-b", instanceId));
  }
}
