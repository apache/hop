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

package org.apache.hop.core.variables.resolver.aws;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.variables.resolver.VariableResolver;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The three authentication types have no credentials in common, so the editor only shows the ones
 * the chosen type can actually use. This checks that the others are out of the way, and -- just as
 * important -- that the options which always apply stay where they are.
 */
@Tag("uitest")
class AwsSecretsManagerWidgetVisibilityTest extends SwtBotTestBase {

  @BeforeAll
  static void registerGuiPluginElements() throws Exception {
    // Puts the @GuiWidgetElement annotations of this plugin into the registry. Without it the
    // composite comes up empty. The registry appends without checking for duplicates, so this only
    // runs when another test class in this module has not already done it.
    //
    if (GuiRegistry.getInstance()
            .findGuiElements(
                AwsSecretsManagerVariableResolver.class.getName(),
                VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
        == null) {
      HopGuiEnvironment.init();
    }
  }

  @Test
  @DisplayName("AUTOMATIC shows no credential fields at all")
  void automaticHidesEveryCredentialField() {
    withWidgets(
        resolver -> {
          // Nothing configured: how a new resolver starts life.
        },
        (resolver, widgets) -> {
          assertVisible(widgets, AwsSecretsManagerVariableResolver.ID_AUTHENTICATION_TYPE);

          assertHidden(widgets, AwsSecretsManagerVariableResolver.ID_ACCESS_KEY);
          assertHidden(widgets, AwsSecretsManagerVariableResolver.ID_SECRET_KEY);
          assertHidden(widgets, AwsSecretsManagerVariableResolver.ID_SESSION_TOKEN);
          assertHidden(widgets, AwsSecretsManagerVariableResolver.ID_CREDENTIALS_FILE);
          assertHidden(widgets, AwsSecretsManagerVariableResolver.ID_PROFILE_NAME);
        });
  }

  @Test
  @DisplayName("ACCESS_KEYS shows the keys and not the credentials file")
  void accessKeysShowsOnlyTheKeys() {
    withWidgets(
        resolver -> resolver.setAuthenticationType(AwsSecretsManagerAuthType.ACCESS_KEYS.name()),
        (resolver, widgets) -> {
          assertVisible(widgets, AwsSecretsManagerVariableResolver.ID_ACCESS_KEY);
          assertVisible(widgets, AwsSecretsManagerVariableResolver.ID_SECRET_KEY);
          assertVisible(widgets, AwsSecretsManagerVariableResolver.ID_SESSION_TOKEN);

          assertHidden(widgets, AwsSecretsManagerVariableResolver.ID_CREDENTIALS_FILE);
          assertHidden(widgets, AwsSecretsManagerVariableResolver.ID_PROFILE_NAME);
        });
  }

  @Test
  @DisplayName("CREDENTIALS_FILE shows the file and profile and not the keys")
  void credentialsFileShowsOnlyTheFileFields() {
    withWidgets(
        resolver ->
            resolver.setAuthenticationType(AwsSecretsManagerAuthType.CREDENTIALS_FILE.name()),
        (resolver, widgets) -> {
          assertVisible(widgets, AwsSecretsManagerVariableResolver.ID_CREDENTIALS_FILE);
          assertVisible(widgets, AwsSecretsManagerVariableResolver.ID_PROFILE_NAME);

          assertHidden(widgets, AwsSecretsManagerVariableResolver.ID_ACCESS_KEY);
          assertHidden(widgets, AwsSecretsManagerVariableResolver.ID_SECRET_KEY);
          assertHidden(widgets, AwsSecretsManagerVariableResolver.ID_SESSION_TOKEN);
        });
  }

  @Test
  @DisplayName("the options that always apply are never hidden")
  void generalOptionsStayVisibleForEveryAuthType() {
    for (AwsSecretsManagerAuthType authType : AwsSecretsManagerAuthType.values()) {
      withWidgets(
          resolver -> resolver.setAuthenticationType(authType.name()),
          (resolver, widgets) -> {
            assertVisible(widgets, AwsSecretsManagerVariableResolver.ID_REGION);
            assertVisible(widgets, AwsSecretsManagerVariableResolver.ID_AUTHENTICATION_TYPE);
            assertVisible(widgets, AwsSecretsManagerVariableResolver.ID_ENDPOINT_OVERRIDE);
            assertVisible(widgets, AwsSecretsManagerVariableResolver.ID_SECRET_NAME_PREFIX);
            assertVisible(widgets, AwsSecretsManagerVariableResolver.ID_VERSION_STAGE);
            assertVisible(widgets, AwsSecretsManagerVariableResolver.ID_CACHE_TTL_SECONDS);
          });
    }
  }

  private void withWidgets(
      Consumer<AwsSecretsManagerVariableResolver> configure,
      BiConsumer<AwsSecretsManagerVariableResolver, GuiCompositeWidgets> assertions) {
    ensureDisplay();

    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      AwsSecretsManagerVariableResolver resolver = new AwsSecretsManagerVariableResolver();
      configure.accept(resolver);

      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(
          resolver, null, shell, VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID, null);
      widgets.setWidgetsContents(resolver, shell, VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID);
      widgets.setWidgetsListener(resolver);
      resolver.widgetsPopulated(widgets);

      shell.layout(true, true);
      shell.pack();

      assertions.accept(resolver, widgets);
    } finally {
      if (!shell.isDisposed()) {
        shell.dispose();
      }
    }
  }

  private void assertVisible(GuiCompositeWidgets widgets, String id) {
    Control control = widgets.getWidgetsMap().get(id);
    assertNotNull(control, "no widget registered for " + id);
    assertTrue(control.getVisible(), id + " should be visible");
    Control label = widgets.getLabelsMap().get(id);
    if (label != null) {
      assertTrue(label.getVisible(), "the label of " + id + " should be visible");
    }
  }

  private void assertHidden(GuiCompositeWidgets widgets, String id) {
    Control control = widgets.getWidgetsMap().get(id);
    assertNotNull(control, "no widget registered for " + id);
    assertFalse(control.getVisible(), id + " should be hidden");
    Control label = widgets.getLabelsMap().get(id);
    if (label != null) {
      assertFalse(label.getVisible(), "the label of " + id + " should be hidden");
    }
  }
}
